from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from dagster import MetadataValue, asset

from rtoe_ue.defs.partitions.dates import INGESTION_DATE_PARTITIONS
from rtoe_ue.defs.resources.manifests import (
    read_satcat_manifest_gz_json,
    write_satcat_manifest_gz_json,
    SatcatManifest,
)

BUCKET = os.environ["S3_BUCKET"]
BASE_PREFIX = "satcat"
MANIFEST_KEY = f"{BASE_PREFIX}/_manifest/satcat_manifest.json.gz"
INGEST_LOG_PREFIX = "ingest_logs/satcat"
PARTITION_TZ = "America/Chicago"


def _s3_key_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def _s3_get_json(s3, bucket: str, key: str) -> Optional[Dict[str, Any]]:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        return json.loads(raw.decode("utf-8"))
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def _s3_put_json(s3, bucket: str, key: str, payload: Dict[str, Any]) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def _normalize_satcat_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal normalization:
      - ensure NORAD_CAT_ID exists and is string-ish
      - ensure FILE numeric
    """
    if "NORAD_CAT_ID" not in df.columns:
        raise RuntimeError("SATCAT response missing NORAD_CAT_ID")
    if "FILE" not in df.columns:
        raise RuntimeError("SATCAT response missing FILE")

    out = df.copy()
    out["NORAD_CAT_ID"] = out["NORAD_CAT_ID"].astype(str).str.strip()

    # FILE is the change-sequence-like value we compare.
    out["FILE"] = pd.to_numeric(out["FILE"], errors="coerce").astype("Int64")

    # Drop unusable rows
    out = out.loc[out["NORAD_CAT_ID"].notna() & (out["NORAD_CAT_ID"] != "")]
    out = out.loc[out["FILE"].notna()]

    return out


def _compute_delta(
    satcat_df: pd.DataFrame,
    manifest: SatcatManifest,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Returns:
      - delta_df: rows whose FILE is newer than manifest max for that NORAD, or NORAD not present in manifest
      - updates: dict of NORAD->new_max_file based on delta_df
    """
    satcat_df = satcat_df.copy()

    # Manifest map is str -> int (or missing); map onto a nullable Int64 series
    satcat_df["manifest_max_file"] = (
        satcat_df["NORAD_CAT_ID"]
        .map(manifest.norad_to_max_file)
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )

    # FILE is already Int64 from _normalize_satcat_df
    file_s = satcat_df["FILE"].astype("Int64")
    max_s = satcat_df["manifest_max_file"]  # Int64 nullable

    # Delta: new NORAD (max is null) OR FILE > max
    # Use fillna(-1) to make the comparison null-safe without floats.
    delta_mask = max_s.isna() | (file_s > max_s.fillna(-1))

    delta_df = satcat_df.loc[delta_mask].drop(columns=["manifest_max_file"])

    # updates: new max per NORAD among delta rows
    updates_series = delta_df.groupby("NORAD_CAT_ID")["FILE"].max()

    # updates_series is nullable Int64; drop nulls defensively and convert to plain int
    updates: Dict[str, int] = updates_series.dropna().astype("int64").to_dict()

    return delta_df, updates


def _df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def _today_partition_key(tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz=tz).date().isoformat()


@asset(
    name="collect_satcat_data",
    partitions_def=INGESTION_DATE_PARTITIONS,
    required_resource_keys={"s3_resource", "spacetrack_resource"},
    description=(
        "Pull full SATCAT from Space-Track, compute delta vs manifest (NORAD->max FILE), "
        "write delta parquet to S3 partitioned by ingestion_date, update manifest. "
        "NOTE: write-once partition key; if data.parquet exists, do not overwrite. "
        "Also skip if ingestion_date != today's partition (API isn't date-parameterized here)."
    ),
    code_version="v5",
)
def collect_satcat_data(context) -> None:
    s3 = context.resources.s3_resource
    spacetrack = context.resources.spacetrack_resource

    for ingestion_date in context.partition_keys:  # Works whether 1 or many

        parquet_key = f"{BASE_PREFIX}/ingestion_date={ingestion_date}/data.parquet"

        # Log keys
        run_id = context.run_id
        run_log_key = f"{INGEST_LOG_PREFIX}/ingestion_date={ingestion_date}/run_id={run_id}/log.json"
        latest_log_key = (
            f"{INGEST_LOG_PREFIX}/ingestion_date={ingestion_date}/latest.json"
        )
        parquet_exists = _s3_key_exists(s3, BUCKET, parquet_key)

        # Check "today" semantics
        today_key = _today_partition_key(PARTITION_TZ)
        is_today = ingestion_date == today_key

        # If parquet exists, do NOT rewrite parquet or manifest.
        if parquet_exists or not is_today:
            # Pull previous values from stable latest summary, so we don't "lose" metadata.
            latest_metadata = _s3_get_json(s3, BUCKET, latest_log_key) or {}

            # Determine skip reason
            if parquet_exists:
                skip_reason = "parquet_already_exists"
            else:
                skip_reason = (
                    "not today's partition: "
                    "satcat API does not support querying past or future dates"
                )

            # Always write a new run log
            now = datetime.now(timezone.utc).isoformat()
            log_body = {
                "asset": "collect_satcat_data",
                "ingestion_date": ingestion_date,
                "run_id": run_id,
                "timestamp_utc": now,
                "status": "skipped_existing_partition",
                "reason": skip_reason,
                "delta_parquet_s3_uri": latest_metadata.get("delta_parquet_s3_uri"),
                "manifest_s3_uri": latest_metadata.get("manifest_s3_uri"),
                "fetched_rows": latest_metadata.get("fetched_rows"),
                "delta_rows": latest_metadata.get("delta_rows"),
                "updated_norads": latest_metadata.get("updated_norads"),
                "did_fetch": False,
                "did_write_parquet": False,
                "did_update_manifest": False,
            }
            _s3_put_json(s3, BUCKET, run_log_key, log_body)
            _s3_put_json(s3, BUCKET, latest_log_key, log_body)

            # Output metadata: keep prior values, but point ingest_log to this run's log
            context.add_output_metadata(
                {
                    "ingestion_date": ingestion_date,
                    "satcat_rows_fetched": latest_metadata.get("fetched_rows"),
                    "satcat_delta_rows": latest_metadata.get("delta_rows"),
                    "delta_parquet": MetadataValue.path(
                        latest_metadata.get("delta_parquet_s3_uri")
                    ),
                    "manifest": MetadataValue.path(
                        latest_metadata.get("manifest_s3_uri")
                    ),
                    "ingest_log": MetadataValue.path(f"s3://{BUCKET}/{run_log_key}"),
                    "latest_summary": MetadataValue.path(
                        f"s3://{BUCKET}/{latest_log_key}"
                    ),
                    "status": "skipped_existing_partition",
                    "reason": skip_reason,
                }
            )
            continue

        # Otherwise: proceed normally (write parquet + update manifest)
        # 1) Read manifest (NORAD -> max FILE)
        manifest = read_satcat_manifest_gz_json(s3, BUCKET, MANIFEST_KEY)

        # 2) Pull full SATCAT data
        raw = spacetrack.fetch_satcat()
        df = pd.DataFrame(raw)
        df = _normalize_satcat_df(df)

        # 3) Compute delta rows
        delta_df, updates = _compute_delta(df, manifest)

        # 4) Write delta parquet to S3
        parquet_bytes = _df_to_parquet_bytes(delta_df)
        s3.put_object(
            Bucket=BUCKET,
            Key=parquet_key,
            Body=parquet_bytes,
            ContentType="application/octet-stream",
        )

        # 5) Update manifest with net-new max FILE values from what we published
        new_map = dict(manifest.norad_to_max_file)
        for norad, new_max in updates.items():
            old = new_map.get(norad)
            if old is None or new_max > old:
                new_map[norad] = new_max
        new_manifest = SatcatManifest(norad_to_max_file=new_map)
        write_satcat_manifest_gz_json(s3, BUCKET, MANIFEST_KEY, new_manifest)

        # 6) Write run-specific ingest log (always)
        now = datetime.now(timezone.utc).isoformat()
        log_body = {
            "asset": "collect_satcat_data",
            "ingestion_date": ingestion_date,
            "run_id": run_id,
            "timestamp_utc": now,
            "status": "materialized",
            "delta_parquet_s3_uri": f"s3://{BUCKET}/{parquet_key}",
            "manifest_s3_uri": f"s3://{BUCKET}/{MANIFEST_KEY}",
            "fetched_rows": int(len(df)),
            "delta_rows": int(len(delta_df)),
            "updated_norads": int(len(updates)),
            "did_fetch": True,
            "did_write_parquet": True,
            "did_update_manifest": True,
        }
        _s3_put_json(s3, BUCKET, run_log_key, log_body)
        _s3_put_json(s3, BUCKET, latest_log_key, log_body)

        # Output metadata
        context.add_output_metadata(
            {
                "ingestion_date": ingestion_date,
                "satcat_rows_fetched": len(df),
                "satcat_delta_rows": len(delta_df),
                "delta_parquet": MetadataValue.path(f"s3://{BUCKET}/{parquet_key}"),
                "manifest": MetadataValue.path(f"s3://{BUCKET}/{MANIFEST_KEY}"),
                "ingest_log": MetadataValue.path(f"s3://{BUCKET}/{run_log_key}"),
                "latest_summary": MetadataValue.path(f"s3://{BUCKET}/{latest_log_key}"),
                "status": "materialized",
            }
        )
