from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from dagster import MetadataValue, asset

from rtoe_ue.defs.partitions.dates import INGESTION_DATE_PARTITIONS
from rtoe_ue.defs.resources.manifests import (
    read_space_weather_manifest_gz_json,
    write_space_weather_manifest_gz_json,
    SpaceWeatherManifest,
)

BUCKET = os.environ["S3_BUCKET"]
BASE_PREFIX = "space_weather"
MANIFEST_KEY = f"{BASE_PREFIX}/_manifest/space_weather_manifest.json.gz"
INGEST_LOG_PREFIX = "ingest_logs/space_weather"
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


def _normalize_space_weather_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal normalization:
      - ensure OBS_DATE exists (as YYYY-MM-DD string)
      - drop rows without OBS_DATE
    """
    if "OBS_DATE" not in df.columns:
        raise RuntimeError("Space weather response missing OBS_DATE")

    out = df.copy()

    # Try to coerce to date, then ISO string
    # Handles strings, datetimes, etc.
    obs = pd.to_datetime(out["OBS_DATE"], errors="coerce", utc=True)
    out["OBS_DATE"] = obs.dt.date.astype("string")

    # Drop unusable rows
    out = out.loc[out["OBS_DATE"].notna() & (out["OBS_DATE"] != "")]
    return out


def _compute_delta(
    api_df: pd.DataFrame, manifest: SpaceWeatherManifest
) -> Tuple[pd.DataFrame, Set[str]]:
    """
    Returns:
      - delta_df: rows whose OBS_DATE not in manifest.obs_dates
      - updates: set of net-new OBS_DATE values present in delta_df
    """
    api_df = api_df.copy()
    existing = manifest.obs_dates
    delta_mask = ~api_df["OBS_DATE"].isin(list(existing))
    delta_df = api_df.loc[delta_mask]
    updates = set(delta_df["OBS_DATE"].dropna().astype(str).tolist())
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
    name="collect_space_weather_data",
    partitions_def=INGESTION_DATE_PARTITIONS,
    required_resource_keys={"s3_resource", "celestrak_resource"},
    description=(
        "Pull full space weather data from CelesTrak, compute delta vs manifest (OBS_DATE list), "
        "write delta parquet to S3 partitioned by ingestion_date, update manifest. "
        "NOTE: write-once partition key; if data.parquet exists, do not overwrite. "
        "Also skip if ingestion_date != today's partition (API isn't date-parameterized here)."
    ),
    code_version="v1",
)
def collect_space_weather_data(context) -> None:
    s3 = context.resources.s3_resource
    celestrak = context.resources.celestrak_resource

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
        # Also skip if not today's partition.
        if parquet_exists or not is_today:
            # Pull previous values from stable latest summary, so we don't "lose" metadata.
            latest_metadata = _s3_get_json(s3, BUCKET, latest_log_key) or {}

            # Determine skip reason
            if parquet_exists:
                skip_reason = "parquet_already_exists"
            else:
                skip_reason = (
                    "not today's partition: "
                    "space weather API does not support querying past or future dates"
                )

            # Always write a new run log
            now = datetime.now(timezone.utc).isoformat()
            log_body = {
                "asset": "collect_space_weather_data",
                "ingestion_date": ingestion_date,
                "run_id": run_id,
                "timestamp_utc": now,
                "status": "skipped_existing_partition",
                "reason": skip_reason,
                "delta_parquet_s3_uri": latest_metadata.get("delta_parquet_s3_uri"),
                "manifest_s3_uri": latest_metadata.get("manifest_s3_uri"),
                "fetched_rows": latest_metadata.get("fetched_rows"),
                "delta_rows": latest_metadata.get("delta_rows"),
                "did_fetch": False,
                "did_write_parquet": False,
                "did_update_manifest": False,
            }
            _s3_put_json(s3, BUCKET, run_log_key, log_body)
            _s3_put_json(s3, BUCKET, latest_log_key, log_body)

            context.add_output_metadata(
                {
                    "ingestion_date": ingestion_date,
                    "space_weather_rows_fetched": latest_metadata.get("fetched_rows"),
                    "space_weather_delta_rows": latest_metadata.get("delta_rows"),
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
        # 1) Read manifest (obs date list)
        manifest = read_space_weather_manifest_gz_json(s3, BUCKET, MANIFEST_KEY)

        # 2) Pull full space weather data from CelesTrak
        df: pd.DataFrame = celestrak.fetch_space_weather()
        df = _normalize_space_weather_df(df)

        # 3) Compute delta rows: OBS_DATE not in manifest
        delta_df, updates = _compute_delta(df, manifest)

        # 4) Write delta parquet to S3
        parquet_bytes = _df_to_parquet_bytes(delta_df)
        s3.put_object(
            Bucket=BUCKET,
            Key=parquet_key,
            Body=parquet_bytes,
            ContentType="application/octet-stream",
        )

        # 5) Update manifest with net-new observation dates from what we published
        new_dates = set(manifest.obs_dates)
        new_dates.update(updates)
        new_manifest = SpaceWeatherManifest(obs_dates=new_dates)
        write_space_weather_manifest_gz_json(s3, BUCKET, MANIFEST_KEY, new_manifest)

        # 6) Write run-specific ingest log (always)
        now = datetime.now(timezone.utc).isoformat()
        log_body = {
            "asset": "collect_space_weather_data",
            "ingestion_date": ingestion_date,
            "run_id": run_id,
            "timestamp_utc": now,
            "status": "materialized",
            "delta_parquet_s3_uri": f"s3://{BUCKET}/{parquet_key}",
            "manifest_s3_uri": f"s3://{BUCKET}/{MANIFEST_KEY}",
            "fetched_rows": int(len(df)),
            "delta_rows": int(len(delta_df)),
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
                "space_weather_rows_fetched": len(df),
                "space_weather_delta_rows": len(delta_df),
                "delta_parquet": MetadataValue.path(f"s3://{BUCKET}/{parquet_key}"),
                "manifest": MetadataValue.path(f"s3://{BUCKET}/{MANIFEST_KEY}"),
                "ingest_log": MetadataValue.path(f"s3://{BUCKET}/{run_log_key}"),
                "latest_summary": MetadataValue.path(f"s3://{BUCKET}/{latest_log_key}"),
                "status": "materialized",
            }
        )
