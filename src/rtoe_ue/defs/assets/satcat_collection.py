from __future__ import annotations

import io
import json
from datetime import datetime, timezone
import os
from typing import Dict, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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
    # Map manifest max file per NORAD into df
    # (manifest keys are strings)
    max_map = manifest.norad_to_max_file
    satcat_df = satcat_df.copy()
    satcat_df["manifest_max_file"] = (
        satcat_df["NORAD_CAT_ID"].map(max_map).astype("float")
    )

    # delta condition: FILE > manifest_max OR manifest_max is null
    file_as_float = satcat_df["FILE"].astype("float")
    delta_mask = satcat_df["manifest_max_file"].isna() | (
        file_as_float > satcat_df["manifest_max_file"]
    )
    delta_df = satcat_df.loc[delta_mask].drop(columns=["manifest_max_file"])

    # compute manifest updates based on delta_df
    # new max file per NORAD = max(FILE) in delta
    updates_series = delta_df.groupby("NORAD_CAT_ID")["FILE"].max().astype(int)
    updates: Dict[str, int] = updates_series.to_dict()

    return delta_df, updates


def _df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


@asset(
    name="collect_satcat_data",
    partitions_def=INGESTION_DATE_PARTITIONS,
    required_resource_keys={"s3_resource", "spacetrack_resource"},
    description=(
        "Pull full SATCAT from Space-Track, compute delta vs manifest (NORAD->max FILE), "
        "write delta parquet to S3 partitioned by ingestion_date, update manifest."
    ),
)
def collect_satcat_data(context) -> None:
    s3 = context.resources.s3_resource
    spacetrack = context.resources.spacetrack_resource

    ingestion_date = context.partition_key  # "YYYY-MM-DD"
    parquet_key = f"{BASE_PREFIX}/ingestion_date={ingestion_date}/data.parquet"

    # 1) Read manifest (NORAD -> max FILE)
    manifest = read_satcat_manifest_gz_json(s3, BUCKET, MANIFEST_KEY)

    # 2) Pull full SATCAT data
    raw = spacetrack.fetch_satcat()
    df = pd.DataFrame(raw)
    df = _normalize_satcat_df(df)

    # 3) Compute delta rows
    delta_df, updates = _compute_delta(df, manifest)

    # 4) Write delta parquet to S3 (even if empty, write an empty parquet for traceability)
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

    # 6) Optional ingest log row (write a small json blob per partition)
    now = datetime.now(timezone.utc).isoformat()
    log_key = f"{INGEST_LOG_PREFIX}/ingestion_date={ingestion_date}/log.json"
    log_body = {
        "asset": "collect_satcat_data",
        "ingestion_date": ingestion_date,
        "written_parquet_s3_uri": f"s3://{BUCKET}/{parquet_key}",
        "manifest_s3_uri": f"s3://{BUCKET}/{MANIFEST_KEY}",
        "fetched_rows": int(len(df)),
        "delta_rows": int(len(delta_df)),
        "updated_norads": int(len(updates)),
        "timestamp_utc": now,
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=log_key,
        Body=json.dumps(log_body, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
    )

    context.add_output_metadata(
        {
            "ingestion_date": ingestion_date,
            "satcat_rows_fetched": len(df),
            "satcat_delta_rows": len(delta_df),
            "delta_parquet": MetadataValue.path(f"s3://{BUCKET}/{parquet_key}"),
            "manifest": MetadataValue.path(f"s3://{BUCKET}/{MANIFEST_KEY}"),
            "ingest_log": MetadataValue.path(f"s3://{BUCKET}/{log_key}"),
        }
    )
