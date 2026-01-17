from __future__ import annotations

import os
from datetime import date
from typing import Iterator

import pandas as pd
from dagster import BackfillPolicy, MetadataValue, asset, AssetMaterialization

from rtoe_ue.defs.assets._utils import (
    S3ParquetRef,
    df_records,
    read_parquet_df_from_s3,
    s3_key_exists,
)
from rtoe_ue.defs.partitions.dates import INGESTION_DATE_PARTITIONS

BUCKET = os.environ["S3_BUCKET"]
BASE_PREFIX = "satcat"


def _transform_satcat(df: pd.DataFrame, ingestion_date: str) -> pd.DataFrame:
    # Expected columns from Space-Track SATCAT include at least NORAD_CAT_ID and FILE.
    # We map additional fields into your satcat table schema.
    out = pd.DataFrame()

    out["norad_cat_id"] = pd.to_numeric(df.get("NORAD_CAT_ID"), errors="coerce").astype(
        "Int64"
    )
    out["file"] = pd.to_numeric(df.get("FILE"), errors="coerce").astype("Int64")

    # Optional columns (string-ish)
    out["object_type"] = df.get("OBJECT_TYPE")
    out["satname"] = df.get("SATNAME")
    out["country"] = df.get("COUNTRY")
    out["site"] = df.get("SITE")
    out["rcs_size"] = df.get("RCS_SIZE")

    # Dates
    out["launch_date"] = pd.to_datetime(
        df.get("LAUNCH"), errors="coerce", utc=True
    ).dt.date
    out["decay_date"] = pd.to_datetime(
        df.get("DECAY"), errors="coerce", utc=True
    ).dt.date

    out["ingestion_date"] = date.fromisoformat(ingestion_date)

    # Drop rows missing required PK parts
    out = out.dropna(subset=["norad_cat_id", "file"])
    out["norad_cat_id"] = out["norad_cat_id"].astype(int)
    out["file"] = out["file"].astype(int)

    # Enforce schema string lengths loosely (Postgres will also enforce)
    for col in ["object_type", "satname", "country", "site", "rcs_size"]:
        if col in out.columns:
            out[col] = out[col].astype("string")

    return out


@asset(
    name="load_satcat_to_rds",
    partitions_def=INGESTION_DATE_PARTITIONS,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    required_resource_keys={"s3_resource", "postgres_resource"},
    deps=["collect_satcat_data"],
    output_required=False,
    code_version="v2",
    output_required=False,
    description="Load SATCAT delta parquet from S3 into Postgres table public.satcat (ON CONFLICT DO NOTHING).",
)
def load_satcat_to_rds(context) -> Iterator[AssetMaterialization]:
    s3 = context.resources.s3_resource
    conn = context.resources.postgres_resource

    for ingestion_date in context.partition_keys:
        key = f"{BASE_PREFIX}/ingestion_date={ingestion_date}/data.parquet"
        ref = S3ParquetRef(bucket=BUCKET, key=key)

        if not s3_key_exists(s3, BUCKET, key):
            yield AssetMaterialization(
                asset_key=context.asset_key,
                partition=ingestion_date,
                metadata={
                    "partition": ingestion_date,
                    "status": "skipped_missing_parquet",
                    "parquet": MetadataValue.path(ref.uri),
                },
            )
            continue

        df = read_parquet_df_from_s3(s3, BUCKET, key)
        rows_in_parquet = len(df)
        df = _transform_satcat(df, ingestion_date)
        rows_after_transform = len(df)

        cols = [
            "norad_cat_id",
            "file",
            "object_type",
            "satname",
            "country",
            "launch_date",
            "site",
            "decay_date",
            "rcs_size",
            "ingestion_date",
        ]
        df = df_records(df, cols)

        if not df:
            yield AssetMaterialization(
                asset_key=context.asset_key,
                partition=ingestion_date,
                metadata={
                    "partition": ingestion_date,
                    "status": "no_rows",
                    "parquet": MetadataValue.path(ref.uri),
                    "rows_inserted": 0,
                },
            )
            continue

        insert_sql = """
            INSERT INTO public.satcat (
                norad_cat_id, file, object_type, satname, country,
                launch_date, site, decay_date, rcs_size, ingestion_date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (norad_cat_id, file) DO NOTHING
        """

        inserted = 0
        with conn.cursor() as cur:
            # Batch insert
            batch_size = 10_000
            for i in range(0, len(df), batch_size):
                batch = df[i : i + batch_size]
                cur.executemany(insert_sql, batch)
                inserted += cur.rowcount if cur.rowcount is not None else 0
            conn.commit()

        yield AssetMaterialization(
            asset_key=context.asset_key,
            partition=ingestion_date,
            metadata={
                "partition": ingestion_date,
                "status": "materialized",
                "parquet": MetadataValue.path(ref.uri),
                "rows_in_parquet": rows_in_parquet,
                "rows_after_transform": rows_after_transform,
                "rows_inserted_estimate": inserted,
                "table": "public.satcat",
            },
        )
