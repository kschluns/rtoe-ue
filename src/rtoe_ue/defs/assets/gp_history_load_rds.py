from __future__ import annotations

import os

import pandas as pd
from dagster import BackfillPolicy, MetadataValue, asset, AssetMaterialization

from rtoe_ue.defs.assets._utils import (
    S3ParquetRef,
    coerce_utc_naive_timestamp,
    df_records,
    read_parquet_df_from_s3,
    s3_key_exists,
)

from rtoe_ue.defs.partitions.dates import CREATION_DATE_PARTITIONS

BUCKET = os.environ["S3_BUCKET"]
BASE_PREFIX = "gp_history"


def _transform_gp_history(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame()

    out["gp_id"] = pd.to_numeric(df.get("GP_ID"), errors="coerce").astype("Int64")
    out["norad_cat_id"] = pd.to_numeric(df.get("NORAD_CAT_ID"), errors="coerce").astype(
        "Int64"
    )

    out["epoch_utc"] = coerce_utc_naive_timestamp(df.get("EPOCH"))

    # TLE lines (Strategy A)
    out["tle_line0"] = df.get("TLE_LINE0")
    out["tle_line1"] = df.get("TLE_LINE1")
    out["tle_line2"] = df.get("TLE_LINE2")

    # Optional
    if "CREATION_DATE" in df.columns:
        out["creation_date_utc"] = coerce_utc_naive_timestamp(df.get("CREATION_DATE"))
    else:
        out["creation_date_utc"] = None

    out = out.dropna(subset=["gp_id", "norad_cat_id", "epoch_utc", "creation_date_utc"])
    out["gp_id"] = out["gp_id"].astype("int64")
    out["norad_cat_id"] = out["norad_cat_id"].astype("int64")

    for col in ["tle_line0", "tle_line1", "tle_line2"]:
        if col in out.columns:
            out[col] = out[col].astype("string")

    return out


@asset(
    name="load_gp_history_to_rds",
    partitions_def=CREATION_DATE_PARTITIONS,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    required_resource_keys={"s3_resource", "postgres_resource"},
    deps=["collect_gp_history_data"],
    output_required=False,
    code_version="v1",
    description="Load gp_history parquet from S3 into Postgres table public.gp_history (ON CONFLICT DO NOTHING).",
)
def load_gp_history_to_rds(context) -> None:
    s3 = context.resources.s3_resource
    conn = context.resources.postgres_resource

    for creation_date in context.partition_keys:
        key = f"{BASE_PREFIX}/creation_date={creation_date}/data.parquet"
        ref = S3ParquetRef(bucket=BUCKET, key=key)

        if not s3_key_exists(s3, BUCKET, key):
            yield AssetMaterialization(
                asset_key=context.asset_key,
                partition=creation_date,
                metadata={
                    "partition": creation_date,
                    "status": "skipped_missing_parquet",
                    "parquet": MetadataValue.path(ref.uri),
                },
            )
            continue

        df = read_parquet_df_from_s3(s3, BUCKET, key)
        rows_in_parquet = len(df)
        df = _transform_gp_history(df)
        rows_after_transform = len(df)

        cols = [
            "gp_id",
            "norad_cat_id",
            "epoch_utc",
            "tle_line0",
            "tle_line1",
            "tle_line2",
            "creation_date_utc",
        ]
        df = df_records(df, cols)

        if not df:
            yield AssetMaterialization(
                asset_key=context.asset_key,
                partition=creation_date,
                metadata={
                    "partition": creation_date,
                    "status": "no_rows",
                    "parquet": MetadataValue.path(ref.uri),
                    "rows_inserted": 0,
                },
            )
            continue

        insert_sql = """
            INSERT INTO public.gp_history (
                gp_id, norad_cat_id, epoch_utc,
                tle_line0, tle_line1, tle_line2,
                creation_date_utc
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (gp_id) DO NOTHING
        """

        inserted = 0
        with conn.cursor() as cur:
            batch_size = 10_000
            for i in range(0, len(df), batch_size):
                batch = df[i : i + batch_size]
                cur.executemany(insert_sql, batch)
                inserted += cur.rowcount if cur.rowcount is not None else 0
            conn.commit()

        yield AssetMaterialization(
            asset_key=context.asset_key,
            partition=creation_date,
            metadata={
                "partition": creation_date,
                "status": "materialized",
                "parquet": MetadataValue.path(ref.uri),
                "rows_in_parquet": rows_in_parquet,
                "rows_after_transform": rows_after_transform,
                "rows_inserted_estimate": inserted,
                "table": "public.gp_history",
            },
        )
