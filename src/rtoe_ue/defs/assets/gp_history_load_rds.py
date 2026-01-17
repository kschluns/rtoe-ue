from __future__ import annotations

import gc
import os
from typing import Iterator

import pandas as pd
import pyarrow as pa
from dagster import BackfillPolicy, MetadataValue, asset, AssetMaterialization

from rtoe_ue.defs.assets._utils import (
    S3ParquetRef,
    coerce_utc_naive_timestamp,
    read_parquet_df_from_s3,
    s3_key_exists,
)

from rtoe_ue.defs.partitions.dates import CREATION_DATE_PARTITIONS

BUCKET = os.environ["S3_BUCKET"]
BASE_PREFIX = "gp_history"


def _transform_gp_history(df: pd.DataFrame) -> pd.DataFrame:
    df["GP_ID"] = pd.to_numeric(df["GP_ID"], errors="coerce").astype("Int64")
    df["NORAD_CAT_ID"] = pd.to_numeric(df["NORAD_CAT_ID"], errors="coerce").astype(
        "Int64"
    )
    df["EPOCH"] = coerce_utc_naive_timestamp(df["EPOCH"])
    df["CREATION_DATE"] = coerce_utc_naive_timestamp(df["CREATION_DATE"])
    mask = (
        df["GP_ID"].notna()
        & df["NORAD_CAT_ID"].notna()
        & df["EPOCH"].notna()
        & df["CREATION_DATE"].notna()
    )
    df = df.loc[mask]
    df["GP_ID"] = df["GP_ID"].astype("int64")
    df["NORAD_CAT_ID"] = df["NORAD_CAT_ID"].astype("int64")

    for col in ["TLE_LINE0", "TLE_LINE1", "TLE_LINE2"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


def load_df_to_rds(conn, df: pd.DataFrame) -> tuple[int, int]:
    """
    Returns (rows_staged, rows_inserted_estimate).

    Notes:
    - rows_inserted_estimate is not guaranteed exact without RETURNING.
    """
    rows_staged = 0
    rows_inserted_est = 0

    create_stage_sql = """
        CREATE TEMP TABLE gp_history_stage (
            gp_id bigint,
            norad_cat_id integer,
            epoch_utc timestamp,
            tle_line0 varchar(27),
            tle_line1 varchar(71),
            tle_line2 varchar(71),
            creation_date_utc timestamp
        ) ON COMMIT DROP;
    """

    copy_sql = """
        COPY gp_history_stage (
            gp_id, norad_cat_id, epoch_utc, tle_line0, tle_line1, tle_line2, creation_date_utc
        )
        FROM STDIN
    """

    merge_sql = """
        INSERT INTO public.gp_history (
            gp_id, norad_cat_id, epoch_utc, tle_line0, tle_line1, tle_line2, creation_date_utc
        )
        SELECT
            gp_id, norad_cat_id, epoch_utc, tle_line0, tle_line1, tle_line2, creation_date_utc
        FROM gp_history_stage
        ON CONFLICT (gp_id) DO NOTHING
    """

    with conn.cursor() as cur:
        cur.execute(create_stage_sql)

        # Stream rows to Postgres using COPY protocol.
        # This does NOT build a huge SQL string, and does NOT send one INSERT per row.
        with cur.copy(copy_sql) as copy:
            # itertuples streams rows without materializing a giant list
            for row in df.itertuples(index=False, name=None):
                # psycopg v3 COPY in CSV mode accepts text lines; write_row handles CSV formatting.
                copy.write_row(row)
                rows_staged += 1

        cur.execute(merge_sql)
        rows_inserted_est = cur.rowcount if cur.rowcount is not None else 0

    return rows_staged, rows_inserted_est


@asset(
    name="load_gp_history_to_rds",
    partitions_def=CREATION_DATE_PARTITIONS,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=100),
    required_resource_keys={"s3_resource", "postgres_resource"},
    deps=["collect_gp_history_data"],
    output_required=False,
    code_version="v3",
    description="Load gp_history parquet from S3 into Postgres table public.gp_history (ON CONFLICT DO NOTHING).",
)
def load_gp_history_to_rds(context) -> Iterator[AssetMaterialization]:
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

        cols = [
            "GP_ID",
            "NORAD_CAT_ID",
            "EPOCH",
            "TLE_LINE0",
            "TLE_LINE1",
            "TLE_LINE2",
            "CREATION_DATE",
        ]
        df = read_parquet_df_from_s3(
            s3,
            BUCKET,
            key,
            columns=cols,
        )
        rows_in_parquet = len(df)
        df = _transform_gp_history(df)
        rows_after_transform = len(df)

        if rows_after_transform == 0:
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

        rows_staged, rows_inserted_est = load_df_to_rds(conn, df)
        conn.commit()

        # Encourage Python + Arrow to release memory between partitions
        del df
        gc.collect()
        pa.default_memory_pool().release_unused()

        yield AssetMaterialization(
            asset_key=context.asset_key,
            partition=creation_date,
            metadata={
                "partition": creation_date,
                "status": "materialized",
                "parquet": MetadataValue.path(ref.uri),
                "rows_in_parquet": rows_in_parquet,
                "rows_after_transform": rows_after_transform,
                "rows_staged": rows_staged,
                "rows_inserted_estimate": rows_inserted_est,
                "table": "public.gp_history",
            },
        )
