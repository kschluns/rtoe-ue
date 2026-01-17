from __future__ import annotations

import os
from datetime import date, datetime, timedelta
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
BASE_PREFIX = "space_weather"


def _kp_to_numeric(x):
    # Your assumption: raw is Kp*10 integer (e.g. 53 -> 5.3).
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    try:
        v = float(x)
    except Exception:
        return None
    return round(v / 10.0, 1)


def _transform_space_weather_day(df: pd.DataFrame, ingestion_date: str) -> pd.DataFrame:
    out = pd.DataFrame()
    out["date_utc"] = pd.to_datetime(df.get("DATE"), errors="coerce", utc=True).dt.date

    out["bsrn"] = pd.to_numeric(df.get("BSRN"), errors="coerce")
    out["nd"] = pd.to_numeric(df.get("ND"), errors="coerce")

    kp_sum = df.get("KP_SUM")
    out["kp_sum"] = kp_sum.map(_kp_to_numeric) if kp_sum is not None else None

    out["ap_avg"] = pd.to_numeric(df.get("AP_AVG"), errors="coerce")

    out["cp"] = pd.to_numeric(df.get("CP"), errors="coerce")
    out["c9"] = pd.to_numeric(df.get("C9"), errors="coerce")
    out["isn"] = pd.to_numeric(df.get("ISN"), errors="coerce")

    out["f107_obs"] = pd.to_numeric(df.get("F10.7_OBS"), errors="coerce")
    out["f107_adj"] = pd.to_numeric(df.get("F10.7_ADJ"), errors="coerce")
    out["f107_data_type"] = df.get("F10.7_DATA_TYPE")

    out["f107_obs_center81"] = pd.to_numeric(
        df.get("F10.7_OBS_CENTER81"), errors="coerce"
    )
    out["f107_obs_last81"] = pd.to_numeric(df.get("F10.7_OBS_LAST81"), errors="coerce")
    out["f107_adj_center81"] = pd.to_numeric(
        df.get("F10.7_ADJ_CENTER81"), errors="coerce"
    )
    out["f107_adj_last81"] = pd.to_numeric(df.get("F10.7_ADJ_LAST81"), errors="coerce")
    out["ingestion_date"] = date.fromisoformat(ingestion_date)

    out = (
        out.dropna(subset=["date_utc"])
        .query("f107_data_type == 'OBS'")
        .reset_index(drop=True)
    )
    return out


def _transform_space_weather_3h(df: pd.DataFrame, ingestion_date: str) -> pd.DataFrame:
    """
    Explode each daily row into 8 bins.
    window_start_utc = date at 00:00 + (idx-1)*3 hours
    """
    rows = []
    for _, r in df.iterrows():
        d = pd.to_datetime(r.get("DATE"), errors="coerce", utc=True)
        if pd.isna(d):
            continue
        day_start = datetime(d.year, d.month, d.day)  # naive UTC as requested

        for i in range(1, 9):
            kp_col = f"KP{i}"
            ap_col = f"AP{i}"

            kp_val = _kp_to_numeric(r.get(kp_col))
            ap_val = r.get(ap_col)

            w_start = day_start + timedelta(hours=(i - 1) * 3)
            w_end = w_start + timedelta(hours=3)

            rows.append(
                {
                    "window_start_utc": w_start,
                    "window_end_utc": w_end,
                    "date_utc": day_start.date(),
                    "window_idx": i,
                    "kp": kp_val,
                    "ap": (
                        int(ap_val)
                        if ap_val is not None and not pd.isna(ap_val)
                        else None
                    ),
                    "ingestion_date": date.fromisoformat(ingestion_date),
                }
            )
    return pd.DataFrame(rows)


@asset(
    name="load_space_weather_to_rds",
    partitions_def=INGESTION_DATE_PARTITIONS,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=10),
    required_resource_keys={"s3_resource", "postgres_resource"},
    deps=["collect_space_weather_data"],
    output_required=False,
    code_version="v2",
    description=(
        "Load Space Weather delta parquet from S3 into Postgres tables "
        "public.space_weather_day and public.space_weather_3h (ON CONFLICT DO NOTHING)."
    ),
)
def load_space_weather_to_rds(context) -> Iterator[AssetMaterialization]:
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

        day_df = _transform_space_weather_day(df, ingestion_date)
        bins_df = _transform_space_weather_3h(df, ingestion_date)

        day_cols = [
            "date_utc",
            "bsrn",
            "nd",
            "kp_sum",
            "ap_avg",
            "cp",
            "c9",
            "isn",
            "f107_obs",
            "f107_adj",
            "f107_data_type",
            "f107_obs_center81",
            "f107_obs_last81",
            "f107_adj_center81",
            "f107_adj_last81",
            "ingestion_date",
        ]
        day_records = df_records(day_df, day_cols)

        bins_cols = [
            "window_start_utc",
            "window_end_utc",
            "date_utc",
            "window_idx",
            "kp",
            "ap",
            "ingestion_date",
        ]
        bins_records = df_records(bins_df, bins_cols)

        day_insert_sql = """
            INSERT INTO public.space_weather_day (
                date_utc, bsrn, nd, kp_sum, ap_avg, cp, c9, isn,
                f107_obs, f107_adj, f107_data_type,
                f107_obs_center81, f107_obs_last81, f107_adj_center81, f107_adj_last81, 
                ingestion_date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (date_utc) DO NOTHING
        """

        bins_insert_sql = """
            INSERT INTO public.space_weather_3h (
                window_start_utc, window_end_utc, date_utc, window_idx, kp, ap, ingestion_date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (window_start_utc) DO NOTHING
        """

        inserted_day = 0
        inserted_bins = 0
        with conn.cursor() as cur:
            # day
            batch_size = 10_000
            for i in range(0, len(day_records), batch_size):
                batch = day_records[i : i + batch_size]
                if batch:
                    cur.executemany(day_insert_sql, batch)
                    inserted_day += cur.rowcount if cur.rowcount is not None else 0

            # bins
            for i in range(0, len(bins_records), batch_size):
                batch = bins_records[i : i + batch_size]
                if batch:
                    cur.executemany(bins_insert_sql, batch)
                    inserted_bins += cur.rowcount if cur.rowcount is not None else 0

            conn.commit()

        yield AssetMaterialization(
            asset_key=context.asset_key,
            partition=ingestion_date,
            metadata={
                "partition": ingestion_date,
                "status": "materialized",
                "parquet": MetadataValue.path(ref.uri),
                "rows_in_parquet": len(df),
                "space_weather_day_rows": len(day_df),
                "space_weather_3h_rows": len(bins_df),
                "rows_inserted_day_estimate": inserted_day,
                "rows_inserted_3h_estimate": inserted_bins,
                "tables": ["public.space_weather_day", "public.space_weather_3h"],
            },
        )
