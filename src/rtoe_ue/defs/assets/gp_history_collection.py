from __future__ import annotations

import gc
import io
import json
import os
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterator, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from dagster import (
    BackfillPolicy,
    Failure,
    MetadataValue,
    asset,
    AssetObservation,
    MaterializeResult,
)

from rtoe_ue.defs.partitions.dates import CREATION_DATE_PARTITIONS

BUCKET = os.environ["S3_BUCKET"]
BASE_PREFIX = "gp_history"
INGEST_LOG_PREFIX = "ingest_logs/gp_history"
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


def _write_df_to_s3_parquet(
    s3,
    bucket: str,
    key: str,
    df: pd.DataFrame,
) -> None:
    """
    Write parquet without creating a giant `bytes` copy.

    Key difference vs your old approach:
      - NO buf.getvalue() (which duplicates the parquet in memory)
      - Upload the buffer directly to S3
    """
    buf = io.BytesIO()
    try:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        # Prefer streaming upload to avoid duplicating bytes
        if hasattr(s3, "upload_fileobj"):
            s3.upload_fileobj(buf, bucket, key)
        else:
            # Fallback (less memory-friendly): still avoids the extra copy from getvalue()
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=buf.getbuffer(),  # no duplicate bytes object
                ContentType="application/octet-stream",
            )
    finally:
        # Critical: explicitly drop Arrow objects and buffers
        try:
            del table
        except NameError:
            pass

        buf.close()
        del buf

        # Encourage Arrow to return pooled memory
        pa.default_memory_pool().release_unused()


def _today_partition_key(tz_name: str) -> str:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz=tz).date().isoformat()


@asset(
    name="collect_gp_history_data",
    partitions_def=CREATION_DATE_PARTITIONS,
    backfill_policy=BackfillPolicy.single_run(),
    required_resource_keys={"s3_resource", "spacetrack_resource"},
    description=(
        "Pull gp_history from Space-Track for a creation_date window, write parquet to S3 "
        "gp_history/creation_date=YYYY-MM-DD/data.parquet. Write ingest logs. "
        "Write-once: if parquet exists, skip (no failure). "
        "Disallow running for today's date: fail but continue other partitions."
    ),
    code_version="v5",
)
def collect_gp_history_data(context) -> Iterator[MaterializeResult]:
    s3 = context.resources.s3_resource
    spacetrack = context.resources.spacetrack_resource

    today_key = _today_partition_key(PARTITION_TZ)
    failures: list[str] = []
    materialized_count = 0
    skipped_count = 0

    for creation_date_key in context.partition_keys:
        parquet_key = f"{BASE_PREFIX}/creation_date={creation_date_key}/data.parquet"

        run_id = context.run_id
        run_log_key = f"{INGEST_LOG_PREFIX}/creation_date={creation_date_key}/run_id={run_id}/log.json"
        latest_log_key = (
            f"{INGEST_LOG_PREFIX}/creation_date={creation_date_key}/latest.json"
        )

        parquet_exists = _s3_key_exists(s3, BUCKET, parquet_key)

        # 1) Skip if parquet exists (no failure), carry forward metadata from latest.json
        if parquet_exists:
            latest_metadata = _s3_get_json(s3, BUCKET, latest_log_key) or {}
            now = datetime.now(timezone.utc).isoformat()

            log_body = {
                "asset": "collect_gp_history_data",
                "creation_date": creation_date_key,
                "run_id": run_id,
                "timestamp_utc": now,
                "status": "skipped_partition",
                "reason": "parquet_already_exists",
                "data_parquet_s3_uri": latest_metadata.get("data_parquet_s3_uri"),
                "fetched_rows": latest_metadata.get("fetched_rows"),
            }
            _s3_put_json(s3, BUCKET, run_log_key, log_body)
            _s3_put_json(s3, BUCKET, latest_log_key, log_body)

            skipped_count += 1

            yield MaterializeResult(
                asset_key=context.asset_key,
                partition=creation_date_key,
                metadata={
                    "creation_date": creation_date_key,
                    "gp_history_rows_fetched": latest_metadata.get("fetched_rows"),
                    "data_parquet": MetadataValue.path(
                        latest_metadata.get("data_parquet_s3_uri")
                    ),
                    "ingest_log": MetadataValue.path(f"s3://{BUCKET}/{run_log_key}"),
                    "latest_summary": MetadataValue.path(
                        f"s3://{BUCKET}/{latest_log_key}"
                    ),
                    "status": "skipped_partition",
                    "reason": "parquet_already_exists",
                },
            )

            continue

        # 2) Disallow today's date: record failure but continue loop
        if creation_date_key == today_key:
            msg = (
                f"Refusing to collect gp_history for today's creation_date={creation_date_key} "
                f"(timezone {PARTITION_TZ})."
            )
            now = datetime.now(timezone.utc).isoformat()
            log_body = {
                "asset": "collect_gp_history_data",
                "creation_date": creation_date_key,
                "run_id": run_id,
                "timestamp_utc": now,
                "status": "failed_partition",
                "reason": "creation_date_is_today",
            }
            _s3_put_json(s3, BUCKET, run_log_key, log_body)
            _s3_put_json(s3, BUCKET, latest_log_key, log_body)

            failures.append(msg)
            continue

        # 3) Materialize: fetch -> write parquet -> log
        try:
            creation_date_obj = date.fromisoformat(creation_date_key)
        except ValueError as e:
            failures.append(
                f"Invalid creation_date partition key: {creation_date_key} ({e})"
            )
            continue

        try:
            raw = spacetrack.fetch_gp_history(creation_date_obj)
            df = pd.DataFrame(raw)
            del raw

            _write_df_to_s3_parquet(
                s3,
                BUCKET,
                parquet_key,
                df,
            )

            # Immediately drop big objects before logging / next iteration
            rows_fetched = int(len(df))
            del df

            # Encourage Python + Arrow to release memory between partitions
            gc.collect()
            pa.default_memory_pool().release_unused()

            now = datetime.now(timezone.utc).isoformat()
            log_body = {
                "asset": "collect_gp_history_data",
                "creation_date": creation_date_key,
                "run_id": run_id,
                "timestamp_utc": now,
                "status": "materialized",
                "data_parquet_s3_uri": f"s3://{BUCKET}/{parquet_key}",
                "fetched_rows": rows_fetched,
            }
            _s3_put_json(s3, BUCKET, run_log_key, log_body)
            _s3_put_json(s3, BUCKET, latest_log_key, log_body)

            materialized_count += 1

            yield MaterializeResult(
                asset_key=context.asset_key,
                partition=creation_date_key,
                metadata={
                    "creation_date": creation_date_key,
                    "gp_history_rows_fetched": rows_fetched,
                    "data_parquet": MetadataValue.path(f"s3://{BUCKET}/{parquet_key}"),
                    "ingest_log": MetadataValue.path(f"s3://{BUCKET}/{run_log_key}"),
                    "latest_summary": MetadataValue.path(
                        f"s3://{BUCKET}/{latest_log_key}"
                    ),
                    "status": "materialized",
                },
            )

        except Failure as e:
            # If SpaceTrackClient exhausts rate limit retries, you get a Failure here.
            now = datetime.now(timezone.utc).isoformat()
            log_body = {
                "asset": "collect_gp_history_data",
                "creation_date": creation_date_key,
                "run_id": run_id,
                "timestamp_utc": now,
                "status": "failed_partition",
                "reason": "spacetrack_failure",
                "error": str(e),
            }
            _s3_put_json(s3, BUCKET, run_log_key, log_body)
            _s3_put_json(s3, BUCKET, latest_log_key, log_body)
            failures.append(f"{creation_date_key}: {e}")
            continue

        except Exception as e:
            now = datetime.now(timezone.utc).isoformat()
            log_body = {
                "asset": "collect_gp_history_data",
                "creation_date": creation_date_key,
                "run_id": run_id,
                "timestamp_utc": now,
                "status": "failed_partition",
                "reason": "unexpected_exception",
                "error": repr(e),
            }
            _s3_put_json(s3, BUCKET, run_log_key, log_body)
            _s3_put_json(s3, BUCKET, latest_log_key, log_body)
            failures.append(f"{creation_date_key}: {repr(e)}")
            continue
        finally:
            # Always attempt to release memory between iterations
            try:
                if "raw" in locals():
                    del raw
                if "df" in locals():
                    del df
            except Exception:
                pass
            gc.collect()
            pa.default_memory_pool().release_unused()

    context.log_event(
        AssetObservation(
            asset_key=context.asset_key,
            metadata={
                "partitions_attempted": len(context.partition_keys),
                "partitions_materialized": materialized_count,
                "partitions_skipped": skipped_count,
                "partitions_failed": len(failures),
            },
        )
    )
    # If any partitions were disallowed or failed, fail the run after processing all partitions.
    # This satisfies: "create a failure in Dagster, but shouldn't prevent the loop continuing other dates."
    if failures:
        raise Failure(
            description="One or more gp_history partitions failed or were refused.",
            metadata={"failures": failures},
        )
