from __future__ import annotations

import gc
import io
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import pyarrow as pa


@dataclass(frozen=True)
class S3ParquetRef:
    bucket: str
    key: str

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


def s3_key_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        # boto3 ClientError is fine to catch generically here
        # because a missing key is the normal control flow.
        msg = str(e)
        if "404" in msg or "NoSuchKey" in msg or "NotFound" in msg:
            return False
        # Some botocore exceptions don't stringify nicely; fall back to safe behavior:
        try:
            code = getattr(getattr(e, "response", None), "get", lambda *_: None)(
                "Error", {}
            ).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
        except Exception:
            pass
        raise


def read_parquet_df_from_s3(
    s3, bucket: str, key: str, columns: List = None
) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    try:
        raw = body.read()
        buf = io.BytesIO(raw)
        try:
            return pd.read_parquet(buf, columns=columns)
        finally:
            buf.close()
            # help refcounting drop big objects sooner
            del buf
            del raw
    finally:
        # Important: return connection to pool promptly
        body.close()
        # Encourage Arrow to return pooled memory
        gc.collect()
        pa.default_memory_pool().release_unused()


def df_records(df: pd.DataFrame, cols: Sequence[str]) -> List[Tuple]:
    # Convert to python tuples in column order (None-safe)
    sub = df.loc[:, list(cols)]
    return [tuple(row) for row in sub.itertuples(index=False, name=None)]


def coerce_utc_naive_timestamp(series: pd.Series) -> pd.Series:
    """
    Parse timestamps and return naive UTC (timestamp w/o tz) as required by your schema.
    """
    ts = pd.to_datetime(series, errors="coerce", utc=True)
    # Convert to naive UTC
    return ts.dt.tz_convert("UTC").dt.tz_localize(None)
