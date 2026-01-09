from __future__ import annotations

import gzip
import io
import json
from dataclasses import dataclass
from typing import Dict, Optional, Set

from botocore.exceptions import ClientError


@dataclass(frozen=True)
class SatcatManifest:
    """
    NORAD_CAT_ID (str) -> max FILE (int)
    """

    norad_to_max_file: Dict[str, int]


def _s3_get_bytes(s3, bucket: str, key: str) -> Optional[bytes]:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            return None
        raise


def read_satcat_manifest_gz_json(s3, bucket: str, key: str) -> SatcatManifest:
    raw = _s3_get_bytes(s3, bucket, key)
    if raw is None:
        return SatcatManifest(norad_to_max_file={})

    with gzip.GzipFile(fileobj=io.BytesIO(raw), mode="rb") as gz:
        payload = gz.read().decode("utf-8")

    data = json.loads(payload)
    # normalize to str->int
    out: Dict[str, int] = {}
    for k, v in data.items():
        if k is None:
            continue
        ks = str(k).strip()
        if not ks:
            continue
        try:
            out[ks] = int(v)
        except Exception:
            # ignore bad values; you can tighten this later
            continue
    return SatcatManifest(norad_to_max_file=out)


def write_satcat_manifest_gz_json(
    s3,
    bucket: str,
    key: str,
    manifest: SatcatManifest,
) -> None:
    payload = json.dumps(
        manifest.norad_to_max_file, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(payload)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/json",
        ContentEncoding="gzip",
    )


@dataclass(frozen=True)
class SpaceWeatherManifest:
    obs_dates: Set[str]  # ISO date strings: YYYY-MM-DD


def read_space_weather_manifest_gz_json(
    s3, bucket: str, key: str
) -> SpaceWeatherManifest:
    """
    Manifest is a gzipped JSON object:
      {"obs_dates":["YYYY-MM-DD", ...]}
    If missing, returns empty manifest.
    """
    raw = _s3_get_bytes(s3, bucket, key)
    if raw is None:
        return SpaceWeatherManifest(obs_dates=set())

    with gzip.GzipFile(fileobj=io.BytesIO(raw), mode="rb") as gz:
        payload = gz.read().decode("utf-8")

    dates = payload.get("obs_dates", [])
    # normalize / de-dupe
    dates = {str(d).strip() for d in dates if str(d).strip()}
    return SpaceWeatherManifest(obs_dates=dates)


def write_space_weather_manifest_gz_json(
    s3, bucket: str, key: str, manifest: SpaceWeatherManifest
) -> None:
    payload = json.dumps(
        {"obs_dates": sorted(manifest.obs_dates)},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(payload)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/json",
        ContentEncoding="gzip",
    )
