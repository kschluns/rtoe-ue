from __future__ import annotations

import os
import time
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import requests
from dagster import Failure, resource


RATE_LIMIT_ERROR_JSON = [
    {
        "error": "You've violated your query rate limit.  Please refer to our Acceptable Use guidelines for further information on how to avoid this message in the future. ( https://www.space-track.org/documentation ) ( https://www.space-track.org/documentation )"
    }
]


class SpaceTrackClient:
    """
    Minimal Space-Track client:
      - logs in once (session cookies)
      - fetches SATCAT full catalog in JSON
      - fetches GP history by CREATION_DATE window [date, date+1)

    Env vars required:
      - SPACE_TRACK_USERNAME
      - SPACE_TRACK_PASSWORD
    """

    BASE_URL = "https://www.space-track.org"

    def __init__(
        self,
        username: str,
        password: str,
        timeout_s: int = 60,
        # MVP backoff schedule when rate-limited (200 but error JSON)
        rate_limit_backoffs_s: Optional[List[int]] = None,
        # Optional per-request spacing to avoid bursting under 30/min:
        min_seconds_between_requests: float = 2.2,
    ):
        self._username = username
        self._password = password
        self._timeout_s = timeout_s
        self._session = requests.Session()
        self._logged_in = False

        self._rate_limit_backoffs_s = rate_limit_backoffs_s or [60, 3600]
        self._min_seconds_between_requests = min_seconds_between_requests
        self._last_request_monotonic: Optional[float] = None

    def login(self) -> None:
        if self._logged_in:
            return

        url = f"{self.BASE_URL}/ajaxauth/login"
        data = {"identity": self._username, "password": self._password}
        resp = self._session.post(url, data=data, timeout=self._timeout_s)
        resp.raise_for_status()

        # Space-Track returns 200 even on some auth failures sometimes;
        # but typically a successful login sets cookies and returns text.
        # We'll treat non-200 as failure; for 200, proceed.
        self._logged_in = True

    def _throttle(self) -> None:
        """
        Simple spacing throttle to reduce risk of hitting 30/min when doing backfills.
        (This is per-process/per-run, not cross-run.)
        """
        if self._last_request_monotonic is None:
            return
        elapsed = time.monotonic() - self._last_request_monotonic
        sleep_s = self._min_seconds_between_requests - elapsed
        if sleep_s > 0:
            time.sleep(sleep_s)

    @staticmethod
    def _is_rate_limit_body(payload: Any) -> bool:
        """
        Space-Track returns HTTP 200 with a JSON body like RATE_LIMIT_ERROR_JSON.
        We treat any list[{"error": "...rate limit..."}] shape as rate limit.
        """
        if not isinstance(payload, list) or len(payload) == 0:
            return False
        first = payload[0]
        if not isinstance(first, dict):
            return False
        err = first.get("error")
        return isinstance(err, str) and "rate limit" in err.lower()

    def _get_json_with_rate_limit_backoff(self, url: str) -> List[Dict[str, Any]]:
        """
        GET -> parse JSON -> if rate-limit body, backoff and retry.
        """
        self.login()

        attempts = 1 + len(self._rate_limit_backoffs_s)
        last_payload: Any = None

        for attempt_idx in range(attempts):
            self._throttle()

            resp = self._session.get(url, timeout=self._timeout_s)
            resp.raise_for_status()

            self._last_request_monotonic = time.monotonic()

            try:
                payload = resp.json()
            except Exception as e:
                raise Failure(
                    description=f"Space-Track returned non-JSON response: {e}"
                ) from e

            last_payload = payload

            if not self._is_rate_limit_body(payload):
                if isinstance(payload, list):
                    # expected shape: list[dict]
                    return payload  # type: ignore[return-value]
                raise Failure(
                    description=f"Unexpected Space-Track JSON shape (expected list): {type(payload)}"
                )

            # Rate limit hit
            if attempt_idx < len(self._rate_limit_backoffs_s):
                time.sleep(self._rate_limit_backoffs_s[attempt_idx])
                continue

        # Exhausted retries
        raise Failure(
            description="Space-Track rate limit hit and retries exhausted.",
            metadata={"url": url, "last_payload": last_payload},
        )

    def fetch_satcat(self) -> List[Dict[str, Any]]:
        """
        Pull full SATCAT catalog.
        Endpoint format:
          /basicspacedata/query/class/satcat/format/json
        """
        url = f"{self.BASE_URL}/basicspacedata/query/class/satcat/format/json"
        return self._get_json_with_rate_limit_backoff(url)


def fetch_gp_history(self, creation_date: date) -> List[Dict[str, Any]]:
    """
    Pull gp_history rows for a specific creation_date window:
      CREATION_DATE/YYYY-MM-DD--YYYY-MM-DD+1/format/json

    Space-Track edge case:
      - If the result set exceeds ~100K rows, Space-Track returns HTTP 500.
      - In that case, split into two queries on NORAD_CAT_ID and concatenate results.
    """
    start = creation_date.isoformat()
    end = (creation_date + timedelta(days=1)).isoformat()

    base = (
        f"{self.BASE_URL}/basicspacedata/query/class/gp_history/"
        f"CREATION_DATE/{start}--{end}"
    )

    url = f"{base}/format/json"

    def _split_fetch() -> List[Dict[str, Any]]:
        url_lt = f"{base}/NORAD_CAT_ID/%3C40001/format/json"
        url_gt = f"{base}/NORAD_CAT_ID/%3E40000/format/json"
        left = self._get_json_with_rate_limit_backoff(url_lt)
        right = self._get_json_with_rate_limit_backoff(url_gt)
        return left + right

    try:
        return self._get_json_with_rate_limit_backoff(url)
    except requests.HTTPError as e:
        resp = getattr(e, "response", None)
        status = getattr(resp, "status_code", None)

        # Treat 500 as "too many rows" for this endpoint and split the query.
        if status == 500:
            return _split_fetch()

        raise


@resource
def spacetrack_resource(_context) -> SpaceTrackClient:
    username = os.getenv("SPACE_TRACK_USERNAME")
    password = os.getenv("SPACE_TRACK_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            "Missing SPACE_TRACK_USERNAME / SPACE_TRACK_PASSWORD env vars."
        )
    return SpaceTrackClient(username=username, password=password, timeout_s=60)
