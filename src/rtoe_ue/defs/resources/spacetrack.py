from __future__ import annotations

import os
from typing import Any, Dict, List

import requests
from dagster import resource


class SpaceTrackClient:
    """
    Minimal Space-Track client:
      - logs in once (session cookies)
      - fetches SATCAT full catalog in JSON

    Env vars required:
      - SPACE_TRACK_USERNAME
      - SPACE_TRACK_PASSWORD
    """

    BASE_URL = "https://www.space-track.org"

    def __init__(self, username: str, password: str, timeout_s: int = 60):
        self._username = username
        self._password = password
        self._timeout_s = timeout_s
        self._session = requests.Session()
        self._logged_in = False

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

    def fetch_satcat(self) -> List[Dict[str, Any]]:
        """
        Pull full SATCAT catalog.
        Endpoint format:
          /basicspacedata/query/class/satcat/format/json
        """
        self.login()
        url = f"{self.BASE_URL}/basicspacedata/query/class/satcat/format/json"
        resp = self._session.get(url, timeout=self._timeout_s)
        resp.raise_for_status()
        return resp.json()


@resource
def spacetrack_resource(_context) -> SpaceTrackClient:
    username = os.getenv("SPACE_TRACK_USERNAME")
    password = os.getenv("SPACE_TRACK_PASSWORD")
    if not username or not password:
        raise RuntimeError(
            "Missing SPACE_TRACK_USERNAME / SPACE_TRACK_PASSWORD env vars."
        )
    return SpaceTrackClient(username=username, password=password, timeout_s=60)
