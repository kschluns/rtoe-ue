from __future__ import annotations

import io
from dataclasses import dataclass

import pandas as pd
import requests
from dagster import Failure, Field, resource


DEFAULT_SW_ALL_CSV_URL = "https://celestrak.org/SpaceData/SW-All.csv"


@dataclass
class CelestrakResource:
    """
    Simple CelesTrak client for reading Space Weather CSV into a pandas DataFrame.
    """

    sw_all_csv_url: str = DEFAULT_SW_ALL_CSV_URL
    timeout_s: int = 30

    def fetch_space_weather_df(self) -> pd.DataFrame:
        """
        Fetch the SW-All.csv and return as a pandas DataFrame.
        """
        try:
            resp = requests.get(self.sw_all_csv_url, timeout=self.timeout_s)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise Failure(
                description=f"Failed to fetch CelesTrak space weather CSV: {e}"
            ) from e

        # CelesTrak serves plain CSV text. Use BytesIO to avoid encoding surprises.
        raw_bytes = resp.content
        try:
            df = pd.read_csv(io.BytesIO(raw_bytes))
        except Exception as e:
            raise Failure(
                description=f"Failed to parse space weather CSV into DataFrame: {e}"
            ) from e

        if df.empty:
            # Not necessarily fatal, but usually indicates upstream issue or format change.
            raise Failure(
                description="CelesTrak space weather CSV parsed to empty DataFrame"
            )

        return df


@resource(
    config_schema={
        "sw_all_csv_url": Field(
            str,
            default_value=DEFAULT_SW_ALL_CSV_URL,
            description="CelesTrak Space Weather CSV URL (SW-All.csv).",
        ),
        "timeout_s": Field(
            int,
            default_value=30,
            description="HTTP timeout in seconds.",
        ),
    }
)
def celestrak_resource(context) -> CelestrakResource:
    return CelestrakResource(
        sw_all_csv_url=context.resource_config["sw_all_csv_url"],
        timeout_s=context.resource_config["timeout_s"],
    )
