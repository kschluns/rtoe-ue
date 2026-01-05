from __future__ import annotations

from pathlib import Path
import yaml

from dagster import Definitions, define_asset_job

from rtoe_ue.defs.assets.satcat_collection import collect_satcat_data
from rtoe_ue.defs.resources.s3 import s3_resource
from rtoe_ue.defs.resources.spacetrack import spacetrack_resource

# later:
# from rtoe_ue.defs.assets.satcat_load_rds import load_satcat_rds
# from ...space_weather_collection import collect_space_weather_data
# from ...space_weather_load_rds import load_space_weather_rds
# from ...gp_history_collection import collect_gp_history_data
# from ...gp_history_load_rds import load_gp_history_rds


def _load_run_config(name: str) -> dict:
    cfg_path = Path(__file__).parent / "defs" / "run_configs" / f"{name}.yaml"
    return yaml.safe_load(cfg_path.read_text()) or {}


COLLECTION_ASSETS = [
    collect_satcat_data,
    # collect_space_weather_data,
    # collect_gp_history_data,
]

LOAD_ASSETS = [
    # load_satcat_rds,
    # load_space_weather_rds,
    # load_gp_history_rds,
]

collection_job = define_asset_job(
    name="collection_job",
    description="Run all data collection assets: SATCAT, Space Weather, GP History.",
    selection=[a.key for a in COLLECTION_ASSETS],
    config=_load_run_config("collection_job"),
)

# later:
# load_rds_job = define_asset_job(
#     name="load_rds_job",
#     selection=[a.key for a in LOAD_ASSETS],
#     config=_load_run_config("load_rds_job"),
# )

defs = Definitions(
    assets=[*COLLECTION_ASSETS],  # later: <-- add LOAD_ASSETS
    jobs=[collection_job],  # later: <-- add load_rds_job
    resources={
        "s3_resource": s3_resource,
        "spacetrack_resource": spacetrack_resource,
        # later: <-- add "postgres_resource": ...,
    },
)
