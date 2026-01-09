from __future__ import annotations

from dagster import Definitions, define_asset_job, in_process_executor

from rtoe_ue.defs.assets.satcat_collection import collect_satcat_data
from rtoe_ue.defs.assets.space_weather_collection import collect_space_weather_data
from rtoe_ue.defs.resources.s3 import s3_resource
from rtoe_ue.defs.resources.spacetrack import spacetrack_resource

# later:
# from rtoe_ue.defs.assets.satcat_load_rds import load_satcat_rds
# from ...space_weather_load_rds import load_space_weather_rds
# from ...gp_history_collection import collect_gp_history_data
# from ...gp_history_load_rds import load_gp_history_rds


COLLECTION_ASSETS = [
    collect_satcat_data,
    collect_space_weather_data,
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
    executor_def=in_process_executor,
)

# later:
# load_rds_job = define_asset_job(
#     name="load_rds_job",
#     selection=[a.key for a in LOAD_ASSETS],
#     executor_def=in_process_executor,
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
