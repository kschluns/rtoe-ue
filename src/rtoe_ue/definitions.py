from __future__ import annotations

from dagster import define_asset_job, Definitions, in_process_executor

from rtoe_ue.defs.assets.gp_history_collection import collect_gp_history_data
from rtoe_ue.defs.assets.gp_history_load_rds import load_gp_history_to_rds
from rtoe_ue.defs.assets.satcat_collection import collect_satcat_data
from rtoe_ue.defs.assets.satcat_load_rds import load_satcat_to_rds
from rtoe_ue.defs.assets.space_weather_collection import collect_space_weather_data
from rtoe_ue.defs.assets.space_weather_load_rds import load_space_weather_to_rds
from rtoe_ue.defs.resources.s3 import s3_resource
from rtoe_ue.defs.resources.postgres import postgres_resource
from rtoe_ue.defs.resources.celestrak import celestrak_resource
from rtoe_ue.defs.resources.spacetrack import spacetrack_resource

COLLECTION_ASSETS = [
    collect_satcat_data,
    collect_space_weather_data,
    collect_gp_history_data,
]

LOAD_ASSETS = [
    load_satcat_to_rds,
    load_space_weather_to_rds,
    load_gp_history_to_rds,
]

satcat_spaceweather_job = define_asset_job(
    name="satcat_spaceweather_job",
    description="Run SATCAT + Space Weather collectors partitioned by ingestion date.",
    selection=["collect_satcat_data", "collect_space_weather_data"],
    executor_def=in_process_executor,
)

gp_history_job = define_asset_job(
    name="gp_history_job",
    description="Run gp_history collector  partitioned by creation date.",
    selection=["collect_gp_history_data"],
    executor_def=in_process_executor,
    tags={
        "concurrency_group": "spacetrack_gp_history",
        "ecs/cpu": "256",
        "ecs/memory": "1024",
    },
)

load_rds_by_ingestion_date_job = define_asset_job(
    name="load_rds_by_ingestion_date_job",
    description="Load S3 parquet into RDS by ingestion date (Postgres).",
    selection=["load_satcat_to_rds", "load_space_weather_to_rds"],
    executor_def=in_process_executor,
)

load_rds_by_creation_date_job = define_asset_job(
    name="load_rds_by_creation_date_job",
    description="Load S3 parquet into RDS by creation date (Postgres).",
    selection=["load_gp_history_to_rds"],
    executor_def=in_process_executor,
)

defs = Definitions(
    assets=[*COLLECTION_ASSETS, *LOAD_ASSETS],
    jobs=[
        satcat_spaceweather_job,
        gp_history_job,
        load_rds_by_ingestion_date_job,
        load_rds_by_creation_date_job,
    ],
    resources={
        "s3_resource": s3_resource,
        "spacetrack_resource": spacetrack_resource,
        "celestrak_resource": celestrak_resource,
        "postgres_resource": postgres_resource,
    },
)
