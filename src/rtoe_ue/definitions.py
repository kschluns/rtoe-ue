from __future__ import annotations

from dagster import Definitions

from rtoe_ue.defs.assets.satcat_collection import collect_satcat_data
from rtoe_ue.defs.resources.s3 import s3_resource
from rtoe_ue.defs.resources.spacetrack import spacetrack_resource


defs = Definitions(
    assets=[collect_satcat_data],
    resources={
        "s3_resource": s3_resource,
        "spacetrack_resource": spacetrack_resource,
    },
)
