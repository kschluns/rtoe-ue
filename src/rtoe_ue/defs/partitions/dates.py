from __future__ import annotations

import os
from dagster import DailyPartitionsDefinition

# Partition key is the ingestion date: "YYYY-MM-DD"
INGESTION_DATE_PARTITIONS = DailyPartitionsDefinition(
    start_date="2026-01-01",
    timezone="America/Chicago",
    # include "today" as a valid partition key
    end_offset=1,
)

# You’ll use this later for GP history creation_date partitions
CREATION_DATE_PARTITIONS = DailyPartitionsDefinition(
    start_date="2025-01-01",
    timezone="America/Chicago",
    # exclude "today" as a valid partition key
    end_offset=0,
)
