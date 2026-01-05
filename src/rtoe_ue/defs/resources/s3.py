from __future__ import annotations

import boto3
from dagster import resource


@resource
def s3_resource(_context):
    """
    Basic boto3 S3 client resource.
    Uses standard AWS env/role auth (ECS task role, etc.).
    """
    return boto3.client("s3")
