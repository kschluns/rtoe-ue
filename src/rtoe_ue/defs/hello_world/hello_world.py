import json
import os
from datetime import datetime, timezone

import boto3
from dagster import AssetExecutionContext, AssetMaterialization, MetadataValue, asset


@asset(name="hello_world")
def hello_world(context: AssetExecutionContext) -> str:
    """
    Smoke-test asset: writes a small JSON object to S3.
    Returns the s3:// URI and emits a materialization with metadata so it shows up nicely in Dagster UI.
    """
    bucket = os.environ["S3_BUCKET"]
    prefix = os.environ.get("S3_PREFIX", "dummy-tests")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix}/hello-{ts}.json"

    payload = {
        "message": "hello from ecs (dagster asset)",
        "timestamp_utc": ts,
    }

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )

    uri = f"s3://{bucket}/{key}"

    # Log + explicit materialization metadata (shows up in the UI)
    context.log.info(f"WROTE {uri}")
    context.log.info(f"payload={payload}")

    context.add_output_metadata(
        {
            "s3_uri": MetadataValue.url(uri),  # renders as link-ish text in UI
            "bucket": bucket,
            "key": key,
            "timestamp_utc": ts,
        }
    )

    # Optional: also emit a materialization event (Dagster UI-friendly)
    context.log_event(
        AssetMaterialization(
            asset_key=context.asset_key,
            description="Wrote hello payload to S3",
            metadata={
                "s3_uri": uri,
                "bucket": bucket,
                "key": key,
                "timestamp_utc": ts,
            },
        )
    )

    return uri
