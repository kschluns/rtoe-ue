import json
import os
from datetime import datetime, timezone

import boto3


def main() -> None:
    bucket = os.environ["S3_BUCKET"]
    prefix = os.environ.get("S3_PREFIX", "dummy-tests")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix}/hello-{ts}.json"

    payload = {
        "message": "hello from ecs",
        "timestamp_utc": ts,
    }

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"WROTE s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
