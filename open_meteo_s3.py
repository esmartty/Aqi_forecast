import json
from datetime import datetime, timezone

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError


BUCKET = "openmeteo"
REGION = "us-west-2"


def s3_client():
    return boto3.client(
        "s3",
        region_name=REGION,
        config=Config(signature_version=UNSIGNED),
    )


def inspect_s3_object(s3, key):
    """Inspect a public S3 object and read it if it is JSON."""
    result = {"key": key}

    try:
        head = s3.head_object(
            Bucket=BUCKET,
            Key=key,
        )

        result["exists"] = True
        result["last_modified_utc"] = (
            head["LastModified"].astimezone(
                timezone.utc
            ).isoformat()
        )
        result["size_bytes"] = head["ContentLength"]
        result["etag"] = head.get("ETag")
        result["content_type"] = head.get("ContentType")

        if key.endswith(".json"):
            obj = s3.get_object(
                Bucket=BUCKET,
                Key=key,
            )
            raw = obj["Body"].read()

            try:
                result["json"] = json.loads(raw)
            except (ValueError, UnicodeDecodeError):
                result["json_text"] = raw.decode(
                    "utf-8",
                    errors="replace",
                )[:5000]

    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = error.get("Code", "Unknown")

        result["exists"] = False
        result["error_code"] = code
        result["error_message"] = error.get(
            "Message",
            str(exc),
        )

    except Exception as exc:
        result["exists"] = None
        result["error"] = repr(exc)

    return result


def inspect_model_run(s3, model: str, run_time: datetime | str):
    if isinstance(run_time, str):
        run_time = datetime.fromisoformat(
            run_time.replace("Z", "+00:00")
        )

    if run_time.tzinfo is None:
        run_time = run_time.replace(tzinfo=timezone.utc)
    else:
        run_time = run_time.astimezone(timezone.utc)

    run_dir = f"{run_time:%Y/%m/%d}/{run_time:%H%M}Z"
    key = f"data_run/{model}/{run_dir}/meta.json"

    return inspect_s3_object(s3, key)
