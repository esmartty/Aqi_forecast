import json
from datetime import datetime, timezone
from pathlib import Path

import requests

from open_meteo_s3 import inspect_model_run, inspect_s3_object, s3_client

# Open-Meteo ECMWF IFS
#https://open-meteo.com/en/docs/model-updates
METADATA_URL = (
    "https://api.open-meteo.com/data/"
    "ecmwf_ifs/static/meta.json"
)

MODEL = "ecmwf_ifs"

OUTPUT_FILE = Path("openmeteo_ecmwf_ifs_report.json")

def format_duration(seconds: float) -> str:
    sign = "-" if seconds < 0 else ""
    seconds = int(abs(seconds))

    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    return f"{sign}{hours} H {minutes} min {secs} secs"

def parse_utc(value):
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )


def utc_from_unix(timestamp):
    if timestamp is None:
        return None

    return datetime.fromtimestamp(
        int(timestamp),
        tz=timezone.utc,
    ).isoformat()


def get_json_url(url):
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def main():
    checked_at = datetime.now(timezone.utc)

    # 1. Read the model's latest-run metadata
    metadata = get_json_url(METADATA_URL)

    run_timestamp = metadata.get(
        "last_run_initialisation_time"
    )

    if run_timestamp is None:
        raise RuntimeError(
            "last_run_initialisation_time missing "
            "from metadata"
        )

    run_dt = datetime.fromtimestamp(
        int(run_timestamp),
        tz=timezone.utc,
    )

    s3 = s3_client()

    # 3. Inspect S3 objects
    report = {
        "checked_at_utc": checked_at.isoformat(),
        "model": MODEL,
        "metadata_url": METADATA_URL,
        "model_updates": metadata,
        "run_time_utc": run_dt.isoformat(),
        "api_available_at_utc": utc_from_unix(
            metadata.get("last_run_availability_time")
        ),
        "api_modification_at_utc": utc_from_unix(
            metadata.get("last_run_modification_time")
        ),
        "data_run_meta": inspect_model_run(s3, MODEL, run_dt),
        "data_spatial": {},
    }

    for filename in (
        "in-progress.json",
        "latest.json",
    ):
        key = f"data_spatial/{MODEL}/{filename}"

        report["data_spatial"][filename] = (
            inspect_s3_object(s3, key)
        )

        # 4. Save report — append to history in the same JSON file
    if OUTPUT_FILE.exists():
        try:
            with OUTPUT_FILE.open("r", encoding="utf-8") as f:
                history = json.load(f)

            # Migrate an existing single-report JSON to a list
            if isinstance(history, dict):
                history = [history]

            if not isinstance(history, list):
                raise ValueError("Expected a JSON object or list of reports")

        except (json.JSONDecodeError, ValueError) as e:
            raise RuntimeError(
                f"Cannot read existing history from {OUTPUT_FILE}: {e}"
            ) from e
    else:
        history = []

    # Add the current run
    history.append(report)

    # Save full history
    OUTPUT_FILE.write_text(
        json.dumps(
            history,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    print(json.dumps(
        report,
        ensure_ascii=False,
        indent=2,
        default=str,
    ))

    # Compare timestamps
    api_available = parse_utc(
        report["api_available_at_utc"]
    )

    s3_modified = parse_utc(
        report["data_run_meta"]["last_modified_utc"]
    )

    meta_created = parse_utc(
        report["data_run_meta"]["json"]["created_at"]
    )

    s3_delay = (s3_modified - api_available).total_seconds()

    meta_delta = (meta_created - api_available).total_seconds()

    print(
        "meta.json created_at - API availability:",
        format_duration(meta_delta),
    )

    print(
        "S3 LastModified - API availability:",
        format_duration(s3_delay),
    )
    
    print(
        f"\nReport saved to: {OUTPUT_FILE.resolve()}"
    )
    print(f"Total reports in history: {len(history)}")

if __name__ == "__main__":
    main()

