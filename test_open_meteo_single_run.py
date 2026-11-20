import json
import time
import logging
from urllib import response
import requests

from pathlib import Path
from datetime import datetime, timezone
from collections import Counter


# ============================================================
# 1. CONFIGURATION
# ============================================================

MODEL = "ecmwf_ifs"

LATITUDE = 50.05
LONGITUDE = 19.95
# response coordinates: "latitude":50.017574,"longitude":19.947643

METADATA_URL = (
    "https://api.open-meteo.com/data/"
    "ecmwf_ifs/static/meta.json"
)

SINGLE_RUN_API_URL = (
    "https://single-runs-api.open-meteo.com/v1/forecast"
)

FORECAST_DAYS = 10

# Open-Meteo recommends waiting 10 minutes after
# last_run_availability_time for data propagation.
MIN_AVAILABILITY_DELAY_SECONDS = 600

REQUEST_TIMEOUT = 180
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 30
MAX_LOCATIONS_PER_REQUEST = 20

OUTPUT_DIR = Path("weather_data")
OUTPUT_DIR.mkdir(exist_ok=True)


# ============================================================
# 2. WEATHER VARIABLES
# ============================================================

HOURLY_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",

    "precipitation",
    "rain",
    "showers",
    "snowfall",

    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",

    "cloud_cover",
    "cloud_cover_low",
    "cloud_cover_mid",
    "cloud_cover_high",

    "pressure_msl",
    "visibility",
    "boundary_layer_height",

    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",

    "cape",
    "convective_inhibition",
]


# ============================================================
# 3. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


# ============================================================
# 4. TIME HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def format_duration(seconds):
    seconds = int(abs(seconds))

    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)

    return f"{hours}h {minutes}m {secs}s"


def unix_to_datetime(timestamp):
    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc,
    )


# ============================================================
# 5. GET LATEST MODEL RUN METADATA
# ============================================================

def get_latest_run_metadata():

    logger.info("Requesting ECMWF IFS metadata...")

    response = requests.get(
        METADATA_URL,
        timeout=30,
    )

    response.raise_for_status()

    metadata = response.json()

    required_fields = [
        "last_run_initialisation_time",
        "last_run_availability_time",
        "last_run_modification_time",
    ]

    for field in required_fields:
        if field not in metadata:
            raise ValueError(
                f"Missing metadata field: {field}"
            )

    run_time = unix_to_datetime(
        metadata["last_run_initialisation_time"]
    )

    api_available_at = unix_to_datetime(
        metadata["last_run_availability_time"]
    )

    modification_time = unix_to_datetime(
        metadata["last_run_modification_time"]
    )

    return {
        "run_time": run_time.strftime("%Y-%m-%dT%H:%M"),
        "run_time_utc": run_time,
        "api_available_at_utc": api_available_at,
        "modification_time_utc": modification_time,
        "metadata": metadata,
    }


# ============================================================
# 6. CHECK RUN AVAILABILITY
# ============================================================

def check_run_availability(run_info):

    now = utc_now()

    available_at = run_info["api_available_at_utc"]

    elapsed_seconds = (
        now - available_at
    ).total_seconds()

    logger.info(
        "Run initialization: %s",
        run_info["run_time"],
    )

    logger.info(
        "API availability time: %s",
        available_at.isoformat(),
    )

    logger.info(
        "Time since availability: %s",
        format_duration(elapsed_seconds),
    )

    if elapsed_seconds < MIN_AVAILABILITY_DELAY_SECONDS:

        remaining = (
            MIN_AVAILABILITY_DELAY_SECONDS - elapsed_seconds
        )

        raise RuntimeError(
            "Run is not ready for download yet. "
            f"Wait another {format_duration(remaining)} "
            "and run the script again."
        )

    logger.info("Run availability check passed.")


# ============================================================
# 7. FETCH SINGLE RUN FORECAST
# ============================================================

def fetch_forecast(
    run_time,
    latitude=None,
    longitude=None,
    bounding_box=None,
    hourly_variables=None,
    forecast_days=FORECAST_DAYS,
):

    params = {
        "models": MODEL,
        "run": run_time,
        "hourly": ",".join(
            HOURLY_VARIABLES
            if hourly_variables is None
            else hourly_variables
        ),
        "forecast_days": forecast_days,
        "timezone": "GMT",
    }

    if latitude is not None:
        params["latitude"] = latitude
    if longitude is not None:
        params["longitude"] = longitude

    if bounding_box is not None:
        params["bounding_box"] = ",".join(
            f"{coordinate:g}" for coordinate in bounding_box
        )

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            logger.info(
                "Requesting Single Runs API. Attempt %s/%s",
                attempt,
                MAX_RETRIES,
            )

            response = requests.get(
                SINGLE_RUN_API_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            # Print the full request URL
            print("Requested URL:", response.request.url)

            response.raise_for_status()

            return response.json()

        except requests.exceptions.HTTPError as error:

            status_code = (
                error.response.status_code
                if error.response is not None
                else None
            )

            # Retry only temporary server/rate-limit errors.
            if status_code not in (429, 500, 502, 503, 504):
                raise

            if attempt == MAX_RETRIES:
                raise

            logger.warning(
                "Temporary API error: %s. Retrying...",
                error,
            )

        except requests.exceptions.RequestException as error:

            if attempt == MAX_RETRIES:
                raise

            logger.warning(
                "Request failed: %s. Retrying...",
                error,
            )

        time.sleep(RETRY_DELAY_SECONDS * attempt)

    raise RuntimeError("Unable to fetch forecast")


# ============================================================
# 8. VALIDATE API RESPONSE
# ============================================================

def validate_forecast(data):

    if not isinstance(data, list):
        data = [data]

    if not data:
        raise ValueError("API returned empty forecast list")

    for location_index, forecast in enumerate(data):

        if not isinstance(forecast, dict):
            raise ValueError(
                f"Invalid forecast format at location {location_index}"
            )

        if "hourly" not in forecast:
            raise ValueError(
                f"Hourly data missing at location {location_index}"
            )

        hourly = forecast["hourly"]

        if "time" not in hourly:
            raise ValueError(
                f"Hourly time field missing at location {location_index}"
            )

        times = hourly["time"]

        if not times:
            raise ValueError(
                f"Empty forecast at location {location_index}"
            )

        missing_variables = []
        invalid_variables = []

        for variable in HOURLY_VARIABLES:

            values = hourly.get(variable)

            if values is None:
                missing_variables.append(variable)
                continue

            if len(values) != len(times):
                invalid_variables.append(variable)

        if missing_variables or invalid_variables:
            raise ValueError(
                f"Location {location_index}: "
                f"Missing variables: {missing_variables}; "
                f"Invalid lengths: {invalid_variables}"
            )

        logger.info(
            "Location %s validated: lat=%s, lon=%s, timestamps=%s",
            location_index + 1,
            forecast.get("latitude"),
            forecast.get("longitude"),
            len(times)
        )

    logger.info(
        "Forecast validation successful for all %s locations.",
        len(data)
    )


# ============================================================
# 9. ANALYZE FORECAST TIME INTERVALS
# ============================================================

def analyze_time_intervals(data):

    if not isinstance(data, list):
        data = [data]

    for location_index, forecast in enumerate(data):

        times = forecast["hourly"]["time"]

        parsed_times = [
            datetime.fromisoformat(t)
            for t in times
        ]

        intervals = Counter(
            int(
                (
                    parsed_times[i] -
                    parsed_times[i - 1]
                ).total_seconds() / 3600
            )
            for i in range(1, len(parsed_times))
        )

        logger.info(
            "Forecast time intervals for location %s (lat=%s, lon=%s):",
            location_index + 1,
            forecast.get("latitude"),
            forecast.get("longitude"),
        )

        for hours, count in sorted(intervals.items()):

            logger.info(
                "%s hour(s): %s intervals",
                hours,
                count,
            )

# ============================================================
# 10. SAVE FORECAST TO JSON
# ============================================================

def save_forecast(run_info, data):

    run_time = run_info["run_time"]

    filename = (
        run_time.replace(":", "")
        .replace("-", "")
    )

    output_file = (
        OUTPUT_DIR /
        f"{MODEL}_{filename}.json"
    )

    report = {
        "model": MODEL,

        "run_time": run_info["run_time"],

        "run_time_utc": (
            run_info["run_time_utc"].isoformat()
        ),

        "api_available_at_utc": (
            run_info["api_available_at_utc"].isoformat()
        ),

        "metadata_modification_time_utc": (
            run_info["modification_time_utc"].isoformat()
        ),

        "first_seen_at_utc": utc_now().isoformat(),

        "latitude": LATITUDE,
        "longitude": LONGITUDE,

        "metadata": run_info["metadata"],

        "forecast": data,
    }

    output_file.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Forecast saved to: %s",
        output_file.resolve(),
    )


# ============================================================
# 11. MAIN
# ============================================================

def main():

    logger.info("Starting ECMWF IFS collector")

    # Step 1: Get latest model run
    run_info = get_latest_run_metadata()

    # Step 2: Check availability
    check_run_availability(run_info)

    # Step 3: Download forecast
    north, west, south, east = (50.2, 19.7, 49.9, 20.2)
    bounding_box = (south, west, north, east)

    data = fetch_forecast(
            run_time=run_info["run_time"],
            bounding_box=bounding_box
        )

    # data = fetch_forecast(
    #     run_time=run_info["run_time"],
    #     latitude=LATITUDE,
    #     longitude=LONGITUDE,
    # )

    print("Forecast data, raw JSON:")
    print(json.dumps(data, indent=2, ensure_ascii=False))

    # Step 4: Validate response
    validate_forecast(data)

    # Step 5: Analyze time intervals
    analyze_time_intervals(data)

    # Step 6: Save result
    save_forecast(
        run_info=run_info,
        data=data,
    )

    logger.info(
        "ECMWF IFS collection completed successfully."
    )

def filter_forecasts_by_bbox(
    forecasts,
    min_lat,
    min_lon,
    max_lat,
    max_lon,
):
    if isinstance(forecasts, dict):
        forecasts = [forecasts]

    filtered_forecasts = []

    for forecast in forecasts:

        lat = forecast["latitude"]
        lon = forecast["longitude"]

        if (
            min_lat <= lat <= max_lat
            and min_lon <= lon <= max_lon
        ):
            filtered_forecasts.append(forecast)

    logger.info(
        "Forecast filtering: %s/%s locations inside bounding box",
        len(filtered_forecasts),
        len(forecasts),
    )

    return filtered_forecasts

def test_single_run_bounding_box():
    north, west, south, east = (50.2, 19.7, 49.9, 20.2)
    bounding_box = (south, west, north, east)
    run_time = "2026-11-04T06:00"

    data = fetch_forecast(
        run_time,
        latitude=None,
        longitude=None,
        bounding_box=bounding_box,
        hourly_variables=["temperature_2m"],
        forecast_days=1,
    )

    filtered_data = filter_forecasts_by_bbox(
        forecasts=data,
        min_lat=49.9,
        min_lon=19.7,
        max_lat=50.2,
        max_lon=20.2,
    )

    if not isinstance(filtered_data, list):
        raise ValueError(
            "Expected a list of forecast responses for a bounding-box "
            f"request, got {type(data).__name__}"
        )

    unique_response_coordinates = set()
    for location_data in filtered_data:
        if "latitude" not in location_data or "longitude" not in location_data:
            raise ValueError(
                "Latitude or longitude missing in bounding-box response"
            )
        unique_response_coordinates.add(
            (
                location_data["latitude"],
                location_data["longitude"],
            )
        )

    result = {
        "bounding_box": list(bounding_box),
        "location_count": len(filtered_data),
        "unique_response_coordinate_count": len(unique_response_coordinates),
        "coordinates": [
            {
                "latitude": response_latitude,
                "longitude": response_longitude,
            }
            for response_latitude, response_longitude
            in sorted(unique_response_coordinates)
        ],
    }

    logger.info(
        "Bounding-box request results:\n%s",
        json.dumps(result, indent=2),
    )


if __name__ == "__main__":

    try:
        #main()
        test_single_run_bounding_box()

    except Exception:

        logger.exception(
            "ECMWF IFS collection failed."
        )

        raise


# Bounding-box request results:
# {
#   "bounding_box": [
#     49.9,
#     19.7,
#     50.2,
#     20.2
#   ],
#   "location_count": 16,
#   "unique_response_coordinate_count": 16,
#   "coordinates": [
#     {
#       "latitude": 49.947273,
#       "longitude": 19.756098
#     },
#     {
#       "latitude": 49.947273,
#       "longitude": 19.912893
#     },
#     {
#       "latitude": 49.947273,
#       "longitude": 20.069687
#     },
#     {
#       "latitude": 50.017574,
#       "longitude": 19.633507
#     },
#     {
#       "latitude": 50.017574,
#       "longitude": 19.790575
#     },
#     {
#       "latitude": 50.017574,
#       "longitude": 19.947643
#     },
#     {
#       "latitude": 50.017574,
#       "longitude": 20.104712
#     },
#     {
#       "latitude": 50.08787,
#       "longitude": 19.667831
#     },
#     {
#       "latitude": 50.08787,
#       "longitude": 19.825174
#     },
#     {
#       "latitude": 50.08787,
#       "longitude": 19.982517
#     },
#     {
#       "latitude": 50.15817,
#       "longitude": 19.702276
#     },
#     {
#       "latitude": 50.15817,
#       "longitude": 19.859894
#     },
#     {
#       "latitude": 50.15817,
#       "longitude": 20.017513
#     },
#     {
#       "latitude": 50.22847,
#       "longitude": 19.736841
#     },
#     {
#       "latitude": 50.22847,
#       "longitude": 19.894735
#     },
#     {
#       "latitude": 50.22847,
#       "longitude": 20.052631
#     }
#   ]
# }