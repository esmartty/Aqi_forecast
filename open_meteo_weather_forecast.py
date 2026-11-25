import json
import time
import logging
import requests

from datetime import datetime, timezone


# ============================================================
# 1. CONFIGURATION
# ============================================================

MODEL = "ecmwf_ifs"

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
NORTH, WEST, SOUTH, EAST = (50.2, 19.7, 49.9, 20.2)



# ============================================================
# 2. WEATHER VARIABLES
# ============================================================

HOURLY_VARIABLES = {
    "temperature_2m": "°C",
    "relative_humidity_2m": "%",
    "dew_point_2m": "°C",
    "precipitation": "mm",
    "rain": "mm",
    "showers": "mm",
    "snowfall": "cm",
    "wind_speed_10m": "km/h",
    "wind_direction_10m": "°",
    "wind_gusts_10m": "km/h",
    "cloud_cover": "%",
    "cloud_cover_low": "%",
    "cloud_cover_mid": "%",
    "cloud_cover_high": "%",
    "pressure_msl": "hPa",
    "visibility": "m",
    "boundary_layer_height": "m",
    "shortwave_radiation": "W/m²",
    "direct_radiation": "W/m²",
    "diffuse_radiation": "W/m²",
    "cape": "J/kg",
    "convective_inhibition": "J/kg",
}


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

def get_latest_run_metadata(model=MODEL):

    logger.info(f"Requesting {model} metadata...")

    response = requests.get(
        "https://api.open-meteo.com/data/"
            f"{model}/static/meta.json",
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
        "model": model,
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
# 7. FILTER FORECASTS BY BOUNDING BOX
# ============================================================

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

# ============================================================
# 9. FETCH SINGLE RUN FORECAST
# ============================================================

def fetch_forecast(
    run_time,
    model=MODEL,
    latitude=None,
    longitude=None,
    bounding_box=None,
    hourly_variables=None,
    forecast_days=FORECAST_DAYS,
):

    params = {
        "models": model,
        "run": run_time,
        "hourly": ",".join(
            HOURLY_VARIABLES.keys()
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
            #print("Requested URL:", response.request.url)

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
# 10. VALIDATE API RESPONSE
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
# 11. MAIN
# ============================================================

def fetch_last_forecast_weather_model_update():

    logger.info("Starting ECMWF IFS collector")

    run_info = get_latest_run_metadata()

    check_run_availability(run_info)

    print("Last model update, raw JSON:")
    print(json.dumps(run_info, indent=2, ensure_ascii=False))

    
    return run_info

def get_forecast_data(run_time):

    bounding_box = (SOUTH, WEST, NORTH, EAST)

    data = fetch_forecast(
            run_time=run_time,
            bounding_box=bounding_box
        )

    filtered_data = filter_forecasts_by_bbox(
            forecasts=data,
            min_lat=SOUTH,
            min_lon=WEST,
            max_lat=NORTH,
            max_lon=EAST,
        )

    #print("Forecast data, raw JSON:")
    #print(json.dumps(filtered_data, indent=2, ensure_ascii=False))

    validate_forecast(filtered_data)

    logger.info(
        "ECMWF IFS collection completed successfully."
    )
    return filtered_data


if __name__ == "__main__":

    try:
        run_time = fetch_last_forecast_weather_model_update()["run_time"]
        get_forecast_data(run_time=run_time)

    except Exception:

        logger.exception(
            "ECMWF IFS collection failed."
        )

        raise

