import requests
from pathlib import Path
import os
from dotenv import load_dotenv
import logging
import time
from openaq_exceptions import SensorHoursIngestionError
from parser import _normalize_datetime_param
from rate_limiter import RateLimiter
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry

# Load local .env for development only
env_path = Path(__file__).resolve().parent / ".env_local"
if env_path.exists():
    load_dotenv(env_path)

OPENAQ_TOKEN = os.environ.get("OPENAQ_TOKEN")
if not OPENAQ_TOKEN:
    raise ValueError("OPENAQ_TOKEN is not set in environment variables!")

# default timeout for requests (seconds)
OPENAQ_TIMEOUT = int(os.environ.get("OPENAQ_TIMEOUT", "30"))


def _timeout_value(timeout):
    if timeout is None:
        timeout = OPENAQ_TIMEOUT
    return (5, timeout) if isinstance(timeout, (int, float)) else timeout


RATE_LIMITER = RateLimiter()


def rate_limited_get(*args, **kwargs):
    RATE_LIMITER.wait()
    resp = requests.get(*args, **kwargs)
    RATE_LIMITER.observe_headers(resp.headers)
    return resp


# # singleton session with retry/backoff configuration
# _session = None
# def get_session():
#     global _session
#     if _session is None:
#         session = requests.Session()
#         _session = session
#     return _session


def json_location_data(timeout=None):
    """
    Fetch air quality locations within Kraków bounding box.
    
    Uses the OpenAQ v3 API to retrieve all monitoring stations/sensors
    within the Kraków region (bbox: west=19.70, south=49.90, east=20.20, north=50.20).
    
    Reference: https://docs.openaq.org/using-the-api/geospatial
    bbox format: minLongitude,minLatitude,maxLongitude,maxLatitude (west,south,east,north)
    
    Args:
        timeout (float, optional): Request timeout in seconds. 
                                  Defaults to OPENAQ_TIMEOUT if not specified.
    
    Returns:
        dict: JSON response from OpenAQ API containing location/station data.
              Raises requests.exceptions.HTTPError on HTTP errors.
    """
    headers = {"X-API-Key": OPENAQ_TOKEN}
    # session = get_session()
    resp = rate_limited_get(
        'https://api.openaq.org/v3/locations?bbox=19.70,49.90,20.20,50.20',
        headers=headers,
        timeout=_timeout_value(timeout),
    )
    resp.raise_for_status()
    return resp.json()


def json_parameter_data(limit=100, page=1, timeout=None):
    headers = {"X-API-Key": OPENAQ_TOKEN}
    params = {"limit": limit, "page": page}
    # session = get_session()
    resp = rate_limited_get(
        'https://api.openaq.org/v3/parameters',
        headers=headers,
        params=params,
        timeout=_timeout_value(timeout),
    )
    resp.raise_for_status()
    return resp.json()


def json_sensor_hours(sensor_id, datetime_from=None, datetime_to=None, limit=100, page=1, timeout=None):
    headers = {"X-API-Key": OPENAQ_TOKEN}
    params = {"limit": limit, "page": page}
    if datetime_from:
        params["datetime_from"] = _normalize_datetime_param(datetime_from)
    if datetime_to:
        params["datetime_to"] = _normalize_datetime_param(datetime_to)

    logger = logging.getLogger(__name__)
    # session = get_session()
    max_attempts = 5
    backoff = 1
    for attempt in range(1, max_attempts + 1):
        try:
            resp = rate_limited_get(
                f"https://api.openaq.org/v3/sensors/{sensor_id}/hours",
                headers=headers,
                params=params,
                timeout=_timeout_value(timeout),
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            
            if status == 429:
                RATE_LIMITER.observe_headers(e.response.headers)
                RATE_LIMITER.wait()
                time.sleep(0.5)
                continue

            if status == 408:
                raise SensorHoursIngestionError(sensor_id=sensor_id,
                datetime_from=datetime_from,
                datetime_to=datetime_to,
                status=408,
                reason="window_too_heavy"
            ) from e

            if status in (500, 502, 503, 504) and attempt < max_attempts:
                logger.warning(
                    "Attempt %d/%d: HTTP %s for sensor %s page %s; retrying after %s seconds",
                    attempt,
                    max_attempts,
                    status,
                    sensor_id,
                    params.get("page"),
                    backoff,
                )
                time.sleep(backoff)
                backoff *= 2
                continue

            logger.exception(
                "HTTP error fetching OpenAQ sensor hours for sensor %s, page %s: %s",
                sensor_id,
                params.get("page"),
                e,
            )
            raise
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < max_attempts:
                logger.warning(
                    "Attempt %d/%d: %s for sensor %s page %s; retrying after %s seconds",
                    attempt,
                    max_attempts,
                    type(e).__name__,
                    sensor_id,
                    params.get("page"),
                    backoff,
                )
                time.sleep(backoff)
                backoff *= 2
                continue
            logger.exception(
                "Network error fetching OpenAQ sensor hours for sensor %s, page %s: %s",
                sensor_id,
                params.get("page"),
                e,
            )
            raise

def json_sensor_data(sensor_id, timeout=None):
    logger = logging.getLogger(__name__)
    headers = {"X-API-Key": OPENAQ_TOKEN}
    # session = get_session()
    max_attempts = 3
    backoff = 1
    for attempt in range(1, max_attempts + 1):
        try:
            resp = rate_limited_get(
                f"https://api.openaq.org/v3/sensors/{sensor_id}",
                headers=headers,
                timeout=_timeout_value(timeout),
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            if attempt < max_attempts:
                logger.warning(
                    "Attempt %d/%d: %s for sensor %s; retrying after %s seconds",
                    attempt,
                    max_attempts,
                    type(e).__name__,
                    sensor_id,
                    backoff,
                )
                time.sleep(backoff)
                backoff *= 2
                continue
            logger.exception("Error fetching OpenAQ sensor data for sensor %s: %s", sensor_id, e)
            raise


if __name__ == "__main__":
    #data = json_location_data()
    data, headers = json_sensor_hours(sensor_id=17947, datetime_from='2019-01-14', datetime_to='2019-01-15')
    print(data)