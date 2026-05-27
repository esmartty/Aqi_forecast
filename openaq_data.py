import requests
from pathlib import Path
import os
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

# singleton session with retry/backoff configuration
_session = None
def get_session():
    global _session
    if _session is None:
        session = requests.Session()
        retries = Retry(
            total=5,
            connect=5,
            read=5,
            status=5,
            backoff_factor=1,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        _session = session
    return _session


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
    session = get_session()
    resp = session.get(
        'https://api.openaq.org/v3/locations?bbox=19.70,49.90,20.20,50.20',
        headers=headers,
        timeout=_timeout_value(timeout),
    )
    resp.raise_for_status()
    return resp.json()


def json_parameter_data(limit=100, page=1, timeout=None):
    headers = {"X-API-Key": OPENAQ_TOKEN}
    params = {"limit": limit, "page": page}
    session = get_session()
    resp = session.get(
        'https://api.openaq.org/v3/parameters',
        headers=headers,
        params=params,
        timeout=_timeout_value(timeout),
    )
    resp.raise_for_status()
    return resp.json()


def json_sensor_hours(sensor_id, date_from=None, date_to=None, limit=100, page=1, timeout=None):
    headers = {"X-API-Key": OPENAQ_TOKEN}
    params = {"limit": limit, "page": page}
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    session = get_session()
    resp = session.get(
        f"https://api.openaq.org/v3/sensors/{sensor_id}/hours",
        headers=headers,
        params=params,
        timeout=_timeout_value(timeout),
    )
    resp.raise_for_status()
    return resp.json()


if __name__ == "__main__":
    #data = json_location_data()
    data = json_sensor_hours(sensor_id=36489, date_from='2026-01-01', date_to='2026-01-02')
    print(data)