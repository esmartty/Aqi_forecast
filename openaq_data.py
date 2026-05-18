import requests
from pathlib import Path
import os
from dotenv import load_dotenv

# Load local .env for development only
env_path = Path(__file__).resolve().parent / ".env_local"
if env_path.exists():
    load_dotenv(env_path)

OPENAQ_TOKEN = os.environ.get("OPENAQ_TOKEN")
if not OPENAQ_TOKEN:
    raise ValueError("OPENAQ_TOKEN is not set in environment variables!")

def json_location_data():
    headers  = {
        "X-API-Key": OPENAQ_TOKEN           
        }
    response = requests.get('https://api.openaq.org/v3/locations?bbox=19.70,49.90,20.20,50.20', headers  = headers )
    #https://docs.openaq.org/using-the-api/geospatial
    #bbox=minLongitude,minLatitude,maxLongitude,maxLatitude (bbox=west,south,east,north)
    #for Krakow:
    # North: 50.20
    # South: 49.90
    # West: 19.70
    # East: 20.20

    response_json = response.json()
    return response_json


def json_parameter_data(limit=100, page=1):
    headers = {
        "X-API-Key": OPENAQ_TOKEN
    }
    params = {
        "limit": limit,
        "page": page,
    }
    response = requests.get(
        'https://api.openaq.org/v3/parameters',
        headers=headers,
        params=params,
    )
    response.raise_for_status()
    return response.json()


def json_sensor_hours(sensor_id, date_from=None, date_to=None, limit=1000, page=1):
    headers = {
        "X-API-Key": OPENAQ_TOKEN
    }
    params = {
        "limit": limit,
        "page": page,
    }
    if date_from:
        params["date_from"] = date_from
    if date_to:
        params["date_to"] = date_to

    response = requests.get(
        f"https://api.openaq.org/v3/sensors/{sensor_id}/hours",
        headers=headers,
        params=params,
    )
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    data = json_location_data()
    print(data)