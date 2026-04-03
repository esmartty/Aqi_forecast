import requests
from pathlib import Path
import os
from dotenv import load_dotenv

# Load local .env for development only
env_path = Path(__file__).resolve().parent / ".env_local"
if env_path.exists():
    load_dotenv(env_path)

TOMTOM_TOKEN = os.environ.get("TOMTOM_TOKEN")
if not TOMTOM_TOKEN:
    raise ValueError("TOMTOM_TOKEN is not set in environment variables!")



def json_data(lat, lon):
    URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={TOMTOM_TOKEN}&point={lat},{lon}"
    try:
        response = requests.get(URL.format(TOMTOM_TOKEN = TOMTOM_TOKEN, lat = lat, lon = lon))
        if response.status_code != 200:
            return None
        return response.json()
    except:
        return None



def extract_tomtom_coords(response_json):
    try:
        coords = response_json["flowSegmentData"]["coordinates"]["coordinate"]
        return [[c["latitude"], c["longitude"]] for c in coords]
    except:
        return None


if __name__ == "__main__":
    data = json_data(50.0977186, 20.0221769)

    coords_array = extract_tomtom_coords(data)

    print("[")
    for c in coords_array:
        print(f"  [{c[0]}, {c[1]}],")
    print("]")
