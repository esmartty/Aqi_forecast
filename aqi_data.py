import requests
from pathlib import Path
import os
from dotenv import load_dotenv

# Load local .env for development only
env_path = Path(__file__).resolve().parent / ".env_local"
if env_path.exists():
    load_dotenv(env_path)

AQI_TOKEN = os.environ.get("AQI_TOKEN")
if not AQI_TOKEN:
    raise ValueError("AQI_TOKEN is not set in environment variables!")

def json_data(station_id):
    payload = {
        'token': AQI_TOKEN
        }
    response = requests.get('https://api.waqi.info/feed/@{station_id}'.format(station_id = station_id), params = payload)
    response_json = response.json()
    return response_json

def get_from_api(station_id_list):
    answer_array = []
    for station_id in station_id_list:
        answer = json_data(station_id)
        answer_data = answer.get("data", {})
        if answer_data != {}:
            answer_array.append(answer_data)
    return answer_array
