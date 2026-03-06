import os
import requests
import json
from dotenv import load_dotenv
from pathlib import Path

min_lat = 49.993129  # min latitude
max_lat = 50.111727  # max latitude
min_lng = 19.790600  # min longitude
max_lng = 20.085858  #  max longitude

# Load local .env for development only
env_path = Path(__file__).resolve().parent / ".env_local"
if env_path.exists():
    load_dotenv(env_path)

AQI_TOKEN = os.environ.get("AQI_TOKEN")
if not AQI_TOKEN:
    raise ValueError("AQI_TOKEN is not set in environment variables!")

step_lat = 0.01  # step by lat
step_lng = 0.01  # step by lng

def add_unique(value, unique_list):
    """
    Adds a value to the list only if it's not already present.
    
    :param value: Value to add to the list.
    :param unique_list: List to which the value is added.
    """
    if value not in unique_list:
        unique_list.append(value)

results = []
current_lat = min_lat
while current_lat <= max_lat:
    current_lng = min_lng
    while current_lng <= max_lng:
        url = f"/feed/geo:{current_lat};{current_lng}/?token={AQI_TOKEN}"
        print(f"Request for: {url}")
        
        response = requests.get(f"https://api.waqi.info{url}")
        if response.status_code == 200:
            add_unique(response.json(), results)

        current_lng += step_lng
    current_lat += step_lat

pretty_json = json.dumps(results, indent=4)
print(pretty_json)