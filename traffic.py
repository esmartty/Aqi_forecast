import requests
from pathlib import Path
import os
from dotenv import load_dotenv
from parser import parse_numeric

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


def parse_traffic_metrics(response_json):
    """
    Extract traffic metrics from TomTom API response.
    
    Returns dict with:
    - current_speed (int, km/h, nullable)
    - free_flow_speed (int, km/h, nullable)
    - current_travel_time (int, seconds, nullable)
    - free_flow_travel_time (int, seconds, nullable)
    - road_closure (bool, nullable)
    - functional_road_class (str, nullable)
    - confidence (float 0-1, nullable)
    - coordinates (list of [lat, lon], nullable)
    
    Returns None if response is invalid or missing flowSegmentData.
    """
    if not response_json:
        return None
    
    try:
        flow_data = response_json.get("flowSegmentData", {})
        
        # Extract metrics using parse_numeric for safe type conversion
        current_speed = parse_numeric(flow_data.get("currentSpeed"), int)
        free_flow_speed = parse_numeric(flow_data.get("freeFlowSpeed"), int)
        current_travel_time = parse_numeric(flow_data.get("currentTravelTime"), int)
        free_flow_travel_time = parse_numeric(flow_data.get("freeFlowTravelTime"), int)
        confidence = parse_numeric(flow_data.get("confidence"), float)
        
        road_closure = flow_data.get("roadClosure")
        if road_closure is not None:
            road_closure = bool(road_closure)
        
        functional_road_class = flow_data.get("frc")
        
        # Extract coordinates
        coordinates = None
        try:
            coords_list = flow_data.get("coordinates", {}).get("coordinate", [])
            if coords_list:
                coordinates = [[float(c["latitude"]), float(c["longitude"])] for c in coords_list]
        except:
            pass
        
        return {
            "current_speed": current_speed,
            "free_flow_speed": free_flow_speed,
            "current_travel_time": current_travel_time,
            "free_flow_travel_time": free_flow_travel_time,
            "road_closure": road_closure,
            "functional_road_class": functional_road_class,
            "confidence": confidence,
            "coordinates": coordinates,
        }
    except Exception:
        return None


if __name__ == "__main__":
    data = json_data(50.0977186, 20.0221769)

    coords_array = extract_tomtom_coords(data)

    print("[")
    for c in coords_array:
        print(f"  [{c[0]}, {c[1]}],")
    print("]")
