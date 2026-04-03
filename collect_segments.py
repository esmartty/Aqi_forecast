import math
import time
import json
from find_roads import get_roads
from traffic import json_data, extract_tomtom_coords

def point_distance_m(lat1, lon1, lat2, lon2):
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return 6371000 * c


def segment_key(coords):
    return tuple((round(lat, 5), round(lon, 5)) for lat, lon in coords)


def pillar_key(lat, lon):
    return f"{lat:.6f},{lon:.6f}"


def is_duplicate_segment(coords, seen_keys):
    if not coords or len(coords) == 0:
        return True

    key = segment_key(coords)
    if key in seen_keys:
        return True

    seen_keys.add(key)
    return False


def collect_segment_for_point(lat, lon):
    resp = json_data(lat, lon)
    if not resp:
        return None

    coords = extract_tomtom_coords(resp)
    if not coords:
        return None

    return coords


def collect_segments(lat, lon):
    roads = get_roads(lat, lon)

    seen_ways = set()
    seen_segments = set()

    result_roads = {}

    for way in roads:
        way_id = way.get("id")

        if way_id in seen_ways:
            continue
        seen_ways.add(way_id)

        geometry = way.get("geometry")
        if not geometry or len(geometry) < 3:
            continue

        sample_points = [
            geometry[0],
            geometry[len(geometry) // 2],
            geometry[-1]
        ]

        candidates = []
        for point in sample_points:
            if not point:
                continue
            lat_p = point.get("lat")
            lon_p = point.get("lon")
            if lat_p is None or lon_p is None:
                continue

            dist = point_distance_m(lat, lon, lat_p, lon_p)
            if dist <= 1000:
                candidates.append((dist, point))

        if not candidates:
            continue

        _, point = min(candidates, key=lambda item: item[0])
        lat_p = point.get("lat")
        lon_p = point.get("lon")

        coords = collect_segment_for_point(lat_p, lon_p)
        if not coords:
            continue

        if is_duplicate_segment(coords, seen_segments):
            continue

        result_roads[pillar_key(lat_p, lon_p)] = coords

        time.sleep(0.15)

    return result_roads


if __name__ == "__main__":
    lat = 49.971047
    lon = 19.830422

    roads = collect_segments(lat, lon)

    print(f"Was found unique road entries: {len(roads)}")
    print(json.dumps(roads, indent=2))