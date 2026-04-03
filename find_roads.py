import requests

# -------------------------------
# Overpass
# -------------------------------
def get_roads(lat, lon, radius=1000):
    query = f"""
    [out:json];
    way(around:{radius},{lat},{lon})
      ["highway"~"motorway|trunk|primary|secondary|tertiary|residential"];
    out geom;
    """

    url = "https://overpass-api.de/api/interpreter"

    try:
        r = requests.post(url, data={"data": query}, timeout=30)
        return r.json()["elements"]
    except:
        return []


if __name__ == "__main__":
    lat = 50.099361
    lon = 20.018317
    print(get_roads(lat, lon))