from pathlib import Path

from flask import Flask, jsonify, abort, request
from sqlalchemy import create_engine, text

from collect_segments import collect_segments, collect_segment_for_point
from db_config import get_database_url

BASE_DIR = Path(__file__).resolve().parent
app = Flask(__name__, static_folder=str(BASE_DIR / "static"), static_url_path="")

engine = create_engine(get_database_url(), future=True)

@app.route("/")
def index():
    return app.send_static_file("stations.html")

@app.route("/api/stations")
def api_stations():
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT station_id, station_name, latitude, longitude "
                "FROM public.stations "
                "ORDER BY station_name NULLS LAST, station_id"
            )
        )
        stations = []
        for row in result:
            stations.append(
                {
                    "station_id": str(row.station_id),
                    "station_name": row.station_name or "",
                    "latitude": float(row.latitude) if row.latitude is not None else None,
                    "longitude": float(row.longitude) if row.longitude is not None else None,
                }
            )
    return jsonify(stations)

@app.route("/api/station/<station_id>")
def api_station(station_id):
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT station_id, station_name, latitude, longitude "
                "FROM public.stations "
                "WHERE station_id::text = :station_id"
            ),
            {"station_id": station_id},
        )
        row = result.fetchone()
        if row is None:
            abort(404, description=f"Station {station_id} not found")
        station = {
            "station_id": str(row.station_id),
            "station_name": row.station_name or "",
            "latitude": float(row.latitude) if row.latitude is not None else None,
            "longitude": float(row.longitude) if row.longitude is not None else None,
        }
    return jsonify(station)

@app.route("/api/station/<station_id>/roads")
def api_station_roads(station_id):
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT latitude, longitude "
                "FROM public.stations "
                "WHERE station_id::text = :station_id"
            ),
            {"station_id": station_id},
        )
        row = result.fetchone()
        if row is None:
            abort(404, description=f"Station {station_id} not found")
        if row.latitude is None or row.longitude is None:
            return jsonify({"error": "Station has no coordinates for road search"}), 400

        latitude = float(row.latitude)
        longitude = float(row.longitude)

    try:
        roads = collect_segments(latitude, longitude)
    except Exception as exc:
        return jsonify({"error": "Unable to collect road segments", "details": str(exc)}), 502

    return jsonify({"roads": roads})


@app.route("/api/road_segment")
def api_road_segment():
    lat = request.args.get("lat")
    lon = request.args.get("lon")
    if lat is None or lon is None:
        return jsonify({"error": "Query parameters lat and lon are required"}), 400

    try:
        latitude = float(lat)
        longitude = float(lon)
    except ValueError:
        return jsonify({"error": "Invalid latitude or longitude"}), 400

    try:
        segment = collect_segment_for_point(latitude, longitude)
    except Exception as exc:
        return jsonify({"error": "Unable to fetch road segment", "details": str(exc)}), 502

    if not segment:
        return jsonify({"error": "No road segment found for provided coordinates"}), 404

    return jsonify({"segment": segment})


@app.route("/api/station/<station_id>/save-pillars", methods=["POST"])
def api_save_pillars(station_id):
    # Validate station exists
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT station_id FROM public.stations WHERE station_id::text = :station_id"),
            {"station_id": station_id},
        )
        if result.fetchone() is None:
            abort(404, description=f"Station {station_id} not found")

    # Parse request data
    data = request.get_json()
    if not data or "pillars" not in data:
        return jsonify({"error": "Request must include 'pillars' array"}), 400

    pillars = data["pillars"]
    if not isinstance(pillars, list) or len(pillars) == 0:
        return jsonify({"error": "'pillars' must be a non-empty array"}), 400

    saved_pillars = []

    try:
        with engine.begin() as conn:
            for pillar in pillars:
                lat = pillar.get("lat")
                lon = pillar.get("lon")
                segment_coords = pillar.get("segment_coordinates")

                if lat is None or lon is None or segment_coords is None:
                    continue

                # Insert or update pillar point
                pillar_sql = text("""
                    INSERT INTO public.pillar_points (latitude, longitude, geom, segment_data, date_update)
                    VALUES (:lat, :lon, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326), :segment_data, NOW())
                    ON CONFLICT (latitude, longitude) DO UPDATE SET
                        segment_data = EXCLUDED.segment_data,
                        date_update = NOW()
                    RETURNING id
                """)

                result = conn.execute(pillar_sql, {
                    "lat": lat,
                    "lon": lon,
                    "segment_data": segment_coords
                })
                pillar_id = result.fetchone()[0]

                # Insert station-pillar relation (ignore if already exists)
                relation_sql = text("""
                    INSERT INTO public.station_pillar_relations (station_id, pillar_point_id, date_update)
                    VALUES (:station_id, :pillar_id, NOW())
                    ON CONFLICT (station_id, pillar_point_id) DO NOTHING
                """)

                conn.execute(relation_sql, {
                    "station_id": int(station_id),
                    "pillar_id": pillar_id
                })

                saved_pillars.append({"id": pillar_id, "lat": lat, "lon": lon})

    except Exception as exc:
        return jsonify({"error": f"Failed to save pillars: {str(exc)}"}), 500

    return jsonify({
        "message": f"Saved {len(saved_pillars)} pillar points",
        "pillars": saved_pillars
    })


@app.errorhandler(404)
def handle_not_found(error):
    if request.path.startswith("/api/"):
        return jsonify({"error": error.description or "Not found"}), 404
    return error

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=True)
