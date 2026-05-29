import datetime
import logging
from sqlalchemy import text
from parser import parse_utc, _json_value
from openaq_sensor_data_service import fetch_openaq_sensor_ids_by_location

import openaq_data

logger = logging.getLogger(__name__)


def insert_openaq_sensor_data(connection, sensor_id):
    """Fetch OpenAQ sensor metadata and insert/update public.openaq_sensor_data."""
    
    sensor_ids = []
    
    if sensor_id is None:
        sensor_ids = fetch_openaq_sensor_ids_by_location(connection)
        print(f"Fetched {len(sensor_ids)} sensor IDs from database for sensor data ingestion")
    else:
        sensor_ids = [sensor_id] 

    if not sensor_ids:
        logger.info("No sensor IDs found in openaq_locations table for sensor data ingestion")
        return

    sql = text(
            """INSERT INTO public.openaq_sensor_data (
                sensor_id,
                parameter_id,
                datetimeFirst_utc,
                datetimeLast_utc,
                coverage,
                latestdatetime_utc,
                latestValue,
                latestCoordinates_latitude,
                latestCoordinates_longitude,
                summary,
                raw_data,
                date_update
            ) VALUES (
                :sensor_id,
                :parameter_id,
                :datetimeFirst_utc,
                :datetimeLast_utc,
                :coverage,
                :latestdatetime_utc,
                :latestValue,
                :latestCoordinates_latitude,
                :latestCoordinates_longitude,
                :summary,
                :raw_data,
                :date_update
            )
            ON CONFLICT (sensor_id)
            DO UPDATE SET
                parameter_id = EXCLUDED.parameter_id,
                datetimeFirst_utc = EXCLUDED.datetimeFirst_utc,
                datetimeLast_utc = EXCLUDED.datetimeLast_utc,
                coverage = EXCLUDED.coverage,
                latestdatetime_utc = EXCLUDED.latestdatetime_utc,
                latestValue = EXCLUDED.latestValue,
                latestCoordinates_latitude = EXCLUDED.latestCoordinates_latitude,
                latestCoordinates_longitude = EXCLUDED.latestCoordinates_longitude,
                summary = EXCLUDED.summary,
                raw_data = EXCLUDED.raw_data,
                date_update = EXCLUDED.date_update
            """)

    for sensor_id in sensor_ids:
        logger.info(f"Starting sensor data ingestion for sensor {sensor_id}")
        try:
            response = openaq_data.json_sensor_data(sensor_id, timeout=None)
        except Exception as e:
            logger.exception(f"Error fetching OpenAQ sensor data for sensor {sensor_id}: {e}")
            continue

        results = response.get("results") if isinstance(response, dict) else None

        if not results:
            logger.warning(f"No results for sensor {sensor_id} found in OpenAQ sensor response")
            continue

        
        for sensor in results:
            parameter_id = sensor.get("parameter", {}).get("id") if sensor.get("parameter") else None
            datetimeFirst_utc = parse_utc(sensor.get("datetimeFirst", {}).get("utc")) if sensor.get("datetimeFirst") else None
            datetimeLast_utc = parse_utc(sensor.get("datetimeLast", {}).get("utc")) if sensor.get("datetimeLast") else None
            coverage = sensor.get("coverage") or {}
            latestdatetime_utc = parse_utc(sensor.get("latest", {}).get("datetime", {}).get("utc")) if sensor.get("latest") else None
            latestValue = sensor.get("latest", {}).get("value") if sensor.get("latest") else None
            if latestValue is not None:
                    try:
                        latestValue = float(latestValue)
                    except (TypeError, ValueError):
                        pass
            latestCoordinates_latitude = sensor.get("latest", {}).get("coordinates", {}).get("latitude") if sensor.get("latest") else None
            latestCoordinates_longitude = sensor.get("latest", {}).get("coordinates", {}).get("longitude") if sensor.get("latest") else None
            summary = sensor.get("summary") or {}
        
        try:
            connection.execute(
                sql,
                {
                    "sensor_id": sensor_id,
                    "parameter_id": parameter_id,
                    "datetimeFirst_utc": datetimeFirst_utc,
                    "datetimeLast_utc": datetimeLast_utc,
                    "coverage": _json_value(coverage),
                    "latestdatetime_utc": latestdatetime_utc,
                    "latestValue": latestValue,
                    "latestCoordinates_latitude": latestCoordinates_latitude,
                    "latestCoordinates_longitude": latestCoordinates_longitude,
                    "summary": _json_value(summary),
                    "raw_data": _json_value(sensor),
                    "date_update": datetime.datetime.now(),
                },
            )
        except Exception as e:
            logger.exception(
                f"Error inserting sensor_id {sensor_id}: {e}"
            )

        logger.info(
                f"Committed sensor {sensor_id} data to database successfully"
            )

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    from sqlalchemy import create_engine
    from db_config import get_database_url

    engine = create_engine(get_database_url())
    with engine.begin() as connection:
        #sensor_id = 34870
        sensor_id = None
        insert_openaq_sensor_data(connection, sensor_id = sensor_id)