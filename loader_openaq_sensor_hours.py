import datetime
import logging
from sqlalchemy import text
from parser import parse_utc, _json_value

import openaq_data
from openaq_sensor_data_service import ( fetch_openaq_sensor_ids, fetch_datetime_range_for_sensor_id,)

logger = logging.getLogger(__name__)


def insert_openaq_sensor_hours(connection, limit=100):
    
    sensor_ids = fetch_openaq_sensor_ids(connection)
    #sensor_ids = [28912, ]
    print(f"Fetched {len(sensor_ids)} sensor IDs from database for sensor data ingestion")

    if not sensor_ids:
        logger.info("No sensor IDs found in openaq_sensor_data table for sensor hour data ingestion")
        return


    sql = text(
        """INSERT INTO public.openaq_sensor_hours (
            sensor_id,
            parameter_id,
            timestamp_utc,
            timestamp_to_utc,
            value,
            coordinates_latitude,
            coordinates_longitude,
            flag_info,
            summary,
            coverage,
            raw_data,
            date_update
        ) VALUES (
            :sensor_id,
            :parameter_id,
            :timestamp_utc,
            :timestamp_to_utc,
            :value,
            :coordinates_latitude,
            :coordinates_longitude,
            :flag_info,
            :summary,
            :coverage,
            :raw_data,
            :date_update
        )
        ON CONFLICT (sensor_id, parameter_id, timestamp_utc)
        DO UPDATE SET
            timestamp_to_utc = EXCLUDED.timestamp_to_utc,
            value = EXCLUDED.value,
            coordinates_latitude = EXCLUDED.coordinates_latitude,
            coordinates_longitude = EXCLUDED.coordinates_longitude,
            flag_info = EXCLUDED.flag_info,
            summary = EXCLUDED.summary,
            coverage = EXCLUDED.coverage,
            raw_data = EXCLUDED.raw_data,
            date_update = EXCLUDED.date_update
        """)

    for sensor_id in sensor_ids:
        logger.info(f"Starting hourly ingestion for sensor {sensor_id}")
        page = 1
        datetime_from, datetime_to = fetch_datetime_range_for_sensor_id(connection, sensor_id)

        while True:
            #connection.begin()
            try:
                response = openaq_data.json_sensor_hours(
                    sensor_id,
                    datetime_from=datetime_from,
                    datetime_to=datetime_to,
                    limit=limit,
                    page=page,
                )
            except Exception as e:
                connection.rollback()
                logger.exception(
                    f"Error fetching OpenAQ sensor hours for sensor {sensor_id}, page {page} for {datetime_from} to {datetime_to}: {e}"
                )
                logger.error(
                    f"Sensor {sensor_id}: page {page} for {datetime_from} to {datetime_to} failed after retries. Previous pages remain saved."
                )
                break

            results = response.get("results") if isinstance(response, dict) else None
            if not results:
                if page == 1:
                    logger.info(f"No hourly results returned for sensor {sensor_id}")
                break

            try:

                for record in results:
                    period = record.get("period") or {}
                    dt_from = period.get("datetimeFrom") or {}
                    dt_to = period.get("datetimeTo") or {}
                    timestamp_utc = parse_utc(dt_from.get("utc"))
                    timestamp_to_utc = parse_utc(dt_to.get("utc"))
                    parameter = record.get("parameter") or {}
                    parameter_id = parameter.get("id") if isinstance(parameter, dict) else None
                    value = record.get("value")
                    if value is not None:
                        try:
                            value = float(value)
                        except (TypeError, ValueError):
                            pass

                    coordinates = record.get("coordinates") or {}
                    coordinates_latitude = coordinates.get("latitude")
                    coordinates_longitude = coordinates.get("longitude")
                    flag_info = record.get("flagInfo")
                    summary = record.get("summary")
                    coverage = record.get("coverage")

                    connection.execute(
                        sql,
                        {
                            "sensor_id": sensor_id,
                            "parameter_id": parameter_id,
                            "timestamp_utc": timestamp_utc,
                            "timestamp_to_utc": timestamp_to_utc,
                            "value": value,
                            "coordinates_latitude": coordinates_latitude,
                            "coordinates_longitude": coordinates_longitude,
                            "flag_info": _json_value(flag_info),
                            "summary": _json_value(summary),
                            "coverage": _json_value(coverage),
                            "raw_data": _json_value(record),
                            "date_update": datetime.datetime.now(),
                        },
                    )
                        
            except Exception as e:
                connection.rollback()
                logger.exception(
                    f"Error inserting sensor hours for sensor {sensor_id} at {timestamp_utc}: {e}"
                )
                logger.error(
                    f"Sensor {sensor_id}: insert failed on page {page} at {timestamp_utc}. Previous pages remain saved."
                )
                break
                
            connection.commit()
            logger.info(
                f"Committed sensor {sensor_id} page {page} ({len(results)} records)"
            )
            
            if len(results) < limit:
                break

            page += 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    from sqlalchemy import create_engine
    from db_config import get_database_url

    engine = create_engine(get_database_url())

    with engine.connect() as connection:
        insert_openaq_sensor_hours(connection)
  