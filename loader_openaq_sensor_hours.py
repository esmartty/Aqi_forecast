import datetime
import json
import logging
from sqlalchemy import text

import openaq_data

logger = logging.getLogger(__name__)


def fetch_openaq_sensor_ids(connection):
    sql = text("SELECT sensor_ids FROM public.openaq_locations WHERE sensor_ids IS NOT NULL")
    rows = connection.execute(sql).mappings().all()
    sensor_id_set = set()
    for row in rows:
        ids = row.get("sensor_ids")
        if isinstance(ids, list):
            sensor_id_set.update(int(i) for i in ids if i is not None)
    return sorted(sensor_id_set)


def _json_value(value):
    return json.dumps(value) if isinstance(value, (dict, list)) else value


def insert_openaq_sensor_hours(connection, date_from, date_to, sensor_ids=None, limit=100):
    if sensor_ids is None:
        #sensor_ids = fetch_openaq_sensor_ids(connection)
        sensor_ids = (29320, 34870)
        print(f"Fetched {len(sensor_ids)} sensor IDs from database for hourly ingestion")

    if not sensor_ids:
        logger.info("No sensor IDs found in openaq_locations for hourly ingestion")
        return

    sql = text(
        """INSERT INTO public.openaq_sensor_hours (
            sensor_id,
            parameter_id,
            timestamp_utc,
            timestamp_local,
            timestamp_to_utc,
            timestamp_to_local,
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
            :timestamp_local,
            :timestamp_to_utc,
            :timestamp_to_local,
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
            timestamp_local = EXCLUDED.timestamp_local,
            timestamp_to_utc = EXCLUDED.timestamp_to_utc,
            timestamp_to_local = EXCLUDED.timestamp_to_local,
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

        while True:
            connection.begin()

            try:
                response = openaq_data.json_sensor_hours(
                    sensor_id,
                    date_from=date_from,
                    date_to=date_to,
                    limit=limit,
                    page=page,
                )
            except Exception as e:
                connection.rollback()
                logger.exception(
                    f"Error fetching OpenAQ sensor hours for sensor {sensor_id}, page {page}: {e}"
                )
                logger.error(
                    f"Sensor {sensor_id}: page {page} failed after retries. Previous pages remain saved."
                )
                break

            results = response.get("results") if isinstance(response, dict) else None
            if not results:
                if page == 1:
                    logger.info(f"No hourly results returned for sensor {sensor_id}")
                connection.commit()
                break

            try:
                for record in results:
                    period = record.get("period") or {}
                    datetime_from = period.get("datetimeFrom") or {}
                    datetime_to = period.get("datetimeTo") or {}
                    timestamp_utc = datetime_from.get("utc")
                    timestamp_local = datetime_from.get("local")
                    timestamp_to_utc = datetime_to.get("utc")
                    timestamp_to_local = datetime_to.get("local")
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
                            "timestamp_local": timestamp_local,
                            "timestamp_to_utc": timestamp_to_utc,
                            "timestamp_to_local": timestamp_to_local,
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
                # transaction.rollback()
                connection.rollback()
                logger.exception(
                    f"Error inserting sensor hours for sensor {sensor_id} at {timestamp_utc}: {e}"
                )
                logger.error(
                    f"Sensor {sensor_id}: insert failed on page {page}. Previous pages remain saved."
                )
                break

            # transaction.commit()
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
        insert_openaq_sensor_hours(connection, date_from = '2026-01-01', date_to = '2026-01-02')
