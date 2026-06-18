import datetime
import logging
from sqlalchemy import text
from parser import parse_utc, _json_value

import openaq_data
from openaq_exceptions import SensorHoursIngestionError
from openaq_sensor_data_service import ( fetch_openaq_sensor_ids, fetch_datetime_range_for_sensor_id,)

logger = logging.getLogger(__name__)


def _ingest_sensor_hours_range(connection, sensor_id, datetime_from, datetime_to, sql, limit=100):
    if datetime_from >= datetime_to:
        return

    duration = datetime_to - datetime_from

    page = 1
    while True:
        try:
            response= openaq_data.json_sensor_hours(
                sensor_id,
                datetime_from=datetime_from,
                datetime_to=datetime_to,
                limit=limit,
                page=page,
            )
        except SensorHoursIngestionError as e:
            if e.status == 408 and duration > datetime.timedelta(days=1):
                midpoint = datetime_from + duration / 2
                logger.warning(
                    "Timeout/408 error for sensor %s in window %s to %s; splitting into %s and %s",
                    sensor_id,
                    datetime_from,
                    datetime_to,
                    datetime_from,
                    midpoint,
                )
                _ingest_sensor_hours_range(connection, sensor_id, datetime_from, midpoint, sql, limit)
                _ingest_sensor_hours_range(connection, sensor_id, midpoint, datetime_to, sql, limit)
                logger.info(
                    "Completed split ingestion for sensor %s in window %s to %s",
                    sensor_id,
                    datetime_from,
                    datetime_to,
                )
                return
            raise SensorHoursIngestionError(
                sensor_id=sensor_id,
                datetime_from=datetime_from,
                datetime_to=datetime_to,
                status=408,
                reason="window_too_heavy"
            ) from e

        results = response.get("results") if isinstance(response, dict) else None
        if not results:
            if page == 1:
                logger.info(f"No hourly results returned for sensor {sensor_id} between {datetime_from} and {datetime_to}")
            return

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
        except Exception:
            connection.rollback()
            logger.exception(
                "Error inserting sensor hours for sensor %s at %s",
                sensor_id,
                timestamp_utc,
            )
            logger.error(
                "Sensor %s: insert failed on page %s for window %s to %s. Previous pages remain saved.",
                sensor_id,
                page,
                datetime_from,
                datetime_to,
            )
            raise

        connection.commit()
        logger.info(
            "Committed sensor %s page %s (%s records) for window %s to %s",
            sensor_id,
            page,
            len(results),
            datetime_from,
            datetime_to,
        )

        if len(results) < limit:
            return

        page += 1


def insert_openaq_sensor_hours(connection, limit=100):
    
    sensor_ids = fetch_openaq_sensor_ids(connection)
    #sensor_ids = [29179, ]
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
        datetime_from, datetime_to = fetch_datetime_range_for_sensor_id(connection, sensor_id)

        if datetime_from == datetime_to:
            logger.info(f"Sensor {sensor_id} is up to date ({datetime_from}), skipping hourly ingestion")
            continue

        try:
            _ingest_sensor_hours_range(connection, sensor_id, datetime_from, datetime_to, sql, limit=limit)
        except Exception as e:
            connection.rollback()
            logger.exception(
                f"Error fetching OpenAQ sensor hours for sensor {sensor_id} for {datetime_from} to {datetime_to}: {e}"
            )
            logger.error(
                f"Sensor {sensor_id}: ingestion failed for {datetime_from} to {datetime_to}. Previous pages remain saved."
            )


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
