import datetime
import json
import logging
from sqlalchemy import create_engine, text

import openaq_data
from db_config import get_database_url

logger = logging.getLogger(__name__)


def extract_sensor_ids(location):
    sensors = location.get("sensors") or []
    if isinstance(sensors, list):
        ids = [int(sensor.get("id")) for sensor in sensors if sensor and sensor.get("id") is not None]
        return ids if ids else None
    return None


def extract_instrument_ids(location):
    instruments = location.get("instruments") or []
    if isinstance(instruments, list):
        ids = [instr.get("id") for instr in instruments if instr and instr.get("id") is not None]
        return ids if ids else None
    return None


def extract_parameter_ids(location):
    sensors = location.get("sensors") or []
    if isinstance(sensors, list):
        ids = set()
        for sensor in sensors:
            if sensor and isinstance(sensor, dict):
                param = sensor.get("parameter")
                if param and isinstance(param, dict):
                    param_id = param.get("id")
                    if param_id is not None:
                        ids.add(int(param_id))
        return sorted(list(ids)) if ids else None
    return None


def insert_openaq_locations(connection):
    """Fetch OpenAQ locations and insert/update public.openaq_locations."""
    try:
        response = openaq_data.json_location_data()
    except Exception as e:
        logger.exception(f"Error fetching OpenAQ location data: {e}")
        raise

    results = response.get("results") if isinstance(response, dict) else None
    if not results:
        logger.warning("No results found in OpenAQ location response")
        return

    sql = text(
        """INSERT INTO public.openaq_locations (
            location_id,
            location_name,
            locality,
            timezone,
            country,
            country_id,
            country_name,
            owner_id,
            owner_name,
            provider_id,
            provider_name,
            is_mobile,
            is_monitor,
            latitude,
            longitude,
            instrument_ids,
            sensor_ids,
            parameter_ids,
            sensors,
            licenses,
            bounds,
            distance,
            datetime_first,
            datetime_last,
            raw_data,
            date_update
        ) VALUES (
            :location_id,
            :location_name,
            :locality,
            :timezone,
            :country,
            :country_id,
            :country_name,
            :owner_id,
            :owner_name,
            :provider_id,
            :provider_name,
            :is_mobile,
            :is_monitor,
            :latitude,
            :longitude,
            :instrument_ids,
            :sensor_ids,
            :parameter_ids,
            :sensors,
            :licenses,
            :bounds,
            :distance,
            :datetime_first,
            :datetime_last,
            :raw_data,
            :date_update
        )
        ON CONFLICT (location_id)
        DO UPDATE SET
            location_name = EXCLUDED.location_name,
            locality = EXCLUDED.locality,
            timezone = EXCLUDED.timezone,
            country = EXCLUDED.country,
            country_id = EXCLUDED.country_id,
            country_name = EXCLUDED.country_name,
            owner_id = EXCLUDED.owner_id,
            owner_name = EXCLUDED.owner_name,
            provider_id = EXCLUDED.provider_id,
            provider_name = EXCLUDED.provider_name,
            is_mobile = EXCLUDED.is_mobile,
            is_monitor = EXCLUDED.is_monitor,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            instrument_ids = EXCLUDED.instrument_ids,
            sensor_ids = EXCLUDED.sensor_ids,
            parameter_ids = EXCLUDED.parameter_ids,
            sensors = EXCLUDED.sensors,
            licenses = EXCLUDED.licenses,
            bounds = EXCLUDED.bounds,
            distance = EXCLUDED.distance,
            datetime_first = EXCLUDED.datetime_first,
            datetime_last = EXCLUDED.datetime_last,
            raw_data = EXCLUDED.raw_data,
            date_update = EXCLUDED.date_update
        """
    )

    for location in results:
        geo = location.get("coordinates") or {}
        country = location.get("country") or {}
        owner = location.get("owner") or {}
        provider = location.get("provider") or {}
        sensor_ids = extract_sensor_ids(location)
        instrument_ids = extract_instrument_ids(location)
        parameter_ids = extract_parameter_ids(location)

        try:
            connection.execute(
                sql,
                {
                    "location_id": location.get("id"),
                    "location_name": location.get("name") or location.get("location"),
                    "locality": location.get("locality"),
                    "timezone": location.get("timezone"),
                    "country": country.get("code") if isinstance(country, dict) else country,
                    "country_id": country.get("id") if isinstance(country, dict) else None,
                    "country_name": country.get("name") if isinstance(country, dict) else None,
                    "owner_id": owner.get("id"),
                    "owner_name": owner.get("name"),
                    "provider_id": provider.get("id"),
                    "provider_name": provider.get("name"),
                    "is_mobile": location.get("isMobile"),
                    "is_monitor": location.get("isMonitor"),
                    "latitude": geo.get("latitude"),
                    "longitude": geo.get("longitude"),
                    "distance": location.get("distance"),
                    "instrument_ids": json.dumps(instrument_ids) if instrument_ids is not None else None,
                    "sensor_ids": sensor_ids,
                    "parameter_ids": parameter_ids,
                    "sensors": json.dumps(location.get("sensors")) if location.get("sensors") is not None else None,
                    "licenses": json.dumps(location.get("licenses")) if location.get("licenses") is not None else None,
                    "bounds": json.dumps(location.get("bounds")) if location.get("bounds") is not None else None,
                    "datetime_first": location.get("datetimeFirst", {}).get("utc") if location.get("datetimeFirst") else None,
                    "datetime_last": location.get("datetimeLast", {}).get("utc") if location.get("datetimeLast") else None,
                    "raw_data": json.dumps(location),
                    "date_update": datetime.datetime.now(),
                },
            )
        except Exception as e:
            logger.exception(f"Error inserting OpenAQ location {location.get('id')}: {e}")


def insert_openaq_parameters(connection, limit=100):
    """Fetch OpenAQ parameter metadata and insert/update public.sensor_parameters."""
    page = 1
    while True:
        try:
            response = openaq_data.json_parameter_data(limit=limit, page=page)
        except Exception as e:
            logger.exception(f"Error fetching OpenAQ parameters page {page}: {e}")
            raise

        results = response.get("results") if isinstance(response, dict) else None
        meta = response.get("meta") or {}

        if not results:
            if page == 1:
                logger.warning("No results found in OpenAQ parameters response")
            break

        sql = text(
            """INSERT INTO public.sensor_parameters (
                parameter_id,
                name,
                units,
                display_name,
                description,
                raw_data,
                date_update
            ) VALUES (
                :parameter_id,
                :name,
                :units,
                :display_name,
                :description,
                :raw_data,
                :date_update
            )
            ON CONFLICT (parameter_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                units = EXCLUDED.units,
                display_name = EXCLUDED.display_name,
                description = EXCLUDED.description,
                raw_data = EXCLUDED.raw_data,
                date_update = EXCLUDED.date_update
            """)
        
        for parameter in results:
            try:
                connection.execute(
                    sql,
                    {
                        "parameter_id": parameter.get("id"),
                        "name": parameter.get("name"),
                        "units": parameter.get("units"),
                        "display_name": parameter.get("displayName"),
                        "description": parameter.get("description"),
                        "raw_data": json.dumps(parameter),
                        "date_update": datetime.datetime.now(),
                    },
                )
            except Exception as e:
                logger.exception(
                    f"Error inserting parameter {parameter.get('id')}: {e}"
                )

        limit_meta = meta.get("limit") or limit
        total = meta.get("found")
        
        if not results:
            break

        if len(results) < limit_meta:
            break
        
        if total is not None:
            max_page = (total + limit_meta  - 1) // limit_meta
            if page >= max_page:
                break

        page += 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    engine = create_engine(get_database_url())
    with engine.begin() as connection:
        insert_openaq_locations(connection)
        insert_openaq_parameters(connection)
