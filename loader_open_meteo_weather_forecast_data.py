import logging
from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid5, NAMESPACE_URL
import json
import requests
from sqlalchemy import create_engine, text

import loader_open_meteo_weather_forecast_data_service 
import open_meteo_weather_forecast
from open_meteo_s3 import inspect_model_run, s3_client
from parser import parse_numeric

logger = logging.getLogger(__name__)

def _current_utc_time():
    return datetime.now(timezone.utc)

def generate_run_id(model: str, run_time_utc: datetime) -> str:
    """
    Generate a deterministic UUID5 from model name and model run time.
    """

    if isinstance(run_time_utc, str):
        run_time_utc = datetime.fromisoformat(
            run_time_utc.replace("Z", "+00:00")
        )

    # Make sure the timestamp is timezone-aware and normalized to UTC.
    if run_time_utc.tzinfo is None:
        run_time_utc = run_time_utc.replace(tzinfo=timezone.utc)
    else:
        run_time_utc = run_time_utc.astimezone(timezone.utc)

    run_time_str = run_time_utc.isoformat(timespec="seconds")

    return str(uuid5(
        NAMESPACE_URL,
        f"{model}:{run_time_str}"
    ))

def query_coord_to_id(connection):
    """Fetch all weather locations from the database."""
    sql = text("""
        SELECT id, latitude, longitude
        FROM public.weather_locations
    """)

    try:
        result = connection.execute(sql)
    
        coord_to_id = {
            (round(row.latitude, 5), round(row.longitude, 5)): row.id
            for row in result
        }

        logger.info(f"Fetched {len(coord_to_id)} camps_points from database")

        return coord_to_id
    except Exception as e:
            logger.error("Error occurred while fetching weather locations: %s", e)
            raise

def insert_weather_location(connection, latitude, longitude):
    """Insert weather location and return existing one (by coords)."""
    latitude = round(latitude, 5)
    longitude = round(longitude, 5)

    sql = text("""
        INSERT INTO public.weather_locations (
            latitude,
            longitude,
            geom
        )
        VALUES (
            :lat,
            :lon,
            ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
        )
        ON CONFLICT (latitude, longitude)
        DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            geom = EXCLUDED.geom
        RETURNING id
    """)

    result = connection.execute(sql, {
        "lat": latitude,
        "lon": longitude
    })

    return result.scalar()

def get_weather_location_id(
    connection,
    latitude,
    longitude,
    coord_to_id
):
    latitude = round(latitude, 5)
    longitude = round(longitude, 5)

    key = (latitude, longitude)

    location_id = coord_to_id.get(key)

    if location_id is not None:
        return location_id

    else:

        new_location_id = insert_weather_location(connection, latitude, longitude)

        coord_to_id[key] = new_location_id

        return new_location_id

def insert_weather_forecast_data(engine, data, run_id):
    """Insert or update hourly forecasts for one model run."""
    variables = open_meteo_weather_forecast.HOURLY_VARIABLES
    forecasts = [data] if isinstance(data, dict) else data
    if not isinstance(forecasts, list) or not forecasts:
        raise ValueError("Forecast data must be a non-empty list of locations")
    if run_id is None:
        raise ValueError("run_id is required to insert forecast data")

    columns = ["weather_location_id", "run_id", "forecast_time"]
    for variable in variables:
        columns.extend((variable, f"{variable}_unit"))

    insert_sql = text(f"""
        INSERT INTO public.weather_forecast_hourly (
            {", ".join(columns)}
        )
        VALUES (
            {", ".join(f":{column}" for column in columns)}
        )
        ON CONFLICT (weather_location_id, forecast_time, run_id)
        DO UPDATE SET
            {", ".join(
                f"{column} = EXCLUDED.{column}"
                for column in columns[3:]
            )}
    """)

    rows = []
    with engine.begin() as connection:
        coord_to_id = query_coord_to_id(connection)

        for location_index, forecast in enumerate(forecasts):
            hourly = forecast.get("hourly")
            if not isinstance(hourly, dict):
                raise ValueError(
                    f"Hourly data missing at location {location_index}"
                )

            forecast_times = hourly.get("time")
            if not isinstance(forecast_times, list) or not forecast_times:
                raise ValueError(
                    f"Hourly times missing at location {location_index}"
                )

            values_by_variable = {}
            for variable in variables:
                values = hourly.get(variable)
                if not isinstance(values, list) or len(values) != len(forecast_times):
                    raise ValueError(
                        f"Invalid {variable} values at location {location_index}"
                    )
                values_by_variable[variable] = values

            location_id = get_weather_location_id(
                connection,
                forecast["latitude"],
                forecast["longitude"],
                coord_to_id,
            )

            for time_index, timestamp in enumerate(forecast_times):
                forecast_time = datetime.fromisoformat(
                    timestamp.replace("Z", "+00:00")
                )
                if forecast_time.tzinfo is None:
                    forecast_time = forecast_time.replace(tzinfo=timezone.utc)
                else:
                    forecast_time = forecast_time.astimezone(timezone.utc)

                row = {
                    "weather_location_id": location_id,
                    "run_id": run_id,
                    "forecast_time": forecast_time,
                }
                for variable, unit in variables.items():
                    row[variable] = values_by_variable[variable][time_index]
                    row[f"{variable}_unit"] = unit
                rows.append(row)

        if rows:
            connection.execute(insert_sql, rows)

    return len(rows)

def insert_model_update_data(engine, model_update_data, load_type: str):
    """Insert or update model run metadata and return run_id."""

    if load_type not in {"historical", "latest"}:
        raise ValueError(f"Unsupported load_type: {load_type}")

    run_id = generate_run_id(
        model_update_data["model"],
        model_update_data["run_time_utc"]
    )

    s3_data_run_meta = inspect_model_run(
        s3_client(),
        model_update_data["model"],
        model_update_data["run_time_utc"],
    )
    if s3_data_run_meta.get("exists") is not True:
        logger.warning(
            "Could not inspect S3 metadata for model run %s: %s",
            model_update_data["run_time_utc"],
            s3_data_run_meta,
        )

    meta_json = s3_data_run_meta.get("json")

    with engine.begin() as connection:
        sql = text("""
            INSERT INTO public.weather_model_runs (
                run_id,
                model,
                run_time_utc,
                api_available_at_utc,
                modification_time_utc,
                load_type,
                s3_last_modified,
                meta_json_created_at,
                metadata,
                date_update
            )
            VALUES (
                :run_id,
                :model,
                :run_time_utc,
                :api_available_at_utc,
                :modification_time_utc,
                :load_type,
                :s3_last_modified,
                :meta_json_created_at,
                CAST(:metadata AS JSONB),
                :date_update
            )
            ON CONFLICT (model, run_time_utc)
            DO UPDATE SET
                api_available_at_utc = EXCLUDED.api_available_at_utc,
                modification_time_utc = EXCLUDED.modification_time_utc,
                load_type = EXCLUDED.load_type,
                s3_last_modified = EXCLUDED.s3_last_modified,
                meta_json_created_at = EXCLUDED.meta_json_created_at,
                metadata = EXCLUDED.metadata,
                date_update = EXCLUDED.date_update
            RETURNING run_id
        """)

        result = connection.execute(sql, {
            "run_id": run_id,
            "model": model_update_data["model"],
            "run_time_utc": model_update_data["run_time_utc"],
            "api_available_at_utc": (
                None
                if load_type == "historical"
                else model_update_data.get("api_available_at_utc")
            ),
            "modification_time_utc": model_update_data.get("modification_time_utc"),
            "load_type": load_type,
            "s3_last_modified": s3_data_run_meta.get("last_modified_utc"),
            "meta_json_created_at": (
                meta_json.get("created_at")
                if isinstance(meta_json, dict)
                else None
            ),
            "metadata": json.dumps(model_update_data.get("metadata")),
            "date_update": datetime.now(timezone.utc)
        })

        return result.scalar_one()


def insert_weather_data_helper(engine, start_date=None, end_date=None):
    """Insert metadata update data and weather forecast data for historical and the latest update type loads."""

    if start_date is None and end_date is None:
        model_update_data = open_meteo_weather_forecast.fetch_last_forecast_weather_model_update()
        run_id = insert_model_update_data(
            engine,
            model_update_data,
            load_type="latest",
        )
        
        #fetch forecast data for the last update
        run_time = model_update_data["run_time"]
        data = open_meteo_weather_forecast.get_forecast_data(run_time=run_time)
        insert_weather_forecast_data(engine, data=data, run_id=run_id)

    else:
        if start_date is None or end_date is None:
            raise ValueError(
                "Both start_date and end_date are required for a historical load"
            )

        current_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        if current_date > end:
            raise ValueError("start_date must be on or before end_date")

        now_utc = _current_utc_time()

        while current_date <= end:
            for hour in (0, 6, 12, 18):
                run_time = datetime.combine(
                    current_date,
                    time(hour=hour),
                    tzinfo=timezone.utc,
                )
                run_time_str = run_time.strftime("%Y-%m-%dT%H:%M")

                if run_time > now_utc:
                    logger.info(
                        "Skipping future weather forecast run %s",
                        run_time_str,
                    )
                    continue

                try:
                    data = open_meteo_weather_forecast.get_forecast_data(
                        run_time=run_time_str
                    )
                    run_id = insert_model_update_data(
                        engine,
                        {
                            "model": open_meteo_weather_forecast.MODEL,
                            "run_time_utc": run_time,
                        },
                        load_type="historical",
                    )
                    insert_weather_forecast_data(
                        engine,
                        data=data,
                        run_id=run_id,
                    )
                except requests.exceptions.HTTPError as error:
                    if (
                        run_time.date() == now_utc.date()
                        and error.response is not None
                        and error.response.status_code == 400
                    ):
                        logger.warning(
                            "Weather forecast run %s is not available yet: %s",
                            run_time_str,
                            error,
                        )
                    else:
                        logger.exception(
                            "Error loading weather forecast data for %s",
                            run_time_str,
                        )
                    continue
                except Exception:
                    logger.exception(
                        "Error loading weather forecast data for %s",
                        run_time_str,
                    )
                    continue

                logger.info(
                    "Finished processing weather forecast data for %s",
                    run_time_str,
                )

            current_date += timedelta(days=1)


def run_weather_data_load(
    engine,
    environment: str,
    start_date=None,
    end_date=None,
):
    if environment not in {"production", "local"}:
        raise ValueError(f"Unsupported environment: {environment}")

    if start_date is not None or end_date is not None:
        if start_date is None or end_date is None:
            raise ValueError(
                "Both start_date and end_date are required for a historical load"
            )
        return insert_weather_data_helper(engine, start_date, end_date)

    if environment == "production":
        return insert_weather_data_helper(engine)

    if hasattr(engine, "connect"):
        with engine.connect() as connection:
            last_historical_date = (
                loader_open_meteo_weather_forecast_data_service
                .fetch_last_historical_open_meteo_weather_forecast_data_date(
                    connection
                )
            )
    else:
        last_historical_date = (
            loader_open_meteo_weather_forecast_data_service
            .fetch_last_historical_open_meteo_weather_forecast_data_date(
                engine
            )
        )

    if last_historical_date is None:
        raise RuntimeError(
            "No historical model runs found in the local database; "
            "a start date is required for the initial historical load."
        )

    last_historical_date_str = last_historical_date.strftime("%Y-%m-%d")
    today_str = date.today().strftime("%Y-%m-%d")
    return insert_weather_data_helper(
        engine,
        last_historical_date_str,
        today_str,
    )


if __name__ == "__main__":
    import argparse

    from db_config import ENVIRONMENT, get_database_url

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-date",
        help="Start date (YYYY-MM-DD) for a manual historical load",
    )
    parser.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD) for a manual historical load",
    )
    args = parser.parse_args()

    engine = create_engine(get_database_url())
    run_weather_data_load(
        engine,
        ENVIRONMENT,
        start_date=args.start_date,
        end_date=args.end_date,
    )