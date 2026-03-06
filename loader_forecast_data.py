import datetime
import logging
from sqlalchemy import text
import aqi_data
from parser import parse_numeric

logger = logging.getLogger(__name__)

def insert_forecast_data(connection, station_id_list):
    """Insert or update forecast table."""
    try:
        data = aqi_data.get_from_api(station_id_list)  # source of AQI data
    except Exception as e:
        logger.exception(f"Error loading forecast table from API: {e}")
        raise
    
    sql = text ("""INSERT INTO public.forecast_data(station_id, measure_date,   forecast_date,   forecasting_pollutant, avg_value,     min_value,   max_value,   date_update)
                VALUES (                            :idx,       :measure_time,  :forecast_date, :forecasting_pollutant, :avg_value,   :min_value,  :max_value,  :date_update)
                ON CONFLICT (station_id, measure_date, forecast_date, forecasting_pollutant) 
                DO UPDATE SET
                              avg_value = EXCLUDED.avg_value,
                              min_value = EXCLUDED.min_value,
                              max_value = EXCLUDED.max_value,
                              date_update = EXCLUDED.date_update 
                """)

    for station in data:
        station_id = station.get("idx")
        measure_time = station.get("time", {}).get("s")
        forecast_data = station.get("forecast", {}).get("daily", {})

        for pollutant in ["o3", "pm10", "pm25", "uvi"]:
            forecasts = forecast_data.get(pollutant, [])
            if not forecasts:
                # insert NULL
                forecast_date = datetime.datetime.strptime(measure_time, "%Y-%m-%d %H:%M:%S").date()
                #logger.info(f"Inserting forecast for station {station_id}, pollutant {pollutant}, forecast_date {forecast_date}, forecast_date_type {type(forecast_date)}")
                
                with connection.begin_nested():
                    connection.execute(sql, {
                        "idx": station_id,
                        "measure_time": measure_time,
                        "forecast_date": forecast_date,
                        "forecasting_pollutant": pollutant,
                        "avg_value": None,
                        "min_value": None,
                        "max_value": None,
                        "date_update": datetime.datetime.now()
                    })
                continue
            for forecast in forecasts:
                try:
                    forecast_date = datetime.datetime.strptime(forecast["day"], "%Y-%m-%d").date()
                    #logger.info(f"Inserting forecast for station {station_id}, pollutant {pollutant}, forecast_date {forecast_date}, forecast_date_type {type(forecast_date)}")
                    with connection.begin_nested():
                        connection.execute(sql, {
                            "idx": station_id,
                            "measure_time": measure_time,
                            "forecast_date": forecast_date,
                            #"forecast_date": forecast["day"], #<class 'str'>
                            "forecasting_pollutant": pollutant,
                            "avg_value": parse_numeric(forecast.get("avg")),
                            "min_value": parse_numeric(forecast.get("min")),
                            "max_value": parse_numeric(forecast.get("max")),
                            "date_update": datetime.datetime.now()
                        })
                except Exception as e:
                    logger.exception(f"Error inserting forecast {pollutant} for station {station_id} on {forecast['day']}: {e}")

