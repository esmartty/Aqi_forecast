from sqlalchemy import text

def fetch_last_historical_open_meteo_weather_forecast_data_date(connection):

    sql = text("""
               SELECT MAX(run_time_utc) AS last_historical_date
               FROM public.weather_model_runs
               WHERE model = :model
	             AND load_type = 'historical';
            """)
    
    row = connection.execute(
        sql,
        {"model": "ecmwf_ifs"},
    ).mappings().first()

    last_historical_date = row["last_historical_date"] if row else None

    return last_historical_date