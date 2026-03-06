import datetime
import logging
from sqlalchemy import text
import aqi_data

logger = logging.getLogger(__name__)

def insert_stations(connection, station_id_list):

    """Insert or update stations table."""   
    try:
        data = aqi_data.get_from_api(station_id_list)  # source of AQI data
    except Exception as e:
        logger.exception(f"Error loading stations data from API: {e}")
        raise

    sql = text ("""INSERT INTO public.stations(station_id,    latitude, longitude, station_name,   date_update)
            VALUES (                                         :idx,         :lat,     :lng,     :station_name,  :date_update)
            ON CONFLICT (station_id) 
            DO UPDATE SET latitude = EXCLUDED.latitude, 
                            longitude = EXCLUDED.longitude, 
                            station_name = EXCLUDED.station_name, 
                            date_update = EXCLUDED.date_update
            """)
            
    for station in data:
        geo = station.get("city", {}).get("geo", [None, None])
        try:
            with connection.begin_nested():
                connection.execute(sql, {
                    "idx": station.get("idx"),
                    "lat": geo[0],
                    "lng": geo[1],
                    "station_name": station.get("city", {}).get("name"),
                    "date_update": datetime.datetime.now(),
                })

        except Exception as e:
            logger.exception(f"Error inserting station {station.get('idx')}: {e}")
