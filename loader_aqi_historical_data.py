import datetime
import logging
from sqlalchemy import text
import aqi_data
from parser import parse_numeric

logger = logging.getLogger(__name__)

def insert_aqi_data(connection, station_id_list):
    """Insert or update aqi_data table."""
    try:
        data = aqi_data.get_from_api(station_id_list)  # source of AQI data
    except Exception as e:
        logger.exception(f"Error loading aqi_data from API: {e}")
        raise
    
    sql = text ("""INSERT INTO public.aqi_data (station_id, air_quality_information, measure_date,   date_update,   co,  dew,   h,   no2,  o3, p,  pm10,   pm25,  so2,  t,  w,  wg)
                VALUES (                                    :idx,       :aqi,                    :measure_time,  :date_update,  :co, :dew, :h,  :no2, :o3, :p, :pm10, :pm25, :so2, :t, :w, :wg)
                ON CONFLICT (station_id, measure_date) 
                DO UPDATE SET air_quality_information = EXCLUDED.air_quality_information, 
                              date_update = EXCLUDED.date_update, 
                              co = EXCLUDED.co, 
                              dew = EXCLUDED.dew,
                              h = EXCLUDED.h,
                              no2 = EXCLUDED.no2,
                              o3 = EXCLUDED.o3,
                              p = EXCLUDED.p,
                              pm10 = EXCLUDED.pm10,
                              pm25 = EXCLUDED.pm25,
                              so2 = EXCLUDED.so2,
                              t = EXCLUDED.t,
                              w = EXCLUDED.w,
                              wg = EXCLUDED.wg 
                """)

    for station in data:
        iaqi = station.get("iaqi", {})
        try:
            with connection.begin_nested():
                connection.execute(sql, {
                    "idx": station.get("idx"),
                    "aqi":  parse_numeric(station.get("aqi"), int),
                    "measure_time": station.get("time", {}).get("s"),
                    "date_update": datetime.datetime.now(),
                    "co":  parse_numeric(iaqi.get("co", {}).get("v")),
                    "dew":  parse_numeric(iaqi.get("dew", {}).get("v")),
                    "h":  parse_numeric(iaqi.get("h", {}).get("v")),
                    "no2":  parse_numeric(iaqi.get("no2", {}).get("v")),
                    "o3":  parse_numeric(iaqi.get("o3", {}).get("v")),
                    "p":  parse_numeric(iaqi.get("p", {}).get("v")),
                    "pm10":  parse_numeric(iaqi.get("pm10", {}).get("v")),
                    "pm25":  parse_numeric(iaqi.get("pm25", {}).get("v")),
                    "so2":  parse_numeric(iaqi.get("so2", {}).get("v")),
                    "t":  parse_numeric(iaqi.get("t", {}).get("v")),
                    "w":  parse_numeric(iaqi.get("w", {}).get("v")),
                    "wg":  parse_numeric(iaqi.get("wg", {}).get("v")),
                })
        except Exception as e:
            logger.exception(f"Error inserting AQI data for station {station.get('idx')}: {e}")
