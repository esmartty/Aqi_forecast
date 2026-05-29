import datetime
import logging
from typing import Dict
from sqlalchemy import Tuple, text


logger = logging.getLogger(__name__)


def fetch_openaq_sensor_ids_by_location(connection):
    sql = text("SELECT sensor_ids FROM public.openaq_locations WHERE sensor_ids IS NOT NULL")
    rows = connection.execute(sql).mappings().all()
    sensor_id_set = set()
    for row in rows:
        ids = row.get("sensor_ids")
        if isinstance(ids, list):
            sensor_id_set.update(int(i) for i in ids if i is not None)
    return sorted(sensor_id_set)


# def fetch_openaq_sensor_ids(connection, sensor_id:int): 
#     sql = text(""" SELECT sensor_id, datetimefirst_utc, datetimelast_utc 
#                FROM public.openaq_sensor_data 
#                WHERE datetimefirst_utc IS NOT NULL """) 
#     rows = connection.execute(sql).mappings().all() 
    
#     sensor_map: Dict[int, Tuple[datetime.datetime, datetime.datetime]] = {}

#     for row in rows: 
#         sensor_id = row["sensor_id"] 
#         if sensor_id is None: 
#             continue 
        
#         sensor_map[sensor_id] = { 
#             "first": row["datetimefirst_utc"], 
#             "last": row["datetimelast_utc"] 
#             } 
    
#     return sensor_map[sensor_id]["first"], sensor_map[sensor_id]["last"]


def fetch_openaq_sensor_ids(connection): 
    sql = text(""" SELECT sensor_id 
               FROM public.openaq_sensor_data 
               WHERE datetimefirst_utc IS NOT NULL """)
    
    rows = connection.execute(sql).mappings().all() 

    sensor_ids = []

    for row in rows: 
        sensor_ids.append(row["sensor_id"]) 
                  
    return sensor_ids

def fetch_openaq_sensor_last_historical_datetime_by_sensor_id(connection, sensor_id:int):

    sql = text("""
               SELECT MAX(timestamp_to_utc) AS last_historical_datetime
                FROM public.openaq_sensor_hours
                WHERE sensor_id = :sensor_id
            """)
    
    row = connection.execute(sql, {"sensor_id": sensor_id}).mappings().first()

    last_historical_datetime = row["last_historical_datetime"] if row else None

    return last_historical_datetime


def fetch_openaq_sensor_first_and_last_datetime_by_sensor_id(connection, sensor_id:int):
    sql = text("""
               SELECT sensor_id, datetimefirst_utc, datetimelast_utc 
               FROM public.openaq_sensor_data 
                WHERE sensor_id = :sensor_id
            """)
    
    row = connection.execute(sql, {"sensor_id": sensor_id}).mappings().first()

    if not row:
        return None, None

    return row["datetimefirst_utc"], row["datetimelast_utc"]


def fetch_datetime_range_for_sensor_id(connection, sensor_id:int):

    datetimefirst_utc = fetch_openaq_sensor_first_and_last_datetime_by_sensor_id(connection, sensor_id)[0]
    datetimelast_utc = fetch_openaq_sensor_first_and_last_datetime_by_sensor_id(connection, sensor_id)[1]
    last_historical_datetime = fetch_openaq_sensor_last_historical_datetime_by_sensor_id(connection, sensor_id)
    
    #print(f"Sensor {sensor_id} - first datetime: {datetimefirst_utc}, last datetime: {datetimelast_utc}, last historical datetime: {last_historical_datetime}")

    datetimefirst_utc = max(datetimefirst_utc, last_historical_datetime) if last_historical_datetime else datetimefirst_utc
    
    return datetimefirst_utc, datetimelast_utc

# _______________________________________________

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    from sqlalchemy import create_engine
    from db_config import get_database_url

    engine = create_engine(get_database_url())

    with engine.connect() as connection:
        #print(fetch_openaq_sensor_first_and_last_datetime_by_sensor_id(connection, 29320))
        #print(fetch_openaq_sensor_last_historical_datetime_by_sensor_id(connection, 29320))
        #print(fetch_datetime_range_for_sensor_id(connection, 29320))
        print(fetch_openaq_sensor_ids(connection))
       