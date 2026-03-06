import datetime
import logging
from sqlalchemy import text
import synop_data

logger = logging.getLogger(__name__)

def insert_synop_data(connection):
    """Insert or update synop_data table."""
    try:
        json_data = synop_data.json_data()
    except Exception as e:
        logger.exception(f"Error loading synop_data from API: {e}")
        raise
    

    sql = text ("""INSERT INTO public.synop_data(station_id,  date,           measurement_hour,  date_update,  station_name, temperature,  wind_speed,       wind_direction,   relative_humidity,    rainfall_sum, pressure)
                VALUES (                                      :id_stacji,  :data_pomiaru,  :godzina_pomiaru,  :date_update, :stacja,      :temperatura, :predkosc_wiatru, :kierunek_wiatru, :wilgotnosc_wzgledna, :suma_opadu,  :cisnienie)
                ON CONFLICT (station_id, date, measurement_hour) 
                DO UPDATE SET date_update = EXCLUDED.date_update, 
                              temperature = EXCLUDED.temperature, 
                              wind_speed = EXCLUDED.wind_speed, 
                              wind_direction = EXCLUDED.wind_direction, 
                              relative_humidity = EXCLUDED.relative_humidity, 
                              rainfall_sum = EXCLUDED.rainfall_sum, 
                              pressure = EXCLUDED.pressure
                """)

    try:
        connection.execute(sql, {
            "id_stacji": json_data['id_stacji'],
            "data_pomiaru": json_data['data_pomiaru'],
            "godzina_pomiaru": json_data['godzina_pomiaru'],
            "date_update": datetime.datetime.now(),
            "stacja": json_data['stacja'],
            "temperatura": json_data['temperatura'],
            "predkosc_wiatru": json_data['predkosc_wiatru'],
            "kierunek_wiatru": json_data['kierunek_wiatru'],
            "wilgotnosc_wzgledna": json_data['wilgotnosc_wzgledna'],
            "suma_opadu": json_data['suma_opadu'],
            "cisnienie": json_data['cisnienie'],
        })
    except Exception as e:
        logger.exception(f"Error inserting synop_data: {e}")
        raise