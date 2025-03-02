from sqlalchemy import create_engine, text
import os
import datetime
import synop_data
import aqi_data
import math  # Import math to use math.nan

DATABASE_NAME = os.environ.get("DATABASE")
DATABASE_USER_NAME = os.environ.get("USER")
DATABASE_PASSWORD = os.environ.get("PASSWORD")
DATABASE_PORT = os.environ.get("PORT")
DATABASE_SERVER = os.environ.get("INTERNAL_HOST")

# Replace with your actual database credentials
DATABASE_URL = "postgresql://{username}:{password}@{server}:{port}/{database}".format(username = DATABASE_USER_NAME, password = DATABASE_PASSWORD, server = DATABASE_SERVER, port = DATABASE_PORT, database = DATABASE_NAME)

# postgresql://USER:PASSWORD@INTERNAL_HOST:PORT/DATABASE

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# insert into synop_data_stations
with engine.connect() as connection:
    json_data = synop_data.json_data()

    sql = text ("""INSERT INTO public.synop_data(station_id,  date,           measurement_hour,  date_update,  station_name, temperature,  wind_speed,       wind_direction,   relative_humidity,    rainfall_sum, pressure)
                VALUES (                         :id_stacji,  :data_pomiaru,  :godzina_pomiaru,  :date_update, :stacja,      :temperatura, :predkosc_wiatru, :kierunek_wiatru, :wilgotnosc_wzgledna, :suma_opadu,  :cisnienie)
                ON CONFLICT (station_id, date, measurement_hour) 
                DO UPDATE SET date_update = EXCLUDED.date_update, 
                              temperature = EXCLUDED.temperature, 
                              wind_speed = EXCLUDED.wind_speed, 
                              wind_direction = EXCLUDED.wind_direction, 
                              relative_humidity = EXCLUDED.relative_humidity, 
                              rainfall_sum = EXCLUDED.rainfall_sum, 
                              pressure = EXCLUDED.pressure
                """)
    connection.execute(sql, {"id_stacji": json_data['id_stacji'],
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
    #connection.commit()  # Commit the transaction

    # insert into stations table    
    sql = text ("""INSERT INTO public.stations(station_id,    latitude, longitude, station_name,   date_update)
                VALUES (                       :idx,          :lat,     :lng,      :station_name,  :date_update)
                ON CONFLICT (station_id) 
                DO UPDATE SET latitude = EXCLUDED.latitude, 
                              longitude = EXCLUDED.longitude, 
                              station_name = EXCLUDED.station_name, 
                              date_update = EXCLUDED.date_update
                """)
    
    station_id_list = ['3401', '3402', '3403', '3407', '8691', '9039', '14683']
    data = aqi_data.get_from_api(station_id_list)
   
    for station in data:
        geo = station.get("city", {}).get("geo", [])
        connection.execute(sql, {"idx": station.get("idx", None),
                                 "lat": geo[0],
                                 "lng": geo[1],
                                 "station_name": station.get("city", {}).get("name", None),
                                 "date_update": datetime.datetime.now(),

                                    })
        #connection.commit()  # Commit the transaction

    # insert into aqi_data table    
    sql = text ("""INSERT INTO public.aqi_data (station_id, air_quality_information, measure_date,   date_update,   co,  dew,   h,   no2,  o3, p,  pm10,   pm25,  so2,  t,  w,  wg)
                VALUES (                        :idx,       :aqi,                    :measure_time,  :date_update,  :co, :dew, :h,  :no2, :o3, :p, :pm10, :pm25, :so2, :t, :w, :wg)
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
    
   
    #{'co': {'v': 0.1}, 'dew': {'v': -9}, 'h': {'v': 85}, 'no2': {'v': 10.3}, 'o3': {'v': 4.5}, 'p': {'v': 1027}, 'pm10': {'v': 57}, 'pm25': {'v': 153}, 'so2': {'v': 1.7}, 't': {'v': -7}, 'w': {'v': 3}, 'wg': {'v': 10.8}}
    for station in data:
        iaqi = station.get("iaqi", {})
        connection.execute(sql, {"idx": station.get("idx", None),
                                 "aqi": station.get("aqi", math.nan),
                                 "measure_time": station.get("time", {}).get("s", None),
                                 "date_update": datetime.datetime.now(),
                                 "co": iaqi.get("co", {}).get("v", math.nan),
                                 "dew": iaqi.get("dew", {}).get("v", math.nan),
                                  "h" : iaqi.get("h", {}).get("v", math.nan),
                                 "no2": iaqi.get("no2", {}).get("v", math.nan),
                                  "o3": iaqi.get("o3", {}).get("v", math.nan),
                                  "p" : iaqi.get("p", {}).get("v", math.nan),
                                  "pm10": iaqi.get("pm10", {}).get("v", math.nan),
                                  "pm25": iaqi.get("pm25", {}).get("v", math.nan),
                                  "so2": iaqi.get("so2", {}).get("v", math.nan),
                                  "t" : iaqi.get("t", {}).get("v", math.nan),
                                  "w": iaqi.get("w", {}).get("v", math.nan),
                                  "wg": iaqi.get("wg", {}).get("v", math.nan)

                                    })
        #connection.commit()  # Commit the transaction



    # insert into forecast_data table    
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
        station_id = station.get("idx", None)
        measure_time = station.get("time", {}).get("s", None)

        # "forecast":{"daily":{"o3":[{"avg":11,"day":"2025-02-15","max":15,"min":6},{"avg":11,"day":"2025-02-16","max":16,"min":5},{"avg":8,"day":"2025-02-17","max":15,"min":2},{"avg":5,"day":"2025-02-18","max":15,"min":1},{"avg":6,"day":"2025-02-19","max":14,"min":1},{"avg":4,"day":"2025-02-20","max":5,"min":4}],"pm10":[{"avg":19,"day":"2025-02-15","max":42,"min":6},{"avg":25,"day":"2025-02-16","max":53,"min":8},{"avg":42,"day":"2025-02-17","max":62,"min":9},{"avg":64,"day":"2025-02-18","max":99,"min":20},{"avg":63,"day":"2025-02-19","max":86,"min":30},{"avg":59,"day":"2025-02-20","max":59,"min":57}],"pm25":[{"avg":57,"day":"2025-02-15","max":114,"min":18},{"avg":76,"day":"2025-02-16","max":147,"min":27},{"avg":114,"day":"2025-02-17","max":158,"min":28},{"avg":151,"day":"2025-02-18","max":195,"min":60},{"avg":154,"day":"2025-02-19","max":182,"min":83},{"avg":158,"day":"2025-02-20","max":158,"min":155}],"uvi":[{"avg":0,"day":"2025-02-15","max":1,"min":0},{"avg":0,"day":"2025-02-16","max":1,"min":0},{"avg":0,"day":"2025-02-17","max":2,"min":0},{"avg":0,"day":"2025-02-18","max":2,"min":0},{"avg":0,"day":"2025-02-19","max":1,"min":0},{"avg":0,"day":"2025-02-20","max":0,"min":0}]}}

        forecast_data = station.get("forecast", {}).get("daily", {})
        
        for pollutant in ["o3", "pm10", "pm25", "uvi"]:  # Iterate over pollutants
            forecasts = forecast_data.get(pollutant, [])
            
            for forecast in forecasts:  # Loop over each forecast day
                connection.execute(sql, {
                    "idx": station_id,
                    "measure_time": measure_time,
                    "forecast_date": forecast["day"],  # Insert each day separately
                    "forecasting_pollutant": pollutant,
                    "avg_value": forecast.get("avg"),
                    "min_value": forecast.get("min"),
                    "max_value": forecast.get("max"),
                    "date_update": datetime.datetime.now()
                })

    connection.commit()