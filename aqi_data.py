import requests
#import json
#import math  # Import math to use math.nan

#station_id_list = ['3401', '3402', '3403', '3407', '8691', '9039', '14683']

def json_data(station_id):
    payload = {
        'token': '1eb2d24c2339f84a9ef202b04eef53207e68ef70'
        }
    response = requests.get('https://api.waqi.info/feed/@{station_id}'.format(station_id = station_id), params = payload)
    response_json = response.json()
    return response_json

def get_from_api(station_id_list):
    answer_array = []
    for station_id in station_id_list:
        answer = json_data(station_id)
        answer_data = answer.get("data", {})
        if answer_data != {}:
            answer_array.append(answer_data)
    return answer_array

#data = get_from_api(station_id_list)

#print(json.dumps(data, indent=4)) # Pretty-print

# Iterate over each station
# for station in data:
#     aqi = station.get("aqi", math.nan)
#     station_idx = station.get("idx", None)
#     geo = station.get("city", {}).get("geo", [])
#     lat = geo[0]
#     lng = geo[1]
#     station_name = station.get("city", {}).get("name", None)

    # #measure_time = station["data"]["time"]['s']
    # measure_time = station.get("time", {}).get("s", None)

    # iaqi = station.get("iaqi", {})

    # # Extract IAQI values into separate variables
    # co = iaqi.get("co", {}).get("v", math.nan)
    # dew = iaqi.get("dew", {}).get("v", math.nan)
    # h = iaqi.get("h", {}).get("v", math.nan)
    # no2 = iaqi.get("no2", {}).get("v", math.nan)
    # o3 = iaqi.get("o3", {}).get("v", math.nan)
    # p = iaqi.get("p", {}).get("v", math.nan)
    # pm10 = iaqi.get("pm10", {}).get("v", math.nan)
    # pm25 = iaqi.get("pm25", {}).get("v", math.nan)
    # so2 = iaqi.get("so2", {}).get("v", math.nan)
    # t = iaqi.get("t", {}).get("v", math.nan)
    # w = iaqi.get("w", {}).get("v", math.nan)
    # wg = iaqi.get("wg", {}).get("v", math.nan)
    
    # # Print extracted values
    # print(f"Station AQI: {aqi}")
    # print(f"Station IDX: {station_idx}")
    # print(f"Latitude: {lat}")
    # print(f"Longitude: {lng}")
    # print(f"Station name: {station_name}")
    # print(f"Datetime: {measure_time}")

    # #{'co': {'v': 0.1}, 'dew': {'v': -9}, 'h': {'v': 85}, 'no2': {'v': 10.3}, 'o3': {'v': 4.5}, 'p': {'v': 1027}, 'pm10': {'v': 57}, 'pm25': {'v': 153}, 'so2': {'v': 1.7}, 't': {'v': -7}, 'w': {'v': 3}, 'wg': {'v': 10.8}}
    # print(f"co: {co}")
    # print(f"dew: {dew}")
    # print(f"o3: {o3}")
    # print(f"'no2': {no2}")
    # print(f"p: {p}")
    # print(f"pm10: {pm10}")
    # print(f"pm25: {pm25}")
    # print(f"so2: {so2}")
    # print(f"t: {t}")
    # print(f"w: {w}")
    # print(f"wg: {wg}")
    # print("-" * 40)


    # # Extract and print forecast values

    # forecast_data = station.get("forecast", {}).get("daily", {})
    # #forecast_data = {} #test for no forecast
    
    # print("Forecast Data:")

    # for pollutant in ["o3", "pm10", "pm25", "uvi"]:  # Iterate only over known forecasting pollutants
    #     print(f"  {pollutant}:")
    #     forecasts = forecast_data.get(pollutant, [])
    
    #     if forecasts: # If the pollutant exists
    #         for forecast in forecasts:
    #             day = forecast["day"]
    #             avg = forecast["avg"]
    #             max_val = forecast["max"]
    #             min_val = forecast["min"]
    #             print(f"    {day}: avg={avg}, max={max_val}, min={min_val}")
    #     else:  # If the pollutant is missing, print NaN
    #          print("    No data available, using NaN")
    #          print(f"    NaN: avg=NaN, max=NaN, min=NaN")

    # print("-" * 40)