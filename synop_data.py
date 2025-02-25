import requests

def json_data():
    
    response = requests.get('https://danepubliczne.imgw.pl/api/data/synop/id/12566')
    response_json = response.json()
    return response_json

# {"id_stacji":"12566","stacja":"Krak\u00f3w","data_pomiaru":"2025-02-10","godzina_pomiaru":"16","temperatura":"-0.6","predkosc_wiatru":"3","kierunek_wiatru":"40","wilgotnosc_wzgledna":"65.7","suma_opadu":"0","cisnienie":"1033"}
#data = json_data()

#print (data["id_stacji"])