import pymongo
import pymongo.server_api
import requests
import json
import dateutil.parser
import time
import datetime
import re

client = pymongo.MongoClient("mongodb+srv://root:root@cluster0.eisd22z.mongodb.net/?retryWrites=true&w=majority", server_api=pymongo.server_api.ServerApi('1'))
db = client.vls

cities_urls = {
    "lille" : "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&timezone=Europe%2FParis",
    "paris" : "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=3000&timezone=Europe%2FParis",
    "rennes": "https://data.rennesmetropole.fr/api/records/1.0/search/?dataset=etat-des-stations-le-velo-star-en-temps-reel&q=&rows=3000&timezone=Europe%2FParis",
    "lyon"  : "https://download.data.grandlyon.com/ws/rdata/jcd_jcdecaux.jcdvelov/all.json"
}

def get_veloStations_by_city(city):
    response = requests.request("GET", cities_urls[city])
    response_json = json.loads(response.text.encode('utf8'))
    if city == "lyon":
        stations_list = response_json.get("values", [])
    else:
        stations_list = response_json.get("records", [])
    return stations_list

while True:
    stations = []
    datas = []

    # Lille
    lille_stations = get_veloStations_by_city('lille')
    for lille_station in lille_stations:
        id = int(re.split("[^0-9]", str(lille_station.get('fields', {}).get('libelle')))[0])
        stations.append(
            {
                '_id': id,
                'name': lille_station.get('fields', {}).get('nom', '').title(),
                'geometry': lille_station.get('geometry'),
                'size': lille_station.get('fields', {}).get('nbvelosdispo') + lille_station.get('fields', {}).get('nbplacesdispo'),
                'source': 
                {
                    'dataset': 'Lille',
                    'id_ext': id
                },
                'tpe': lille_station.get('fields', {}).get('type', '') == 'AVEC TPE',
                'available': lille_station.get('fields', {}).get('etat', '') == 'EN SERVICE'
            }
        )
        datas.append(
            {
                'bike_available': lille_station.get('fields', {}).get('nbvelosdispo'),
                'stand_available': lille_station.get('fields', {}).get('nbplacesdispo'),
                'date': dateutil.parser.parse(lille_station.get('fields', {}).get('datemiseajour'), ignoretz=True),
                'station_id': id
            }
        )
        
    # Paris
    paris_stations = get_veloStations_by_city('paris')
    for paris_station in paris_stations:
        id = int(re.split("[^0-9]", str(paris_station.get('fields', {}).get('stationcode')))[0])
        stations.append(
            {
                '_id': id,
                'name': paris_station.get('fields', {}).get('name', '').title(),
                'geometry': paris_station.get('geometry'),
                'size': paris_station.get('fields', {}).get('capacity'),
                'source': {
                    'dataset': 'Paris',
                    'id_ext': id
                },
                'tpe': paris_station.get('fields', {}).get('is_renting', '') == 'OUI',
                'available': paris_station.get('fields', {}).get('is_installed', '') == 'OUI'
            }
        )
        datas.append(
            {
                'bike_available': paris_station.get('fields', {}).get('numbikesavailable'),
                'stand_available': paris_station.get('fields', {}).get('numdocksavailable'),
                'date': dateutil.parser.parse(paris_station.get('fields', {}).get('duedate'), ignoretz=True),
                'station_id': id
            }
        )

    # Rennes
    rennes_stations = get_veloStations_by_city('rennes')
    for rennes_station in rennes_stations:
        id = int(re.split("[^0-9]", str(rennes_station.get('fields', {}).get('idstation')))[0])
        stations.append(
            {
                '_id': id,
                'name': rennes_station.get('fields', {}).get('nom', '').title(),
                'geometry': rennes_station.get('geometry'),
                'size': rennes_station.get('fields', {}).get('nombreemplacementsactuels'),
                'source': {
                    'dataset': 'Rennes',
                    'id_ext': id
                },
                'available': rennes_station.get('fields', {}).get('etat', '') == 'En fonctionnement'
            }
        )
        datas.append(
            {
                'bike_available': rennes_station.get('fields', {}).get('nombrevelosdisponibles'),
                'stand_available': rennes_station.get('fields', {}).get('nombreemplacementsdisponibles'),
                'date': dateutil.parser.parse(rennes_station.get('fields', {}).get('lastupdate'), ignoretz=True),
                'station_id': id
            }
        )

    # Lyon
    lyon_stations = get_veloStations_by_city('lyon')
    for lyon_station in lyon_stations:
        id = int(re.split("[^0-9]", str(lyon_station.get('number')))[0])
        stations.append(
            {
                '_id': id,
                'name': lyon_station.get('name', '').title(),
                'geometry': {
                    'type': 'Point',
                    'coordinates': [float(lyon_station.get('lng')), float(lyon_station.get('lat'))]
                },
                'size': lyon_station.get('bike_stands'),
                'source': {
                    'dataset': 'Lyon',
                    'id_ext': id
                },
                'tpe': lyon_station.get('banking'),
                'available': lyon_station.get('status') == 'OPEN'
            }
        )
        datas.append(
            {
                'bike_available': lyon_station.get('available_bikes'),
                'stand_available': lyon_station.get('available_bike_stands'),
                'date': dateutil.parser.parse(lyon_station.get('last_update'), ignoretz=True)+datetime.timedelta(hours=1),
                'station_id': id
            }
        )
    
    # Add all in stations & datas collections
    for index, station in enumerate(stations):
        db.stations.replace_one({'_id':station.get('_id')}, station, upsert=True)
        if index%200 == 0:
            print(index)
    
    db.datas.insert_many(datas, ordered=False)
    
    print(db.stations.count_documents({}), db.datas.count_documents({}))   
    time.sleep(500)