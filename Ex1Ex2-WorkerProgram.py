import pymongo
import pymongo.server_api
import requests
import json
import dateutil.parser
import time

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
        stations.append(
            {
                '_id': int(lille_station.get('fields', {}).get('libelle')),
                'name': lille_station.get('fields', {}).get('nom', '').title(),
                'geometry': lille_station.get('geometry'),
                'size': lille_station.get('fields', {}).get('nbvelosdispo') + lille_station.get('fields', {}).get('nbplacesdispo'),
                'source': 
                {
                    'dataset': 'Lille',
                    'id_ext': int(lille_station.get('fields', {}).get('libelle'))
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
                'station_id': int(lille_station.get('fields', {}).get('libelle'))
            }
        )
        
    # Paris
    paris_stations = get_veloStations_by_city('paris')
    for paris_station in paris_stations:
        stations.append(
            {
                '_id': int(paris_station.get('fields', {}).get('stationcode')),
                'name': paris_station.get('fields', {}).get('name', '').title(),
                'geometry': paris_station.get('geometry'),
                'size': paris_station.get('fields', {}).get('capacity'),
                'source': {
                    'dataset': 'Paris',
                    'id_ext': int(paris_station.get('fields', {}).get('stationcode'))
                },
                'tpe': paris_station.get('fields', {}).get('is_renting', '') == 'OUI',
                'available': paris_station.get('fields', {}).get('is_installed', '') == 'OUI'
            }
        )
        datas.append(
            {
                'bike_available': paris_station.get('fields', {}).get('numbikesavailable'),
                'stand_available': paris_station.get('fields', {}).get('numdocksavailable'),
                'date': dateutil.parser.parse(paris_station..get('fields', {}).get('duedate'), ignoretz=True),
                'station_id': int(paris_station.get('fields', {}).get('stationcode'))
            }
        )

    # Rennes
    rennes_stations = get_veloStations_by_city('rennes')
    for rennes_station in rennes_stations:
        stations.append(
            {
                '_id': int(rennes_station.get('fields', {}).get('idstation')),
                'name': rennes_station.get('fields', {}).get('nom', '').title(),
                'geometry': rennes_station.get('geometry'),
                'size': rennes_station.get('fields', {}).get('nombreemplacementsactuels'),
                'source': {
                    'dataset': 'Rennes',
                    'id_ext': int(rennes_station.get('fields', {}).get('idstation'))
                },
                'available': rennes_station.get('fields', {}).get('etat', '') == 'En fonctionnement'
            }
        )
        datas.append(
            {
                'bike_available': rennes_station.get('fields', {}).get('nombrevelosdisponibles'),
                'stand_available': rennes_station.get('fields', {}).get('nombreemplacementsdisponibles'),
                'date': dateutil.parser.parse(rennes_station..get('fields', {}).get('lastupdate'), ignoretz=True),
                'station_id': int(rennes_station.get('fields', {}).get('idstation'))
            }
        )

    # Lyon
    lyon_stations = get_veloStations_by_city('lyon')
    for lyon_station in lyon_stations:
        stations.append(
            {
                '_id': int(lyon_station.get('number')),
                'name': lyon_station.get('name', '').title(),
                'geometry': {
                    'type': 'Point',
                    'coordinates': [lyon_station.get('lng'), lyon_station.get('lat')]
                },
                'size': lyon_station.get('bike_stands'),
                'source': {
                    'dataset': 'Lyon',
                    'id_ext': int(lyon_station.get('number'))
                },
                'tpe': lyon_station.get('banking')
                'available': lyon_station.get('status') == 'OPEN'
            }
        )
        datas.append(
            {
                'bike_available': lyon_station.get('available_bikes'),
                'stand_available': lyon_station.get('available_bike_stands'),
                'date': dateutil.parser.parse(lyon_station.get('last_update'), ignoretz=True)+timedelta(hours=1),
                'station_id': int(lyon_station.get('number'))
            }
        )
    
    # Add all in stations & datas collections
    for station in stations:
        db.stations.update_one({'_id':station.get('_id')}, { '$set' : station}, upsert=True)
        previous_update = db.datas.find_one({'station_id': station.get('_id')}).sort('date', pymongo.DESCENDING).limit(1)
        if dateutil.parser.parse(previous_update) != station.get('date'):
            db.datas.insert_one(station)
    
    #db.datas.insert_many(datas, ordered=False)
    
    print(db.stations.count_documents({}), db.datas.count_documents({}))   
    time.sleep(500)