import pymongo
import pymongo.server_api
import requests
import json
import dateutil.parser
import time

client = pymongo.MongoClient("mongodb+srv://root:root@cluster0.eisd22z.mongodb.net/?retryWrites=true&w=majority", server_api=pymongo.server_api.ServerApi('1'))
db = client.vls

def get_veloStations_by_city(city_url):
    response = requests.request("GET", city_url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

cities_urls = {
    "lille" : "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion",
    "paris" : "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=3000&facet=etatconnexion"
}

while True:
    stations = []
    datas = []

    # Lille
    lille_stations = get_veloStations_by_city(cities_urls["lille"])
    for lille_station in lille_stations:
        stations.append(
            {
                '_id': lille_station.get('fields', {}).get('libelle'),
                'name': lille_station.get('fields', {}).get('nom', '').title(),
                'geometry': lille_station.get('geometry'),
                'size': lille_station.get('fields', {}).get('nbvelosdispo') + lille_station.get('fields', {}).get('nbplacesdispo'),
                'source': 
                {
                    'dataset': 'Lille',
                    'id_ext': lille_station.get('fields', {}).get('libelle')
                },
                'tpe': lille_station.get('fields', {}).get('type', '') == 'AVEC TPE'
            }
        )
        datas.append(
            {
                'bike_available': lille_station.get('fields', {}).get('nbvelosdispo'),
                'stand_available': lille_station.get('fields', {}).get('nbplacesdispo'),
                'date': dateutil.parser.parse(lille_station.get('record_timestamp')),
                'station_id': lille_station.get('fields', {}).get('libelle')
            }
        )
        
    # Paris
    paris_stations = get_veloStations_by_city(cities_urls['paris'])
    for paris_station in paris_stations:
        stations.append(
            {
                '_id': paris_station.get('fields', {}).get('stationcode'),
                'name': paris_station.get('fields', {}).get('name', '').title(),
                'geometry': paris_station.get('geometry'),
                'size': paris_station.get('fields', {}).get('capacity'),
                'source': {
                    'dataset': 'Paris',
                    'id_ext': paris_station.get('fields', {}).get('stationcode')
                },
                'tpe': paris_station.get('fields', {}).get('is_renting', '') == 'OUI'
            }
        )
        datas.append(
            {
                'bike_available': paris_station.get('fields', {}).get('numbikesavailable'),
                'stand_available': paris_station.get('fields', {}).get('numdocksavailable'),
                'date': dateutil.parser.parse(paris_station.get('record_timestamp')),
                'station_id': paris_station.get('fields', {}).get('stationcode')
            }
        )

    for elem in stations:
        db.datas.update_many({'_id':elem.get('_id')}, { '$set' : elem}, upsert=True)
    db.datas.insert_many(datas, ordered=False)
    
    print(db.stations.count_documents({}), db.datas.count_documents({}))   
    time.sleep(10)