
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import json
import dateutil.parser
import time
import pymongo


client = MongoClient("mongodb+srv://root:root@cluster0.eisd22z.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))

db = client.vls

# exercice 1 : get stations name & location from urls and store in vls.stations
def get_veloStations_by_city(city_url):
    response = requests.request("GET", city_url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

cities_urls = {
    "Lille" : "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion",
    "Paris" : "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=3000&facet=etatconnexion"
}

velo_stations = [
    {
        "city" : city,
        "stations" : get_veloStations_by_city(url)
    }
    for city, url in cities_urls.items()
]

#print(velo_stations[0].get("stations")[0].get("datasetid"))

for city in velo_stations:
    for station in city.get("stations"):
        if(city.get("city") == "Lille"):
            db.stations.update_one(
                { '_id' : station.get('fields', {}).get('libelle') },
                {
                    'name': station.get('fields', {}).get('nom', '').title(),
                    'geometry': station.get('geometry'),
                    'size': station.get('fields', {}).get('nbvelosdispo') + station.get('fields', {}).get('nbplacesdispo'),
                    'source': {
                        'dataset': 'Lille',
                        'id_ext': station.get('fields', {}).get('libelle')
                    },
                    'tpe': station.get('fields', {}).get('type', '') == 'AVEC TPE'
                },
                upsert=True
            )
        if(city.get('city') == 'Paris'):
            db.stations.update_one(
                { '_id' : station.get('fields', {}).get('stationcode') },
                {
                    'name': station.get('fields', {}).get('name', '').title(),
                    'geometry': station.get('geometry'),
                    'size': station.get('fields', {}).get('capacity'),
                    'source': {
                        'dataset': 'Paris',
                        'id_ext': station.get('fields', {}).get('stationcode')
                    },
                    'tpe': station.get('fields', {}).get('is_renting', '') == 'OUI'
                },
                upsert=True
            )

# for city in velo_stations:
#     for station in city.get("stations"):
#         if(city.get("city") == "Lille"):
#             print(station.get('fields', {}).get('libelle'))