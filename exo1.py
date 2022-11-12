
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
    "lille" : "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion",
    "paris" : "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=3000&facet=etatconnexion"
}

# Lille
vlilles = get_veloStations_by_city(cities_urls["lille"])
vlilles_to_insert = [
    {
        '_id': elem.get('fields', {}).get('libelle'),
        'name': elem.get('fields', {}).get('nom', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('nbvelosdispo') + elem.get('fields', {}).get('nbplacesdispo'),
        'source': {
            'dataset': 'Lille',
            'id_ext': elem.get('fields', {}).get('libelle')
        },
        'tpe': elem.get('fields', {}).get('type', '') == 'AVEC TPE'
    }
    for elem in vlilles
]
try: 
    db.stations.update_many({}, vlilles_to_insert, upsert=True)
except:
    pass

#Paris
velibs = get_veloStations_by_city(cities_urls["paris"])
velibs_to_insert = [
    {
        '_id': elem.get('fields', {}).get('stationcode'),
        'name': elem.get('fields', {}).get('name', '').title(),
        'geometry': elem.get('geometry'),
        'size': elem.get('fields', {}).get('capacity'),
        'source': {
            'dataset': 'Paris',
            'id_ext': elem.get('fields', {}).get('stationcode')
        },
        'tpe': elem.get('fields', {}).get('is_renting', '') == 'OUI'
    }
    for elem in velibs
]
try: 
    db.stations.update_many({}, velibs_to_insert, upsert=True)
except:
    pass
