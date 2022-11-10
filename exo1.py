
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import json
import dateutil.parser
import time
import pymongo


client = MongoClient("mongodb+srv://root:root@cluster0.eisd22z.mongodb.net/?retryWrites=true&w=majority", server_api=ServerApi('1'))

db = client.vls

# exercice 1
def get_veloStations_by_city(url):
    response = requests.request("GET", url)
    response_json = json.loads(response.text.encode('utf8'))
    return response_json.get("records", [])

cities_urls = {
    "lille" : "https://opendata.lillemetropole.fr/api/records/1.0/search/?dataset=vlille-realtime&q=&rows=3000&facet=libelle&facet=nom&facet=commune&facet=etat&facet=type&facet=etatconnexion",
    "paris" : "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&q=&rows=3000&facet=etatconnexion"
}


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
    db.stations.insert_many(vlilles_to_insert, ordered=False)
except:
    pass

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
    db.stations.insert_many(velibs_to_insert, ordered=False)
except:
    pass

# exercice 2
while True:
    print('update')
    vlilles = get_veloStations_by_city(cities_urls["lille"])
    lille_datas = [
        {
            "bike_available": elem.get('fields', {}).get('nbvelosdispo'),
            "stand_available": elem.get('fields', {}).get('nbplacesdispo'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('datemiseajour')),
            "station_id": elem.get('fields', {}).get('libelle')
        }
        for elem in vlilles
    ]
    
    for data in lille_datas:
        last_station_update = db.datas.find({ '$and': [{ "station_id":{'$exists':True}},{ "station_id":data["station_id"]}]}).sort("date", -1).limit(1)[0]
        #print(last_station_update["station_id"])
        if(last_station_update["bike_available"] != data["bike_available"] or last_station_update["stand_available"] != data["stand_available"]):
            db.datas.insert_one(data)
        db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, { "$set": data }, upsert=True)

    velibs = get_veloStations_by_city(cities_urls["paris"])
    paris_datas = [
        {
            "bike_available": elem.get('fields', {}).get('numbikesavailable'),
            "stand_available": elem.get('fields', {}).get('numdocksavailable'),
            "date": dateutil.parser.parse(elem.get('fields', {}).get('duedate')),
            "station_id": elem.get('fields', {}).get('stationcode')
        }
        for elem in velibs
    ]
    
    for data in paris_datas:
        last_station_update = db.datas.find({ '$and': [{ "station_id":{'$exists':True}},{ "station_id":data["station_id"]}]}).sort("date", -1).limit(1)[0]
        if(last_station_update["bike_available"] != data["bike_available"] or last_station_update["stand_available"] != data["stand_available"]):
            db.datas.insert_one(data)
        db.datas.update_one({'date': data["date"], "station_id": data["station_id"]}, { "$set": data }, upsert=True)

    time.sleep(10)
