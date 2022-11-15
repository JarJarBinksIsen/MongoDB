import bson.son
import pymongo

client = pymongo.MongoClient("mongodb+srv://root:root@cluster0.eisd22z.mongodb.net")
db = client.vls

# ISEN GPS location : 50.6342041,3.0465664
user_position_long = float(input("Your longitude ? (Enter 3.0465664 for ISEN Lille longitude) :"))
user_position_lat = float(input("Your latitude (Enter 50.6342041 for ISEN Lille latitude) :"))


db.stations.create_index([("geometry", pymongo.GEOSPHERE)])
results = db.stations.find(
    {
        'geometry': 
        {
            '$near': bson.son.SON(
                [
                    ('$geometry',
                    bson.son.SON(
                        [
                            ('type', 'Point'),
                            ('coordinates', [user_position_long, user_position_lat])
                        ]
                    )),
                    ('$maxDistance', 300)
                ]
            )
        }
    }
)

for result in results:
    last_station_datas = db.datas.find({'station_id':result['_id']}).sort([('date', pymongo.DESCENDING), ('_id', pymongo.DESCENDING)]).limit(1)
    for el in last_station_datas:
        print(result['name'], ':', el['bike_available'], ' velos dispos et ', el['stand_available'], ' stands dispos')
