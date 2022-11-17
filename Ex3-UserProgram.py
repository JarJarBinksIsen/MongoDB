import bson.son
import pymongo

client = pymongo.MongoClient("mongodb+srv://root:root@cluster0.eisd22z.mongodb.net")
db = client.vls

# ISEN GPS location : 50.6342041,3.0465664
user_position_long = input("Your longitude ? (Enter 3.0465664 for ISEN Lille longitude) :")
try:
    user_position_long = float(user_position_long)
except:
    user_position_long = 3.0465664
    print('Incorrect entry. Default value 3.0465664 is attributed\n')

user_position_lat = input("Your latitude (Enter 50.6342041 for ISEN Lille latitude) :")
try:
    user_position_lat = float(user_position_lat)
except:
    user_position_lat =  50.6342041
    print('Incorrect entry. Default value 50.6342041 is attributed\n')

dist = input("To what maximum distance (integer in meters) do you want ? ")
try:
    dist = int(dist)
except:
    dist = 300
    print('Incorrect entry. Default value 300m is attributed\n')

results = db.stations.find(
    {
        'geometry': {
            '$near': {
                '$geometry': {
                    'type': 'Point',
                    'coordinates': [user_position_long, user_position_lat]
                },
                '$maxDistance': dist
            }
        }
    }
)
# results = db.stations.find(
#     {
#         'geometry': 
#         {
#             '$near': bson.son.SON(
#                 [
#                     ('$geometry',
#                     bson.son.SON(
#                         [
#                             ('type', 'Point'),
#                             ('coordinates', [user_position_long, user_position_lat])
#                         ]
#                     )),
#                     ('$maxDistance', dist)
#                 ]
#             )
#         }
#     }
# )

for result in results:
    #last_station_datas = db.datas.find({'station_id':result['_id']}).sort([('date', pymongo.DESCENDING), ('_id', pymongo.DESCENDING)]).limit(1)
    last_station_datas = db.datas.find({'station_id':result['_id']}).sort('date', -1).limit(1)
    for el in last_station_datas:
        print(result['name'], ':', el['bike_available'], ' velos dispos et ', el['stand_available'], ' stands dispos')
