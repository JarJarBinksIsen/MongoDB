from pymongo import MongoClient, GEOSPHERE
from bson.son import SON

client = MongoClient("mongodb+srv://root:root@cluster0.eisd22z.mongodb.net")
db = client.vls

# ISEN GPS location : 50.6342041,3.0465664
user_position_long = float(input("Your longitude ? (Enter 3.0465664 for ISEN Lille longitude) :"))
user_position_lat = float(input("Your latitude (Enter 50.6342041 for ISEN Lille latitude) :"))

user_position_long = 3.0465664
user_position_lat = 50.6342041

db.stations.create_index([("geometry", GEOSPHERE)])
results = db.stations.find(
    {
        'geometry': 
        {
            '$near': SON(
                [
                    ('$geometry',
                    SON(
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
    print(result)
