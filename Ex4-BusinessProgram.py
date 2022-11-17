import pymongo
import pymongo.server_api
import requests
import json
import time

client = pymongo.MongoClient("mongodb+srv://root:root@cluster0.eisd22z.mongodb.net/?retryWrites=true&w=majority", server_api=pymongo.server_api.ServerApi('1'))
db = client.vls

def finder():
    search = str(input('Enter chars to search :'))
    #db.stations.create_index([('name', pymongo.TEXT)])
    results = db.stations.find(
        { 'name': { '$regex': search, '$options': 'i' } }
    )
    for result in results:
        print(result)
    menu()

def updater():
    station_id = int(input('Which station (_id) ?\n'))
    print('\nFIELDS :\n1) name\n2) geometry\n3) size\n4) tpe')
    field_num = int(input('Which field do you want to update ?\n'))
    field_names = ['_id', 'name', 'geometry', 'size', 'tpe']
    if field_num > 4:
        print('Incorrect')
        updater()
    field = field_names[field_num]
    if field_num == 1:
        val = str(input('New value ? '))
    elif field_num == 2:
        long = float(input('Longitude ? '))
        lat = float(input('Latitude ? '))
        val = { 'type' : 'Point', 'coordinates' : [long, lat]}
    elif field_num == 3:
        val = int(input('New value ? '))
    elif field_num == 4:
        val = bool(input('tpe -> true or false ? '))
    db.stations.update_one(
            { '_id' : station_id },
            { '$set' : { field : val } }
        )
    print('Updated', station_id, field, val)
    menu()

def deleter():
    station_id = int(input('Which station (_id) ?\n'))
    sure = input('It will delete the station and all its datas. Are you sure (y/n) ? ')
    if sure == 'y':
        db.datas.delete_many({'station_id': station_id})
        db.stations.delete_one({'_id': station_id})
        print('Deleted')
    else:
        print('Operation aborted')
    menu()

def deactivate():
    polygone = []
    while True:
        print('Enter q to quit')
        point_lg = input('New coordinate longitude : ')
        if point_lg == 'q':
            break
        point_lat = input('New coordinate latitude : ')
        if point_lg == 'q':
            break
        polygone.append([float(point_lg), float(point_lat)])

    #polygone.append(polygone[0])
    #area = db.stations.update_many(
    area = db.stations.find(
        {
            'geometry': {
                '$geoWithin': {
                    '$geometry': {
                        'type' : "Polygon" ,
                        'coordinates': [ [ 
                            [ 3.0527401, 50.6360707 ],
                            [ 3.0343294, 50.6348459 ],
                            [ 3.0372906, 50.6262437 ],
                            [ 3.0592203, 50.6267065 ],
                            [ 3.0527401, 50.6360707 ]
                        ] ]
                    }
                }
            }
        }
        # ,{
        #     '$set': { 'available': False }
        # }
    )
    # area = db.stations.find(
    #     {
    #         'geometry': 
    #         {
    #             '$geoWithin': bson.son.SON(
    #                 [
    #                     ('$geometry',
    #                     bson.son.SON(
    #                         [
    #                             ('type', 'Polygon'),
    #                             ('coordinates', [polygone])
    #                         ]
    #                     ))
    #                 ]
    #             )
    #         }
    #     }
    # )
    for station in area:
        print(station)
    print('Deactivated')
    
def stat():
    #db.datas.aggregate([])
    match_stations_datas = db.datas.aggregate(
        [
            {
                '$project': {
                    '_id': '$station_id',
                    'hour': { '$hour': "$date" },
                    'dayOfWeek': { '$dayOfWeek': "$date" },
                    'bikes': '$bike_available',
                }
            },
            {
                '$match': {
                    'dayOfWeek': {'$gt': 1, '$lt': 7},
                    'hour': 18
                }
            },
            {
                '$group': {
                    '_id': '$_id',
                    'average_bikes': { '$avg': '$bikes' }
                }
            },
            {
                '$lookup': {
                    'from': 'stations',
                    'localField': '_id',
                    'foreignField': '_id',
                    'as': 'station'
                }
            },
            {
                '$project': {
                    'average_bikes': 1,
                    'the_station': { '$arrayElemAt': ['$station', 0] }
                }
            },
            {
                '$project': {
                    'average_bikes': 1,
                    'station': '$the_station',
                    'ratio' : {
                        '$divide': [
                            { '$toDouble': '$average_bikes' },
                            { '$toDouble': '$the_station.size' }
                        ]
                    }
                }
            },
            {
                '$match': {
                    'ratio': { '$lt': 0.2 }
                }
            }
        ]
    )
    for res in match_stations_datas:
        print('In city', res.get('station').get('source').get('dataset')+',', 'station :', res.get('station').get('name')+',', 'with id', res['_id'],'has a ratio of', round(res.get('ratio')*100, 1), '%')
    menu()

def menu():
    print('MENU :')
    print('1) Find stations with name')
    print('2) Update a station (need its _id)')
    print("3) Delete a staion and its datas (need station's _id)")
    print('4) Deactivate all staions inside an area')
    print('5) give all stations with a ratio bike/total_stand under 20% between 18h and 19h00 (monday to friday)')
    print('Press 0 to exit')
    choice = int(input())

    if choice == 1:
        finder()
    elif choice == 2:
        updater()
    elif choice == 3:
        deleter()
    elif choice == 4:
        deactivate()
    elif choice == 5:
        stat()
    elif choice == 0:
        exit()
    else:
        print('Must choose')
        menu()

menu()