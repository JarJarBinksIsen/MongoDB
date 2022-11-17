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
    print('Deactivated')
    
def stat():
    ratio_results = []
    stations = db.stations.find({})
    for station in stations:
        selection = []
        eachStationDatas = db.datas.find({'station_id': station['_id']})
        for station_data in eachStationDatas:
            if station_data['date'].weekday() < 5 and station_data['date'].hour == 18:
                selection.append(station_data['_id'])
        enum_bikes = []
        for sel in selection:
            aStation_selection = db.datas.find_one({'_id': sel})
            if aStation_selection['station_id'] == station['_id']:
                enum_bikes.append(aStation_selection['bike_available'])
        tmp = 0
        for bikes in enum_bikes:
            tmp += bikes
        if(len(enum_bikes) > 0):
            mean = tmp/len(enum_bikes)
            ratio = mean/station['size']
            if ratio < 0.2:
                ratio_results.append(
                    {
                        'ratio': ratio,
                        'id': station['_id'],
                        'name' : station['name'],
                        'city': station['source'].get('dataset')
                        
                    }
                )
    for ratio_result in ratio_results:
        print('In city', ratio_result.get('city'), '- station', ratio_result.get('name'), 'with id', ratio_result['id'],'has a', round(ratio_result.get('ratio')*100, 1), '% ratio')
    print('For a total of', len(ratio_results), 'stations')
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