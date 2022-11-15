import pymongo
import pymongo.server_api
import requests
import json
import dateutil.parser
import time

client = pymongo.MongoClient("mongodb+srv://root:root@cluster0.eisd22z.mongodb.net/?retryWrites=true&w=majority", server_api=pymongo.server_api.ServerApi('1'))
db = client.vls

def launch_finder():
    search = str(input('Enter chars to search :'))
    #db.stations.create_index([('name', pymongo.TEXT)])
    results = db.stations.find(
        { 'name': { '$regex': search, '$options': 'i' } }
        #{ 'score': { '$meta': 'textScore' } }
    )
    #.sort([('score', pymongo.DESCENDING)])
    for result in results:
        print(result)
    menu()

def updater():
    station_id = int(input('Which station (_id) ?\n'))
    print('FIELDS :\n1) name\n2) geometry\n3) size\n4) tpe')
    field_num = int(input('\nWhich field do you want to update\n'))
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
    print('Updated', station_id, field)

def deleter():
    print('Deleted')

def stat():
    db.datas.find()
    print('Here it is')

def menu():
    print('MENU :')
    print('1) Find stations with name')
    print('2) Update a station (need its _id)')
    print("3) Delete a staion and its datas (need station's _id)")
    print('4) give all stations with a ratio bike/total_stand under 20% between 18h and 19h00 (monday to friday)')
    choice = int(input())

    if choice == 1:
        launch_finder()
    elif choice == 2:
        updater()
    elif choice == 3:
        deleter()
    elif choice == 4:
        stat()
    else:
        print('Must choose')
        menu()

menu()