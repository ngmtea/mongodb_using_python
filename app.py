from pymongo import MongoClient
import json

connection_string = "mongodb://localhost:27017/"
client = MongoClient(connection_string)

restaurants_collection = client["trainingDB"]["restaurants"]

#Task 1
f = open('./restaurants_insert_one.json')
doc = json.load(f)
restaurants_collection.insert_one(doc)

#Task 2
f = open('./restaurants_insert_many.json')
docs = json.load(f)
restaurants_collection.insert_many(docs)

#Task 3
all_restaurants = restaurants_collection.find({})
for restaurant in all_restaurants:
    print(restaurant)

#Task 4&5
all_restaurants_task4 = restaurants_collection.find({},{"restaurant_id" : 1,"name":1,"borough":1,"cuisine" :1,"_id":0})
for restaurant in all_restaurants_task4:
    print(restaurant)

#Task 6
borough_Bronx_restaurants = restaurants_collection.find({"borough": "Bronx"})
for restaurant in borough_Bronx_restaurants:
    print(restaurant)

#Task 7
first_5_Bronx_restaurants = borough_Bronx_restaurants.limit(5)
for restaurant in first_5_Bronx_restaurants:
    print(restaurant)

#Task 8
next_5_Bronx_restaurants = borough_Bronx_restaurants.skip(5).limit(5)
for restaurant in next_5_Bronx_restaurants:
    print(restaurant)

#Task 9
score_greater_90_restaurants = restaurants_collection.find({"grades" : { "$elemMatch":{"score":{"$gt" : 90}}}})
for restaurant in score_greater_90_restaurants:
    print(restaurant)

#Task 10
score_81_to_99_restaurants = restaurants_collection.find({"grades" : { "$elemMatch":{"score":{"$gt" : 80 , "$lt" :100}}}})
for restaurant in score_81_to_99_restaurants:
    print(restaurant)

#Task 11
#do not prepare any cuisine of 'American', score more than 70, latitude less than -65.754168
restaurants_task11 = restaurants_collection.find({  "cuisine": {"$ne": "American"},
                                                    "grades" : { "$elemMatch":{"score":{"$gt" : 70}}},
                                                    "address.coord" : {"$lt" : -65.754168}
                                                })
for restaurant in restaurants_task11:
    print(restaurant)

#Task 12
#belong to the borough Bronx, prepared either American or Chinese dish
restaurants_task12 = restaurants_collection.find({"borough": "Bronx", "cuisine": {"$in": ["American", "Chinese"]}})
for restaurant in restaurants_task12:
    print(restaurant)

#Task 13
#Id, name, borough and cuisine for those restaurants which belong to the borough Staten Island or Queens or Bronxor Brooklyn
borough_list = ["Staten Island", "Queens", "Bronxor Brooklyn"]
restaurants_task13 = restaurants_collection.find({"borough": {"$in": borough_list}},
                                                 {"restaurant_id" : 1,"name":1,"borough":1,"cuisine" :1,})

for restaurant in restaurants_task13:
    print(restaurant)

#Task 14
#not belonging to the borough Staten Island or Queens or Bronxor Brooklyn
not_borough_list = ["Staten Island", "Queens", "Bronxor Brooklyn"]
restaurants_task14 = restaurants_collection.find({"borough": {"$nin": not_borough_list}},
                                                 {"restaurant_id" : 1,"name":1,"borough":1,"cuisine" :1})

for restaurant in restaurants_task14:
    print(restaurant)

#Task 15
#Id, name, address and geographical location for those restaurants where 2nd element of coord array contains a value which is more than 42 and upto 52
restaurants_task15 = restaurants_collection.find({"address.coord.1": {"$gt" : 42, "$lte" : 52}},
                                                 {"restaurant_id" : 1,"name":1,"address":1,"coord":1})

for restaurant in restaurants_task15:
    print(restaurant)

#Task 16
#all the addresses contains the street or not
check_street = True
for restaurant in all_restaurants:
    if 'street' not in restaurant['address']:
        check_street = False
        print(restaurant)
        break
print(check_street)

#Task17
#Update document has restaurant_id is "00000001": Change field grades from "One Star" to "Five Star".
restaurants_collection.find_one_and_update({"restaurant_id": "00000001"},{"$set": {"grades": "Five Star"}})

#Task18
#Update documents have "borough" is "Hai Ba Trung": Change field "borough" from "Hai Ba Trung" to "Hanoi".
restaurants_collection.update_many({"borough": "Hai Ba Trung"},{"$set": {"borough": "Ha Noi"}})

#Task19
#Delete the document which has restaurant_id is "00000001"
restaurants_collection.delete_one({"restaurant_id": "00000001"})

#Task20
#Delete all documents which have field "borough" is "Hanoi"
restaurants_collection.delete_many({"borough": "Ha Noi"})