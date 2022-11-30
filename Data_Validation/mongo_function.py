from pymongo import MongoClient
from pprint import pprint
from dateutil import parser



def get_mongo():
    items = []
    col = "workspaces"
    BASE_MONGO_URL = "mongodb://localhost:{port}/?readPreference=secondary&directConnection=true&ssl=false"
    mongo_url = BASE_MONGO_URL.format(port=47017)
    client = MongoClient(mongo_url)
    db = client["taskworld_enterprise_new_staging"]
    collection = db[col]
    # x = collection.count_documents({})
    # print(f"{col}: {x} document(s)")
    #print(collection.find({title: 'MongoDB and Python'}).count())
    
    min_date = '2021-10-01T00:00:00.000Z'
    max_date = '2021-12-14T00:00:00.000Z'
    min_datetime = parser.parse(min_date)
    max_datetime = parser.parse(max_date)
    query_json = { "updated": { "$gte": min_datetime, "$lte": max_datetime } }
    x = collection.find(query_json)
    for data in x:
        items.append(data)
    print(f"{col}: {len(items)} document(s)")
if __name__ == "__main__":
    get_mongo()
