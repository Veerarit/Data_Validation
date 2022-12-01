from dateutil import parser
from pymongo import MongoClient


def get_mongo(table, min_date, max_date):
    items = []
    col = str(table)
    BASE_MONGO_URL = "mongodb://localhost:{port}/?readPreference=secondary&directConnection=true&ssl=false"
    mongo_url = BASE_MONGO_URL.format(port=47017)
    client = MongoClient(mongo_url)
    db = client["taskworld_enterprise_new_staging"]
    collection = db[col]
    # x = collection.count_documents({})
    # print(f"{col}: {x} document(s)")
    # print(collection.find({title: 'MongoDB and Python'}).count())

    min_date = f"{min_date}T00:00:00.000Z"
    max_date = f"{max_date}T00:00:00.000Z"
    min_datetime = parser.parse(min_date)
    max_datetime = parser.parse(max_date)
    query_json = {"updated": {"$gte": min_datetime, "$lte": max_datetime}}
    x = collection.find(query_json)
    for data in x:
        items.append(data)
    # print(f"MongoDB Staging | {col} collection: {len(items)} document(s)")
    return len(items)
