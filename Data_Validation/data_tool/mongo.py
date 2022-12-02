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
    min_date = f"{min_date}T00:00:00.000Z"
    max_date = f"{max_date}T00:00:00.000Z"
    min_datetime = parser.parse(min_date)
    max_datetime = parser.parse(max_date)
    # query_json = {"created": {"$gte": min_datetime, "$lte": max_datetime}}
    query_json = {"created": {"$gte": min_datetime, "$lte": max_datetime}},{ "$count": "count" }
    #x = collection.find(query_json)
    x = collection.aggregate(query_json)
    print(x)
    # for data in x:
    #     items.append(data)
    # print(len(items))

    # for doc in db[collection].aggregate([
    #     {"created": {"$gte": min_datetime, "$lte": max_datetime}},
    #     { "$count": "count" }
    #     ]):
    #     print(doc)

if __name__ == '__main__':
    table = str('tasks')
    min_date = str('2021-10-11')
    max_date = str('2021-10-15')
    get_mongo(table, min_date, max_date)


# --run once a day
# --`util.daily_mongo_bq_count`
# --run_date
# --server
# --collection_name
# --mongo_count
# --bq_count


#def mongo_bq_comparison(server_tag, collection, date):
#24 hours
#print out one row per hour
#00 --> 23
#create pipeline
#create utility