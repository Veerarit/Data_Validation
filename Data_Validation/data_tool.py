# from datatool.cli import get_bq, get_mongo


"""
This flow gets data from MongoDB and BigQuery then compare the number of records/documents.

"""


from dateutil import parser
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from pymongo import MongoClient
import sys

#TODO Add yaml file instead of dictionary here
with open(os.path.join(os.path.dirname(__file__), f"collections.json"), "r") as f:
    COLLECTIONS_CONFIG = json.load(f)




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
    print(f"MongoDB Staging | {col} collection: {len(items)} document(s)")


def get_bq(table, min_date, max_date):
    key_path = "config.json"
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    output = []

    with open(os.path.join(os.path.dirname(__file__), f"sql/count.sql"), "r") as f:
        id = COLLECTIONS_CONFIG.get(f'{table}')
        QUERY = f.read().format(table=table, id=id, min_date=min_date, max_date=max_date)

    query_job = client.query(QUERY)

    rows = query_job.result()
    for row in rows:
        output.append(row)
    print(
        f"BigQuery Staging | mongo.dim_{table}: {output[0][0]} record(s)"
    )


if __name__ == "__main__":
    table = str(sys.argv[1])
    min_date = str(sys.argv[2])
    max_date = str(sys.argv[3])
    print(f"Timeframe : {min_date} --> {max_date}")
    get_mongo(table, min_date, max_date)
    get_bq(table, min_date, max_date)
    #TODO print out the difference between 2 sources
    #TODO print out the whole 24hrs period compare hour by hour
    #TODO make it's more useful
