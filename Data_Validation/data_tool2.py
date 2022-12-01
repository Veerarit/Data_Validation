"""
This flow gets data from MongoDB and BigQuery then compare the number of records/documents.

"""

import click
from dateutil import parser
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
from pymongo import MongoClient
import sys


COLLECTIONS_CONFIG = dict(
    audits="audit_id",
    automations="automation_id",
    channels="channel_id",
    deleted_tasks="deleted_task_id",
    feedback_company_values="feedback_company_value_id",
    feedbacks="feedback_id",
    memberships_log="membership_id",
    memberships="membership_id",
    messages="message_id",
    payments_audits="id",
    #payments_transcation="payments_transactions_id",
    payments="payment_id",
    project_groups="project_group_id",
    projects="project_id",
    rolesv2="rolesv2_id",
    stripe_transaction_logs="stripe_transaction_log_id",
    subtask_links="link_id",
    tags="tag_id",
    tasklists="tasklist_id",
    tasks="task_id",
    time_tracks="time_track_id",
    transaction_logs="transaction_log_id",
    users="user_id",
    workspace_user_histories="workspace_user_histories_id",
    # workspaces_current_membership="workspaces_current_membership_id",
    workspaces="workspace_id",
    workspace_members="workspace_id",
)




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
