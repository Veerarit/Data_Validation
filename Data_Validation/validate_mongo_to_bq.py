from dateutil import parser
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from pymongo import MongoClient

COLLECTIONS_CONFIG = dict(
    audits="audit_id",
    automations="automation_id",
    channels="channel_id",
    deleted_tasks="deleted_task_id",
    feedback_company_values="feedback_company_value_id",
    feedbacks="feedbacks_id",
    memberships_log="memberships_log_id",
    memberships="membership_id",
    messages="message_id",
    payments_audits="payments_audits_id",
    payments_transcation="payments_transactions_id",
    payments="payment_id",
    project_groups="project_groups_id",
    projects="projects_id",
    rolesv2="rolesv2_id",
    stripe_transaction_log="stripe_transaction_log_id",
    subtask_links="subtask_link_id",
    tags="tag_id",
    tasklists="tasklist_id",
    tasks="task_id",
    time_tracks="time_track_id",
    transaction_logs="transaction_log_id",
    users="users_id",
    workspace_user_histories="workspace_user_histories_id",
    workspaces_current_membership="workspaces_current_membership_id",
    workspaces="workspace_id",
    workspace_members="workspace_members_id",
)


def get_mongo(min_time, max_time):
    items = []
    col = "workspaces"
    BASE_MONGO_URL = "mongodb://localhost:{port}/?readPreference=secondary&directConnection=true&ssl=false"
    mongo_url = BASE_MONGO_URL.format(port=47017)
    client = MongoClient(mongo_url)
    db = client["taskworld_enterprise_new_staging"]
    collection = db[col]
    # x = collection.count_documents({})
    # print(f"{col}: {x} document(s)")
    # print(collection.find({title: 'MongoDB and Python'}).count())

    min_date = f"{min_time}T00:00:00.000Z"
    max_date = f"{max_time}T00:00:00.000Z"
    min_datetime = parser.parse(min_date)
    max_datetime = parser.parse(max_date)
    query_json = {"updated": {"$gte": min_datetime, "$lte": max_datetime}}
    x = collection.find(query_json)
    for data in x:
        items.append(data)
    print(f"MongoDB Staging | {col}: {len(items)} document(s)")


def get_bq(table, min_time, max_time):
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

    with open(os.path.join(os.path.dirname(__file__), f"sql/{table}.sql"), "r") as f:
        QUERY = f.read().format(table=table, min_time=min_time, max_time=max_time)

    query_job = client.query(QUERY)

    rows = query_job.result()
    for row in rows:
        output.append(row)
    print(
        f"BigQuery Staging | mongo.{table}_raw_changelog: {output[0][0]} record(s)"
    )


if __name__ == "__main__":
    table = "workspaces"
    min_date = "2021-10-01"
    max_date = "2021-12-14"
    print(f"Timeframe : {min_date} --> {max_date}")
    get_mongo(min_date, max_date)
    get_bq(table, min_date, max_date)
