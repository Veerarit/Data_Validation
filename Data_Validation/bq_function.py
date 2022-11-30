from google.cloud import bigquery
from google.oauth2 import service_account
import os


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
    print(output[0][0])


if __name__ == "__main__":
    table = "deleted_tasks"
    min_time = "2013-01-01"
    max_time = "2013-12-31"
    get_bq(table, min_time, max_time)
