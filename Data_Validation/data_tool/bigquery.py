from dateutil import parser
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os


with open(os.path.join(os.path.dirname(__file__), f"collections.json"), "r") as f:
    COLLECTIONS_CONFIG = json.load(f)


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
        id = COLLECTIONS_CONFIG.get(f"{table}")
        QUERY = f.read().format(
            table=table, id=id, min_date=min_date, max_date=max_date
        )

    query_job = client.query(QUERY)

    rows = query_job.result()
    for row in rows:
        output.append(row)
        
    return output[0][0]
