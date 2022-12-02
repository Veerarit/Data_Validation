# TODO compare 2 sources
from bigquery import get_bq
from mongo import get_mongo


def comparison(table, min_date, max_date):
    bq_count = get_bq(table, min_date, max_date)
    mongo_count = get_mongo(table, min_date, max_date)
    print("mongo_count: ", mongo_count)
    print("bq_count: ", bq_count)
    output_string = ''
    if mongo_count > bq_count:
        output_string = f"Mongo > BQ: {mongo_count - bq_count} record(s)"
        print(output_string)
    elif bq_count > mongo_count:
        output_string = f"BQ > Mongo: {bq_count - mongo_count} record(s)"
        print(output_string)
    else:
        output_string = "No Diff"
        print(output_string)

    return output_string
