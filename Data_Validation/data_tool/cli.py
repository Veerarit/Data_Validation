#TODO import results from mongo and bigquery
import click
from compare import comparison


@click.command()
@click.option("--collection", prompt="required collection")
@click.option("--min_date", prompt="start date of timeframe")
@click.option("--max_date", prompt="end date of timeframe")


def run(collection, min_date, max_date):
    print(f"Timeframe : {min_date} --> {max_date}")
    comparison(collection, min_date, max_date)


if __name__ == '__main__':
    run()


#TODO print out the whole 24hrs period compare hour by hour
#TODO create a table and storing the data into
#TODO migrate to prefect 

# util.mongo.



# # def test_24hr_earlier(selected_date):
#     selected_date.strftime("%Y-%m-%d, %H:%M:%S")
#     return (
#         (datetime(int(selected_date)) - timedelta(hours=24))
#         .replace(minute=0, second=0, microsecond=000000)
#         .isoformat()
#     )

# def get_max_time(p_max_time):
#     if p_max_time:
#         return pendulum.parser.parse(p_max_time).isoformat()
#     return (
#         (datetime.utcnow() - timedelta(hours=24))
#         .replace(minute=23, second=59, microsecond=999999)
#         .isoformat()
#     )