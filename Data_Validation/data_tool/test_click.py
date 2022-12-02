import click


@click.command()
# @click.option("--count", default=1, help="Number of greetings.")
# @click.option("--name", prompt="Your name", help="The person to greet.")
# @click.option("--count", default=1)
# @click.option("--name", prompt="Your name")
@click.option("--collection", prompt="needed collection")
@click.option("--min_date", prompt="start date of timeframe")
@click.option("--max_date", prompt="end date of timeframe")

# def hello(count, name):
#     """Simple program that greets NAME for a total of COUNT times."""
#     for x in range(count):
#         click.echo(f"Hello {name}!")


def test(collection, min_date, max_date):
    click.echo(f"Collection: {collection}")
    click.echo(f"min_date: {min_date}")
    click.echo(f"max_date: {max_date}")


if __name__ == "__main__":
    test()
