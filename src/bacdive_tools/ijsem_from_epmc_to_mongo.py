import requests
import time
import click
from pymongo import MongoClient

BASE_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
QUERY = 'JOURNAL:"International Journal of Systematic and Evolutionary Microbiology"'


@click.command()
@click.option("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB URI")
@click.option("--db", default="europepmc", help="MongoDB database name")
@click.option("--collection", default="ijsem_articles", help="MongoDB collection for articles")
@click.option("--state-collection", default="ijsem_state", help="MongoDB collection for pagination state")
@click.option("--page-size", default=1000, show_default=True, help="Number of records per page")
@click.option("--sleep", default=1, show_default=True, help="Delay between requests (seconds)")
@click.option("--from-scratch", is_flag=True, help="Start over by clearing saved cursor state")
def fetch_articles(mongo_uri, db, collection, state_collection, page_size, sleep, from_scratch):
    """Fetch IJSEM article metadata from Europe PMC and store in MongoDB (restart-safe)."""
    client = MongoClient(mongo_uri)
    db_obj = client[db]
    articles = db_obj[collection]
    state = db_obj[state_collection]

    if from_scratch:
        click.echo("⚠️  Clearing cursor state and starting from scratch...")
        state.delete_one({"_id": "cursor"})

    # Resume from previous cursor if exists
    state_doc = state.find_one({"_id": "cursor"})
    cursor = state_doc["value"] if state_doc else "*"
    total_downloaded = 0

    click.echo(f"Starting download from cursor: {cursor}")

    while True:
        params = {
            "query": QUERY,
            "format": "json",
            "pageSize": page_size,
            "sort_date": "y",
            "cursorMark": cursor,
        }

        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        results = data.get("resultList", {}).get("result", [])
        if not results:
            click.echo("No more results.")
            break

        # Insert/update articles
        for article in results:
            key = {"id": article.get("id")}
            articles.update_one(key, {"$set": article}, upsert=True)

        total_downloaded += len(results)
        click.echo(f"Downloaded {len(results)} articles, total {total_downloaded}")

        # Save new cursor for restart safety
        new_cursor = data.get("nextCursorMark")
        if not new_cursor or new_cursor == cursor:
            click.echo("Reached the end of results.")
            break

        cursor = new_cursor
        state.update_one({"_id": "cursor"}, {"$set": {"value": cursor}}, upsert=True)

        time.sleep(sleep)

    click.echo("✅ Download complete")


if __name__ == "__main__":
    fetch_articles()
