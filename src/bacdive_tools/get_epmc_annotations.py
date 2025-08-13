import click
import requests
import time
import hashlib
from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.errors import BulkWriteError

API_URL = "https://www.ebi.ac.uk/europepmc/annotations_api/annotationsByArticleIds"

def generate_annotation_id(annotation):
    """Generate a synthetic ID for annotations without one."""
    # Create ID from article_id + exact text + section + type
    content = f"{annotation.get('article_id', '')}{annotation.get('exact', '')}{annotation.get('section', '')}{annotation.get('type', '')}"
    return hashlib.md5(content.encode()).hexdigest()

def fetch_annotations_for_chunk(article_ids):
    """Fetch annotations for a chunk of article IDs."""
    query = ",".join(article_ids)
    url = f"{API_URL}?articleIds={query}&format=json"
    
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    data = response.json()
    
    annotations = []
    for article in data:
        for ann in article.get("annotations", []):
            # Add article metadata to each annotation
            ann["article_id"] = article.get("extId")
            ann["pmcid"] = article.get("pmcid")
            ann["source"] = article.get("source")
            
            # Ensure every annotation has an ID
            if not ann.get("id"):
                ann["id"] = generate_annotation_id(ann)
                ann["synthetic_id"] = True
            
            annotations.append(ann)
    
    return annotations

@click.command()
@click.option("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB URI")
@click.option("--db", default="europepmc", help="MongoDB database name")
@click.option("--collection", default="organism_annotations", help="Target MongoDB collection for annotations")
@click.option("--source-collection", default="ijsem_articles", help="MongoDB collection containing IJSEM articles")
@click.option("--state-collection", default="annotation_state", help="MongoDB collection for processing state")
@click.option("--chunk-size", default=50, show_default=True, help="Number of articles per API request")
@click.option("--sleep", default=1, show_default=True, help="Delay between requests (seconds)")
@click.option("--from-scratch", is_flag=True, help="Start over by clearing saved progress state")
def get_annotations(mongo_uri, db, collection, source_collection, state_collection, chunk_size, sleep, from_scratch):
    """Fetch organism annotations for IJSEM articles from Europe PMC and store in MongoDB (restart-safe)."""
    client = MongoClient(mongo_uri)
    db_obj = client[db]
    annotations_coll = db_obj[collection]
    articles_coll = db_obj[source_collection]
    state_coll = db_obj[state_collection]
    
    if from_scratch:
        click.echo("⚠️  Clearing processing state and starting from scratch...")
        state_coll.delete_one({"_id": "progress"})
    
    # Ensure indexes for efficient operations
    annotations_coll.create_index([("id", ASCENDING)], unique=True)
    
    # Get all article IDs that need processing
    all_article_ids = [f"MED:{doc['pmid']}" for doc in articles_coll.find({}, {"pmid": 1}) if "pmid" in doc]
    
    # Resume from previous progress if exists
    state_doc = state_coll.find_one({"_id": "progress"})
    start_index = state_doc["value"] if state_doc else 0
    total_processed = 0
    
    click.echo(f"Found {len(all_article_ids)} articles to process")
    click.echo(f"Starting from index: {start_index}")
    
    # Process in chunks
    for i in range(start_index, len(all_article_ids), chunk_size):
        chunk = all_article_ids[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        total_chunks = (len(all_article_ids) + chunk_size - 1) // chunk_size
        
        try:
            click.echo(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} articles)")
            
            # Fetch annotations for this chunk
            annotations = fetch_annotations_for_chunk(chunk)
            
            if annotations:
                # Prepare bulk operations
                operations = []
                for ann in annotations:
                    operations.append(
                        UpdateOne(
                            {"id": ann["id"]},
                            {"$set": ann},
                            upsert=True
                        )
                    )
                
                # Execute bulk write
                try:
                    result = annotations_coll.bulk_write(operations, ordered=False)
                    inserted = result.upserted_count
                    updated = result.modified_count
                    click.echo(f"Saved {len(annotations)} annotations (inserted: {inserted}, updated: {updated})")
                    total_processed += len(annotations)
                    
                except BulkWriteError as e:
                    click.echo(f"Bulk write error: {len(e.details['writeErrors'])} failed operations")
                    # Continue processing despite errors
                    total_processed += len(annotations) - len(e.details['writeErrors'])
            else:
                click.echo("No annotations found for this chunk")
            
            # Save progress state
            state_coll.update_one(
                {"_id": "progress"}, 
                {"$set": {"value": i + chunk_size}}, 
                upsert=True
            )
            
            # Rate limiting
            time.sleep(sleep)
            
        except requests.RequestException as e:
            click.echo(f"Request failed for chunk {chunk_num}: {e}")
            click.echo("Will retry this chunk on next run")
            break
        except Exception as e:
            click.echo(f"Unexpected error in chunk {chunk_num}: {e}")
            break
    
    click.echo(f"✅ Processing complete. Total annotations processed: {total_processed}")

if __name__ == "__main__":
    get_annotations()
