import csv
import time
import click
from pymongo import MongoClient

def parse_path_counts_file(path_counts_file):
    """Parse a whitespace or tab-separated path-counts file into a list of dicts."""
    paths = []
    with open(path_counts_file) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(maxsplit=1)
            if len(parts) == 2:
                count, path = parts
                paths.append({"count": int(count), "path": path})
    return paths

def build_field_expression(path):
    """
    Build a MongoDB field expression using $getField to support special characters.
    Arrays [] are handled outside this function.
    """
    parts = [p for p in path.replace("[]", ".").split(".") if p]

    expr = "$" + parts[0]
    for part in parts[1:]:
        expr = {"$getField": {"field": part, "input": expr}}
    return expr

def count_distinct_for_path(collection, path):
    """Count distinct values for a MongoDB path, handling arrays and special characters."""
    pipeline = []

    # Handle arrays
    array_parts = path.split("[]")
    for part in array_parts[:-1]:  # unwind for each array except last
        cleaned = part.strip(".")
        if cleaned:
            pipeline.append({
                "$unwind": {
                    "path": f"${cleaned}",
                    "preserveNullAndEmptyArrays": False
                }
            })

    # Build field expression
    field_expr = build_field_expression(path)

    # Match docs where the field is not null
    pipeline.append({
        "$match": {
            "$expr": {"$ne": [field_expr, None]}
        }
    })

    # Group by field value and count distinct
    pipeline.append({"$group": {"_id": field_expr}})
    pipeline.append({"$count": "distinct_count"})

    result = list(collection.aggregate(pipeline))
    return result[0]["distinct_count"] if result else 0

@click.command()
@click.option("--mongo-uri", default="mongodb://localhost:27017",
              help="MongoDB connection URI (default: mongodb://localhost:27017).")
@click.option("--db", default="bacdive", help="MongoDB database name (default: bacdive).")
@click.option("--collection", default="strains", help="MongoDB collection name (default: strains).")
@click.option("--path-counts-file", type=click.Path(exists=True), required=True,
              help="Path to a text file with two columns: count and path.")
@click.option("--output", default="data/bacdive_distinct_value_counts.tsv",
              help="Output TSV file path (default: data/bacdive_distinct_value_counts.tsv).")
@click.option("--min-count", default=1, type=int,
              help="Minimum count threshold from path-counts file (default: 1).")
def cli(mongo_uri, db, collection, path_counts_file, output, min_count):
    """
    Generate a TSV report of distinct value counts for each path in the provided path-counts file.
    """
    click.echo(f"Connecting to {mongo_uri}/{db}.{collection}")

    client = MongoClient(mongo_uri)
    coll = client[db][collection]

    # Parse paths
    path_counts = parse_path_counts_file(path_counts_file)
    paths = [p["path"] for p in path_counts if p["count"] >= min_count]

    click.echo(f"Processing {len(paths)} paths (min_count={min_count})...\n")

    with open(output, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["path", "distinct_value_count"])

        for i, path in enumerate(paths, start=1):
            start_time = time.time()
            try:
                count = count_distinct_for_path(coll, path)
                elapsed = time.time() - start_time
                writer.writerow([path, count])
                click.echo(f"[{i}/{len(paths)}] {path}: {count} distinct values (took {elapsed:.2f}s)")
            except Exception as e:
                click.echo(f"⚠️ Skipped {path} due to error: {e}")

    click.echo(f"\n✅ Report written to {output}")
