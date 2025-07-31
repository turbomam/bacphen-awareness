import csv
import click
from pymongo import MongoClient

def build_field_expression(path):
    """Build MongoDB field expression using $getField for special characters."""
    parts = [p for p in path.replace("[]", ".").split(".") if p]
    expr = "$" + parts[0]
    for part in parts[1:]:
        expr = {"$getField": {"field": part, "input": expr}}
    return expr

def extract_unique_values(collection, path):
    """Return unique values for a MongoDB path using aggregation."""
    pipeline = []
    array_parts = path.split("[]")
    for part in array_parts[:-1]:
        cleaned = part.strip(".")
        if cleaned:
            pipeline.append({"$unwind": {"path": f"${cleaned}", "preserveNullAndEmptyArrays": False}})
    field_expr = build_field_expression(path)
    pipeline.append({"$match": {"$expr": {"$ne": [field_expr, None]}}})
    pipeline.append({"$group": {"_id": field_expr}})
    results = list(collection.aggregate(pipeline))
    return [r["_id"] for r in results if r["_id"] is not None]

@click.command()
@click.option("--mongo-uri", default="mongodb://localhost:27017",
              help="MongoDB connection URI.")
@click.option("--db", default="bacdive", help="MongoDB database name.")
@click.option("--collection", default="strains", help="MongoDB collection name.")
@click.option("--merged-file", type=click.Path(exists=True), required=True,
              help="TSV file with path_count and distinct_value_count.")
@click.option("--output", default="data/bacdive_enum_values.tsv",
              help="Output TSV file for path-value pairs.")
def export_enum_values(mongo_uri, db, collection, merged_file, output):
    """Export unique values for paths with 2–15 distinct values into a TSV."""
    import pandas as pd
    client = MongoClient(mongo_uri)
    coll = client[db][collection]

    # Read merged path counts
    df = pd.read_csv(merged_file, sep="\t")
    paths = df[(df["distinct_value_count"] >= 2) & (df["distinct_value_count"] <= 15)]["path"].tolist()

    click.echo(f"Processing {len(paths)} paths (2–15 unique values)")

    with open(output, "w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["path", "value"])

        for path in paths:
            try:
                values = extract_unique_values(coll, path)
                for v in values:
                    writer.writerow([path, v])
                click.echo(f"{path}: {len(values)} values")
            except Exception as e:
                click.echo(f"⚠️ Skipped {path} due to error: {e}")

    click.echo(f"✅ Export complete: {output}")

if __name__ == "__main__":
    export_enum_values()
