import click
import csv
from pymongo import MongoClient

def build_field_expression(path):
    """Build MongoDB field expression using $getField for special characters."""
    parts = [p for p in path.replace("[]", ".").split(".") if p]
    expr = "$" + parts[0]
    for part in parts[1:]:
        expr = {"$getField": {"field": part, "input": expr}}
    return expr

def tabulate_path_values(collection, path):
    """Return tabulated values for a MongoDB path using aggregation."""
    pipeline = []
    
    # Handle array unwinding for paths with []
    array_parts = path.split("[]")
    for part in array_parts[:-1]:
        cleaned = part.strip(".")
        if cleaned:
            pipeline.append({"$unwind": {"path": f"${cleaned}", "preserveNullAndEmptyArrays": False}})
    
    # Build field expression and group by value with counts
    field_expr = build_field_expression(path)
    pipeline.extend([
        {"$match": {"$expr": {"$ne": [field_expr, None]}}},
        {"$group": {"_id": field_expr, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])
    
    results = list(collection.aggregate(pipeline))
    return [(r["_id"], r["count"]) for r in results if r["_id"] is not None]

@click.command()
@click.option("--mongo-uri", default="mongodb://localhost:27017",
              help="MongoDB connection URI.")
@click.option("--db", default="bacdive", help="MongoDB database name.")
@click.option("--collection", default="strains", help="MongoDB collection name.")
@click.option("--path", required=True,
              help="MongoDB path to tabulate (e.g., '[].Culture and growth conditions.culture medium.growth')")
@click.option("--output", help="Output TSV file (if not specified, prints to console)")
def main(mongo_uri, db, collection, path, output):
    """Tabulate unique values and their counts for a given MongoDB path."""
    client = MongoClient(mongo_uri)
    coll = client[db][collection]
    
    click.echo(f"Tabulating values for path: {path}")
    
    try:
        value_counts = tabulate_path_values(coll, path)
        
        if not value_counts:
            click.echo("No values found for this path.")
            return
        
        total_count = sum(count for _, count in value_counts)
        
        if output:
            # Write to TSV file
            with open(output, 'w', newline='') as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerow(['value', 'count', 'percentage'])
                for value, count in value_counts:
                    percentage = (count / total_count) * 100
                    writer.writerow([value, count, f"{percentage:.1f}"])
            click.echo(f"✅ Results saved to {output}")
            click.echo(f"Found {len(value_counts)} unique values ({total_count} total occurrences)")
        else:
            # Print to console
            click.echo(f"\nFound {len(value_counts)} unique values ({total_count} total occurrences):\n")
            
            max_value_width = max(len(str(value)) for value, _ in value_counts)
            header = f"{'Value':<{max_value_width}} | {'Count':>8} | {'%':>6}"
            click.echo(header)
            click.echo("-" * len(header))
            
            for value, count in value_counts:
                percentage = (count / total_count) * 100
                click.echo(f"{str(value):<{max_value_width}} | {count:>8} | {percentage:>5.1f}%")
            
    except Exception as e:
        click.echo(f"⚠️ Error processing path '{path}': {e}")

if __name__ == "__main__":
    main()