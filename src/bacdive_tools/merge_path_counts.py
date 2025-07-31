import csv
import click
import pandas as pd

@click.command()
@click.option("--path-counts-file", required=True, type=click.Path(exists=True),
              help="Path to the original path counts text file (count + path).")
@click.option("--distinct-values-file", required=True, type=click.Path(exists=True),
              help="Path to the distinct value counts TSV file.")
@click.option("--output", default="data/bacdive_path_counts_merged.tsv",
              help="Output TSV file path.")
def merge(path_counts_file, distinct_values_file, output):
    """
    Merge path counts with distinct value counts into one TSV file.
    """

    # Read path counts manually to handle paths with spaces
    path_counts_data = []
    with open(path_counts_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(maxsplit=1)
            if len(parts) == 2:
                count, path = parts
                path_counts_data.append({"path_count": int(count), "path": path})

    path_counts = pd.DataFrame(path_counts_data)
    path_counts["order"] = range(len(path_counts))  # Preserve original order

    # Load distinct value counts
    distinct_counts = pd.read_csv(
        distinct_values_file,
        sep="\t",
        header=0
    )

    # Merge on path
    merged = pd.merge(path_counts, distinct_counts, on="path", how="left")
    merged = merged.sort_values("order").drop(columns=["order"])

    # Fill missing distinct values with 0
    merged["distinct_value_count"] = merged["distinct_value_count"].fillna(0).astype(int)

    # Write merged TSV
    merged.to_csv(output, sep="\t", index=False)

    click.echo(f"✅ Merged file written to {output}")

if __name__ == "__main__":
    merge()
