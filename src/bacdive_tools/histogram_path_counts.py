import click
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FixedLocator

@click.command()
@click.option("--merged-file", required=True, type=click.Path(exists=True),
              help="Path to merged TSV file with path_count and distinct_value_count.")
@click.option("--output", default="data/bacdive_distinct_value_histogram.png",
              help="Output PNG file for histogram.")
@click.option("--bins", default=50, type=int,
              help="Number of bins for the histogram (default: 50).")
def histogram(merged_file, output, bins):
    """
    Generate a histogram of distinct value counts using a log-transformed X-axis
    with scientific notation labels.
    """
    # Load data
    df = pd.read_csv(merged_file, sep="\t")

    if "distinct_value_count" not in df.columns:
        raise click.ClickException("The file must contain a 'distinct_value_count' column.")

    # Log transform for plotting (log1p handles zero safely)
    df["log_distinct_value_count"] = np.log1p(df["distinct_value_count"])

    # Plot histogram
    plt.figure(figsize=(10, 6))
    plt.hist(df["log_distinct_value_count"], bins=bins, edgecolor="black")
    plt.ylabel("Number of Paths")
    plt.xlabel("Distinct Value Count")
    plt.title("Histogram of Distinct Value Counts (Log-Compressed X-axis)")
    plt.yscale("log")  # Log scale for Y-axis for better visualization
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Custom ticks in scientific notation
    ax = plt.gca()
    max_value = df["distinct_value_count"].max()

    # Generate ticks for powers of 10 up to max_value
    raw_ticks = [10**i for i in range(0, int(np.log10(max_value)) + 1)]
    raw_ticks = [1] + raw_ticks  # ensure 1 is included

    log_ticks = np.log1p(raw_ticks)
    ax.set_xticks(log_ticks)
    ax.set_xticklabels([f"{t:.0e}" for t in raw_ticks])  # scientific notation

    plt.savefig(output, dpi=150)
    click.echo(f"✅ Histogram saved to {output}")

if __name__ == "__main__":
    histogram()
