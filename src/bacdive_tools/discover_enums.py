import re
import click
import pandas as pd
from pymongo import MongoClient

# ---------- Heuristic Thresholds ----------
MIN_ENUM_VALUES = 2
MAX_ENUM_VALUES = 15
NUMERIC_RATIO_THRESHOLD = 0.5
LONG_TEXT_THRESHOLD = 50  # characters
LONG_TEXT_RATIO_THRESHOLD = 0.3

# ---------- Utility Functions ----------

def is_numeric(value):
    """Check if a value is numeric-like."""
    if pd.isna(value):
        return False
    return bool(re.fullmatch(r"^-?\d+(\.\d+)?$", str(value).strip()))

def is_long_text(value):
    """Check if a value looks like long text or non-biological metadata."""
    if pd.isna(value):
        return False
    text = str(value)
    if len(text) > LONG_TEXT_THRESHOLD:
        return True
    if any(sym in text for sym in ["©", "http", "://"]):
        return True
    return False

def extract_lineage(path):
    """Get lineage (all parent levels except last)."""
    return ".".join(path.split(".")[:-1])

def merge_scalar_and_list_paths(df):
    """Merge paths like foo.bar and foo.bar.[] into one logical path group."""
    canonical_map = {}
    for path in df["path"]:
        if path.endswith(".[]"):
            canonical_map[path] = path[:-3]
        else:
            canonical_map[path] = path
    df["canonical_path"] = df["path"].map(canonical_map)
    return df

@click.command()
@click.option("--mongo-uri", default="mongodb://localhost:27017",
              help="MongoDB connection URI.")
@click.option("--db", default="bacdive", help="MongoDB database name.")
@click.option("--collection", default="strains", help="MongoDB collection name.")
@click.option("--merged-file", type=click.Path(exists=True), required=True,
              help="TSV file with path_count and distinct_value_count.")
@click.option("--values-file", type=click.Path(exists=True), required=True,
              help="TSV file with path and value pairs.")
@click.option("--output-prefix", default="data/bacdive_enum_discovery",
              help="Prefix for output files (TSV).")
def discover_enums(mongo_uri, db, collection, merged_file, values_file, output_prefix):
    """
    Discover candidate enums from BacDive data with context-aware rules.
    Produces:
      1. *_enum_value_pairs.tsv
      2. *_path_to_enum.tsv
      3. *_decision_log.tsv
      4. *_enum_merge_log.tsv
    """
    # Load data
    merged = pd.read_csv(merged_file, sep="\t")
    values = pd.read_csv(values_file, sep="\t")

    # Merge scalar/list paths
    merged = merge_scalar_and_list_paths(merged)
    values = merge_scalar_and_list_paths(values)

    # Filter initial candidates (2+ values baseline)
    candidates = merged[(merged["distinct_value_count"] >= MIN_ENUM_VALUES)]

    # Map values per canonical path
    values_grouped = values.groupby("canonical_path")["value"].apply(list).to_dict()

    # Prepare decision log and outputs
    decision_log = []
    enum_value_pairs = []
    path_to_enum = []

    # Build lineage map
    candidates["lineage"] = candidates["canonical_path"].apply(extract_lineage)
    lineage_groups = candidates.groupby("lineage")

    enum_counter = 1

    for lineage, group in lineage_groups:
        siblings = list(group["canonical_path"].unique())
        sibling_values = {s: values_grouped.get(s, []) for s in siblings}

        sibling_enum_flags = {}
        sibling_value_counts = {}

        for path, vals in sibling_values.items():
            vals = [v for v in vals if pd.notna(v)]
            if not vals:
                sibling_enum_flags[path] = False
                continue

            numeric_ratio = sum(is_numeric(v) for v in vals) / len(vals)
            long_text_ratio = sum(is_long_text(v) for v in vals) / len(vals)

            if numeric_ratio > NUMERIC_RATIO_THRESHOLD:
                sibling_enum_flags[path] = False
                decision_log.append([path, "exclude", "numeric-dominated", lineage])
                continue

            if long_text_ratio > LONG_TEXT_RATIO_THRESHOLD:
                sibling_enum_flags[path] = False
                decision_log.append([path, "exclude", "long-text-non-biological", lineage])
                continue

            sibling_value_counts[path] = len(set(vals))
            sibling_enum_flags[path] = True

        include_paths = [p for p, flag in sibling_enum_flags.items() if flag]
        if not include_paths:
            continue

        if any(sibling_value_counts.get(p, 0) > MAX_ENUM_VALUES for p in include_paths):
            majority_below_limit = sum(v <= MAX_ENUM_VALUES for v in sibling_value_counts.values()) > len(sibling_value_counts) / 2
            if majority_below_limit:
                decision_log.append([lineage, "include_override", "lineage-override", lineage])
            else:
                include_paths = [p for p in include_paths if sibling_value_counts.get(p, 0) <= MAX_ENUM_VALUES]

        enum_id = f"Enum_{enum_counter:04d}"
        enum_counter += 1

        all_enum_values = set()
        for p in include_paths:
            all_enum_values |= set(values_grouped.get(p, []))

        for val in sorted(v for v in all_enum_values if pd.notna(v)):
            enum_value_pairs.append([enum_id, val])

        for p in include_paths:
            path_to_enum.append([p, enum_id])
            decision_log.append([p, "include", "categorical", lineage])

    # -----------------------------------
    # Deduplicate enums by value set
    # -----------------------------------
    click.echo("🔍 Deduplicating enums with identical value sets...")

    enum_to_values = (
        pd.DataFrame(enum_value_pairs, columns=["enum", "value"])
        .groupby("enum")["value"]
        .apply(lambda vals: frozenset(vals))
        .to_dict()
    )

    value_set_to_enum = {}
    enum_remap = {}
    canonical_counter = 1

    for enum_id, value_set in enum_to_values.items():
        if value_set not in value_set_to_enum:
            canonical_enum = f"Enum_{canonical_counter:04d}"
            value_set_to_enum[value_set] = canonical_enum
            canonical_counter += 1
        enum_remap[enum_id] = value_set_to_enum[value_set]

    for row in enum_value_pairs:
        row[0] = enum_remap[row[0]]

    for row in path_to_enum:
        row[1] = enum_remap[row[1]]

    merged_enums = {old: new for old, new in enum_remap.items() if old != new}
    merge_log = pd.DataFrame(list(merged_enums.items()), columns=["old_enum", "new_enum"])

    if not merge_log.empty:
        click.echo(f"✅ Deduplicated {len(merged_enums)} enums into canonical IDs")
    else:
        click.echo("✅ No duplicate enums found")

    # Write outputs
    enum_values_path = f"{output_prefix}_enum_value_pairs.tsv"
    path_to_enum_path = f"{output_prefix}_path_to_enum.tsv"
    decision_log_path = f"{output_prefix}_decision_log.tsv"
    merge_log_path = f"{output_prefix}_enum_merge_log.tsv"

    pd.DataFrame(enum_value_pairs, columns=["enum", "value"]).drop_duplicates().to_csv(enum_values_path, sep="\t", index=False)
    pd.DataFrame(path_to_enum, columns=["path", "enum"]).drop_duplicates().to_csv(path_to_enum_path, sep="\t", index=False)
    pd.DataFrame(decision_log, columns=["path", "decision", "reason", "lineage"]).to_csv(decision_log_path, sep="\t", index=False)
    merge_log.to_csv(merge_log_path, sep="\t", index=False)

    click.echo(f"✅ Enum discovery complete")
    click.echo(f"  - Enums & values: {enum_values_path}")
    click.echo(f"  - Path-to-enum: {path_to_enum_path}")
    click.echo(f"  - Decision log: {decision_log_path}")
    click.echo(f"  - Merge log: {merge_log_path}")
