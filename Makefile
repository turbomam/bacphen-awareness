# MongoDB connection options:
# - Defaults: MONGO_URI=mongodb://localhost:27017, MONGO_DB=bacdive, MONGO_COLLECTION=strains
# - Override on command line or via environment variables.
# - Authentication examples:
#     make MONGO_URI="mongodb://user:pass@localhost:27017" data/bacdive_distinct_value_counts.tsv
#     make MONGO_URI="mongodb://user:pass@localhost:27017/?authSource=admin" data/bacdive_distinct_value_counts.tsv
#     make MONGO_URI="mongodb+srv://user:pass@cluster0.mongodb.net" MONGO_DB=bacdive data/bacdive_distinct_value_counts.tsv

PATH_COUNTS_FILE := data/954eac922928d7abfd6130e7cc64a88c/bacdive_strains_path_counts.txt
MONGO_URI ?= mongodb://localhost:27017
MONGO_DB ?= bacdive
MONGO_COLLECTION ?= strains

# Path to BacDive JSON dump
BACDIVE_JSON := data/954eac922928d7abfd6130e7cc64a88c/bacdive_strains.json

.PHONY: mongo/import mongo/index

# Target: Import BacDive data into MongoDB
mongo/import: $(BACDIVE_JSON)
	mongoimport --uri=$(MONGO_URI) \
	    --db=$(MONGO_DB) \
	    --collection=$(MONGO_COLLECTION) \
	    --drop \
	    --file=$< \
	    --jsonArray

# Target: Create an index for NCBI tax IDs
mongo/index:
	mongosh "$(MONGO_URI)/$(MONGO_DB)" --eval \
	    'db.$(MONGO_COLLECTION).createIndex({"General.NCBI tax id.NCBI tax id": 1})'

# Target: Generate the distinct value counts report
data/bacdive_distinct_value_counts.tsv: $(PATH_COUNTS_FILE)
	uv run bacdive-tools \
	    --mongo-uri $(MONGO_URI) \
	    --db $(MONGO_DB) \
	    --collection $(MONGO_COLLECTION) \
	    --path-counts-file $< \
	    --output $@

data/bacdive_path_counts_merged.tsv: data/bacdive_distinct_value_counts.tsv data/954eac922928d7abfd6130e7cc64a88c/bacdive_strains_path_counts.txt
	uv run merge-path-counts \
	    --path-counts-file data/954eac922928d7abfd6130e7cc64a88c/bacdive_strains_path_counts.txt \
	    --distinct-values-file data/bacdive_distinct_value_counts.tsv \
	    --output $@

# data/bacdive_distinct_value_histogram.png: data/bacdive_path_counts_merged.tsv
# 	uv run histogram-path-counts \
# 	    --merged-file $< \
# 	    --output $@

# data/bacdive_enum_values.tsv: data/bacdive_path_counts_merged.tsv
# 	uv run export-enum-values \
# 	    --merged-file $< \
# 	    --output $@

# Enum discovery outputs
ENUM_OUTPUT_PREFIX := data/bacdive

# make data/bacdive_enum_value_pairs.tsv

$(ENUM_OUTPUT_PREFIX)_enum_value_pairs.tsv \
$(ENUM_OUTPUT_PREFIX)_path_to_enum.tsv \
$(ENUM_OUTPUT_PREFIX)_decision_log.tsv: data/bacdive_path_counts_merged.tsv data/bacdive_enum_values.tsv
	uv run discover-enums \
	    --merged-file data/bacdive_path_counts_merged.tsv \
	    --values-file data/bacdive_enum_values.tsv \
	    --output-prefix $(ENUM_OUTPUT_PREFIX)
