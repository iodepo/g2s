import polars as pl
import os

# Define input file and output directory
INPUT_FILE = "/home/fils/scratch/graph2solr/stores/files/dataset_results_sparql_grouped_augmented.parquet"
OUTPUT_DIR = "./parquet/"

os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"Reading data from {INPUT_FILE}...")
try:
    df = pl.read_parquet(INPUT_FILE)
except Exception as e:
    print(f"Error: Could not read the Parquet file at {INPUT_FILE}.\nDetails: {e}")
    exit()

# --- Data Preparation ---

# 1. Ensure 'txt_regions' is a list of strings (decode from JSON if needed)
if df.schema.get("txt_regions") == pl.Utf8:
    print("Detected 'txt_regions' as string, decoding from JSON...")
    df = df.with_columns(
        pl.col("txt_regions")
        .str.json_decode(dtype=pl.List(pl.Utf8))
        .alias("txt_regions")
    )
# Handle cases where the column might not be a list type after reading
elif not isinstance(df.schema.get("txt_regions"), (pl.List, pl.Array)):
     print(f"Warning: 'txt_regions' is not a List or String. Type is {df.schema.get('txt_regions')}. Filling with empty lists.")
     # Replace non-list column with empty lists to avoid crashing on explode.
     df = df.with_columns(pl.lit([], dtype=pl.List(pl.Utf8)).alias("txt_regions"))


# 2. De-duplicate regions within each list before exploding
# We use map_elements as the .arr namespace seems to have issues with this dataset.
def unique_regions(region_list):
    if region_list is None:
        return []
    # Use a set to find unique regions, filtering out any Nones.
    return list(set(r for r in region_list if r is not None))

print("De-duplicating regions within each record...")
df = df.with_columns(
    pl.col("txt_regions")
    .map_elements(unique_regions, return_dtype=pl.List(pl.Utf8))
    .alias("txt_regions")
)


# 3. Explode the DataFrame on the 'txt_regions' column.
# This creates a new row for each region in a record's list.
print("Exploding DataFrame by region...")
df_exploded = df.explode("txt_regions")

# 4. Filter out records where the region is null or an empty string after exploding.
df_exploded = df_exploded.filter(pl.col("txt_regions").is_not_null() & (pl.col("txt_regions") != ""))
print(f"DataFrame has {len(df_exploded)} rows after exploding and cleaning.")


# --- Grouping and Writing ---

# 5. Group by provider and the single region value.
print("Grouping data by provider and individual region...")
grouped = df_exploded.group_by(["txt_provider", "txt_regions"])

# Count the number of distinct groups.
num_groups = grouped.agg(pl.count()).height # .agg() returns a DataFrame, get its length
print(f"Found {num_groups} distinct provider-region files to create.")
print("-" * 50)

# 6. Iterate over the groups and write each to a separate Parquet file.
for (provider, region), group_df in grouped:
    if not provider or not region:
        print(f"Skipping group with empty provider or region: ('{provider}', '{region}')")
        continue

    # Sanitize for filename
    provider_safe = str(provider).replace('/', '_').replace(' ', '_')
    region_safe = str(region).replace('/', '_').replace(' ', '_')

    filename = f"provider_{provider_safe}_region_{region_safe}.parquet"
    output_file = os.path.join(OUTPUT_DIR, filename)

    print(f"Writing {len(group_df)} rows to {output_file}...")
    group_df.write_parquet(output_file)

print("-" * 50)
print("Processing complete.")
