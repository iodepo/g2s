import polars as pl

# Read the Parquet file (use lazy for larger data to defer computation)
df = pl.scan_parquet("/home/fils/scratch/graph2solr/stores/files/dataset_results_sparql_grouped_augmented.parquet")  # Lazy reading

# Get distinct provider-region pairs (optional, for preview or validation)
distinct_groups = df.select(["txt_provider", "txt_regions"]).unique().collect()
print(distinct_groups)  # To inspect unique pairs

print(len(distinct_groups))  # To inspect unique pairs

print("---"*10)

# Loop over each unique provider-region pair row to write each group as a separate Parquet
for row in distinct_groups.iter_rows():
    provider_val = row[0]  # txt_provider
    region_val = row[1]    # txt_regions (as list[str])

    print(provider_val)
    print(region_val)

    # Sort the region list and join with underscores for a clean filename
    region_str = "_".join(sorted(region_val)) if isinstance(region_val, list) else str(region_val)

    # Filter the group and write as a new Parquet file
    group_df = df.filter((pl.col("txt_provider") == provider_val) & (pl.col("txt_regions") == region_val)).collect()
    output_path = f"./provider_{provider_val}_region_{region_str}.parquet"
    group_df.write_parquet(output_path)
