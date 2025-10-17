import polars as pl

# Read the Parquet file (use lazy for larger data to defer computation)
df = pl.scan_parquet("/home/fils/scratch/graph2solr/stores/files/dataset_results_sparql_grouped_augmented.parquet")  # Lazy reading

# Get distinct provider-region pairs (optional, for preview or validation)
distinct_groups = df.select(["txt_provider", "txt_regions"]).unique().collect()
print(distinct_groups)  # To inspect unique pairs

# Group by provider and region, then loop to write each group as a separate Parquet
for provider_val in distinct_groups["txt_provider"].unique().to_list():
    for region_val in distinct_groups.filter(pl.col("txt_provider") == provider_val)["txt_regions"].unique().to_list():
        # Filter the group and write as a new Parquet file
        group_df = df.filter((pl.col("txt_provider") == provider_val) & (pl.col("txt_regions") == region_val)).collect()
        output_path = f"./provider_{provider_val}_region_{region_val}.parquet"
        group_df.write_parquet(output_path)

# If you prefer a more optimized lazy approach without pre-collecting distincts:
# df = df.group_by(["provider", "region"])
# Then, in a loop (this is pseudo-code for simplicity):
# for (prov, reg), group in df:  # Polars group operations are eager here
#     group.write_parquet(f"/path/to/output/provider_{prov}_region_{reg}.parquet")
