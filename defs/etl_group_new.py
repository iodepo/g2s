import lancedb
import polars as pl

def group_mode_new(source, sink):
    # Parquet file path
    pfile = "./stores/files/results_sparql.parquet"

    # Get column names from the parquet schema to ensure we only load what we need
    schema = pl.read_parquet_schema(pfile)
    all_columns = list(schema.keys())

    # Scan parquet with specific columns instead of loading everything
    df = pl.scan_parquet(pfile)

    # Define a reusable function for unique column aggregation to avoid repetition
    def unique_agg(column_name):
        return pl.col(column_name).unique().alias(f"txt_{column_name}")

    # Build aggregation expressions, avoiding redundant operations
    agg_expressions = [
        pl.col("g").unique().alias("txt_g"),
        pl.col("type").min().alias("type"),
        unique_agg("name"),  # Only create it once, will reference as txt_name
    ]

    # Add other unique text columns using a loop to reduce code repetition
    text_columns = [
        "description", "contributor", "contenturl", "url", "temporalCoverage", "provider",
        "variableMeasured", "keywords", "courseName", "location", "iritype", "sameAs", "citation",
        "license", "includedInDataCatalog", "memberOf", "parentOrganization",
        "knowsAbout", "affiliation", "category", "vehicleConfiguration",
        "vehicleSpecialUsage", "jobTitle", "knowsLanguage",
        "educationalCredentialAwarded", "author", "hasCourseInstance",
        "areaServed", "startDate", "endDate", "wkt", "place_name"
    ]

    for column in text_columns:
        if column == "description":
            # Special case for description as it's both min and unique
            agg_expressions.append(pl.col("description").min().alias("description"))

        # Add unique aggregation for all text columns
        if column in all_columns:  # Only if column exists in schema
            agg_expressions.append(unique_agg(column))

    # Add min aggregations
    min_columns = ["version", "geom", "lat", "long", "datePublished", "dateModified"]
    for column in min_columns:
        if column in all_columns:
            if column == "version":
                agg_expressions.append(pl.col(column).min().alias(f"txt_{column}"))
            elif column == "geom":
                agg_expressions.append(pl.col(column).unique().min().alias("the_geom"))
            else:
                agg_expressions.append(pl.col(column).unique().min().alias(f"txt_{column}"))

    # Execute the query with the new streaming engine and appropriate chunk size
    result = (
        df.group_by("id")
        .agg(agg_expressions)
    ).collect(streaming=True)

    # TODO  remove all rows where name is empty, NONE or NONE
    # Filter out null values first
    result = result.filter(pl.col("txt_name").is_not_null())

    # Iterate through columns and apply drop_nulls to list type columns
    drop_null_expressions = []
    for col_name in result.columns:
        if isinstance(result[col_name].dtype, pl.List):
            drop_null_expressions.append(pl.col(col_name).list.drop_nulls().alias(col_name))

    if drop_null_expressions:
        result = result.with_columns(drop_null_expressions)

    print("Saving Parquet")
    result.write_parquet("./stores/files/group_new_result.parquet")

    # Create or get LanceDB table and write data
    print("Saving Lance")
    db = lancedb.connect("./stores/lancedb")
    source = "sparql_results"
    db.create_table(f"{source}_grouped", data=result, mode="overwrite")


