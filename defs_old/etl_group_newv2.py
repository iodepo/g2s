import lancedb
import polars as pl

def group_mode_new(source, sink):
    # Set a smaller chunk size for streaming engine
    pl.Config.set_streaming_chunk_size(10000)  # Set appropriate chunk size [[1]](https://docs.pola.rs/py-polars/html/reference/api/polars.Config.set_streaming_chunk_size.html)
    
    # Parquet file path
    pfile = "./stores/files/results_sparql.parquet"
    
    # Scan parquet with lazy evaluation
    df = pl.scan_parquet(pfile)
    
    # Define a reusable function for unique column aggregation to avoid repetition
    def unique_agg(column_name):
        return pl.col(column_name).unique().alias(f"txt_{column_name}")
    
    # Build aggregation list
    agg_expressions = [
        pl.col("g").unique().alias("txt_g"),
        pl.col("type").min().alias("type"),
        pl.col("name").unique().alias("txt_name"),
        pl.col("description").min().alias("description"),
    ]
    
    # Add other unique aggregations
    text_columns = [
        "contributor", "description", "contenturl", "url", "temporalCoverage",
        "keywords", "courseName", "location", "iritype", "sameAs", "citation",
        "license", "includedInDataCatalog", "memberOf", "parentOrganization",
        "knowsAbout", "affiliation", "category", "vehicleConfiguration",
        "vehicleSpecialUsage", "jobTitle", "knowsLanguage", 
        "educationalCredentialAwarded", "author", "hasCourseInstance",
        "areaServed", "startDate", "endDate", "wkt", "place_name"
    ]
    
    for column in text_columns:
        agg_expressions.append(unique_agg(column))
    
    # Add min aggregations
    agg_expressions.extend([
        pl.col("version").min().alias("txt_version"),
        pl.col("geom").unique().min().alias("the_geom"),
        pl.col("lat").unique().min().alias("txt_lat"),
        pl.col("long").unique().min().alias("txt_long"),
        pl.col("datePublished").unique().min().alias("txt_datePublished"),
        pl.col("dateModified").unique().min().alias("txt_dateModified"),
    ])
    
    # Execute the query with streaming
    result = (
        df.group_by("id")
        .agg(agg_expressions)
    ).collect(streaming=True)
    
    # print("Saving Parquet")
    # # Use row groups to control memory when writing
    # result.write_parquet(
    #     "./stores/files/group_new_result.parquet",
    #     row_group_size=10000  # Control memory usage while writing [[2]](https://docs.pola.rs/docs/python/dev/reference/api/polars.LazyFrame.sink_parquet.html)
    # )
    #
    # # Create or get LanceDB table and write data
    # print("Saving Lance")
    # db = lancedb.connect("./stores/lancedb")
    # source = "sparql_results"
    # db.create_table(f"{source}_grouped", data=result, mode="overwrite")