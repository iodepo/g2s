from io import StringIO
from pathlib import Path

import lancedb
import polars as pl
import requests


def query_mode(source, sink, query, table):
    """Handle query mode operations"""
    print(f"Query mode: Processing data from {source} to {sink}")
    # Add query-specific logic here

    url = source
    params = {
        "timeout": "600s",
        "access-token": "odis_7643543846_6dMISzlPrD7i"
    }
    headers = {
        "Accept": "text/csv",
        "Content-type": "application/sparql-query"
    }

    # Read the SPARQL query from file
    with open(query, "r") as file:
        query = file.read()

    # Send the request
    response = requests.post(url, params=params, headers=headers, data=query)

    # Load response into Polars DataFrame
    # df = pl.read_csv(StringIO(response.text))
    df = pl.read_csv(StringIO(response.text), truncate_ragged_lines=True)

    ## TEMPORAL  ----------------------------------
    # TODO move to augmentation?

    # Method 2: Using pattern matching with coalescing (recommended)
    # df = df.with_columns(
    #     pl.when(
    #         pl.col("datePublished").str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False).is_not_null()
    #     )
    #     .then(pl.col("datePublished").str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False))
    #     .otherwise(None)
    #     .alias("datePublished")
    # )
    #
    # # Then convert back to string in your desired format
    # df = df.with_columns(
    #     pl.when(pl.col("datePublished").is_not_null())
    #     .then(pl.col("datePublished").dt.strftime("%Y-%m-%d"))  # or any format you want
    #     .otherwise(None)
    #     .alias("datePublished")
    # )

    # -----------------------------------------------------


    df = df.with_columns(
        # Step 1: Convert string to datetime with auto-detection of ISO format
        pl.col("datePublished").str.to_datetime(format=None, strict=False).alias("datePublished")
    )

    # Step 2: Convert datetime back to string in desired format
    df = df.with_columns(
        pl.when(pl.col("datePublished").is_not_null())
        .then(pl.col("datePublished").dt.strftime("%Y-%m-%d"))  # You can use any format here
        .otherwise(pl.col("datePublished").dt.strftime("0000-01-01"))  # was otherwise(None)
        .alias("datePublished")
    )

    df = df.with_columns(
        # Step 1: Convert string to datetime with auto-detection of ISO format
        pl.col("dateModified").str.to_datetime(format=None, strict=False).alias("dateModified")
    )

    # Step 2: Convert datetime back to string in desired format
    df = df.with_columns(
        pl.when(pl.col("dateModified").is_not_null())
        .then(pl.col("dateModified").dt.strftime("%Y-%m-%d"))  # You can use any format here
        .otherwise(pl.col("dateModified").dt.strftime("0000-01-01"))  # was None
        .alias("dateModified")
    )


    # -----------------------------------------------------
    # df = df.with_columns(
    #     pl.when(
    #         pl.col("dateModified").str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False).is_not_null()
    #     )
    #     .then(pl.col("dateModified").str.strptime(pl.Datetime, format="%Y-%m-%d", strict=False))
    #     .otherwise(None)
    #     .alias("dateModified")
    # )
    #
    # # Then convert back to string in your desired format
    # df = df.with_columns(
    #     pl.when(pl.col("dateModified").is_not_null())
    #     .then(pl.col("dateModified").dt.strftime("%Y-%m-%d"))  # or any format you want
    #     .otherwise(None)
    #     .alias("dateModified")
    # )

    # Save to CSV
    # print(f"Saving CSV")
    # df.write_csv(sink)

    # Save to parquet
    # TODO:  just check if file_path is .csv or .parquet and save accordingly
    print("Saving to file: ", sink)
    df.write_parquet(sink)

    print(len(df))

    # Create or get LanceDB table and write data
    print("Saving LanceDB table: ", table, "")
    db = lancedb.connect("./stores/lancedb")
    tbl = db.create_table(table, data=df, mode="overwrite")
    print(tbl)
