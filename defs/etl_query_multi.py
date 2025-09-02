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

    # Read the SPARQL query from a file
    with open("./SPARQL/q4_person.rq", "r") as file:
        query1 = file.read()

    # Send the request
    response1 = requests.post(url, params=params, headers=headers, data=query1)

    # Read the SPARQL query from a file
    with open("./SPARQL/q4_vehicle.rq", "r") as file:
        query2 = file.read()

    # Send the request
    response2 = requests.post(url, params=params, headers=headers, data=query2)

    # Load responses into separate Polars DataFrames
    df1 = pl.read_csv(StringIO(response1.text), truncate_ragged_lines=True)
    df2 = pl.read_csv(StringIO(response2.text), truncate_ragged_lines=True)

    # Concatenate the DataFrames vertically
    df = pl.concat([df1, df2], how="vertical")

    # Load response into Polars DataFrame
    # df = pl.read_csv(StringIO(response.text))
    # df = pl.read_csv(StringIO(response.text), truncate_ragged_lines=True)

    ## TEMPORAL  ----------------------------------
    # TODO move to augmentation?

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
