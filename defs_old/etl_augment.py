import lancedb
import numpy as np
import pandas as pd
import polars as pl
import hashlib

# from defs import graphshapers
from defs import regionFor
from defs import spatial
from datetime import date
from datetime import datetime
import re

def compute_md5(value):
    if value is None:
        return None
    return hashlib.md5(value.encode()).hexdigest()

def augment_mode(source):
    print(f"Augment mode: Processing data from lancedb table {source} to a file")

    # source = "sparql_results_grouped"
    dblocation = "./stores/lancedb"
    table_name = source

    # Connect to LanceDB
    db = lancedb.connect(dblocation)
    table = db[table_name]

    df = pl.from_arrow(table.to_arrow())

    print("DataFrame columns:", df.columns)

    ## Some basic columns add

    # In Polars, you'd write it like this:
    df = df.with_columns([
        pl.col('id').map_elements(compute_md5).alias('index_id'),
        pl.lit(date.today().isoformat()).alias('indexed_ts'),
        pl.lit('{"@context": "https://schema.org/", "@type": "CreativeWork", "name": "OIH document" }').alias('json_source')  # TODO fix this
    ])


    ## TEMPORAL Section ---------------------------------------------------------------------------------------

    def create_iso_date_expr(col_expr: pl.Expr) -> pl.Expr:
        """
        Creates a Polars expression to parse a string column into a Date type.

        This function is designed to replace the original to_iso_date by
        using Polars' internal, optimized string parsing capabilities. It's
        significantly faster than a Python UDF (apply).

        Parameters:
        col_expr (pl.Expr): A Polars expression representing the column to parse.

        Returns:
        pl.Expr: A Polars expression that converts the string to a pl.Date object.
        """
        # Common date formats to try for parsing
        date_formats = [
            "%Y-%m-%d",    # 2023-12-31
            "%m/%d/%Y",    # 12/31/2023
            "%d/%m/%Y",    # 31/12/2023
            "%d-%m-%Y",    # 31-12-2023
            "%m-%d-%Y",    # 12-31-2023
            "%Y/%m/%d",    # 2023/12/31
            "%d.%m.%Y",    # 31.12.2023
            "%B %d, %Y",   # December 31, 2023
            "%d %B %Y",    # 31 December 2023
            "%Y%m%d",      # 20231231
        ]

        # Expression to handle 'YYYY' year-only strings by converting to 'YYYY-01-01'
        processed_expr = (
            pl.when(col_expr.str.strip_chars().str.len_chars() == 4)
            .then(col_expr.str.strip_chars() + "-01-01")
            .otherwise(col_expr)
        )

        # Create a list of parsing expressions, one for each format
        parse_expressions = [
            processed_expr.str.strptime(pl.Date, format=fmt, strict=False)
            for fmt in date_formats
        ]

        # Use coalesce to pick the first successful parse for each row
        return pl.coalesce(parse_expressions)

        # Use strptime with multiple formats. It will return null for unparseable dates.
        # return processed_expr.str.strptime(pl.Date, formats=date_formats, strict=False)


    # Process temporalCoverage if the column exists
    if "txt_temporalCoverage" in df.columns:
        print("Processing txt_temporalCoverage")

        # First, let's examine the data structure
        print("Sample txt_temporalCoverage values:")
        sample_values = df.select("txt_temporalCoverage").head(5)
        print(sample_values)
        print("Column dtype:", df.select("txt_temporalCoverage").dtypes[0])

        # Define helper functions to safely extract temporal data
        def safe_extract_first_element(lst):
            """Safely extract first element from list"""
            if lst is None or len(lst) == 0:
                return None
            return lst[0]

        def safe_split_temporal(temporal_str):
            """Safely split temporal string into start and end dates"""
            if temporal_str is None:
                return None, None

            if "/" in temporal_str:
                parts = temporal_str.split("/")
                start = parts[0] if len(parts) > 0 else None
                end = parts[1] if len(parts) > 1 else None
                return start, end
            else:
                # No slash, treat whole string as start date
                return temporal_str, None

        # Step 1: Extract first element from list using map_elements
        df = df.with_columns([
            pl.col("txt_temporalCoverage")
            .map_elements(safe_extract_first_element, return_dtype=pl.String)
            .alias('temporal_coverage_str')
        ])

        # Step 2: Split into start and end date strings
        temporal_split = df.select("temporal_coverage_str").with_columns([
            pl.col("temporal_coverage_str")
            .map_elements(
                lambda x: safe_split_temporal(x)[0],
                return_dtype=pl.String
            ).alias('start_date_str'),

            pl.col("temporal_coverage_str")
            .map_elements(
                lambda x: safe_split_temporal(x)[1],
                return_dtype=pl.String
            ).alias('end_date_str')
        ])

        # Add the split columns back to main dataframe
        df = df.with_columns([
            temporal_split.select("start_date_str").to_series(),
            temporal_split.select("end_date_str").to_series()
        ])

        # Step 3: Parse the dates using your existing function
        start_date_expr = create_iso_date_expr(pl.col("start_date_str"))
        end_date_expr = create_iso_date_expr(pl.col("end_date_str"))

        df = df.with_columns([
            # start_date_expr.alias('dt_startDate'),   # startDate and endDate seem to move through OK, it's the just the years I need.
            # end_date_expr.alias('dt_endDate'),
            start_date_expr.dt.year().cast(pl.String).alias('n_startYear'),
            end_date_expr.dt.year().cast(pl.String).alias('n_endYear')
        ])

        # Clean up intermediate columns if desired
        df = df.drop(['start_date_str', 'end_date_str'])

    else:
        print("NOTE: no temporal data found")

    ## SPATIAL Geometry section --------------------------------------------------------------------------------

    # Convert the column to a NumPy array
    filteredgeom_array = df["the_geom"].to_numpy()

    bool_terms = ["has_geom"]
    for term in bool_terms:
        print(f"Processing {term}")
        # Process the data using spatial.gj with error handling
        has_geom_values = ['true' if not pd.isna(x) else 'false' for x in filteredgeom_array]

        # Ensure the array length matches the DataFrame length and add back in
        if len(has_geom_values) != len(df):
            raise ValueError(f"Length mismatch: {term} does not match DataFrame length")

        df = df.with_columns(
            pl.Series(term, has_geom_values)
        )

    flt_terms = ["area", "length"]
    for term in flt_terms:
        print(f"Processing {term}")
        # Process the data using spatial.gj with error handling
        geoprocess_array = np.array([
            spatial.gj(str(x), term) if x is not None else np.nan
            for x in filteredgeom_array
        ], dtype=np.float64)

        # Ensure the array length matches the DataFrame length and add back in
        if len(geoprocess_array) != len(df):
            raise ValueError(f"Length mismatch: {term} does not match DataFrame length")

        df = df.with_columns(
            pl.Series(term, geoprocess_array)
        )

    str_terms = ["centroid", "wkt", "geojson"]
    for term in str_terms:
        print(f"Processing {term}")
        # Process the data using spatial.gj with error handling
        geoprocess_array = np.array([
            spatial.gj(str(x), term) if x is not None else np.nan
            for x in filteredgeom_array
        ], dtype=np.str_)

        # Ensure the array length matches the DataFrame length and add back in
        if len(geoprocess_array) != len(df):
            raise ValueError(f"Length mismatch: {term} does not match DataFrame length")

        df = df.with_columns(
            pl.Series(term, geoprocess_array)  ## TODO needs to know the ID and Type it came in with!
        )

    ## REGION code -----------------------------------------------------------------------------------------

    # Assuming df is already a Polars DataFrame
    # Define a function to handle None/null values and convert lists to strings
    def safe_region_for_name(x):
        if x is None:
            return []
        result = regionFor.name(x)
        if result is None or (isinstance(result, (list, tuple)) and len(result) == 0):
            return []
        return result

    def safe_region_for_address(x):
        if x is None:
            return []
        result = regionFor.address(x)
        if result is None or (isinstance(result, (list, tuple)) and len(result) == 0):
            return []
        return result

    def safe_region_for_country(x):
        if x is None:
            return []
        result = regionFor.countryLastProcessing(x)
        if result is None or (isinstance(result, (list, tuple)) and len(result) == 0):
            return []
        return result

    def safe_region_for_feature(x):
        if x is None:
            return []
        result = regionFor.feature(x)
        if result is None or (isinstance(result, (list, tuple)) and len(result) == 0):
            return []
        return result

    # Process each column conditionally
    if "name" in df.columns:
        print("Processing region for name")
        df = df.with_columns(
            nregion=pl.col("name").map_elements(safe_region_for_name, return_dtype=pl.List(pl.String))
            # TODO rename back to nregion then join all the arrays in the various columns
        )

    if "address" in df.columns:
        print("Processing region for address")
        df = df.with_columns(
            aregion=pl.col("address").map_elements(safe_region_for_address, return_dtype=pl.List(pl.String))
        )

    if "addressCountry" in df.columns:
        print("Processing region for addressCountry")
        df = df.with_columns(
            cregion=pl.col("addressCountry").map_elements(safe_region_for_country, return_dtype=pl.List(pl.String))
        )

    if "wkt" in df.columns:
        print("Processing region for wkt")
        df = df.with_columns(
            fregion=pl.col("wkt").map_elements(safe_region_for_feature, return_dtype=pl.List(pl.String))
        )

    def combine_regions(row):
        all_regions = set()

        # List of possible region columns
        region_cols = ['nregion', 'aregion', 'cregion', 'fregion']

        for col in region_cols:
            # Check if a column exists in the DataFrame and the value isn't None
            if col in row.keys() and row[col] is not None:
                # As row[col] is now directly a list, just add the elements into the set
                all_regions.update(row[col])

        # print(all_regions)
        # print(list(all_regions) if all_regions else [])
        return list(all_regions) if all_regions else []

    # Add the combined regions column
    print("Combining regions")
    df = df.with_columns(
        txt_regions=pl.struct(df.columns).map_elements(combine_regions, return_dtype=pl.List(pl.String))
    )


    def process_list(txt_g):
        """
        Process the column txt_g to find the first match for the pattern:
        urn:gleaner.io:oih:abc123:

        Args:
        txt_g (list of str): A list of strings to search for the pattern.

        Returns:
        str: The value in the abc123 location if a match is found, otherwise "OIH".
        """
        if txt_g is None or all(item is None for item in txt_g):
            return "OIH"

        # Define the regex pattern
        pattern = r"urn:gleaner.io:oih:([a-zA-Z0-9]+):"

        # Iterate through the list and search for a match
        for item in txt_g:
            if item is not None:
                match = re.search(pattern, item)
                if match:
                    return match.group(1)  # Return the captured group (abc123)

        # If no match is found, return "OIH"
        return "OIH"

    if "txt_g" in df.columns:
        print("Identify Provider")
        df = df.with_columns(
            txt_provider=pl.col("txt_g").map_elements(process_list, return_dtype=pl.String)
        )
    # df = df.with_columns(
    #     txt_regions=df.map_elements(combine_regions, return_dtype=pl.List(pl.String))
    # )


    ## Copy txt_name to name --------------------------------------------------------------------------------
    df = df.with_columns(pl.col("txt_name").alias("name"))

    ## TEXT section -----------------------------------------------------------------------------------------
    print("Add field: text")

    def process_value(x):
        if isinstance(x, list):
            return " ".join(str(item) for item in x)
        return str(x) if x is not None else ""

    ## concat columns since copy fields in solr not working
    df = df.with_columns(
        pl.struct([col for col in ["name", "description"] if col in df.columns])
        .map_elements(
            lambda x: " ".join(process_value(x.get(col, "")) for col in x.keys()).strip(),
            return_dtype=pl.String
        )
        .alias("text")
    )


    ## IO section -----------------------------------------------------------------------------------------

    print("Saving Parquet")
    df.write_parquet("./stores/files/test_augmented.parquet")

    print("Saving Lance")
    db.create_table(f"{source}_augmented", data=df, mode="overwrite")

    # process region ##

    # TO ADD
    #
    #     if "name" in df.columns:
    #         df['name'] = df['name'].astype(str)  # why is this needed?
    #
    #         # TODO, incorporate Jeff's code as a Lambda function (will need to support multiple possible regions per entry)
    #     if "name" in df.columns:
    #         print("Processing region for name")
    #         df['nregion'] = df['name'].apply(lambda x: regionFor.name(x) if x else x)
    #     if "address" in df.columns:
    #         print("Processing region for address")
    #         df['aregion'] = df['address'].apply(lambda x: regionFor.address(x) if x else x)
    #     if "addressCountry" in df.columns:
    #         print("Processing region for addressCountry")
    #         df['cregion'] = df['addressCountry'].apply(
    #             lambda x: regionFor.countryLastProcessing(x) if x else x)
    #     if "wkt" in df.columns:
    #         print("Processing region for wkt")
    #         df['fregion'] = df['wkt'].apply(lambda x: regionFor.feature(x) if x else x)

    # ---------------------------------------------------
    #
    # def augment_mode_pandas(source):
    #     print(f"Augment mode: Processing data from lancedb table {source} to a file")
    #
    #     # source = "sparql_results_grouped"
    #     dblocation = "./stores/lancedb"
    #     table_name = source
    #
    #     # Connect to LanceDB
    #     db = lancedb.connect(dblocation)
    #     table = db[table_name]
    #
    #     df = table.to_pandas()
    #
    #     # process the dataframe
    #     print("Processing Stage: Geospatial centroid")
    #
    #     df['filteredgeom'] = df['the_geom'].apply(lambda x: np.nan if graphshapers.contains_alpha(x) else x)
    #
    #     print("Processing Stage: Geospatial centroid")
    #     df['centroid'] = df['filteredgeom'].apply(lambda x: spatial.gj(str(x), "centroid"))
    #     df['centroid'] = df['centroid'].astype(str)
    #
    #     print("Processing Stage: Geospatial length")
    #     df['length'] = df['filteredgeom'].apply(lambda x: spatial.gj(str(x), "length"))
    #
    #     print("Processing Stage: Geospatial area")
    #     df['area'] = df['filteredgeom'].apply(lambda x: spatial.gj(str(x), "area"))
    #
    #     print("Processing Stage: Geospatial wkt")
    #     df['wkt'] = df['filteredgeom'].apply(lambda x: spatial.gj(str(x), "wkt"))
    #     df['wkt'] = df['wkt'].astype(str)
    #
    #     print("Processing Stage: Geospatial geojson")
    #     df['geojson'] = df['filteredgeom'].apply(lambda x: spatial.gj(str(x), "geojson"))
    #     df['geojson'] = df['geojson'].astype(str)
    #
    #     print(df.head())
