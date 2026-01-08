import hashlib
from datetime import date
import duckdb
import lancedb
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import compute as pc
def compute_md5(value):
    if value is None:
        return None
    return hashlib.md5(value.encode()).hexdigest()

def group_mode_duckdb_optimized(source, sink):
    db = lancedb.connect("./stores/lancedb")
    table = db.open_table(source)
    arrow_table = table.to_arrow()

    conn = duckdb.connect()
    conn.execute("SET memory_limit='16GB'")  # Adjust based on your available system memory
    conn.register("arrow_table", arrow_table)

    # Read the SPARQL query from a file
    with open("./SPARQL/duckdbSQL_old.sql", "r") as file:
        query = file.read()

    # Stream results directly to Lance
    result = conn.execute(query)
    print("Saving Lance")
    db.create_table(f"{source}_grouped", data=result.arrow(), mode="overwrite")

    conn.close()

    # Example usage
    table_name = f"{source}_grouped"
    output_file = "exported_data.parquet"

    # Open the table
    table = db[table_name]

    # Read all data from the table
    df = table.to_arrow()

    # Write the data to a Parquet file
    pq.write_table(df, output_file)

    print(f"Exported {table_name} to {output_file}")

def group_mode_duckdb(source, sink):
    db = lancedb.connect("./stores/lancedb")
    table = db.open_table(source)
    arrow_table = table.to_lance()
    # arrow_table = table.to_arrow()

    con = duckdb.connect()

    # Read the SPARQL query from a file
    with open("./SPARQL/duckdbSQL.sql", "r") as file:
        query = file.read()

    query = con.execute(query)
    record_batch_reader = query.fetch_record_batch()
    chunk = record_batch_reader.read_next_batch()

    print(type(chunk))

    # r = duckdb.query(query)
    # df = r.fetchdf()
    # print(len(df))

def group_mode_duckdbv2(source, sink):
    db = lancedb.connect("./stores/lancedb")
    table = db.open_table(source)

    # Fetch all data from LanceDB as a PyArrow table
    arrow_table = table.to_arrow()  # This gets the whole table

    con = duckdb.connect()

    # Create a DuckDB view from the Arrow table
    con.register("my_arrow_table", arrow_table) # This makes the table accessible to SQL

    # Now your SQL query works correctly
    with open("./SPARQL/duckdbSQL.sql", "r") as file:
        query = file.read()

    # Execute the query (now using the DuckDB view)
    arrow_stream = con.execute(query).fetch_arrow_table().to_batches()

    for batch in arrow_stream:
        print(f"Processing batch of size: {batch.num_rows}")
        # ... your processing logic here ...

        # Write to LanceDB (if needed)
        # sink_table = db.open_table(sink)
        # sink_table.add(batch)

        batch = None  # Explicitly release memory (optional but recommended)

    con.close()
    # db.close()

def group_mode_duckdbv3(source, sink):
    db = lancedb.connect("./stores/lancedb")
    table = db.open_table(source)
    arrow_table = table.to_arrow()

    print(table.count_rows())

    con = duckdb.connect()
    con.register("my_arrow_table", arrow_table)

    partial_query = """
SELECT id, g, type, name, description, contributor, contenturl, url, temporalCoverage, keywords, courseName, location, iritype, sameAs, citation, license, version, includedInDataCatalog, memberOf, parentOrganization, knowsAbout, affiliation, category, vehicleConfiguration, vehicleSpecialUsage, jobTitle, knowsLanguage, educationalCredentialAwarded, author, hasCourseInstance, areaServed, startDate, endDate, wkt, geom, lat, long, place_name, datePublished, dateModified
FROM my_arrow_table
    """
    partial_stream = con.execute(partial_query).fetch_arrow_table().to_batches()

    aggregated_data = {}  # Dictionary to store aggregated results

    for batch in partial_stream:
        print(f"Processing partial batch of size: {batch.num_rows}")

        for i in range(batch.num_rows):  # Iterate through rows in the batch
            id_value = batch.column("id")[i].as_py() # Get the 'id' value
            if id_value not in aggregated_data:
                aggregated_data[id_value] = {} # Initialize for this ID
                for j in range(len(batch.columns)): # Initialize other columns as lists
                    col_name = batch.field(j).name
                    if col_name not in ["id", "geom", "lat", "long", "datePublished", "dateModified"]:
                        aggregated_data[id_value][col_name] = []
                    elif col_name in ["geom", "lat", "long", "datePublished", "dateModified"]:
                        aggregated_data[id_value][col_name] = None # For min aggregations

            for j in range(len(batch.columns)): # Aggregate data
                col_name = batch.field(j).name
                value = batch.column(j)[i].as_py()

                if col_name not in ["id", "geom", "lat", "long", "datePublished", "dateModified"]:
                    if value not in aggregated_data[id_value][col_name]:  # Distinct aggregation
                        aggregated_data[id_value][col_name].append(value)
                elif col_name in ["geom", "lat", "long", "datePublished", "dateModified"]:
                    if aggregated_data[id_value][col_name] is None or value < aggregated_data[id_value][col_name]:
                        aggregated_data[id_value][col_name] = value

        batch = None

    # Convert aggregated data to Arrow batches
    arrow_batches = []
    for id_val, row_data in aggregated_data.items():
        row_data["id"] = id_val  # Add 'id' back to row data
        arrow_row = []
        fields = []
        num_rows = 1  # Initialize the number of rows

        for col_name, value in row_data.items():
            if isinstance(value, list):
                arrow_row.append(pa.array(value, type=pa.list_(pa.string())))
                fields.append(pa.field(col_name, pa.list_(pa.string())))
                num_rows = len(value) if value else 1 # Get the length from lists
            else:  # Scalar value
                arrow_row.append(pa.array([value] * num_rows, type=pa.string())) # Repeat scalar value
                fields.append(pa.field(col_name, pa.string()))

        batch = pa.RecordBatch.from_arrays(arrow_row, schema=pa.schema(fields))
        arrow_batches.append(batch)

    # ... (Rest of the code for writing to LanceDB and closing connections)
    sink_table = db.open_table(sink)
    for final_batch in arrow_batches:
        print(f"Writing final batch of size: {final_batch.num_rows}")
        sink_table.add(final_batch)
        final_batch = None

    con.close()

def group_mode_old(source, sink):
    print(f"Group mode (old): Processing data from {source} ")

    db = lancedb.connect("./stores/lancedb")
    table = db.open_table(source)
    print(table.count_rows())
    arrow_table = table.to_lance()  # was to_arrow()
    # pq.write_table(arrow_table, "test.parquet")
    # print(arrow_table.shape)

    conn = duckdb.connect()
    conn.execute("SET memory_limit='16GB'")

    conn.register("arrow_table", arrow_table)

    # Read the SPARQL query from a file
    with open("./SPARQL/duckdbSQL_old.sql", "r") as file:
        query = file.read()

    df = conn.execute(query).fetchdf()
    print(len(df))

    # TODO  remove to augmentation
    df['index_id'] = df['id'].apply(compute_md5)
    df["indexed_ts"] = date.today().isoformat() # Add a new column with today's date in ISO format
    df["json_source"] = """{"@context": "https://schema.org/", "@type": "Person", "name": "John Doe" }"""

    print("Saving Parquet")
    df.to_parquet("./stores/testgroupout.parquet")

    # Create or get LanceDB table and write data
    print("Saving Lance")
    db.create_table(f"{source}_grouped", data=df, mode="overwrite")

def group_mode_orig(source, sink):
    print(f"Group mode: Processing data from {source} to {sink}")

    db = lancedb.connect("./stores/lancedb")
    table = db.open_table(source)
    arrow_table = table.to_arrow()

    conn = duckdb.connect()
    conn.register("arrow_table", arrow_table)

    # Get the column names
    # columns = conn.execute("PRAGMA table_info('arrow_table')").fetchall()
    # column_names = [col[1] for col in columns]

    # Read the SPARQL query from a file
    with open("./SPARQL/duckdbSQL_old.sql", "r") as file:
        query = file.read()

    df = conn.execute(query).fetchdf()

    # TODO  remove to augmentation
    df['index_id'] = df['id'].apply(compute_md5)
    df["indexed_ts"] = date.today().isoformat() # Add a new column with today's date in ISO format
    df["json_source"] = """{"@context": "https://schema.org/", "@type": "Person", "name": "John Doe" }"""

    # save results to a file (make an optional)
    # df.to_csv(sink)
    print("Saving Parquet")
    df.to_parquet("./stores/testgroupout.parquet")

    # Create or get LanceDB table and write data
    print("Saving Lance")
    db.create_table(f"{source}_grouped", data=df, mode="overwrite")

def optimized_group_mode(source, sink):
    db = lancedb.connect("./stores/lancedb")
    table = db.open_table(source)
    arrow_table = table.to_arrow()

    conn = duckdb.connect()
    conn.execute("SET memory_limit='4GB'")  # Adjust based on your system
    conn.register("arrow_table", arrow_table)

    # Read the SPARQL query from a file
    with open("./SPARQL/duckdbSQL_old.sql", "r") as file:
        query = file.read()

    # Stream results in batches instead of loading everything at once
    result_batches = conn.execute(query).fetch_arrow_table().to_batches()
    
    # Process data in batches
    all_batches = []
    for batch in result_batches:
        print(f"Processing batch of size: {batch.num_rows}")
        # Do any batch processing here
        all_batches.append(batch)
        
    # Combine all batches if needed
    print("Saving Lance")
    db.create_table(f"{source}_grouped", data=all_batches, mode="overwrite")
    
    conn.close()