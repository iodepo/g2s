import duckdb

# Connect to DuckDB
conn = duckdb.connect()

duckdb.install_extension("httpfs")
duckdb.install_extension("ducklake")

# Install the ducklake extension
conn.execute("LOAD ducklake")
conn.execute("LOAD httpfs")

conn.execute("SET s3_url_style='path'")
conn.execute("SET s3_endpoint='ossapi.oceaninfohub.org'")
conn.execute("SET s3_use_ssl=false")  # or false if not using HTTPS
conn.execute("SET s3_access_key_id='your_access'")
conn.execute("SET s3_secret_access_key='your_secret'")

files = [
    'http://ossapi.oceaninfohub.org/public/assets/obis.parquet',
    'http://ossapi.oceaninfohub.org/public/assets/cioos.parquet',
    'http://ossapi.oceaninfohub.org/public/assets/edmo.parquet',
    'http://ossapi.oceaninfohub.org/public/assets/emodnet.parquet',
    'http://ossapi.oceaninfohub.org/public/assets/obps.parquet',
    'http://ossapi.oceaninfohub.org/public/assets/oceanexperts.parquet'
]

print("=== Creating a New DuckLake Database ===")
# Create and attach to a DuckLake database
conn.execute("ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake")
conn.execute("USE my_ducklake")
print("Created and attached to DuckLake: my_ducklake.ducklake")

# FROM read_parquet('http://ossapi.oceaninfohub.org/public/assets/obis.parquet')
# FROM read_parquet(['{file_list}'])
# FROM  read_parquet('s3://public/assets/*.parquet', union_by_name=True)

file_list = "', '".join(files)

conn.execute(f"""
    CREATE TABLE oih_lake AS
    FROM read_parquet(['{file_list}'], union_by_name=True)
""")
print("Created table 'oih_lake' from Parquet data")

# Check how many entries we have
count = conn.execute("SELECT COUNT(*) FROM oih_lake").fetchone()[0]
print(f"Total stations: {count}")
