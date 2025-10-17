import duckdb

# Connect to DuckDB
conn = duckdb.connect()

# Install the ducklake extension
conn.execute("INSTALL ducklake")
conn.execute("LOAD ducklake")

print("=== Creating a New DuckLake Database ===")
# Create and attach to a DuckLake database
conn.execute("ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake")
conn.execute("USE my_ducklake")
print("Created and attached to DuckLake: my_ducklake.ducklake")

print("\n=== Importing Data ===")
# Create a table from the Netherlands train stations dataset
conn.execute("""
    CREATE TABLE nl_train_stations AS
    FROM 'https://blobs.duckdb.org/nl_stations.csv'
""")
print("Created table 'nl_train_stations' from CSV data")

# Check how many stations we have
count = conn.execute("SELECT COUNT(*) FROM nl_train_stations").fetchone()[0]
print(f"Total stations: {count}")

print("\n=== Checking Data Files ===")
# See what files were created
files = conn.execute("SELECT * FROM glob('my_ducklake.ducklake.files/**/*')").fetchall()
print(f"Number of data files: {len(files)}")
for file in files:
    print(f"  - {file[0]}")

print("\n=== Querying Data ===")
# Query some data
stations = conn.execute("""
    SELECT code, name_long
    FROM nl_train_stations
    WHERE code = 'ASB'
""").fetchall()
print("Amsterdam Bijlmer ArenA station:")
for station in stations:
    print(f"  Code: {station[0]}, Name: {station[1]}")

print("\n=== Updating Data ===")
# Update the station name to reflect the stadium rename
conn.execute("""
    UPDATE nl_train_stations
    SET name_long='Johan Cruijff ArenA'
    WHERE code = 'ASB'
""")
print("Updated station name to 'Johan Cruijff ArenA'")

# Verify the update
updated = conn.execute("""
    SELECT name_long
    FROM nl_train_stations
    WHERE code = 'ASB'
""").fetchone()[0]
print(f"New name: {updated}")

print("\n=== Checking Files After Update ===")
# See that more files appeared after the update
files_after = conn.execute("SELECT * FROM glob('my_ducklake.ducklake.files/**/*')").fetchall()
print(f"Number of data files after update: {len(files_after)}")
for file in files_after:
    print(f"  - {file[0]}")

print("\n=== Viewing Snapshots (Time Travel) ===")
# Query the snapshots to see version history
snapshots = conn.execute("SELECT * FROM my_ducklake.snapshots()").fetchall()
print(f"Total snapshots: {len(snapshots)}")
for i, snapshot in enumerate(snapshots):
    print(f"  Snapshot {i}: {snapshot}")

# Time travel - query old version
print("\n=== Time Travel: Querying Previous Versions ===")
name_v1 = conn.execute("""
    SELECT name_long
    FROM nl_train_stations AT (VERSION => 1)
    WHERE code = 'ASB'
""").fetchone()[0]
print(f"Version 1 name: {name_v1}")

name_v2 = conn.execute("""
    SELECT name_long
    FROM nl_train_stations AT (VERSION => 2)
    WHERE code = 'ASB'
""").fetchone()[0]
print(f"Version 2 name: {name_v2}")

print("\n=== Detaching from DuckLake ===")
# Switch to memory database and detach
conn.execute("USE memory")
conn.execute("DETACH my_ducklake")
print("Detached from DuckLake")

print("\n=== Re-attaching to Existing DuckLake ===")
# Demonstrate attaching to an existing DuckLake
conn.execute("ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake")
conn.execute("USE my_ducklake")
print("Re-attached to existing DuckLake")

# Verify data is still there
station_count = conn.execute("SELECT COUNT(*) FROM nl_train_stations").fetchone()[0]
print(f"Stations after re-attach: {station_count}")

# Close connection
conn.close()
print("\n=== Done! ===")
