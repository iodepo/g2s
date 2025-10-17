import duckdb

# Connect to DuckDB
conn = duckdb.connect()

duckdb.install_extension("httpfs")
duckdb.install_extension("ducklake")

# Install the ducklake extension
conn.execute("LOAD ducklake")

print("=== Creating a New DuckLake Database ===")
# Create and attach to a DuckLake database
conn.execute("ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake")
conn.execute("USE my_ducklake")
print("Attached to DuckLake: my_ducklake.ducklake")

# Check how many entries we have
count = conn.execute("SELECT COUNT(*) FROM oih_lake").fetchone()[0]
print(f"Total stations: {count}")

desc = conn.execute("DESCRIBE oih_lake").fetch_df()
print(desc.head(20))

kw = conn.execute("""SELECT DISTINCT unnest(string_split(keywords, ',')) AS keyword
FROM oih_lake
WHERE keywords IS NOT NULL;  -- Optional: Exclude NULLs""").fetch_df()

print(kw.head(20))

kw_count = conn.execute("""SELECT keyword, COUNT(*) AS count
FROM (
SELECT DISTINCT unnest(string_split(keywords, ',')) AS keyword
FROM oih_lake
WHERE keywords IS NOT NULL
) AS flattened
GROUP BY keyword
ORDER BY count DESC, keyword ASC;  -- Sort by frequency (desc), then alphabetically
""").fetch_df()

print(kw_count.head(20))
