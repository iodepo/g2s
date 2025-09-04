# Qlever to Solr (g2s)

## About

The g2s application is a multi-stage data processing pipeline that extracts data from a Qlever SPARQL endpoint, processes it, and loads it into a Solr index. The process involves the following stages:

1.  **Query**: Executes a SPARQL query against a Qlever endpoint and stores the results in a Parquet file and a LanceDB table.
2.  **Group**: Groups the data in the LanceDB table based on a SQL query and stores the result in a new LanceDB table.
3.  **Augment**: Augments the grouped data with additional information, such as geospatial data and temporal data.
4.  **JSONL**: Converts the augmented data into a JSONL file suitable for Solr.
5.  **Batch**: Loads the JSONL file into a Solr index in batches.

## Project Structure

```
/scratch/g2s/
├───.dockerignore
├───.gitignore
├───.python-version
├───Dockerfile
├───main.py
├───pyproject.toml
├───README.md
├───uv.lock
├───workflow.sh
├───defs/
│   ├───datashaping.py
│   ├───etl_augment.py
│   ├───etl_batch.py
│   ├───etl_group.py
│   ├───etl_jsonl.py
│   └───etl_query.py
│   └───...
├───SPARQL/
│   ├───duckdbSQL.sql
│   ├───...
└───stores/
    ├───files/
    ├───lancedb/
    └───solrInputFiles/
```

*   `main.py`: The main entry point for the application. It uses `argparse` to handle different processing stages (modes).
*   `defs/`: Contains the Python modules for each processing stage (e.g., `etl_query.py`, `etl_group.py`).
*   `SPARQL/`: Contains the SPARQL and SQL queries used in the pipeline.
*   `stores/`: The default directory for storing intermediate and final data files.
    *   `files/`: Stores Parquet files.
    *   `lancedb/`: Stores LanceDB tables.
    *   `solrInputFiles/`: Stores JSONL files for Solr.
*   `Dockerfile`: For building the Docker image.
*   `workflow.sh`: A shell script that demonstrates the full pipeline execution.
*   `pyproject.toml`: Defines the project dependencies.

## Docker Build

To build the Docker image for this project, run the following command from the project root:

```bash
docker build -t g2s-app .
```

### Running the Default Command

The default command executes the `query` stage.

```bash
docker run --rm g2s-app
```

### Overriding the Command

You can override the default arguments by appending them to the `docker run` command. For example, to run the `query` command with a different source URL:

```bash
docker run --rm g2s-app query --source "http://another-source.com" --sink "./stores/files/results.parquet" --query "./SPARQL/some_other_query.rq" --table "my_results"
```

## Example command with volume mounts

To use local SPARQL query collection and a local directory for the storage of the generated files, use a command like.

```bash
docker run --rm \
-v $PWD/SPARQL:/app/SPARQL \
-v $PWD/stores:/app/stores \
g2s-app query --source  "http://ghost.lan:7007" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/dataCatalog.rq"  --table "sparql_results"
```

## Run commands

Each of the commands (`query`, `group`, `augment`, `jsonl`, and `batch`) can be run individually. Here are examples of how to run each command locally using `uv`:

```bash
# Query
uv run main.py query --source "http://ghost.lan:7007" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/dataCatalog.rq" --table "sparql_results"

# Group
uv run main.py group --source "sparql_results" --sink './stores/files/results_long_grouped.csv'

# Augment
uv run main.py augment --source "sparql_results_grouped"

# JSONL
uv run main.py jsonl --source "sparql_results_grouped_augmented"

# Batch
uv run main.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "http://localhost:8983/solr/my_core"
```

## Workflow

The `workflow.sh` script provides an example of how to run the entire pipeline for different data types. It executes the `query`, `group`, `augment`, `jsonl`, and `batch` stages in sequence for each data type.

```bash
#!/bin/bash

SPARQL_ENDPOINT="http://localhost:7007"
SOLR_ENDPOINT="http://oih.ioc-africa.org:8983/solr/ckan"

echo "----------> dataCatalog"
python main.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/dataCatalog.rq"  --table "sparql_results"
python main.py group --source "sparql_results" --sink './stores/files/results_long_grouped.csv'
python main.py augment --source "sparql_results_grouped"
python main.py jsonl --source "sparql_results_grouped_augmented"
python main.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

# ... (repeated for other data types)
```
