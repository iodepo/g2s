#!/bin/bash

## Do I need to remove the lancedb each time too?

## Clear the existing solr index
#  curl -X POST 'http://oih.ioc-africa.org:8983/solr/ckan/update?commit=true' -H 'Content-Type: application/json' -d '{"delete": {"query": "*:*"}}'

SPARQL_ENDPOINT="http://ghost.lan:7007"
SOLR_ENDPOINT="http://ghost.lan:8983/solr/ckan"

# List of types to process
types=("datacatalog" "dataset" "map" "documents" "organization" "person" "projects" "training" "vessels")

for type in "${types[@]}"; do
    echo "----------> ${type}"

    # Special case for query file (dataCatalog has mixed case, others lowercase)
    if [[ "$type" == "datacatalog" ]]; then
        query_file="./SPARQL/unionByType/dataCatalog.rq"
    else
        query_file="./SPARQL/unionByType/${type}.rq"
    fi

    python graph2solr.py query --source "${SPARQL_ENDPOINT}" --sink "./stores/files/${type}_results_sparql.parquet" --query "${query_file}" --table "${type}_sparql_results"
    python graph2solr.py group --source "${type}_sparql_results" --sink "./stores/files/${type}_results_sparql_grouped.parquet" --table "${type}_sparql_results_grouped"
    python graph2solr.py augment --source "${type}_sparql_results_grouped" --sink "./stores/files/${type}_results_sparql_grouped_augmented.parquet" --table "${type}_sparql_results_grouped_augmented"
    python graph2solr.py jsonl --source "${type}_sparql_results_grouped_augmented" --sink "./stores/solrInputFiles/${type}_sparql_results_grouped_augmented.jsonl"
    python graph2solr.py batch --source "./stores/solrInputFiles/${type}_sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"
    echo "\n"
done
