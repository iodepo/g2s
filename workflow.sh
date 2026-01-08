#!/bin/bash

## Do I need to remove the lancedb each time too?

## Clear the existing solr index
#  curl -X POST 'http://oih.ioc-africa.org:8983/solr/ckan/update?commit=true' -H 'Content-Type: application/json' -d '{"delete": {"query": "*:*"}}'

SPARQL_ENDPOINT="http://ghost.lan:7007"
#SOLR_ENDPOINT="http://oih.ioc-africa.org:8983/solr/ckan"
SOLR_ENDPOINT="http://ghost.lan:8983/solr/ckan"

echo "----------> dataCatalog"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/dataCatalog.rq"  --table "sparql_results"
python graph2solr.py group --source "sparql_results" --sink './stores/files/group_new_result.parquet' --table "sparql_results_grouped"
python graph2solr.py augment --source "sparql_results_grouped" --sink "./stores/files/test_augmented.parquet" --table   "sparql_results_grouped_augmented"
python graph2solr.py jsonl --source "sparql_results_grouped_augmented" --sink  ./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl
#python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "http://ghost.lan:8983/solr/ckan"
python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> dataset"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/dataset.rq"  --table "sparql_results"
python graph2solr.py group --source "sparql_results" --sink './stores/files/results_long_grouped.csv'
python graph2solr.py augment --source "sparql_results_grouped"
python graph2solr.py jsonl --source "sparql_results_grouped_augmented"
#python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "http://ghost.lan:8983/solr/ckan"
python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> map"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/map.rq"  --table "sparql_results"
python graph2solr.py group --source "sparql_results" --sink './stores/files/results_long_grouped.csv'
python graph2solr.py augment --source "sparql_results_grouped"
python graph2solr.py jsonl --source "sparql_results_grouped_augmented"
#python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "http://ghost.lan:8983/solr/ckan"
python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> documents"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/documents.rq"  --table "sparql_results"
python graph2solr.py group --source "sparql_results" --sink './stores/files/results_long_grouped.csv'
python graph2solr.py augment --source "sparql_results_grouped"
python graph2solr.py jsonl --source "sparql_results_grouped_augmented"
#python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "http://ghost.lan:8983/solr/ckan"
python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> organization"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/organization.rq"  --table "sparql_results"
python graph2solr.py group --source "sparql_results" --sink './stores/files/results_long_grouped.csv'
python graph2solr.py augment --source "sparql_results_grouped"
python graph2solr.py jsonl --source "sparql_results_grouped_augmented"
#python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "http://ghost.lan:8983/solr/ckan"
python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> person"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/person.rq"  --table "sparql_results"
python graph2solr.py group --source "sparql_results" --sink './stores/files/results_long_grouped.csv'
python graph2solr.py augment --source "sparql_results_grouped"
python graph2solr.py jsonl --source "sparql_results_grouped_augmented"
#python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "http://ghost.lan:8983/solr/ckan"
python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> projects"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/projects.rq"  --table "sparql_results"
python graph2solr.py group --source "sparql_results" --sink './stores/files/results_long_grouped.csv'
python graph2solr.py augment --source "sparql_results_grouped"
python graph2solr.py jsonl --source "sparql_results_grouped_augmented"
#python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "http://ghost.lan:8983/solr/ckan"
python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> training"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/training.rq"  --table "sparql_results"
python graph2solr.py group --source "sparql_results" --sink './stores/files/results_long_grouped.csv'
python graph2solr.py augment --source "sparql_results_grouped"
python graph2solr.py jsonl --source "sparql_results_grouped_augmented"
#python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "http://ghost.lan:8983/solr/ckan"
python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> vessels"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/results_sparql.parquet" --query "./SPARQL/unionByType/vessels.rq"  --table "sparql_results"
python graph2solr.py group --source "sparql_results" --sink './stores/files/results_long_grouped.csv'
python graph2solr.py augment --source "sparql_results_grouped"
python graph2solr.py jsonl --source "sparql_results_grouped_augmented"
#python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "http://ghost.lan:8983/solr/ckan"
python graph2solr.py batch --source "./stores/solrInputFiles/sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"
