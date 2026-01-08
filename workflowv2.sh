#!/bin/bash

## Do I need to remove the lancedb each time too?

## Clear the existing solr index
#  curl -X POST 'http://oih.ioc-africa.org:8983/solr/ckan/update?commit=true' -H 'Content-Type: application/json' -d '{"delete": {"query": "*:*"}}'

SPARQL_ENDPOINT="http://ghost.lan:7007"
#SOLR_ENDPOINT="http://oih.ioc-africa.org:8983/solr/ckan"
SOLR_ENDPOINT="http://ghost.lan:8983/solr/ckan"

echo "----------> dataCatalog"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/datacatalog_results_sparql.parquet" --query "./SPARQL/unionByType/dataCatalog.rq"  --table "datacatalog_sparql_results"
python graph2solr.py group --source "datacatalog_sparql_results" --sink './stores/files/datacatalog_results_sparql_grouped.parquet' --table "datacatalog_sparql_results_grouped"
python graph2solr.py augment --source "datacatalog_sparql_results_grouped" --sink "./stores/files/datacatalog_results_sparql_grouped_augmented.parquet" --table   "datacatalog_sparql_results_grouped_augmented"
python graph2solr.py jsonl --source "datacatalog_sparql_results_grouped_augmented" --sink  "./stores/solrInputFiles/datacatalog_sparql_results_grouped_augmented.jsonl"
python graph2solr.py batch --source "./stores/solrInputFiles/datacatalog_sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> dataset"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/dataset_results_sparql.parquet" --query "./SPARQL/unionByType/dataset.rq"  --table "dataset_sparql_results"
python graph2solr.py group --source "dataset_sparql_results" --sink './stores/files/dataset_results_sparql_grouped.parquet' --table "dataset_sparql_results_grouped"
python graph2solr.py augment --source "dataset_sparql_results_grouped" --sink "./stores/files/dataset_results_sparql_grouped_augmented.parquet" --table   "dataset_sparql_results_grouped_augmented"
python graph2solr.py jsonl --source "dataset_sparql_results_grouped_augmented" --sink  "./stores/solrInputFiles/dataset_sparql_results_grouped_augmented.jsonl"
python graph2solr.py batch --source "./stores/solrInputFiles/dataset_sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> map"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/map_results_sparql.parquet" --query "./SPARQL/unionByType/map.rq"  --table "map_sparql_results"
python graph2solr.py group --source "map_sparql_results" --sink './stores/files/map_results_sparql_grouped.parquet' --table "map_sparql_results_grouped"
python graph2solr.py augment --source "map_sparql_results_grouped" --sink "./stores/files/map_results_sparql_grouped_augmented.parquet" --table   "map_sparql_results_grouped_augmented"
python graph2solr.py jsonl --source "map_sparql_results_grouped_augmented" --sink  "./stores/solrInputFiles/map_sparql_results_grouped_augmented.jsonl"
python graph2solr.py batch --source "./stores/solrInputFiles/map_sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> documents"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/documents_results_sparql.parquet" --query "./SPARQL/unionByType/documents.rq"  --table "documents_sparql_results"
python graph2solr.py group --source "documents_sparql_results" --sink './stores/files/documents_results_sparql_grouped.parquet' --table "documents_sparql_results_grouped"
python graph2solr.py augment --source "documents_sparql_results_grouped" --sink "./stores/files/documents_results_sparql_grouped_augmented.parquet" --table   "documents_sparql_results_grouped_augmented"
python graph2solr.py jsonl --source "documents_sparql_results_grouped_augmented" --sink  "./stores/solrInputFiles/documents_sparql_results_grouped_augmented.jsonl"
python graph2solr.py batch --source "./stores/solrInputFiles/documents_sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> organization"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/organization_results_sparql.parquet" --query "./SPARQL/unionByType/organization.rq"  --table "organization_sparql_results"
python graph2solr.py group --source "organization_sparql_results" --sink './stores/files/organization_results_sparql_grouped.parquet' --table "organization_sparql_results_grouped"
python graph2solr.py augment --source "organization_sparql_results_grouped" --sink "./stores/files/organization_results_sparql_grouped_augmented.parquet" --table   "organization_sparql_results_grouped_augmented"
python graph2solr.py jsonl --source "organization_sparql_results_grouped_augmented" --sink  "./stores/solrInputFiles/organization_sparql_results_grouped_augmented.jsonl"
python graph2solr.py batch --source "./stores/solrInputFiles/organization_sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> person"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/person_results_sparql.parquet" --query "./SPARQL/unionByType/person.rq"  --table "person_sparql_results"
python graph2solr.py group --source "person_sparql_results" --sink './stores/files/person_results_sparql_grouped.parquet' --table "person_sparql_results_grouped"
python graph2solr.py augment --source "person_sparql_results_grouped" --sink "./stores/files/person_results_sparql_grouped_augmented.parquet" --table   "person_sparql_results_grouped_augmented"
python graph2solr.py jsonl --source "person_sparql_results_grouped_augmented" --sink  "./stores/solrInputFiles/person_sparql_results_grouped_augmented.jsonl"
python graph2solr.py batch --source "./stores/solrInputFiles/person_sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> projects"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/projects_results_sparql.parquet" --query "./SPARQL/unionByType/projects.rq"  --table "projects_sparql_results"
python graph2solr.py group --source "projects_sparql_results" --sink './stores/files/projects_results_sparql_grouped.parquet' --table "projects_sparql_results_grouped"
python graph2solr.py augment --source "projects_sparql_results_grouped" --sink "./stores/files/projects_results_sparql_grouped_augmented.parquet" --table   "projects_sparql_results_grouped_augmented"
python graph2solr.py jsonl --source "projects_sparql_results_grouped_augmented" --sink  "./stores/solrInputFiles/projects_sparql_results_grouped_augmented.jsonl"
python graph2solr.py batch --source "./stores/solrInputFiles/projects_sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> training"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/training_results_sparql.parquet" --query "./SPARQL/unionByType/training.rq"  --table "training_sparql_results"
python graph2solr.py group --source "training_sparql_results" --sink './stores/files/training_results_sparql_grouped.parquet' --table "training_sparql_results_grouped"
python graph2solr.py augment --source "training_sparql_results_grouped" --sink "./stores/files/training_results_sparql_grouped_augmented.parquet" --table   "training_sparql_results_grouped_augmented"
python graph2solr.py jsonl --source "training_sparql_results_grouped_augmented" --sink  "./stores/solrInputFiles/training_sparql_results_grouped_augmented.jsonl"
python graph2solr.py batch --source "./stores/solrInputFiles/training_sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"

echo "----------> vessels"
python graph2solr.py query --source   "${SPARQL_ENDPOINT}" --sink "./stores/files/vessels_results_sparql.parquet" --query "./SPARQL/unionByType/vessels.rq"  --table "vessels_sparql_results"
python graph2solr.py group --source "vessels_sparql_results" --sink './stores/files/vessels_results_sparql_grouped.parquet' --table "vessels_sparql_results_grouped"
python graph2solr.py augment --source "vessels_sparql_results_grouped" --sink "./stores/files/vessels_results_sparql_grouped_augmented.parquet" --table   "vessels_sparql_results_grouped_augmented"
python graph2solr.py jsonl --source "vessels_sparql_results_grouped_augmented" --sink  "./stores/solrInputFiles/vessels_sparql_results_grouped_augmented.jsonl"
python graph2solr.py batch --source "./stores/solrInputFiles/vessels_sparql_results_grouped_augmented.jsonl" --sink "${SOLR_ENDPOINT}"
