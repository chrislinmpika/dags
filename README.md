# EDS Data Pipeline - DAGs Airflow

Pipeline de transformation Bronze → Silver pour les données biologiques EDS.

## Structure

- `bronze_to_silver_dag.py`: DAG Airflow principal pour la transformation des données
- `dbt/eds_omop/`: Projet dbt pour transformations OMOP

## Déploiement

Le repo est synchronisé automatiquement via Git Sync dans Airflow.

## Pipeline

1. **Bronze** : Fichiers CSV bruts dans MinIO
2. **Staging** : Transformation et nettoyage via dbt
3. **Silver** : Tables OMOP dans Iceberg/Trino
