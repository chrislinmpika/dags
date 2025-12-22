"""
DAG Airflow: Bronze CSV → Staging → Silver OMOP
Version 3.1.1 - Production Ready (Fixed Typo & Atomic Staging)
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
from minio import Minio
from trino.dbapi import connect

# ============================================================================
# Configuration
# ============================================================================
MINIO_ENDPOINT = "minio-api.ns-data-platform.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

TRINO_HOST = "my-trino-trino.ns-data-platform.svc.cluster.local"
TRINO_PORT = 8080
TRINO_USER = "trino"

BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"
BATCH_SIZE = 50000

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 22),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bronze_to_silver_omop_v2',
    default_args=default_args,
    description='Pipeline incremental robuste avec Iceberg et Idempotence',
    schedule='0 */6 * * *',
    catchup=False,
    tags=['eds', 'omop', 'incremental', 'production'],
)

# --- Task 1: Initialize Metadata ---
create_tracking_table = SQLExecuteQueryOperator(
    task_id='create_tracking_table',
    conn_id='trino_default',
    sql=f"""
        CREATE TABLE IF NOT EXISTS iceberg.silver._file_tracking (
            file_name VARCHAR,
            processed_at TIMESTAMP(3) WITH TIME ZONE,
            row_count BIGINT,
            status VARCHAR
        ) WITH (
            format = 'PARQUET',
            location = 's3://{SILVER_BUCKET}/_metadata/file_tracking/'
        )
    """,
    dag=dag,
)

# --- Task 2: Identify New Files ---
def check_for_new_files(**context):
    minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    objects = minio_client.list_objects(BRONZE_BUCKET, prefix="biological_results_", recursive=True)
    all_files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]
    
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="iceberg", schema="silver")
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT file_name FROM _file_tracking WHERE status = 'SUCCESS'")
        processed_files = set(row[0] for row in cursor.fetchall())
    finally:
        conn.close()
    
    new_files = [f for f in all_files if f not in processed_files]
    files_to_process = new_files[:10]
    
    if not files_to_process:
        logging.info("No new files to process.")
        return False
        
    context['ti'].xcom_push(key='files_to_process', value=files_to_process)
    return True

identify_files_task = ShortCircuitOperator(
    task_id='identify_new_files',
    python_callable=check_for_new_files,
    dag=dag,
)

# --- Task 3: Prepare Staging (Atomic Replace) ---
prepare_staging = SQLExecuteQueryOperator(
    task_id='prepare_staging',
    conn_id='trino_default',
    sql="""
        CREATE OR REPLACE TABLE iceberg.silver.staging_biological_results (
            visit_id VARCHAR, visit_date_utc VARCHAR, visit_rank VARCHAR, 
            patient_id VARCHAR, report_id VARCHAR, laboratory_uuid VARCHAR, 
            sub_laboratory_uuid VARCHAR, site_laboratory_uuid VARCHAR, 
            source_file VARCHAR, load_timestamp TIMESTAMP(3) WITH TIME ZONE
        ) WITH (
            format = 'PARQUET'
        )
    """,
    dag=dag,
)

# --- Task 4: Load Data ---
def load_csv_to_staging(**context):
    files = context['ti'].xcom_pull(key='files_to_process', task_ids='identify_new_files')
    minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="iceberg", schema="silver")
    
    try:
        cursor = conn.cursor()
        for f in files:
            logging.info(f"Streaming {f} from MinIO to Trino...")
            obj = minio_client.get_object(BRONZE_BUCKET, f)
            df = pd.read_csv(obj)
            df['source_file'] = f
            df['load_timestamp'] = datetime.now()
            
            for col in df.columns:
                if col not in ['source_file', 'load_timestamp']:
                    df[col] = df[col].astype(str)
            
            insert_sql = "INSERT INTO staging_biological_results VALUES (?,?,?,?,?,?,?,?,?,?)"
            for i in range(0, len(df), BATCH_SIZE):
                batch = df.iloc[i:i+BATCH_SIZE]
                cursor.executemany(insert_sql, [tuple(r) for r in batch.values])
    finally:
        conn.close()

load_staging_task = PythonOperator(
    task_id='load_csv_to_staging',
    python_callable=load_csv_to_staging,
    dag=dag,
)

# --- Task 5: DBT Transformations ---
dbt_run = BashOperator(
    task_id='dbt_transform_to_silver',
    bash_command='cd /opt/airflow/dbt/eds_omop && dbt run --models silver.*',
    dag=dag,
)

# --- Task 6: Finalize Tracking ---
def commit_tracking_success(**context):
    files = context['ti'].xcom_pull(key='files_to_process', task_ids='identify_new_files')
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="iceberg", schema="silver")
    try:
        cursor = conn.cursor()
        for f in files:
            cursor.execute("DELETE FROM _file_tracking WHERE file_name = ?", (f,))
            cursor.execute(
                "INSERT INTO _file_tracking (file_name, processed_at, status) VALUES (?, ?, ?)",
                (f, datetime.now(), 'SUCCESS')
            )
    finally:
        conn.close()

update_tracking_task = PythonOperator(
    task_id='update_tracking_success',
    python_callable=commit_tracking_success,
    dag=dag,
)

# --- Task 7: Cleanup ---
cleanup_staging = SQLExecuteQueryOperator(
    task_id='cleanup_staging',
    conn_id='trino_default',
    sql="DROP TABLE IF EXISTS iceberg.silver.staging_biological_results",
    dag=dag,
)

# --- Dependencies ---
# Fixed the typo here: cleanup_stagingk -> cleanup_staging
create_tracking_table >> identify_files_task >> prepare_staging >> load_staging_task >> dbt_run >> update_tracking_task >> cleanup_staging