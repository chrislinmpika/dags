"""
DAG Airflow: Bronze CSV → Staging → Silver OMOP
Pipeline incrémental avec tracking des fichiers traités
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.trino.operators.trino import TrinoOperator
from datetime import datetime, timedelta
import pandas as pd
from minio import Minio
from trino.dbapi import connect
import logging

# Configuration
MINIO_ENDPOINT = "minio-api.ns-data-platform.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
TRINO_HOST = "my-trino-trino.ns-data-platform.svc.cluster.local"
TRINO_PORT = 8080
BRONZE_BUCKET = "bronze"
BATCH_SIZE = 50000

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bronze_to_silver_omop',
    default_args=default_args,
    description='Load CSV from Bronze to Silver OMOP via staging',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    tags=['eds', 'omop', 'incremental'],
)

# ============================================================================
# Task 1: Créer la table de tracking (si nécessaire)
# ============================================================================

create_tracking_table = TrinoOperator(
    task_id='create_tracking_table',
    trino_conn_id='trino_default',
    sql="""
        CREATE TABLE IF NOT EXISTS iceberg.silver._file_tracking (
            file_name VARCHAR,
            processed_at TIMESTAMP(3) WITH TIME ZONE,
            row_count BIGINT,
            status VARCHAR
        ) WITH (
            format = 'PARQUET',
            location = 's3://eds-silver/_metadata/file_tracking/'
        )
    """,
    dag=dag,
)

# ============================================================================
# Task 2: Identifier les nouveaux fichiers CSV
# ============================================================================

def identify_new_files(**context):
    """
    Liste les fichiers CSV dans Bronze et identifie ceux non encore traités
    """
    logging.info("Connecting to MinIO...")
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    # Lister tous les CSV dans Bronze
    objects = minio_client.list_objects(BRONZE_BUCKET, prefix="biological_results_", recursive=True)
    all_files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]
    logging.info(f"Found {len(all_files)} CSV files in Bronze")
    
    # Récupérer les fichiers déjà traités
    logging.info("Connecting to Trino...")
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="trino",
        catalog="iceberg",
        schema="silver"
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT file_name FROM _file_tracking WHERE status = 'SUCCESS'")
    processed_files = set(row[0] for row in cursor.fetchall())
    cursor.close()
    conn.close()
    
    logging.info(f"Already processed: {len(processed_files)} files")
    
    # Identifier les nouveaux fichiers
    new_files = [f for f in all_files if f not in processed_files]
    logging.info(f"New files to process: {len(new_files)}")
    
    # Limiter à 10 fichiers par run (pour éviter les timeouts)
    files_to_process = new_files[:10]
    
    # Push to XCom
    context['ti'].xcom_push(key='files_to_process', value=files_to_process)
    
    if not files_to_process:
        logging.info("No new files to process, skipping downstream tasks")
        return "skip"
    
    return "process"

identify_files_task = PythonOperator(
    task_id='identify_new_files',
    python_callable=identify_new_files,
    dag=dag,
)

# ============================================================================
# Task 3: Créer/Vider la table staging
# ============================================================================

prepare_staging = TrinoOperator(
    task_id='prepare_staging',
    trino_conn_id='trino_default',
    sql="""
        -- Supprimer l'ancienne table staging si elle existe
        DROP TABLE IF EXISTS memory.default.staging_biological_results;
        
        -- Créer une nouvelle table staging
        CREATE TABLE memory.default.staging_biological_results (
            visit_id VARCHAR,
            visit_date_utc VARCHAR,
            visit_rank VARCHAR,
            patient_id VARCHAR,
            report_id VARCHAR,
            laboratory_uuid VARCHAR,
            sub_laboratory_uuid VARCHAR,
            site_laboratory_uuid VARCHAR,
            source_file VARCHAR,
            load_timestamp TIMESTAMP(3) WITH TIME ZONE
        ) WITH (
            format = 'PARQUET'
        )
    """,
    dag=dag,
)

# ============================================================================
# Task 4: Charger les CSV dans staging
# ============================================================================

def load_csv_to_staging(**context):
    """
    Lit les nouveaux CSV depuis MinIO et les charge dans la table staging Trino
    """
    files_to_process = context['ti'].xcom_pull(key='files_to_process', task_ids='identify_new_files')
    
    if not files_to_process:
        logging.info("No files to process")
        return
    
    logging.info(f"Processing {len(files_to_process)} files")
    
    # Connexions
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="trino",
        catalog="memory",
        schema="default"
    )
    cursor = conn.cursor()
    
    total_rows = 0
    
    for file_name in files_to_process:
        try:
            logging.info(f"Processing {file_name}...")
            
            # Lire le CSV depuis MinIO
            csv_obj = minio_client.get_object(BRONZE_BUCKET, file_name)
            df = pd.read_csv(csv_obj)
            
            logging.info(f"  Read {len(df)} rows from {file_name}")
            
            # Ajouter les colonnes metadata
            df['source_file'] = file_name
            df['load_timestamp'] = datetime.now()
            
            # Convertir toutes les colonnes en string sauf metadata
            for col in df.columns:
                if col not in ['source_file', 'load_timestamp']:
                    df[col] = df[col].astype(str)
            
            # Insérer dans staging par batch
            insert_query = """
                INSERT INTO staging_biological_results 
                (visit_id, visit_date_utc, visit_rank, patient_id, report_id, 
                 laboratory_uuid, sub_laboratory_uuid, site_laboratory_uuid, 
                 source_file, load_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            for i in range(0, len(df), BATCH_SIZE):
                batch = df.iloc[i:i+BATCH_SIZE]
                rows = [tuple(row) for row in batch.values]
                cursor.executemany(insert_query, rows)
                logging.info(f"  Inserted batch {i//BATCH_SIZE + 1} ({len(rows)} rows)")
            
            total_rows += len(df)
            
            # Marquer le fichier comme traité
            tracking_conn = connect(
                host=TRINO_HOST,
                port=TRINO_PORT,
                user="trino",
                catalog="iceberg",
                schema="silver"
            )
            tracking_cursor = tracking_conn.cursor()
            tracking_cursor.execute(
                "INSERT INTO _file_tracking (file_name, processed_at, row_count, status) VALUES (?, ?, ?, ?)",
                (file_name, datetime.now(), len(df), 'SUCCESS')
            )
            tracking_cursor.close()
            tracking_conn.close()
            
            logging.info(f"  ✅ Successfully processed {file_name}")
            
        except Exception as e:
            logging.error(f"  ❌ Error processing {file_name}: {e}")
            
            # Marquer comme failed
            tracking_conn = connect(
                host=TRINO_HOST,
                port=TRINO_PORT,
                user="trino",
                catalog="iceberg",
                schema="silver"
            )
            tracking_cursor = tracking_conn.cursor()
            tracking_cursor.execute(
                "INSERT INTO _file_tracking (file_name, processed_at, row_count, status) VALUES (?, ?, ?, ?)",
                (file_name, datetime.now(), 0, 'FAILED')
            )
            tracking_cursor.close()
            tracking_conn.close()
            
            continue
    
    cursor.close()
    conn.close()
    
    logging.info(f"Total rows loaded to staging: {total_rows}")
    context['ti'].xcom_push(key='total_rows_loaded', value=total_rows)

load_staging_task = PythonOperator(
    task_id='load_csv_to_staging',
    python_callable=load_csv_to_staging,
    dag=dag,
)

# ============================================================================
# Task 5: Vérifier que staging contient des données
# ============================================================================

check_staging = TrinoOperator(
    task_id='check_staging_has_data',
    trino_conn_id='trino_default',
    sql="""
        SELECT 
            COUNT(*) as row_count,
            COUNT(DISTINCT source_file) as file_count
        FROM memory.default.staging_biological_results
    """,
    dag=dag,
)

# ============================================================================
# Task 6: Exécuter dbt pour transformer staging → silver
# ============================================================================

dbt_run = BashOperator(
    task_id='dbt_transform_to_silver',
    bash_command='cd /opt/airflow/dbt/eds_omop && dbt run --models silver.* --profiles-dir .',
    dag=dag,
)

# ============================================================================
# Task 7: Exécuter les tests dbt
# ============================================================================

dbt_test = BashOperator(
    task_id='dbt_test_silver',
    bash_command='cd /opt/airflow/dbt/eds_omop && dbt test --models silver.* --profiles-dir .',
    dag=dag,
)

# ============================================================================
# Task 8: Nettoyer la table staging
# ============================================================================

cleanup_staging = TrinoOperator(
    task_id='cleanup_staging',
    trino_conn_id='trino_default',
    sql="DROP TABLE IF EXISTS memory.default.staging_biological_results",
    dag=dag,
)

# ============================================================================
# Pipeline DAG
# ============================================================================

create_tracking_table >> identify_files_task >> prepare_staging >> load_staging_task >> check_staging >> dbt_run >> dbt_test >> cleanup_staging
