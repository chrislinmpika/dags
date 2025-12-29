"""
OMOP Vocabulary Setup DAG
Downloads and loads OHDSI Athena vocabularies into Iceberg tables for concept mapping.

This DAG is designed to run once to set up the vocabulary foundation.
Re-run only when vocabulary updates are needed.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.trino.operators.trino import TrinoOperator
import requests
import os
import zipfile
import pandas as pd
from minio import Minio
from trino.dbapi import connect

# Configuration
MINIO_ENDPOINT = "minio-api.ns-data-platform.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
TRINO_HOST = "my-trino-trino.ns-data-platform.svc.cluster.local"
TRINO_PORT = 8080
TRINO_USER = "trino"
VOCABULARY_BUCKET = "omop-vocabularies"
ATHENA_DOWNLOAD_URL = "https://athena.ohdsi.org/api/v1/vocabularies"

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'setup_omop_vocabularies',
    default_args=default_args,
    description='One-time setup of OMOP vocabularies from OHDSI Athena',
    schedule=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['omop', 'vocabularies', 'setup'],
)

def check_vocabularies_exist(**context):
    """Check if OMOP vocabulary tables already exist."""
    try:
        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog="iceberg",
            schema="vocabularies"
        )
        cursor = conn.cursor()

        # Check if vocabulary tables exist
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'vocabularies'
              AND table_name IN ('concept', 'vocabulary', 'concept_relationship')
        """)

        existing_tables = [row[0] for row in cursor.fetchall()]
        conn.close()

        if len(existing_tables) >= 3:
            print(f"✅ Found existing vocabulary tables: {existing_tables}")
            return 'vocabularies_exist_skip'
        else:
            print(f"⚠️ Missing vocabulary tables. Found: {existing_tables}")
            return 'create_vocabulary_schema'

    except Exception as e:
        print(f"❌ Error checking vocabularies: {e}")
        return 'create_vocabulary_schema'

def download_athena_vocabularies(**context):
    """
    Download OHDSI Athena vocabularies.
    Note: This requires manual vocabulary bundle creation at https://athena.ohdsi.org/
    """
    import tempfile

    print("📥 Downloading OMOP vocabularies...")
    print("⚠️ Manual Step Required:")
    print("1. Go to https://athena.ohdsi.org/vocabulary/list")
    print("2. Create a vocabulary bundle with: LOINC, UCUM, SNOMED, RxNorm, Gender, Race")
    print("3. Download the vocabulary zip file")
    print("4. Upload it to MinIO bucket 'omop-vocabularies' as 'athena-vocabularies.zip'")

    # Check if vocabularies are uploaded to MinIO
    minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    try:
        # Create bucket if it doesn't exist
        if not minio_client.bucket_exists(VOCABULARY_BUCKET):
            minio_client.make_bucket(VOCABULARY_BUCKET)
            print(f"📁 Created bucket: {VOCABULARY_BUCKET}")

        # Check if vocabulary file exists
        objects = list(minio_client.list_objects(VOCABULARY_BUCKET, prefix="athena-vocabularies"))
        if not objects:
            raise Exception("❌ Vocabulary file not found. Please upload 'athena-vocabularies.zip' to MinIO.")

        print("✅ Vocabulary file found in MinIO")

        # Download vocabulary file
        with tempfile.TemporaryDirectory() as temp_dir:
            vocab_path = os.path.join(temp_dir, "athena-vocabularies.zip")

            minio_client.fget_object(VOCABULARY_BUCKET, "athena-vocabularies.zip", vocab_path)
            print(f"📥 Downloaded vocabularies to: {vocab_path}")

            # Extract and validate
            with zipfile.ZipFile(vocab_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            # Check for required vocabulary files
            required_files = ['CONCEPT.csv', 'VOCABULARY.csv', 'CONCEPT_RELATIONSHIP.csv']
            extracted_files = os.listdir(temp_dir)

            missing_files = [f for f in required_files if f not in extracted_files]
            if missing_files:
                raise Exception(f"❌ Missing vocabulary files: {missing_files}")

            print(f"✅ Extracted vocabulary files: {extracted_files}")

            # Store extraction path for next task
            context['task_instance'].xcom_push(key='vocab_temp_dir', value=temp_dir)

    except Exception as e:
        print(f"❌ Error downloading vocabularies: {e}")
        raise

def load_vocabulary_to_iceberg(**context):
    """Load vocabulary CSV files into Iceberg tables with proper data types and partitioning."""

    print("📊 Loading vocabularies into Iceberg tables...")

    # Get vocabulary files location
    vocab_temp_dir = context['task_instance'].xcom_pull(key='vocab_temp_dir', task_ids='download_vocabularies')

    if not vocab_temp_dir or not os.path.exists(vocab_temp_dir):
        # Re-download if needed
        minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            vocab_path = os.path.join(temp_dir, "athena-vocabularies.zip")
            minio_client.fget_object(VOCABULARY_BUCKET, "athena-vocabularies.zip", vocab_path)

            with zipfile.ZipFile(vocab_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            vocab_temp_dir = temp_dir

    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="iceberg", schema="vocabularies")
    cursor = conn.cursor()

    try:
        # Load CONCEPT table (largest, ~5M rows)
        concept_file = os.path.join(vocab_temp_dir, "CONCEPT.csv")
        if os.path.exists(concept_file):
            print("📋 Loading CONCEPT table...")

            # Stream load in chunks to avoid memory issues
            chunk_size = 10000
            total_rows = 0

            for chunk_df in pd.read_csv(concept_file, sep='\t', chunksize=chunk_size, dtype=str):
                # Clean data types
                chunk_df = chunk_df.fillna('')

                # Insert chunk
                insert_sql = """
                    INSERT INTO concept
                    (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id,
                     standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                for _, row in chunk_df.iterrows():
                    cursor.execute(insert_sql, tuple(row.values))

                total_rows += len(chunk_df)
                print(f"  ✅ Loaded {len(chunk_df)} concept rows (total: {total_rows})")

        # Load VOCABULARY table
        vocab_file = os.path.join(vocab_temp_dir, "VOCABULARY.csv")
        if os.path.exists(vocab_file):
            print("📚 Loading VOCABULARY table...")

            vocab_df = pd.read_csv(vocab_file, sep='\t', dtype=str).fillna('')

            insert_sql = """
                INSERT INTO vocabulary
                (vocabulary_id, vocabulary_name, vocabulary_reference, vocabulary_version, vocabulary_concept_id)
                VALUES (?, ?, ?, ?, ?)
            """

            for _, row in vocab_df.iterrows():
                cursor.execute(insert_sql, tuple(row.values))

            print(f"  ✅ Loaded {len(vocab_df)} vocabulary rows")

        # Load CONCEPT_RELATIONSHIP table (for concept mapping)
        rel_file = os.path.join(vocab_temp_dir, "CONCEPT_RELATIONSHIP.csv")
        if os.path.exists(rel_file):
            print("🔗 Loading CONCEPT_RELATIONSHIP table...")

            chunk_size = 50000  # Larger chunks for relationships
            total_rows = 0

            for chunk_df in pd.read_csv(rel_file, sep='\t', chunksize=chunk_size, dtype=str):
                chunk_df = chunk_df.fillna('')

                insert_sql = """
                    INSERT INTO concept_relationship
                    (concept_id_1, concept_id_2, relationship_id, valid_start_date,
                     valid_end_date, invalid_reason)
                    VALUES (?, ?, ?, ?, ?, ?)
                """

                for _, row in chunk_df.iterrows():
                    cursor.execute(insert_sql, tuple(row.values))

                total_rows += len(chunk_df)
                print(f"  ✅ Loaded {len(chunk_df)} relationship rows (total: {total_rows})")

        print("✅ All vocabulary tables loaded successfully!")

    except Exception as e:
        print(f"❌ Error loading vocabularies: {e}")
        raise
    finally:
        conn.close()

# Task definitions
check_vocabularies_branch = BranchPythonOperator(
    task_id='check_vocabularies_exist',
    python_callable=check_vocabularies_exist,
    dag=dag,
)

vocabularies_exist_skip = BashOperator(
    task_id='vocabularies_exist_skip',
    bash_command='echo "✅ OMOP vocabularies already exist. Skipping setup."',
    dag=dag,
)

create_vocabulary_schema = TrinoOperator(
    task_id='create_vocabulary_schema',
    conn_id='trino_default',
    sql="""
        CREATE SCHEMA IF NOT EXISTS iceberg.vocabularies
        WITH (location = 's3://eds-lakehouse/vocabularies/')
    """,
    dag=dag,
)

create_concept_table = TrinoOperator(
    task_id='create_concept_table',
    conn_id='trino_default',
    sql="""
        CREATE TABLE IF NOT EXISTS iceberg.vocabularies.concept (
            concept_id INTEGER,
            concept_name VARCHAR,
            domain_id VARCHAR,
            vocabulary_id VARCHAR,
            concept_class_id VARCHAR,
            standard_concept VARCHAR,
            concept_code VARCHAR,
            valid_start_date DATE,
            valid_end_date DATE,
            invalid_reason VARCHAR
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['bucket(concept_id, 256)', 'vocabulary_id'],
            sorted_by = ARRAY['concept_id', 'concept_name'],
            location = 's3://eds-lakehouse/vocabularies/concept/'
        )
    """,
    dag=dag,
)

create_vocabulary_table = TrinoOperator(
    task_id='create_vocabulary_table',
    conn_id='trino_default',
    sql="""
        CREATE TABLE IF NOT EXISTS iceberg.vocabularies.vocabulary (
            vocabulary_id VARCHAR,
            vocabulary_name VARCHAR,
            vocabulary_reference VARCHAR,
            vocabulary_version VARCHAR,
            vocabulary_concept_id INTEGER
        ) WITH (
            format = 'PARQUET',
            location = 's3://eds-lakehouse/vocabularies/vocabulary/'
        )
    """,
    dag=dag,
)

create_concept_relationship_table = TrinoOperator(
    task_id='create_concept_relationship_table',
    conn_id='trino_default',
    sql="""
        CREATE TABLE IF NOT EXISTS iceberg.vocabularies.concept_relationship (
            concept_id_1 INTEGER,
            concept_id_2 INTEGER,
            relationship_id VARCHAR,
            valid_start_date DATE,
            valid_end_date DATE,
            invalid_reason VARCHAR
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['bucket(concept_id_1, 128)', 'relationship_id'],
            sorted_by = ARRAY['concept_id_1', 'concept_id_2'],
            location = 's3://eds-lakehouse/vocabularies/concept_relationship/'
        )
    """,
    dag=dag,
)

download_vocabularies = PythonOperator(
    task_id='download_vocabularies',
    python_callable=download_athena_vocabularies,
    dag=dag,
)

load_vocabularies = PythonOperator(
    task_id='load_vocabularies',
    python_callable=load_vocabulary_to_iceberg,
    dag=dag,
)

validate_vocabularies = TrinoOperator(
    task_id='validate_vocabularies',
    conn_id='trino_default',
    sql="""
        -- Validate vocabulary loading
        SELECT
            'CONCEPT' as table_name,
            COUNT(*) as row_count,
            COUNT(DISTINCT vocabulary_id) as vocabulary_count
        FROM iceberg.vocabularies.concept
        WHERE concept_id > 0

        UNION ALL

        SELECT
            'VOCABULARY' as table_name,
            COUNT(*) as row_count,
            COUNT(DISTINCT vocabulary_id) as vocabulary_count
        FROM iceberg.vocabularies.vocabulary

        UNION ALL

        SELECT
            'CONCEPT_RELATIONSHIP' as table_name,
            COUNT(*) as row_count,
            COUNT(DISTINCT relationship_id) as vocabulary_count
        FROM iceberg.vocabularies.concept_relationship
    """,
    dag=dag,
)

# Define dependencies
check_vocabularies_branch >> [vocabularies_exist_skip, create_vocabulary_schema]

create_vocabulary_schema >> [
    create_concept_table,
    create_vocabulary_table,
    create_concept_relationship_table
]

[create_concept_table, create_vocabulary_table, create_concept_relationship_table] >> download_vocabularies

download_vocabularies >> load_vocabularies >> validate_vocabularies