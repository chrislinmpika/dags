"""
Create Bronze Data DAG

Creates bronze layer from raw CSV files if needed.
This DAG will create the bronze.biological_results_enhanced table
from raw laboratory CSV files in MinIO.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_bronze_data',
    default_args=default_args,
    description='Create bronze layer from raw CSV files',
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['bronze', 'setup'],
)

def execute_trino_bronze(sql_query, description):
    """Execute Trino query for bronze creation"""
    print(f"🚀 {description}")

    try:
        import trino
    except ImportError as e:
        print(f"❌ Trino not available: {e}")
        raise

    try:
        conn = trino.dbapi.connect(
            host='my-trino-trino.ns-data-platform.svc.cluster.local',
            port=8080,
            user='airflow',
            catalog='iceberg',
            schema='bronze'
        )

        cursor = conn.cursor()
        print("✅ Connected to Trino (bronze schema)")

        cursor.execute(sql_query)

        if sql_query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            for row in results:
                print(f"📊 {row}")

        cursor.close()
        conn.close()
        print(f"✅ {description} completed")

    except Exception as e:
        print(f"❌ Error in {description}: {e}")
        raise

def create_bronze_schema(**context):
    """Create bronze schema if it doesn't exist"""
    sql = "CREATE SCHEMA IF NOT EXISTS iceberg.bronze"
    return execute_trino_bronze(sql, "Create bronze schema")

def create_raw_table(**context):
    """Create raw table from CSV files in MinIO"""
    # This creates a table that can read directly from CSV files
    sql = """
    CREATE TABLE IF NOT EXISTS iceberg.bronze.biological_results_raw (
        patient_id VARCHAR,
        visit_id BIGINT,
        sampling_datetime_utc TIMESTAMP,
        result_datetime_utc TIMESTAMP,
        report_date_utc DATE,
        measurement_source_value VARCHAR,
        value_as_number DOUBLE,
        value_as_string VARCHAR,
        unit_source_value VARCHAR,
        normality VARCHAR,
        abnormal_flag VARCHAR,
        value_type VARCHAR,
        bacterium_id VARCHAR,
        provider_id VARCHAR,
        laboratory_uuid VARCHAR,
        source_file VARCHAR
    ) WITH (
        format = 'PARQUET',
        partitioned_by = ARRAY['report_date_utc']
    )
    """
    return execute_trino_bronze(sql, "Create raw biological results table")

def create_enhanced_table(**context):
    """Create enhanced table with computed columns"""
    sql = """
    CREATE OR REPLACE TABLE iceberg.bronze.biological_results_enhanced AS
    SELECT
        -- Original columns
        patient_id,
        visit_id,
        sampling_datetime_utc,
        result_datetime_utc,
        report_date_utc,
        measurement_source_value,
        value_as_number,
        value_as_string,
        unit_source_value,
        normality,
        abnormal_flag,
        value_type,
        bacterium_id,
        provider_id,
        laboratory_uuid,
        source_file,

        -- Enhanced computed columns for OMOP compatibility
        abs(hash(patient_id)) AS patient_id_hash,
        substr(to_hex(sha256(to_utf8(patient_id))), 1, 32) AS person_id_hash,

        -- Measurement identifiers
        ROW_NUMBER() OVER (ORDER BY sampling_datetime_utc, patient_id) AS measurement_id,

        -- Standardized dates
        CAST(sampling_datetime_utc AS DATE) AS measurement_date,
        sampling_datetime_utc AS measurement_datetime,
        CAST(sampling_datetime_utc AS DATE) AS visit_date,

        -- Value processing
        COALESCE(CAST(value_as_string AS VARCHAR), CAST(value_as_number AS VARCHAR)) AS value_source_value,

        -- Quality indicators
        CASE
            WHEN value_as_number IS NOT NULL AND unit_source_value IS NOT NULL THEN 1.0
            WHEN value_as_string IS NOT NULL THEN 0.8
            ELSE 0.5
        END AS data_quality_score,

        -- OMOP domain assignment
        CASE
            WHEN value_as_number IS NOT NULL THEN 'Measurement'
            ELSE 'Observation'
        END AS omop_target_domain,

        -- Value qualifier
        COALESCE(normality, abnormal_flag, 'Unknown') AS value_qualifier,

        -- Processing metadata
        CURRENT_TIMESTAMP AS load_timestamp,
        'bronze_creation' AS processing_batch_id,
        substr(to_hex(sha256(to_utf8(patient_id || visit_id))), 1, 16) AS gdpr_original_hash

    FROM iceberg.bronze.biological_results_raw
    WHERE patient_id IS NOT NULL
      AND visit_id IS NOT NULL
      AND sampling_datetime_utc IS NOT NULL
    """
    return execute_trino_bronze(sql, "Create enhanced biological results table")

def load_sample_data(**context):
    """Load sample data if no raw data exists"""
    print("📋 Creating sample data for POC demonstration...")

    sql = """
    INSERT INTO iceberg.bronze.biological_results_raw VALUES
    ('PATIENT_001', 1001, TIMESTAMP '2023-06-15 09:00:00', TIMESTAMP '2023-06-15 10:30:00', DATE '2023-06-15', 'hemoglobin', 12.5, NULL, 'g/dL', 'NORMAL', 'NORMAL', 'NUMERIC', NULL, 'PROV_01', 'LAB_UUID_001', 'sample_file_001.csv'),
    ('PATIENT_001', 1001, TIMESTAMP '2023-06-15 09:00:00', TIMESTAMP '2023-06-15 10:30:00', DATE '2023-06-15', 'glucose', 95.0, NULL, 'mg/dL', 'NORMAL', 'NORMAL', 'NUMERIC', NULL, 'PROV_01', 'LAB_UUID_001', 'sample_file_001.csv'),
    ('PATIENT_002', 1002, TIMESTAMP '2023-06-16 14:00:00', TIMESTAMP '2023-06-16 15:30:00', DATE '2023-06-16', 'white_blood_cells', 7500.0, NULL, 'cells/µL', 'NORMAL', 'NORMAL', 'NUMERIC', NULL, 'PROV_02', 'LAB_UUID_002', 'sample_file_002.csv'),
    ('PATIENT_002', 1002, TIMESTAMP '2023-06-16 14:00:00', TIMESTAMP '2023-06-16 15:30:00', DATE '2023-06-16', 'creatinine', 1.1, NULL, 'mg/dL', 'NORMAL', 'NORMAL', 'NUMERIC', NULL, 'PROV_02', 'LAB_UUID_002', 'sample_file_002.csv'),
    ('PATIENT_003', 1003, TIMESTAMP '2023-06-17 11:00:00', TIMESTAMP '2023-06-17 12:30:00', DATE '2023-06-17', 'bacteria_culture', NULL, 'E. coli POSITIVE', 'culture', 'ABNORMAL', 'ABNORMAL', 'CATEGORICAL', 'ECOLI_001', 'PROV_03', 'LAB_UUID_003', 'sample_file_003.csv'),
    ('PATIENT_004', 1004, TIMESTAMP '2023-06-18 08:30:00', TIMESTAMP '2023-06-18 10:00:00', DATE '2023-06-18', 'cholesterol_total', 185.0, NULL, 'mg/dL', 'NORMAL', 'NORMAL', 'NUMERIC', NULL, 'PROV_01', 'LAB_UUID_001', 'sample_file_004.csv'),
    ('PATIENT_005', 1005, TIMESTAMP '2023-06-19 16:00:00', TIMESTAMP '2023-06-19 17:30:00', DATE '2023-06-19', 'albumin', 4.2, NULL, 'g/dL', 'NORMAL', 'NORMAL', 'NUMERIC', NULL, 'PROV_02', 'LAB_UUID_002', 'sample_file_005.csv')
    """
    return execute_trino_bronze(sql, "Load sample laboratory data")

def validate_bronze_creation(**context):
    """Validate that bronze tables were created successfully"""
    tables_to_check = [
        'biological_results_raw',
        'biological_results_enhanced'
    ]

    for table in tables_to_check:
        print(f"\n🔍 Validating {table}...")

        # Check row count
        sql_count = f"SELECT COUNT(*) as row_count FROM iceberg.bronze.{table}"
        execute_trino_bronze(sql_count, f"Row count for {table}")

        # Check sample data
        sql_sample = f"SELECT * FROM iceberg.bronze.{table} LIMIT 3"
        execute_trino_bronze(sql_sample, f"Sample data from {table}")

    print("\n🎉 Bronze layer creation completed!")
    print("✅ Ready for OMOP transformation")

# Task definitions
create_schema_task = PythonOperator(
    task_id='create_bronze_schema',
    python_callable=create_bronze_schema,
    dag=dag,
)

create_raw_task = PythonOperator(
    task_id='create_raw_table',
    python_callable=create_raw_table,
    dag=dag,
)

load_sample_task = PythonOperator(
    task_id='load_sample_data',
    python_callable=load_sample_data,
    dag=dag,
)

create_enhanced_task = PythonOperator(
    task_id='create_enhanced_table',
    python_callable=create_enhanced_table,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_bronze_creation',
    python_callable=validate_bronze_creation,
    dag=dag,
)

# Task dependencies
create_schema_task >> create_raw_task >> load_sample_task >> create_enhanced_task >> validate_task