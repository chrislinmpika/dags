"""
CSV to Bronze DAG - Process actual CSV files from MinIO

This DAG reads the biological_results CSV files discovered in MinIO
and creates the proper bronze table from them.

Based on discovery results showing:
- biological_results_0000.csv through 0007.csv in MinIO bronze bucket
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'csv_to_bronze',
    default_args=default_args,
    description='Process CSV files from MinIO to create bronze table',
    schedule=None,
    catchup=False,
    tags=['csv', 'bronze', 'minio'],
)

def execute_bronze_trino(sql_query, description):
    """Execute Trino for bronze operations"""
    print(f"🚀 {description}")

    try:
        import trino
        print("✅ Trino module loaded")
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
        print("✅ Connected to Trino (iceberg.bronze)")

        print(f"📝 Executing SQL: {sql_query[:200]}...")
        cursor.execute(sql_query)

        if sql_query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            for row in results:
                print(f"📊 {row}")
            cursor.close()
            conn.close()
            return results

        cursor.close()
        conn.close()
        print(f"✅ {description} completed")
        return "success"

    except Exception as e:
        print(f"❌ Error in {description}: {e}")
        print(f"🔧 Error details: {str(e)}")
        raise

def create_bronze_schema(**context):
    """Create bronze schema if it doesn't exist"""
    sql = "CREATE SCHEMA IF NOT EXISTS iceberg.bronze"
    return execute_bronze_trino(sql, "Create bronze schema")

def create_external_csv_table(**context):
    """Create external table pointing to CSV files in MinIO"""

    # Create external table that can read CSV files from MinIO
    sql = """
    CREATE TABLE IF NOT EXISTS iceberg.bronze.biological_results_csv (
        patient_id VARCHAR,
        visit_id BIGINT,
        sampling_datetime_utc VARCHAR,
        result_datetime_utc VARCHAR,
        report_date_utc VARCHAR,
        measurement_source_value VARCHAR,
        value_as_number VARCHAR,
        value_as_string VARCHAR,
        unit_source_value VARCHAR,
        normality VARCHAR,
        abnormal_flag VARCHAR,
        value_type VARCHAR,
        bacterium_id VARCHAR,
        provider_id VARCHAR,
        laboratory_uuid VARCHAR
    ) WITH (
        format = 'CSV',
        location = 's3a://bronze/biological_results/',
        external_location = 's3a://bronze/biological_results/'
    )
    """

    return execute_bronze_trino(sql, "Create external CSV table")

def create_enhanced_bronze_from_csv(**context):
    """Create enhanced bronze table from CSV data"""

    sql = """
    CREATE OR REPLACE TABLE iceberg.bronze.biological_results_enhanced AS
    SELECT
        -- Original CSV columns
        patient_id,
        visit_id,

        -- Convert string dates to proper timestamps
        TRY(CAST(sampling_datetime_utc AS TIMESTAMP)) AS sampling_datetime_utc,
        TRY(CAST(result_datetime_utc AS TIMESTAMP)) AS result_datetime_utc,
        TRY(CAST(report_date_utc AS DATE)) AS report_date_utc,

        measurement_source_value,

        -- Convert numeric values
        TRY(CAST(value_as_number AS DOUBLE)) AS value_as_number,
        value_as_string,
        unit_source_value,
        normality,
        abnormal_flag,
        value_type,
        bacterium_id,
        provider_id,
        laboratory_uuid,

        -- Add computed columns for OMOP
        'csv_import' AS source_file,
        abs(hash(patient_id)) AS patient_id_hash,
        substr(to_hex(sha256(to_utf8(patient_id))), 1, 32) AS person_id_hash,

        ROW_NUMBER() OVER (ORDER BY sampling_datetime_utc, patient_id) AS measurement_id,

        TRY(CAST(sampling_datetime_utc AS DATE)) AS measurement_date,
        TRY(CAST(sampling_datetime_utc AS TIMESTAMP)) AS measurement_datetime,
        TRY(CAST(sampling_datetime_utc AS DATE)) AS visit_date,

        COALESCE(value_as_string, CAST(TRY(CAST(value_as_number AS DOUBLE)) AS VARCHAR)) AS value_source_value,

        CASE
            WHEN TRY(CAST(value_as_number AS DOUBLE)) IS NOT NULL AND unit_source_value IS NOT NULL THEN 1.0
            WHEN value_as_string IS NOT NULL THEN 0.8
            ELSE 0.5
        END AS data_quality_score,

        CASE
            WHEN TRY(CAST(value_as_number AS DOUBLE)) IS NOT NULL THEN 'Measurement'
            ELSE 'Observation'
        END AS omop_target_domain,

        COALESCE(normality, abnormal_flag, 'Unknown') AS value_qualifier,

        CURRENT_TIMESTAMP AS load_timestamp,
        'csv_to_bronze' AS processing_batch_id,
        substr(to_hex(sha256(to_utf8(patient_id || CAST(visit_id AS VARCHAR)))), 1, 16) AS gdpr_original_hash

    FROM iceberg.bronze.biological_results_csv
    WHERE patient_id IS NOT NULL
      AND visit_id IS NOT NULL
      AND sampling_datetime_utc IS NOT NULL
      AND sampling_datetime_utc != ''
    """

    return execute_bronze_trino(sql, "Create enhanced bronze table from CSV")

def validate_bronze_creation(**context):
    """Validate the bronze table was created successfully"""

    validation_queries = [
        ("Row count", "SELECT COUNT(*) as total_rows FROM iceberg.bronze.biological_results_enhanced"),
        ("Unique patients", "SELECT COUNT(DISTINCT person_id_hash) as unique_patients FROM iceberg.bronze.biological_results_enhanced"),
        ("Date range", "SELECT MIN(measurement_date) as earliest, MAX(measurement_date) as latest FROM iceberg.bronze.biological_results_enhanced"),
        ("Sample data", "SELECT measurement_source_value, value_as_number, unit_source_value FROM iceberg.bronze.biological_results_enhanced LIMIT 5")
    ]

    results = {}
    for desc, query in validation_queries:
        print(f"\n🔍 {desc}:")
        result = execute_bronze_trino(query, desc)
        results[desc] = result

    print("\n🎉 Bronze table creation validation completed!")
    return results

# Task definitions
create_schema_task = PythonOperator(
    task_id='create_bronze_schema',
    python_callable=create_bronze_schema,
    dag=dag,
)

create_csv_table_task = PythonOperator(
    task_id='create_external_csv_table',
    python_callable=create_external_csv_table,
    dag=dag,
)

create_enhanced_task = PythonOperator(
    task_id='create_enhanced_bronze_from_csv',
    python_callable=create_enhanced_bronze_from_csv,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_bronze_creation',
    python_callable=validate_bronze_creation,
    dag=dag,
)

# Task dependencies
create_schema_task >> create_csv_table_task >> create_enhanced_task >> validate_task