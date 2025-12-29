"""
STEP 2: Bronze CSV Ingestion - Industrial Scale

Processes 998 CSV files (99.8 GB) from MinIO into Iceberg bronze tables.
Designed for high-volume parallel processing with proper error handling.

Based on Step 1 results:
- 998 CSV files discovered
- ~100MB per file
- ~998M records total
- Requires efficient batch processing
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'omop-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'step2_bronze_ingestion',
    default_args=default_args,
    description='Step 2: Ingest 998 CSV files into Iceberg bronze tables',
    schedule=None,  # Manual trigger after Step 1
    catchup=False,
    tags=['step2', 'bronze', 'ingestion', 'high-volume'],
)

def execute_trino_bronze(sql_query, description):
    """Execute Trino queries for bronze operations"""
    print(f"🚀 {description}")
    print(f"📝 SQL: {sql_query[:200]}...")

    try:
        import trino
        print("✅ Trino module available")
    except ImportError as e:
        print(f"❌ Trino not available: {e}")
        raise Exception("Trino required for bronze operations")

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

        cursor.execute(sql_query)

        if sql_query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            for row in results[:10]:  # Show first 10 rows only
                print(f"📊 {row}")
            if len(results) > 10:
                print(f"... and {len(results) - 10} more rows")
            cursor.close()
            conn.close()
            return results

        cursor.close()
        conn.close()
        print(f"✅ {description} completed successfully")
        return "success"

    except Exception as e:
        print(f"❌ Error in {description}: {str(e)}")
        raise

def prepare_bronze_environment(**context):
    """Prepare bronze schema and drop existing tables for fresh start"""
    print("🏗️  STEP 2: Preparing bronze environment...")

    # Create bronze schema
    sql_schema = "CREATE SCHEMA IF NOT EXISTS iceberg.bronze"
    execute_trino_bronze(sql_schema, "Create bronze schema")

    # Drop existing bronze tables for clean rebuild
    tables_to_drop = [
        'biological_results_raw',
        'biological_results_enhanced'
    ]

    for table in tables_to_drop:
        try:
            sql_drop = f"DROP TABLE IF EXISTS iceberg.bronze.{table}"
            execute_trino_bronze(sql_drop, f"Drop existing {table}")
        except Exception as e:
            print(f"⚠️  Could not drop {table}: {e}")

    print("✅ Bronze environment prepared for fresh ingestion")
    return "environment_ready"

def create_external_csv_table(**context):
    """Create external table pointing to all CSV files in MinIO"""
    print("📁 STEP 2: Creating external table for CSV files...")

    # Create external table that can read all CSV files from MinIO bronze bucket
    sql = """
    CREATE TABLE IF NOT EXISTS iceberg.bronze.biological_results_external (
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
        location = 's3a://bronze/',
        skip_header_line_count = 1
    )
    """

    return execute_trino_bronze(sql, "Create external CSV table")

def validate_external_table(**context):
    """Validate external table can read CSV data"""
    print("🔍 STEP 2: Validating external table access...")

    validation_queries = [
        ("Total row count", "SELECT COUNT(*) as total_rows FROM iceberg.bronze.biological_results_external"),
        ("Sample data check", "SELECT patient_id, visit_id, measurement_source_value FROM iceberg.bronze.biological_results_external LIMIT 5"),
        ("Data types check", "SELECT COUNT(DISTINCT patient_id) as unique_patients, COUNT(DISTINCT visit_id) as unique_visits FROM iceberg.bronze.biological_results_external LIMIT 1")
    ]

    results = {}
    for desc, query in validation_queries:
        try:
            print(f"\n🔍 {desc}:")
            result = execute_trino_bronze(query, desc)
            results[desc] = result
        except Exception as e:
            print(f"⚠️  {desc} failed: {e}")
            results[desc] = f"ERROR: {e}"

    # Store validation results for next task
    context['task_instance'].xcom_push(key='validation_results', value=results)

    print("✅ External table validation completed")
    return results

def create_bronze_raw_table(**context):
    """Create bronze raw table with proper data types from external CSV data"""
    print("⚗️  STEP 2: Creating bronze raw table with data type conversion...")

    sql = """
    CREATE TABLE iceberg.bronze.biological_results_raw AS
    SELECT
        -- Original columns with proper data types
        patient_id,
        visit_id,

        -- Convert string dates/times to proper types
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

        -- Add metadata columns
        CURRENT_TIMESTAMP AS load_timestamp,
        'step2_bronze_ingestion' AS processing_batch

    FROM iceberg.bronze.biological_results_external
    WHERE patient_id IS NOT NULL
      AND patient_id != ''
      AND visit_id IS NOT NULL
    """

    return execute_trino_bronze(sql, "Create bronze raw table")

def create_bronze_enhanced_table(**context):
    """Create enhanced bronze table with OMOP-ready computed columns"""
    print("⚡ STEP 2: Creating enhanced bronze table with OMOP columns...")

    sql = """
    CREATE TABLE iceberg.bronze.biological_results_enhanced AS
    SELECT
        -- All raw columns
        *,

        -- OMOP-ready computed columns
        abs(hash(patient_id)) AS patient_id_hash,
        substr(to_hex(sha256(to_utf8(patient_id))), 1, 32) AS person_id_hash,

        -- Generate measurement IDs
        ROW_NUMBER() OVER (ORDER BY sampling_datetime_utc, patient_id, visit_id) AS measurement_id,

        -- Standardized dates for OMOP
        CAST(sampling_datetime_utc AS DATE) AS measurement_date,
        sampling_datetime_utc AS measurement_datetime,
        CAST(sampling_datetime_utc AS DATE) AS visit_date,

        -- Value processing
        COALESCE(
            value_as_string,
            CAST(value_as_number AS VARCHAR)
        ) AS value_source_value,

        -- Data quality scoring
        CASE
            WHEN value_as_number IS NOT NULL AND unit_source_value IS NOT NULL THEN 1.0
            WHEN value_as_string IS NOT NULL AND value_as_string != '' THEN 0.8
            ELSE 0.5
        END AS data_quality_score,

        -- OMOP domain assignment
        CASE
            WHEN value_as_number IS NOT NULL THEN 'Measurement'
            WHEN value_as_string IS NOT NULL THEN 'Observation'
            ELSE 'Unknown'
        END AS omop_target_domain,

        -- Value qualifier
        COALESCE(normality, abnormal_flag, 'Unknown') AS value_qualifier,

        -- GDPR hash for audit
        substr(to_hex(sha256(to_utf8(patient_id || CAST(visit_id AS VARCHAR)))), 1, 16) AS gdpr_original_hash

    FROM iceberg.bronze.biological_results_raw
    """

    return execute_trino_bronze(sql, "Create enhanced bronze table")

def validate_bronze_ingestion(**context):
    """Validate complete bronze ingestion success"""
    print("✅ STEP 2: Final validation of bronze ingestion...")

    tables = ['biological_results_raw', 'biological_results_enhanced']

    for table in tables:
        print(f"\n📊 Validating {table}:")

        # Row count
        sql_count = f"SELECT COUNT(*) as row_count FROM iceberg.bronze.{table}"
        execute_trino_bronze(sql_count, f"Row count for {table}")

        # Sample data
        sql_sample = f"SELECT * FROM iceberg.bronze.{table} LIMIT 3"
        execute_trino_bronze(sql_sample, f"Sample data from {table}")

    # Final summary
    sql_summary = """
    SELECT
        'BRONZE INGESTION SUMMARY' as summary_type,
        COUNT(*) as total_records,
        COUNT(DISTINCT person_id_hash) as unique_patients,
        COUNT(DISTINCT visit_id) as unique_visits,
        MIN(measurement_date) as earliest_date,
        MAX(measurement_date) as latest_date,
        ROUND(AVG(data_quality_score), 3) as avg_data_quality
    FROM iceberg.bronze.biological_results_enhanced
    """

    summary = execute_trino_bronze(sql_summary, "Bronze ingestion summary")

    print("\n🎉 STEP 2 BRONZE INGESTION COMPLETED!")
    print("✅ Ready for Step 3: Data Quality & Cleaning")

    return summary

# Task definitions
prepare_env = PythonOperator(
    task_id='prepare_bronze_environment',
    python_callable=prepare_bronze_environment,
    dag=dag,
)

create_external = PythonOperator(
    task_id='create_external_csv_table',
    python_callable=create_external_csv_table,
    dag=dag,
)

validate_external = PythonOperator(
    task_id='validate_external_table',
    python_callable=validate_external_table,
    dag=dag,
)

create_raw = PythonOperator(
    task_id='create_bronze_raw_table',
    python_callable=create_bronze_raw_table,
    dag=dag,
)

create_enhanced = PythonOperator(
    task_id='create_bronze_enhanced_table',
    python_callable=create_bronze_enhanced_table,
    dag=dag,
)

validate_ingestion = PythonOperator(
    task_id='validate_bronze_ingestion',
    python_callable=validate_bronze_ingestion,
    dag=dag,
)

# Task dependencies - sequential for data integrity
prepare_env >> create_external >> validate_external >> create_raw >> create_enhanced >> validate_ingestion