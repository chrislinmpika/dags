"""
Simple Bronze Setup - Minimal approach that works

Creates just what we need for the POC without complex schema operations.
This DAG will create the bronze table with sample data directly.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'simple_bronze_setup',
    default_args=default_args,
    description='Simple bronze setup that works',
    schedule=None,
    catchup=False,
    tags=['bronze', 'simple', 'poc'],
)

def execute_simple_trino(sql_query, description):
    """Execute Trino with better error handling"""
    print(f"🚀 {description}")
    print(f"📝 SQL: {sql_query[:200]}...")

    try:
        import trino
    except ImportError as e:
        print(f"❌ Trino module error: {e}")
        raise

    try:
        # Connect with minimal configuration
        conn = trino.dbapi.connect(
            host='my-trino-trino.ns-data-platform.svc.cluster.local',
            port=8080,
            user='airflow',
            catalog='iceberg'
            # Don't specify schema initially
        )

        cursor = conn.cursor()
        print("✅ Connected to Trino")

        # Execute the query
        cursor.execute(sql_query)
        print("✅ Query executed successfully")

        # If SELECT, show results
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
        print(f"🔍 Error type: {type(e).__name__}")
        raise

def create_bronze_with_data(**context):
    """Create bronze table directly with data - no schema operations"""

    # First try to create the table with data in one go
    sql = """
    CREATE OR REPLACE TABLE iceberg.bronze.biological_results_enhanced AS
    SELECT * FROM (
        VALUES
        (
            'PATIENT_001',
            BIGINT '1001',
            TIMESTAMP '2023-06-15 09:00:00',
            TIMESTAMP '2023-06-15 10:30:00',
            DATE '2023-06-15',
            'hemoglobin',
            DOUBLE '12.5',
            CAST(NULL AS VARCHAR),
            'g/dL',
            'NORMAL',
            'NORMAL',
            'NUMERIC',
            CAST(NULL AS VARCHAR),
            'PROV_01',
            'LAB_UUID_001',
            'sample_file_001.csv',
            BIGINT '123456789',
            'a1b2c3d4e5f6',
            BIGINT '1',
            DATE '2023-06-15',
            TIMESTAMP '2023-06-15 09:00:00',
            DATE '2023-06-15',
            '12.5',
            DOUBLE '1.0',
            'Measurement',
            'NORMAL',
            CURRENT_TIMESTAMP,
            'bronze_creation',
            'hash123'
        ),
        (
            'PATIENT_002',
            BIGINT '1002',
            TIMESTAMP '2023-06-16 14:00:00',
            TIMESTAMP '2023-06-16 15:30:00',
            DATE '2023-06-16',
            'glucose',
            DOUBLE '95.0',
            CAST(NULL AS VARCHAR),
            'mg/dL',
            'NORMAL',
            'NORMAL',
            'NUMERIC',
            CAST(NULL AS VARCHAR),
            'PROV_02',
            'LAB_UUID_002',
            'sample_file_002.csv',
            BIGINT '234567890',
            'b2c3d4e5f6a7',
            BIGINT '2',
            DATE '2023-06-16',
            TIMESTAMP '2023-06-16 14:00:00',
            DATE '2023-06-16',
            '95.0',
            DOUBLE '1.0',
            'Measurement',
            'NORMAL',
            CURRENT_TIMESTAMP,
            'bronze_creation',
            'hash234'
        ),
        (
            'PATIENT_003',
            BIGINT '1003',
            TIMESTAMP '2023-06-17 11:00:00',
            TIMESTAMP '2023-06-17 12:30:00',
            DATE '2023-06-17',
            'white_blood_cells',
            DOUBLE '7500.0',
            CAST(NULL AS VARCHAR),
            'cells/µL',
            'NORMAL',
            'NORMAL',
            'NUMERIC',
            CAST(NULL AS VARCHAR),
            'PROV_03',
            'LAB_UUID_003',
            'sample_file_003.csv',
            BIGINT '345678901',
            'c3d4e5f6a7b8',
            BIGINT '3',
            DATE '2023-06-17',
            TIMESTAMP '2023-06-17 11:00:00',
            DATE '2023-06-17',
            '7500.0',
            DOUBLE '1.0',
            'Measurement',
            'NORMAL',
            CURRENT_TIMESTAMP,
            'bronze_creation',
            'hash345'
        )
    ) AS t (
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
        patient_id_hash,
        person_id_hash,
        measurement_id,
        measurement_date,
        measurement_datetime,
        visit_date,
        value_source_value,
        data_quality_score,
        omop_target_domain,
        value_qualifier,
        load_timestamp,
        processing_batch_id,
        gdpr_original_hash
    )
    """

    return execute_simple_trino(sql, "Create bronze table with sample data")

def validate_bronze_simple(**context):
    """Validate the bronze table was created"""
    sql = """
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT person_id_hash) as unique_patients
    FROM iceberg.bronze.biological_results_enhanced
    """

    return execute_simple_trino(sql, "Validate bronze table creation")

def test_bronze_query(**context):
    """Test that we can query the bronze data"""
    sql = """
    SELECT
        measurement_source_value,
        value_as_number,
        unit_source_value
    FROM iceberg.bronze.biological_results_enhanced
    LIMIT 3
    """

    return execute_simple_trino(sql, "Test bronze data query")

# Task definitions
create_bronze_task = PythonOperator(
    task_id='create_bronze_with_data',
    python_callable=create_bronze_with_data,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_bronze_simple',
    python_callable=validate_bronze_simple,
    dag=dag,
)

test_task = PythonOperator(
    task_id='test_bronze_query',
    python_callable=test_bronze_query,
    dag=dag,
)

# Dependencies
create_bronze_task >> validate_task >> test_task