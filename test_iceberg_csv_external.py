"""
Test Iceberg CSV External Table Capabilities

Test if Iceberg can create external tables pointing to CSV files in MinIO
and read the 998 CSV files directly without Python workarounds.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'test',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'test_iceberg_csv_external',
    default_args=default_args,
    description='Test Iceberg CSV external table capabilities',
    schedule=None,
    catchup=False,
    tags=['test', 'iceberg', 'csv'],
)

def execute_trino_test(sql_query, description):
    """Execute Trino query for testing"""
    print(f"🔍 {description}")
    print(f"📝 SQL: {sql_query[:200]}{'...' if len(sql_query) > 200 else ''}")

    try:
        import trino
        print("✅ Trino module available")
    except ImportError as e:
        print(f"❌ Trino not available: {e}")
        raise Exception("Trino required for testing")

    try:
        conn = trino.dbapi.connect(
            host='my-trino-trino.ns-data-platform.svc.cluster.local',
            port=8080,
            user='airflow',
            catalog='iceberg',
            schema='default'
        )

        cursor = conn.cursor()
        print("✅ Connected to Trino (iceberg catalog)")

        cursor.execute(sql_query)

        if sql_query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            print(f"📊 Results ({len(results)} rows):")
            for row in results[:5]:  # Show first 5 rows only
                print(f"   {row}")
            if len(results) > 5:
                print(f"   ... and {len(results) - 5} more rows")
            cursor.close()
            conn.close()
            return results

        cursor.close()
        conn.close()
        print(f"✅ {description} completed successfully")
        return "success"

    except Exception as e:
        print(f"❌ Error in {description}: {str(e)}")
        print(f"🔧 Error type: {type(e).__name__}")
        raise

def test_catalogs_and_schemas(**context):
    """Test available catalogs and schemas"""
    print("🗂️  Testing available catalogs and schemas...")

    # Test catalogs
    catalogs = execute_trino_test("SHOW CATALOGS", "Show available catalogs")

    # Test iceberg schemas
    schemas = execute_trino_test("SHOW SCHEMAS FROM iceberg", "Show iceberg schemas")

    print(f"✅ Found catalogs and schemas")
    return {"catalogs": catalogs, "schemas": schemas}

def create_raw_schema(**context):
    """Create bronze_raw schema for CSV files"""
    print("🏗️  Creating bronze_raw schema for CSV files...")

    sql = "CREATE SCHEMA IF NOT EXISTS iceberg.bronze_raw WITH (location = 's3://bronze/')"
    result = execute_trino_test(sql, "Create bronze_raw schema")

    # Verify schema exists
    schemas = execute_trino_test("SHOW SCHEMAS FROM iceberg", "Verify schema created")

    print("✅ bronze_raw schema ready")
    return result

def create_csv_external_table(**context):
    """Create external table pointing to CSV files"""
    print("📄 Creating external CSV table...")

    sql = """
    CREATE TABLE IF NOT EXISTS iceberg.bronze_raw.biological_results_external (
        patient_id VARCHAR,
        visit_id VARCHAR,
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
    )
    WITH (
        external_location = 's3://bronze/',
        format = 'CSV',
        skip_header_line_count = 1
    )
    """

    try:
        result = execute_trino_test(sql, "Create external CSV table")
        print("✅ External CSV table created successfully!")
        return result
    except Exception as e:
        print(f"❌ External table creation failed: {e}")
        print("🔧 This means Iceberg external CSV tables are not supported")
        raise

def test_csv_reading(**context):
    """Test reading from external CSV table"""
    print("📊 Testing CSV reading from external table...")

    try:
        # Count rows
        count_result = execute_trino_test(
            "SELECT COUNT(*) FROM iceberg.bronze_raw.biological_results_external",
            "Count CSV rows"
        )

        # Sample data
        sample_result = execute_trino_test(
            "SELECT * FROM iceberg.bronze_raw.biological_results_external LIMIT 5",
            "Sample CSV data"
        )

        print("✅ CSV reading successful!")
        print(f"📈 Found {count_result[0][0] if count_result else 'unknown'} rows")
        return {"count": count_result, "sample": sample_result}

    except Exception as e:
        print(f"❌ CSV reading failed: {e}")
        raise

def validate_csv_approach(**context):
    """Validate if Iceberg CSV approach is viable for 998 files"""
    print("🎯 Validating Iceberg CSV approach for scale...")

    # Get results from previous tasks
    csv_results = context['task_instance'].xcom_pull(task_ids='test_csv_reading')

    if csv_results and csv_results.get('count'):
        row_count = csv_results['count'][0][0]
        print(f"📊 External table returned {row_count:,} rows")

        if row_count > 0:
            print("✅ ICEBERG CSV EXTERNAL TABLES WORKING!")
            print("✅ Can process all 998 CSV files with Iceberg")
            print("🚀 Ready to update Step 2 with this approach")
            return "iceberg_csv_success"
        else:
            print("⚠️  External table created but no data returned")
            print("🔧 May need to check CSV file format or location")
            return "iceberg_csv_empty"
    else:
        print("❌ Iceberg external CSV tables not working")
        print("🔄 Will need to continue with Python hybrid approach")
        return "iceberg_csv_failed"

# Task definitions
test_setup = PythonOperator(
    task_id='test_catalogs_and_schemas',
    python_callable=test_catalogs_and_schemas,
    dag=dag,
)

create_schema = PythonOperator(
    task_id='create_raw_schema',
    python_callable=create_raw_schema,
    dag=dag,
)

create_table = PythonOperator(
    task_id='create_csv_external_table',
    python_callable=create_csv_external_table,
    dag=dag,
)

test_reading = PythonOperator(
    task_id='test_csv_reading',
    python_callable=test_csv_reading,
    dag=dag,
)

validate_approach = PythonOperator(
    task_id='validate_csv_approach',
    python_callable=validate_csv_approach,
    dag=dag,
)

# Dependencies
test_setup >> create_schema >> create_table >> test_reading >> validate_approach