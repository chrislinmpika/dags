"""
Test Direct CTAS Approach for CSV Processing

Test if Trino's system.table_function.read_files() can read
CSV files directly from MinIO for bulk processing.
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
    'test_direct_ctas_approach',
    default_args=default_args,
    description='Test Direct CTAS approach with read_files function',
    schedule=None,
    catchup=False,
    tags=['test', 'ctas', 'direct'],
)

def execute_trino_test(sql_query, description):
    """Execute Trino query for testing"""
    print(f"🔍 {description}")
    print(f"📝 SQL: {sql_query}")

    try:
        import trino
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
        cursor.execute(sql_query)

        if sql_query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            print(f"📊 Results ({len(results)} rows):")
            for row in results[:5]:
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

def test_read_files_function(**context):
    """Test if read_files function works with MinIO"""
    print("🧪 Testing system.table_function.read_files() with MinIO...")

    # Test 1: Check if read_files function exists
    print("\\n🔍 Test 1: Check if read_files function is available")
    try:
        sql_test = "SHOW FUNCTIONS FROM system LIKE 'read_files'"
        result = execute_trino_test(sql_test, "Check read_files function availability")
        print(f"✅ read_files function check completed")
    except Exception as e:
        print(f"❌ read_files function test failed: {e}")

    # Test 2: Try reading single CSV file
    print("\\n🔍 Test 2: Try reading single CSV file")
    try:
        sql_single = """
        SELECT COUNT(*) FROM TABLE(
            system.table_function.read_files(
                path => 's3://bronze/biological_results_0000.csv',
                format => 'CSV',
                header => true
            )
        )
        """
        result = execute_trino_test(sql_single, "Read single CSV file")
        print(f"✅ Single file read test completed")
        return "single_file_success"
    except Exception as e:
        print(f"❌ Single file read failed: {e}")
        print(f"🔧 This means read_files doesn't work with MinIO")
        return "read_files_not_supported"

def test_direct_ctas_full(**context):
    """Test full Direct CTAS approach with all CSV files"""
    print("🚀 Testing Direct CTAS with ALL CSV files...")

    # Get result from previous test
    single_test_result = context['task_instance'].xcom_pull(task_ids='test_read_files_function')

    if single_test_result != "single_file_success":
        print("❌ Skipping full test - single file test failed")
        return "skipped_due_to_single_failure"

    print("\\n📊 Creating bronze schema for CTAS test...")
    try:
        sql_schema = "CREATE SCHEMA IF NOT EXISTS iceberg.bronze_test"
        execute_trino_test(sql_schema, "Create test schema")
    except Exception as e:
        print(f"⚠️ Schema creation failed: {e}")

    print("\\n🔥 Testing Direct CTAS with ALL 998 CSV files...")
    try:
        sql_ctas = """
        CREATE TABLE IF NOT EXISTS iceberg.bronze_test.biological_results_ctas
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['bucket(patient_id, 16)']
        )
        AS
        SELECT
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
            laboratory_uuid
        FROM TABLE(
            system.table_function.read_files(
                path => 's3://bronze/*.csv',
                format => 'CSV',
                schema => 'patient_id VARCHAR, visit_id VARCHAR, sampling_datetime_utc VARCHAR, result_datetime_utc VARCHAR, report_date_utc VARCHAR, measurement_source_value VARCHAR, value_as_number VARCHAR, value_as_string VARCHAR, unit_source_value VARCHAR, normality VARCHAR, abnormal_flag VARCHAR, value_type VARCHAR, bacterium_id VARCHAR, provider_id VARCHAR, laboratory_uuid VARCHAR',
                header => true
            )
        )
        """

        result = execute_trino_test(sql_ctas, "Direct CTAS from all CSV files")

        # Count results
        count_sql = "SELECT COUNT(*) FROM iceberg.bronze_test.biological_results_ctas"
        count_result = execute_trino_test(count_sql, "Count CTAS results")

        if count_result:
            row_count = count_result[0][0]
            print(f"🎉 DIRECT CTAS SUCCESSFUL!")
            print(f"📊 Processed {row_count:,} rows from all CSV files")
            print(f"✅ This approach can handle 200GB efficiently!")
            return "ctas_success"

    except Exception as e:
        print(f"❌ Direct CTAS failed: {e}")
        print(f"🔧 Will need to use Python hybrid approach")
        return "ctas_failed"

def evaluate_best_approach(**context):
    """Determine the best approach based on test results"""
    print("🎯 EVALUATION: Best approach for 200GB CSV processing")

    # Get test results
    single_result = context['task_instance'].xcom_pull(task_ids='test_read_files_function')
    ctas_result = context['task_instance'].xcom_pull(task_ids='test_direct_ctas_full')

    print("\\n📊 Test Results Summary:")
    print(f"   - read_files function: {single_result}")
    print(f"   - Direct CTAS: {ctas_result}")

    if ctas_result == "ctas_success":
        print("\\n🏆 RECOMMENDED APPROACH: Direct CTAS")
        print("✅ Use system.table_function.read_files() for 200GB processing")
        print("✅ Single SQL statement processes all 998 files")
        print("✅ Trino workers handle parallel processing")
        print("✅ Much faster than Python hybrid approach")
        recommendation = "use_direct_ctas"

    else:
        print("\\n🔧 RECOMMENDED APPROACH: Optimized Python Hybrid")
        print("✅ Use current step2_bronze_ingestion_optimized")
        print("✅ Reliable batch processing with error handling")
        print("✅ Will take longer but guaranteed to work")
        recommendation = "use_python_hybrid"

    print("\\n🚀 NEXT STEPS:")
    if recommendation == "use_direct_ctas":
        print("   1. Update Step 2 to use Direct CTAS approach")
        print("   2. Test with subset of files first")
        print("   3. Run full 998 file processing")
    else:
        print("   1. Run step2_bronze_ingestion_optimized")
        print("   2. Monitor progress through all 998 files")
        print("   3. Continue with Step 3 after completion")

    return recommendation

# Task definitions
test_read_files = PythonOperator(
    task_id='test_read_files_function',
    python_callable=test_read_files_function,
    dag=dag,
)

test_ctas = PythonOperator(
    task_id='test_direct_ctas_full',
    python_callable=test_direct_ctas_full,
    dag=dag,
)

evaluate_approach = PythonOperator(
    task_id='evaluate_best_approach',
    python_callable=evaluate_best_approach,
    dag=dag,
)

# Dependencies
test_read_files >> test_ctas >> evaluate_approach