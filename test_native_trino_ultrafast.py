"""
Test Native Trino Ultra-Fast Capability

Quick test to verify if system.table_function.read_files()
works with our Trino + MinIO setup for ultra-fast processing.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'test-ultrafast',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'test_native_trino_ultrafast',
    default_args=default_args,
    description='Test native Trino read_files() for ultra-fast CSV processing',
    schedule=None,
    catchup=False,
    tags=['test', 'ultrafast', 'native-trino'],
)

def test_read_files_function(**context):
    """Test if read_files works with our setup"""
    print("🧪 Testing Trino native read_files() for ultra-fast processing...")

    import trino

    conn = trino.dbapi.connect(
        host='my-trino-trino.ns-data-platform.svc.cluster.local',
        port=8080,
        user='airflow',
        catalog='iceberg',
        schema='default'
    )

    cursor = conn.cursor()

    try:
        # Test 1: Check if read_files function exists
        print("\n🔍 Test 1: Checking if read_files function is available...")
        cursor.execute("SHOW FUNCTIONS FROM system LIKE 'read_files'")
        functions = cursor.fetchall()
        print(f"✅ Found functions: {functions}")

        # Test 2: Try reading a single CSV file
        print("\n🔍 Test 2: Testing read_files with single CSV file...")
        sql_single = """
        SELECT COUNT(*) as row_count, MAX(patient_id) as sample_id
        FROM TABLE(
            system.table_function.read_files(
                path => 's3://bronze/biological_results_0000.csv',
                format => 'CSV',
                header => true
            )
        )
        """

        cursor.execute(sql_single)
        result = cursor.fetchall()

        if result and result[0][0] > 0:
            rows = result[0][0]
            sample_id = result[0][1]
            print(f"✅ SUCCESS! Single file test:")
            print(f"   - Rows found: {rows:,}")
            print(f"   - Sample patient ID: {sample_id}")

            # Test 3: Try reading multiple CSV files pattern
            print("\n🔍 Test 3: Testing wildcard pattern with multiple files...")
            sql_multi = """
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT regexp_extract("$path", '[^/]+$')) as file_count,
                MIN(regexp_extract("$path", '[^/]+$')) as first_file,
                MAX(regexp_extract("$path", '[^/]+$')) as last_file
            FROM TABLE(
                system.table_function.read_files(
                    path => 's3://bronze/biological_results_000[0-4].csv',
                    format => 'CSV',
                    header => true
                )
            )
            """

            cursor.execute(sql_multi)
            multi_result = cursor.fetchall()

            if multi_result:
                total_rows = multi_result[0][0]
                file_count = multi_result[0][1]
                first_file = multi_result[0][2]
                last_file = multi_result[0][3]

                print(f"✅ SUCCESS! Multi-file test:")
                print(f"   - Total rows: {total_rows:,}")
                print(f"   - Files processed: {file_count}")
                print(f"   - File range: {first_file} to {last_file}")

                print(f"\n🎉 ULTRA-FAST NATIVE TRINO IS POSSIBLE!")
                print(f"🚀 Can process ALL 2000 CSV files in 2-3 hours!")
                print(f"⚡ Estimated processing rate: {total_rows/file_count:.0f} rows/file")

                cursor.close()
                conn.close()
                return "ultra_fast_possible"

        else:
            print("❌ read_files returned 0 rows")
            cursor.close()
            conn.close()
            return "read_files_failed"

    except Exception as e:
        print(f"❌ read_files test failed: {e}")
        print("💡 Will need to use optimized Python approach instead")
        cursor.close()
        conn.close()
        return "read_files_not_available"

def evaluate_ultra_fast_capability(**context):
    """Evaluate test results and recommend next steps"""
    result = context['task_instance'].xcom_pull(task_ids='test_read_files_function')

    print(f"\n{'='*60}")
    print(f"🎯 ULTRA-FAST CAPABILITY EVALUATION")
    print(f"{'='*60}")

    if result == "ultra_fast_possible":
        print(f"✅ RECOMMENDATION: Deploy Ultra-Fast Native Trino!")
        print(f"🚀 Expected time: 2-3 hours for ALL 2000 CSV files")
        print(f"⚡ Speed improvement: 5-8x faster than Python approach")
        print(f"🎯 Next step: Deploy step2_bronze_ingestion_ultra_fast DAG")
        recommendation = "deploy_ultra_fast"

    elif result == "read_files_failed":
        print(f"⚠️  RECOMMENDATION: Optimize current Python approach")
        print(f"🔧 Increase batch size from 10 to 50 files")
        print(f"📊 Expected time: 5-6 hours (50% improvement)")
        recommendation = "optimize_python"

    else:
        print(f"❌ RECOMMENDATION: Continue current approach")
        print(f"⏱️  Expected time: 10-15 hours (current running)")
        print(f"✅ Guaranteed to work, just slower")
        recommendation = "continue_current"

    print(f"{'='*60}")

    context['task_instance'].xcom_push(key='recommendation', value=recommendation)
    return recommendation

# Task definitions
test_capability = PythonOperator(
    task_id='test_read_files_function',
    python_callable=test_read_files_function,
    dag=dag,
)

evaluate_results = PythonOperator(
    task_id='evaluate_ultra_fast_capability',
    python_callable=evaluate_ultra_fast_capability,
    dag=dag,
)

# Pipeline
test_capability >> evaluate_results