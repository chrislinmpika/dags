"""
Diagnose Trino-MinIO Connection Issues

This DAG will identify exactly what's blocking Trino from reading
the 998 CSV files from MinIO and provide the fix.

GOAL: Process real CSV files, not sample data workarounds
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'diagnostic',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'diagnose_trino_minio',
    default_args=default_args,
    description='Diagnose Trino-MinIO connectivity for CSV processing',
    schedule=None,
    catchup=False,
    tags=['diagnostic', 'trino', 'minio'],
)

def execute_trino_diagnostic(sql_query, description):
    """Execute Trino query for diagnostic purposes"""
    print(f"🔍 {description}")
    print(f"📝 SQL: {sql_query}")

    try:
        import trino
    except ImportError as e:
        print(f"❌ Trino module error: {e}")
        raise

    try:
        conn = trino.dbapi.connect(
            host='my-trino-trino.ns-data-platform.svc.cluster.local',
            port=8080,
            user='airflow',
            catalog='iceberg'  # Start with iceberg catalog
        )

        cursor = conn.cursor()
        print("✅ Connected to Trino")

        cursor.execute(sql_query)
        results = cursor.fetchall()

        print(f"📊 Results ({len(results)} rows):")
        for row in results:
            print(f"   {row}")

        cursor.close()
        conn.close()
        return results

    except Exception as e:
        print(f"❌ Query failed: {str(e)}")
        print(f"🔧 Error type: {type(e).__name__}")
        return f"ERROR: {str(e)}"

def check_available_catalogs(**context):
    """Check what catalogs are available in Trino"""
    print("🔍 DIAGNOSIS: Checking available Trino catalogs...")

    sql = "SHOW CATALOGS"
    results = execute_trino_diagnostic(sql, "Available catalogs")

    # Look for S3/MinIO related catalogs
    if results and results != "ERROR":
        catalogs = [row[0] for row in results]
        print(f"\n📋 Found {len(catalogs)} catalogs:")
        for cat in catalogs:
            print(f"   - {cat}")

        s3_catalogs = [cat for cat in catalogs if 's3' in cat.lower() or 'minio' in cat.lower()]
        if s3_catalogs:
            print(f"\n✅ S3/MinIO catalogs found: {s3_catalogs}")
        else:
            print("\n⚠️  No S3/MinIO catalogs found")

        context['task_instance'].xcom_push(key='catalogs', value=catalogs)
        context['task_instance'].xcom_push(key='s3_catalogs', value=s3_catalogs)

    return results

def test_s3_catalog_connection(**context):
    """Test if there's an S3 catalog we can use"""
    print("🔍 DIAGNOSIS: Testing S3 catalog connectivity...")

    catalogs = context['task_instance'].xcom_pull(task_ids='check_available_catalogs', key='catalogs')
    s3_catalogs = context['task_instance'].xcom_pull(task_ids='check_available_catalogs', key='s3_catalogs')

    if not catalogs:
        print("❌ No catalog information available")
        return "no_catalogs"

    # Try common S3 catalog names
    possible_s3_catalogs = ['s3', 'minio', 'hive', 'delta'] + (s3_catalogs or [])

    results = {}
    for catalog in possible_s3_catalogs:
        if catalog in catalogs:
            print(f"\n🔍 Testing catalog: {catalog}")
            try:
                # Connect to this specific catalog
                conn = trino.dbapi.connect(
                    host='my-trino-trino.ns-data-platform.svc.cluster.local',
                    port=8080,
                    user='airflow',
                    catalog=catalog
                )

                cursor = conn.cursor()

                # Try to show schemas
                cursor.execute("SHOW SCHEMAS")
                schemas = cursor.fetchall()

                print(f"✅ {catalog} catalog accessible")
                print(f"📁 Schemas: {[s[0] for s in schemas]}")

                results[catalog] = {
                    'accessible': True,
                    'schemas': [s[0] for s in schemas]
                }

                cursor.close()
                conn.close()

            except Exception as e:
                print(f"❌ {catalog} catalog failed: {str(e)}")
                results[catalog] = {
                    'accessible': False,
                    'error': str(e)
                }

    context['task_instance'].xcom_push(key='s3_test_results', value=results)
    return results

def test_direct_s3_access(**context):
    """Test direct S3 access patterns for MinIO"""
    print("🔍 DIAGNOSIS: Testing direct S3/MinIO access patterns...")

    # Common S3 access patterns to try
    test_patterns = [
        {
            'name': 'Direct S3A access',
            'sql': "SELECT * FROM s3a://bronze/ LIMIT 1",
            'description': 'Direct s3a:// protocol access'
        },
        {
            'name': 'Hive S3 access',
            'sql': "SELECT '$path' FROM \"s3a://bronze/\"",
            'description': 'Hive-style S3 access'
        },
        {
            'name': 'Information schema check',
            'sql': "SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE table_name LIKE '%s3%' OR table_name LIKE '%minio%'",
            'description': 'Check for existing S3 tables'
        }
    ]

    results = {}
    for test in test_patterns:
        print(f"\n🧪 Testing: {test['name']}")
        print(f"📝 {test['description']}")

        try:
            result = execute_trino_diagnostic(test['sql'], test['name'])
            results[test['name']] = {
                'success': True,
                'result': result
            }

        except Exception as e:
            results[test['name']] = {
                'success': False,
                'error': str(e)
            }

    context['task_instance'].xcom_push(key='s3_access_results', value=results)
    return results

def test_minio_bucket_access(**context):
    """Test specific MinIO bucket access"""
    print("🔍 DIAGNOSIS: Testing MinIO bronze bucket access...")

    # Test different ways to access bronze bucket
    access_patterns = [
        "SELECT * FROM s3.bronze.biological_results_0000",
        "SELECT * FROM minio.bronze.biological_results_0000",
        "SELECT * FROM hive.bronze.biological_results_0000",
        "SELECT * FROM \"s3a://bronze/biological_results_0000.csv\"",
        "SHOW TABLES FROM s3.bronze",
        "SHOW TABLES FROM minio.bronze",
        "SHOW TABLES FROM hive.bronze"
    ]

    results = {}
    for i, pattern in enumerate(access_patterns):
        print(f"\n🧪 Test {i+1}: {pattern}")

        try:
            result = execute_trino_diagnostic(pattern, f"Access pattern {i+1}")
            results[f"pattern_{i+1}"] = {
                'sql': pattern,
                'success': True,
                'result': result
            }

        except Exception as e:
            results[f"pattern_{i+1}"] = {
                'sql': pattern,
                'success': False,
                'error': str(e)
            }

    context['task_instance'].xcom_push(key='bucket_access_results', value=results)
    return results

def generate_diagnosis_report(**context):
    """Generate comprehensive diagnosis report and solution"""
    print("📋 DIAGNOSIS REPORT: Trino-MinIO Connection")
    print("="*60)

    # Get all test results
    catalogs = context['task_instance'].xcom_pull(task_ids='check_available_catalogs', key='catalogs')
    s3_test_results = context['task_instance'].xcom_pull(task_ids='test_s3_catalog_connection', key='s3_test_results')
    s3_access_results = context['task_instance'].xcom_pull(task_ids='test_direct_s3_access', key='s3_access_results')
    bucket_access_results = context['task_instance'].xcom_pull(task_ids='test_minio_bucket_access', key='bucket_access_results')

    print(f"\n🗂️  AVAILABLE CATALOGS:")
    for catalog in (catalogs or []):
        print(f"   - {catalog}")

    print(f"\n🔍 S3 CATALOG TEST RESULTS:")
    for catalog, result in (s3_test_results or {}).items():
        if result.get('accessible'):
            print(f"   ✅ {catalog}: {result.get('schemas', [])}")
        else:
            print(f"   ❌ {catalog}: {result.get('error', 'Failed')}")

    print(f"\n🧪 S3 ACCESS PATTERN RESULTS:")
    for pattern, result in (s3_access_results or {}).items():
        status = "✅" if result.get('success') else "❌"
        print(f"   {status} {pattern}")

    print(f"\n🪣 BUCKET ACCESS RESULTS:")
    for pattern, result in (bucket_access_results or {}).items():
        status = "✅" if result.get('success') else "❌"
        print(f"   {status} {result.get('sql', 'Unknown')}")

    # Generate solution recommendations
    print(f"\n🎯 SOLUTION RECOMMENDATIONS:")

    if s3_test_results:
        working_catalogs = [cat for cat, result in s3_test_results.items() if result.get('accessible')]
        if working_catalogs:
            print(f"✅ Use working catalog: {working_catalogs[0]}")
            print(f"📝 Recommended approach: Create external table with catalog {working_catalogs[0]}")
        else:
            print("❌ No S3 catalogs accessible")
            print("🔧 Need to configure Trino S3 connector")

    if bucket_access_results:
        working_patterns = [result for result in bucket_access_results.values() if result.get('success')]
        if working_patterns:
            print(f"✅ Working access pattern found")
            print(f"📝 Use: {working_patterns[0].get('sql')}")

    print("="*60)
    print("🔄 NEXT STEPS: Based on results above, configure Step 2 to use working approach")

    return "diagnosis_complete"

# Task definitions
check_catalogs = PythonOperator(
    task_id='check_available_catalogs',
    python_callable=check_available_catalogs,
    dag=dag,
)

test_s3_catalogs = PythonOperator(
    task_id='test_s3_catalog_connection',
    python_callable=test_s3_catalog_connection,
    dag=dag,
)

test_s3_access = PythonOperator(
    task_id='test_direct_s3_access',
    python_callable=test_direct_s3_access,
    dag=dag,
)

test_bucket_access = PythonOperator(
    task_id='test_minio_bucket_access',
    python_callable=test_minio_bucket_access,
    dag=dag,
)

generate_report = PythonOperator(
    task_id='generate_diagnosis_report',
    python_callable=generate_diagnosis_report,
    dag=dag,
)

# Dependencies
check_catalogs >> [test_s3_catalogs, test_s3_access, test_bucket_access] >> generate_report