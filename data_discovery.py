"""
Data Discovery DAG - Find what data we actually have

This DAG will explore and report on all available data sources
to understand what we're working with for the OMOP transformation.
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
    'data_discovery',
    default_args=default_args,
    description='Discover available data sources and structure',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['discovery', 'exploration'],
)

def execute_trino_discovery(sql_query, description):
    """Execute Trino query safely with error handling"""
    print(f"🔍 {description}")

    try:
        import trino
        print("✅ Trino module loaded")
    except ImportError as e:
        print(f"❌ Trino not available: {e}")
        return None

    try:
        conn = trino.dbapi.connect(
            host='my-trino-trino.ns-data-platform.svc.cluster.local',
            port=8080,
            user='airflow',
            catalog='iceberg',
            schema='default'  # Start with default schema
        )

        cursor = conn.cursor()
        print(f"✅ Connected to Trino")

        cursor.execute(sql_query)
        results = cursor.fetchall()

        print(f"📊 Results for {description}:")
        for row in results:
            print(f"   {row}")

        cursor.close()
        conn.close()
        return results

    except Exception as e:
        print(f"⚠️  Query failed: {e}")
        return None

def discover_catalogs(**context):
    """Discover all available catalogs"""
    sql = "SHOW CATALOGS"
    return execute_trino_discovery(sql, "Available Catalogs")

def discover_schemas(**context):
    """Discover schemas in iceberg catalog"""
    sql = "SHOW SCHEMAS FROM iceberg"
    return execute_trino_discovery(sql, "Iceberg Schemas")

def discover_bronze_tables(**context):
    """Discover tables in bronze schema"""
    sql = "SHOW TABLES FROM iceberg.bronze"
    results = execute_trino_discovery(sql, "Bronze Tables")

    # Store results for next tasks
    if results:
        context['task_instance'].xcom_push(key='bronze_tables', value=[row[0] for row in results])
    return results

def discover_table_structures(**context):
    """Discover structure of bronze tables"""
    bronze_tables = context['task_instance'].xcom_pull(task_ids='discover_bronze_tables', key='bronze_tables')

    if not bronze_tables:
        print("❌ No bronze tables found")
        return

    for table_name in bronze_tables:
        print(f"\n📋 Analyzing table: {table_name}")

        # Get table structure
        sql_describe = f"DESCRIBE iceberg.bronze.{table_name}"
        execute_trino_discovery(sql_describe, f"Structure of {table_name}")

        # Get row count
        sql_count = f"SELECT COUNT(*) as row_count FROM iceberg.bronze.{table_name}"
        execute_trino_discovery(sql_count, f"Row count for {table_name}")

        # Get sample data
        sql_sample = f"SELECT * FROM iceberg.bronze.{table_name} LIMIT 5"
        execute_trino_discovery(sql_sample, f"Sample data from {table_name}")

def check_minio_data(**context):
    """Check what data is available in MinIO"""
    print("🗄️  Checking MinIO data availability...")

    try:
        import boto3
        print("✅ boto3 available")

        # Try to connect to MinIO (S3-compatible)
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio-api.ns-data-platform.svc.cluster.local:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
            region_name='us-east-1'
        )

        # List buckets
        buckets = s3_client.list_buckets()
        print("📁 Available buckets:")
        for bucket in buckets['Buckets']:
            print(f"   - {bucket['Name']}")

            # List objects in bucket
            try:
                objects = s3_client.list_objects_v2(Bucket=bucket['Name'], MaxKeys=10)
                if 'Contents' in objects:
                    print(f"   📄 Files in {bucket['Name']}:")
                    for obj in objects['Contents']:
                        print(f"      - {obj['Key']} ({obj['Size']} bytes)")
            except Exception as e:
                print(f"   ⚠️  Cannot list objects in {bucket['Name']}: {e}")

    except ImportError:
        print("❌ boto3 not available - cannot check MinIO")
    except Exception as e:
        print(f"⚠️  MinIO connection failed: {e}")

def generate_recommendations(**context):
    """Generate recommendations based on discovery"""
    print("\n🎯 DISCOVERY SUMMARY AND RECOMMENDATIONS")
    print("="*50)

    # Get bronze tables info
    bronze_tables = context['task_instance'].xcom_pull(task_ids='discover_bronze_tables', key='bronze_tables')

    if bronze_tables:
        print(f"✅ Found {len(bronze_tables)} bronze table(s):")
        for table in bronze_tables:
            print(f"   - iceberg.bronze.{table}")
        print("\n💡 RECOMMENDATION: Use existing bronze tables")

    else:
        print("❌ No bronze tables found")
        print("\n💡 RECOMMENDATION: Create bronze tables from raw data")
        print("   Next step: Check MinIO for CSV files")

    print("\n🔄 NEXT ACTIONS:")
    print("1. Review the discovery results above")
    print("2. Update bronze_to_silver_omop DAG with actual table names")
    print("3. Create missing bronze tables if needed")
    print("4. Run complete OMOP transformation")

# Task definitions
discover_catalogs_task = PythonOperator(
    task_id='discover_catalogs',
    python_callable=discover_catalogs,
    dag=dag,
)

discover_schemas_task = PythonOperator(
    task_id='discover_schemas',
    python_callable=discover_schemas,
    dag=dag,
)

discover_bronze_task = PythonOperator(
    task_id='discover_bronze_tables',
    python_callable=discover_bronze_tables,
    dag=dag,
)

discover_structures_task = PythonOperator(
    task_id='discover_table_structures',
    python_callable=discover_table_structures,
    dag=dag,
)

check_minio_task = PythonOperator(
    task_id='check_minio_data',
    python_callable=check_minio_data,
    dag=dag,
)

generate_recommendations_task = PythonOperator(
    task_id='generate_recommendations',
    python_callable=generate_recommendations,
    dag=dag,
)

# Task dependencies
discover_catalogs_task >> discover_schemas_task >> discover_bronze_task >> [discover_structures_task, check_minio_task] >> generate_recommendations_task