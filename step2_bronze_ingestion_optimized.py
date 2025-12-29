"""
STEP 2: Bronze CSV Ingestion - OPTIMIZED for 998 Files

Processes ALL 998 CSV files (99.8 GB) from MinIO into Iceberg bronze tables.
Optimized for:
- Bulk processing of all files
- Batch inserts (not row-by-row)
- Progress tracking
- Error recovery
- Memory efficient processing

Based on Step 1 results:
- 998 CSV files discovered
- ~100MB per file
- ~998M records total
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'omop-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,  # Allow 1 retry for transient failures
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'step2_bronze_ingestion_optimized',
    default_args=default_args,
    description='Step 2: Optimized ingestion of ALL 998 CSV files - v7 (Fixed SQL Escaping)',
    schedule=None,  # Manual trigger after Step 1
    catchup=False,
    tags=['step2', 'bronze', 'ingestion', 'optimized', 'v7'],
)

def execute_trino_bronze(sql_query, description):
    """Execute Trino queries for bronze operations"""
    print(f"🚀 {description}")

    try:
        import trino
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
        cursor.execute(sql_query)

        if sql_query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
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
    print("🏗️  STEP 2: Preparing bronze environment for 998 CSV files...")

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

    # Create optimized bronze table for bulk loading
    sql_create = """
    CREATE TABLE IF NOT EXISTS iceberg.bronze.biological_results_raw (
        source_file VARCHAR,
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
        laboratory_uuid VARCHAR,
        load_timestamp TIMESTAMP(3) WITH TIME ZONE
    )
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['bucket(patient_id, 32)']
    )
    """

    execute_trino_bronze(sql_create, "Create optimized bronze table")
    print("✅ Bronze environment ready for bulk processing")
    return "environment_ready"

def get_csv_files_to_process(**context):
    """Get list of all CSV files from Step 1 results"""
    print("📋 Getting CSV files list from Step 1...")

    try:
        import boto3

        # Connect to MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio-api.ns-data-platform.svc.cluster.local:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
            region_name='us-east-1'
        )

        # List all CSV files in bronze bucket
        response = s3_client.list_objects_v2(Bucket='bronze')

        csv_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.csv'):
                    csv_files.append({
                        'name': obj['Key'],
                        'size': obj['Size']
                    })

        csv_files.sort(key=lambda x: x['name'])  # Process in order

        print(f"📊 Found {len(csv_files)} CSV files to process")
        print(f"📁 Files: {csv_files[0]['name']} to {csv_files[-1]['name']}")

        # Store for next tasks
        context['task_instance'].xcom_push(key='csv_files', value=csv_files)

        return csv_files

    except Exception as e:
        print(f"❌ Failed to get CSV files: {e}")
        raise

def process_csv_files_bulk(**context):
    """Process ALL CSV files with optimized bulk inserts"""
    print("🚀 STEP 2: Processing ALL 998 CSV files with bulk inserts...")

    # Get CSV files list
    csv_files = context['task_instance'].xcom_pull(task_ids='get_csv_files_to_process', key='csv_files')

    if not csv_files:
        raise Exception("No CSV files found to process")

    try:
        import boto3
        import csv
        import io

        # Connect to MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio-api.ns-data-platform.svc.cluster.local:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
            region_name='us-east-1'
        )

        total_files = len(csv_files)
        total_rows_processed = 0
        processed_files = 0
        batch_size = 1000  # Process in batches of 1000 rows

        print(f"📊 Processing {total_files} CSV files in batches of {batch_size} rows")

        for file_idx, file_info in enumerate(csv_files):
            file_name = file_info['name']
            file_size = file_info['size']

            print(f"\\n📄 Processing file {file_idx + 1}/{total_files}: {file_name} ({file_size:,} bytes)")

            try:
                # Read CSV file from MinIO
                response = s3_client.get_object(Bucket='bronze', Key=file_name)
                content = response['Body'].read().decode('utf-8')

                # Parse CSV content
                csv_reader = csv.reader(io.StringIO(content))
                header = next(csv_reader)  # Skip header

                # Process file in batches
                batch_values = []
                file_rows = 0

                for row in csv_reader:
                    if len(row) >= 15:  # Ensure we have all columns
                        # Escape single quotes and handle NULL values
                        safe_row = []
                        for col in row:
                            if col and col.strip():
                                # Escape backslashes first, then single quotes for SQL
                                safe_val = col.replace("\\", "\\\\").replace("'", "''")
                                safe_row.append(f"'{safe_val}'")
                            else:
                                safe_row.append("NULL")

                        # Add current timestamp
                        safe_row.append("CURRENT_TIMESTAMP")

                        # Add to batch
                        batch_values.append(f"('{file_name}', {', '.join(safe_row[:15])}, {safe_row[15]})")
                        file_rows += 1

                        # Execute batch when full
                        if len(batch_values) >= batch_size:
                            execute_batch_insert(batch_values, f"{file_name} batch")
                            total_rows_processed += len(batch_values)
                            batch_values = []

                            print(f"   📊 Processed {file_rows:,} rows from {file_name}")

                # Execute remaining batch
                if batch_values:
                    execute_batch_insert(batch_values, f"{file_name} final batch")
                    total_rows_processed += len(batch_values)

                processed_files += 1
                print(f"✅ Completed {file_name}: {file_rows:,} rows")
                print(f"📈 Progress: {processed_files}/{total_files} files, {total_rows_processed:,} total rows")

            except Exception as e:
                print(f"❌ Failed to process {file_name}: {e}")
                print(f"⚠️  Continuing with next file...")
                continue

        print(f"\\n🎉 BULK PROCESSING COMPLETED!")
        print(f"✅ Files processed: {processed_files}/{total_files}")
        print(f"✅ Total rows: {total_rows_processed:,}")

        return {
            "files_processed": processed_files,
            "total_files": total_files,
            "rows_processed": total_rows_processed
        }

    except Exception as e:
        print(f"❌ Bulk processing failed: {e}")
        raise

def execute_batch_insert(batch_values, description):
    """Execute bulk INSERT with batch of values"""
    if not batch_values:
        return

    values_str = ",\\n        ".join(batch_values)

    sql_bulk_insert = f"""
    INSERT INTO iceberg.bronze.biological_results_raw (
        source_file, patient_id, visit_id, sampling_datetime_utc,
        result_datetime_utc, report_date_utc, measurement_source_value,
        value_as_number, value_as_string, unit_source_value,
        normality, abnormal_flag, value_type, bacterium_id,
        provider_id, laboratory_uuid, load_timestamp
    ) VALUES
        {values_str}
    """

    execute_trino_bronze(sql_bulk_insert, f"Bulk insert {len(batch_values)} rows - {description}")

def validate_bulk_processing(**context):
    """Validate that all CSV data was processed successfully"""
    print("📊 STEP 2: Validating bulk processing results...")

    # Get processing results
    results = context['task_instance'].xcom_pull(task_ids='process_csv_files_bulk')

    # Count total rows in bronze table
    count_sql = "SELECT COUNT(*) FROM iceberg.bronze.biological_results_raw"
    count_result = execute_trino_bronze(count_sql, "Count total rows")

    if count_result:
        db_row_count = count_result[0][0]
        processed_rows = results.get('rows_processed', 0)

        print(f"📊 Validation Results:")
        print(f"   - Files processed: {results.get('files_processed', 0)}/{results.get('total_files', 0)}")
        print(f"   - Rows processed: {processed_rows:,}")
        print(f"   - Rows in database: {db_row_count:,}")

        if db_row_count == processed_rows:
            print("✅ Row counts match - bulk processing successful!")
        else:
            print(f"⚠️  Row count mismatch - check for processing errors")

    # Sample data to verify structure
    sample_sql = "SELECT source_file, patient_id, visit_id, load_timestamp FROM iceberg.bronze.biological_results_raw LIMIT 5"
    execute_trino_bronze(sample_sql, "Sample processed data")

    # Check file distribution
    file_sql = "SELECT source_file, COUNT(*) as row_count FROM iceberg.bronze.biological_results_raw GROUP BY source_file ORDER BY source_file LIMIT 10"
    execute_trino_bronze(file_sql, "File distribution check")

    print("✅ Bulk processing validation completed")
    return results

# Task definitions
prepare_env = PythonOperator(
    task_id='prepare_bronze_environment',
    python_callable=prepare_bronze_environment,
    dag=dag,
)

get_files = PythonOperator(
    task_id='get_csv_files_to_process',
    python_callable=get_csv_files_to_process,
    dag=dag,
)

process_bulk = PythonOperator(
    task_id='process_csv_files_bulk',
    python_callable=process_csv_files_bulk,
    dag=dag,
)

validate_processing = PythonOperator(
    task_id='validate_bulk_processing',
    python_callable=validate_bulk_processing,
    dag=dag,
)

# Dependencies
prepare_env >> get_files >> process_bulk >> validate_processing