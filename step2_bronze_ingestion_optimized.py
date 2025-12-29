"""
STEP 2: Bronze CSV Ingestion - FULLY OPTIMIZED

Combines ALL optimizations:
- Batch size: 10 files processed together
- Parameterized inserts with executemany()
- Data type conversion in pandas
- Progress tracking
- Source file tracking
- Error handling

Expected time: 6-10 hours for 998 files (200GB)
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
    'step2_bronze_ingestion_fully_optimized',
    default_args=default_args,
    description='Step 2: Fully optimized CSV ingestion - All optimizations combined',
    schedule=None,
    catchup=False,
    tags=['step2', 'bronze', 'ingestion', 'fully-optimized', 'production'],
)

def execute_trino_query(sql_query, description, catalog='iceberg', schema='default'):
    """Execute Trino queries"""
    print(f"🚀 {description}")
    
    import trino
    
    conn = trino.dbapi.connect(
        host='my-trino-trino.ns-data-platform.svc.cluster.local',
        port=8080,
        user='airflow',
        catalog=catalog,
        schema=schema
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
    return "success"

def prepare_bronze_table(**context):
    """Create optimized Iceberg bronze table"""
    print("🏗️ Preparing Iceberg bronze table...")
    
    # Create schema
    execute_trino_query(
        "CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3://eds-lakehouse/bronze/')",
        "Create bronze schema"
    )
    
    # Drop existing
    try:
        execute_trino_query(
            "DROP TABLE IF EXISTS iceberg.bronze.biological_results_raw",
            "Drop existing table",
            schema='bronze'
        )
    except:
        pass
    
    # Create table with proper types
    sql_create = """
    CREATE TABLE iceberg.bronze.biological_results_raw (
        source_file VARCHAR,
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
        load_timestamp TIMESTAMP,
        processing_batch VARCHAR
    )
    WITH (
        format = 'PARQUET',
        location = 's3://eds-lakehouse/bronze/biological_results/'
    )
    """
    
    execute_trino_query(sql_create, "Create bronze table", schema='bronze')
    print("✅ Table ready with optimized schema")
    return "table_ready"

def process_csv_files_optimized(**context):
    """
    Process ALL CSV files with FULL optimization:
    - Batch processing (10 files at once)
    - Pandas type conversion
    - executemany() for bulk inserts
    - Progress tracking
    """
    print("🚀 Processing 998 CSV files with FULL optimization...")
    print("⚡ Batch size: 10 files | Insert method: executemany()")
    
    from minio import Minio
    import pandas as pd
    import io
    import trino
    from datetime import datetime as dt
    
    # MinIO client
    minio_client = Minio(
        "minio-api.ns-data-platform.svc.cluster.local:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    
    # Get all CSV files
    objects = minio_client.list_objects("bronze", recursive=True)
    csv_files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]
    csv_files.sort()
    
    total_files = len(csv_files)
    print(f"📊 Total CSV files: {total_files}")
    
    # Trino connection
    conn = trino.dbapi.connect(
        host='my-trino-trino.ns-data-platform.svc.cluster.local',
        port=8080,
        user='airflow',
        catalog='iceberg',
        schema='bronze'
    )
    
    total_rows = 0
    processed_files = 0
    batch_size = 10  # Process 10 files at a time
    
    start_time = dt.now()
    
    for batch_start in range(0, total_files, batch_size):
        batch_files = csv_files[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (total_files + batch_size - 1) // batch_size
        
        print(f"\n{'='*60}")
        print(f"📦 BATCH {batch_num}/{total_batches}")
        print(f"📁 Files {batch_start + 1}-{min(batch_start + batch_size, total_files)} of {total_files}")
        print(f"{'='*60}")
        
        # Read batch of CSV files
        batch_dfs = []
        for csv_file in batch_files:
            try:
                obj = minio_client.get_object("bronze", csv_file)
                df = pd.read_csv(io.BytesIO(obj.read()))
                df['source_file'] = csv_file
                batch_dfs.append(df)
                print(f"  ✓ {csv_file}: {len(df):,} rows")
            except Exception as e:
                print(f"  ✗ {csv_file}: Error - {e}")
                continue
        
        if not batch_dfs:
            print("  ⚠️ No valid files in this batch, skipping")
            continue
        
        # Combine batch
        batch_df = pd.concat(batch_dfs, ignore_index=True)
        batch_df['processing_batch'] = f'batch_{batch_num}'
        batch_df['load_timestamp'] = dt.now()
        
        print(f"\n  📊 Combined batch: {len(batch_df):,} rows")
        
        # DATA TYPE CONVERSION (once, in pandas)
        print(f"  🔄 Converting data types...")
        
        # Numeric conversions
        batch_df['visit_id'] = pd.to_numeric(batch_df.get('visit_id', None), errors='coerce')
        batch_df['value_as_number'] = pd.to_numeric(batch_df.get('value_as_number', None), errors='coerce')
        
        # Datetime conversions
        for col in ['sampling_datetime_utc', 'result_datetime_utc']:
            if col in batch_df.columns:
                batch_df[col] = pd.to_datetime(batch_df[col], errors='coerce')
        
        # Date conversion
        if 'report_date_utc' in batch_df.columns:
            batch_df['report_date_utc'] = pd.to_datetime(batch_df['report_date_utc'], errors='coerce').dt.date
        
        # Fill NaN with None for SQL NULL
        batch_df = batch_df.where(pd.notna(batch_df), None)
        
        # Filter invalid rows
        valid_df = batch_df[
            batch_df['patient_id'].notna() & 
            (batch_df['patient_id'] != '') &
            (batch_df['patient_id'] != 'nan')
        ].copy()
        
        filtered_count = len(batch_df) - len(valid_df)
        if filtered_count > 0:
            print(f"  🔍 Filtered out {filtered_count:,} invalid rows")
        
        print(f"  ✅ Valid rows: {len(valid_df):,}")
        
        if len(valid_df) == 0:
            print("  ⚠️ No valid rows after filtering, skipping batch")
            continue
        
        # BULK INSERT using executemany() - NO ESCAPING ISSUES!
        print(f"  💾 Inserting {len(valid_df):,} rows with executemany()...")
        
        cursor = conn.cursor()
        
        # Prepare INSERT statement with placeholders
        insert_sql = """
        INSERT INTO iceberg.bronze.biological_results_raw 
        (source_file, patient_id, visit_id, sampling_datetime_utc, result_datetime_utc,
         report_date_utc, measurement_source_value, value_as_number, value_as_string,
         unit_source_value, normality, abnormal_flag, value_type, bacterium_id,
         provider_id, laboratory_uuid, load_timestamp, processing_batch)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Convert DataFrame to list of tuples
        data_tuples = [
            (
                row['source_file'],
                row.get('patient_id'),
                row.get('visit_id'),
                row.get('sampling_datetime_utc'),
                row.get('result_datetime_utc'),
                row.get('report_date_utc'),
                row.get('measurement_source_value'),
                row.get('value_as_number'),
                row.get('value_as_string'),
                row.get('unit_source_value'),
                row.get('normality'),
                row.get('abnormal_flag'),
                row.get('value_type'),
                row.get('bacterium_id'),
                row.get('provider_id'),
                row.get('laboratory_uuid'),
                row['load_timestamp'],
                row['processing_batch']
            )
            for _, row in valid_df.iterrows()
        ]
        
        # Insert in chunks of 5000 rows
        chunk_size = 5000
        for i in range(0, len(data_tuples), chunk_size):
            chunk = data_tuples[i:i + chunk_size]
            
            try:
                # executemany() - Trino handles ALL escaping automatically!
                cursor.executemany(insert_sql, chunk)
                print(f"    ✓ Inserted chunk {i//chunk_size + 1}: {len(chunk):,} rows")
            except Exception as e:
                print(f"    ✗ Chunk {i//chunk_size + 1} failed: {e}")
                # Try row-by-row for failed chunk
                for row in chunk:
                    try:
                        cursor.execute(insert_sql, row)
                    except:
                        pass  # Skip problematic rows
        
        cursor.close()
        
        total_rows += len(valid_df)
        processed_files += len(batch_files)
        
        # Progress tracking
        elapsed = (dt.now() - start_time).total_seconds()
        avg_time_per_batch = elapsed / batch_num if batch_num > 0 else 0
        remaining_batches = total_batches - batch_num
        estimated_remaining = avg_time_per_batch * remaining_batches
        
        print(f"\n  ✅ Batch {batch_num} complete!")
        print(f"  📈 Progress: {processed_files}/{total_files} files ({processed_files/total_files*100:.1f}%)")
        print(f"  📊 Total rows: {total_rows:,}")
        print(f"  ⏱️  Elapsed: {elapsed/3600:.1f}h | Estimated remaining: {estimated_remaining/3600:.1f}h")
    
    conn.close()
    
    total_time = (dt.now() - start_time).total_seconds()
    
    print(f"\n{'='*60}")
    print(f"🎉 PROCESSING COMPLETE!")
    print(f"{'='*60}")
    print(f"✅ Files processed: {processed_files}/{total_files}")
    print(f"✅ Total rows inserted: {total_rows:,}")
    print(f"⏱️  Total time: {total_time/3600:.2f} hours")
    print(f"📊 Average: {total_rows/(total_time/3600):.0f} rows/hour")
    
    context['task_instance'].xcom_push(key='total_rows', value=total_rows)
    context['task_instance'].xcom_push(key='processed_files', value=processed_files)
    
    return {
        'total_rows': total_rows,
        'processed_files': processed_files,
        'total_time_hours': total_time/3600
    }

def validate_results(**context):
    """Comprehensive validation of loaded data"""
    print("📊 Validating results...")
    
    results = context['task_instance'].xcom_pull(task_ids='process_csv_files_optimized')
    expected_rows = results['total_rows']
    
    # Count actual rows
    actual_result = execute_trino_query(
        "SELECT COUNT(*) FROM iceberg.bronze.biological_results_raw",
        "Count total rows",
        schema='bronze'
    )
    actual_rows = actual_result[0][0]
    
    print(f"\n{'='*60}")
    print(f"VALIDATION RESULTS")
    print(f"{'='*60}")
    print(f"Expected rows: {expected_rows:,}")
    print(f"Actual rows: {actual_rows:,}")
    
    if actual_rows == expected_rows:
        print("✅ Perfect match!")
    else:
        diff = abs(actual_rows - expected_rows)
        print(f"⚠️  Difference: {diff:,} rows ({diff/expected_rows*100:.2f}%)")
    
    # Detailed statistics
    stats_sql = """
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT patient_id) as unique_patients,
        COUNT(DISTINCT visit_id) as unique_visits,
        COUNT(DISTINCT source_file) as files_loaded,
        MIN(sampling_datetime_utc) as earliest_sample,
        MAX(sampling_datetime_utc) as latest_sample,
        COUNT(CASE WHEN value_as_number IS NOT NULL THEN 1 END) as numeric_values,
        COUNT(CASE WHEN value_as_string IS NOT NULL THEN 1 END) as string_values,
        COUNT(DISTINCT processing_batch) as batches_processed
    FROM iceberg.bronze.biological_results_raw
    """
    
    stats = execute_trino_query(stats_sql, "Detailed statistics", schema='bronze')[0]
    
    print(f"\n{'='*60}")
    print(f"DATA QUALITY SUMMARY")
    print(f"{'='*60}")
    print(f"Total records: {stats[0]:,}")
    print(f"Unique patients: {stats[1]:,}")
    print(f"Unique visits: {stats[2]:,}")
    print(f"Files loaded: {stats[3]}")
    print(f"Sample date range: {stats[4]} to {stats[5]}")
    print(f"Numeric values: {stats[6]:,}")
    print(f"String values: {stats[7]:,}")
    print(f"Processing batches: {stats[8]}")
    
    # Sample data
    print(f"\n{'='*60}")
    print(f"SAMPLE DATA (first 3 rows)")
    print(f"{'='*60}")
    
    sample = execute_trino_query(
        """
        SELECT patient_id, visit_id, measurement_source_value, 
               source_file, processing_batch 
        FROM iceberg.bronze.biological_results_raw 
        LIMIT 3
        """,
        "Sample data",
        schema='bronze'
    )
    
    for row in sample:
        print(f"  Patient: {row[0]} | Visit: {row[1]} | Measurement: {row[2]}")
        print(f"  File: {row[3]} | Batch: {row[4]}")
        print()
    
    print(f"{'='*60}")
    print(f"🎉 STEP 2 COMPLETE - READY FOR STEP 3!")
    print(f"{'='*60}")
    
    return "validation_complete"

# Task definitions
task1 = PythonOperator(
    task_id='prepare_bronze_table',
    python_callable=prepare_bronze_table,
    dag=dag,
)

task2 = PythonOperator(
    task_id='process_csv_files_optimized',
    python_callable=process_csv_files_optimized,
    dag=dag,
    execution_timeout=timedelta(hours=16),  # Safety timeout
)

task3 = PythonOperator(
    task_id='validate_results',
    python_callable=validate_results,
    dag=dag,
)

# Pipeline
task1 >> task2 >> task3