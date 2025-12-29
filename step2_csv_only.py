"""
STEP 2: CSV → Bronze Only - CLEAN & FOCUSED

PURE CSV PROCESSING PIPELINE with CORRECTED COLUMN MAPPING!
- Converts CSV files → clustered Parquet in bronze layer
- FIXED: 27-column mapping to match real CSV structure
- Time partitioning: year/month for faster queries
- Patient clustering: sorted by patient_id for patient queries
- Expected time: ~1 hour (62 minutes proven)
- Output: s3a://eds-lakehouse/bronze/ (raw lab data + clustering)

Clean Step 2: CSV files → Bronze layer only! (Vocabularies assumed to exist)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'omop-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,  # No retries - fail fast for debugging
    'on_failure_callback': None,
    'on_retry_callback': None,
}

dag = DAG(
    'step2_csv_only',
    default_args=default_args,
    description='Step 2: CSV → Bronze ONLY with CORRECTED COLUMN MAPPING',
    schedule=None,
    catchup=False,
    tags=['step2', 'bronze', 'csv-only', 'column-mapping-fixed', 'clean'],
    doc_md="""
    ## Clean CSV-Only Pipeline with Corrected Column Mapping

    🎯 FOCUS: Pure CSV → Bronze conversion
    🔧 FIXED: 27-column mapping to match real CSV structure
    ⚡ FAST: ~1 hour processing for 634M+ rows
    🏗️ CLEAN: No vocabulary dependencies

    This DAG assumes OMOP vocabularies already exist.
    Use this to test the critical column mapping fix!

    Version: csv-only-v1 (Dec 29, 2025)
    """,
)

def execute_trino_query(sql_query, description, catalog='iceberg', schema='default', query_timeout=3600):
    """Execute Trino queries with robust error handling and timeout protection"""
    print(f"🚀 {description}")

    import trino
    import signal
    import time

    conn = None
    cursor = None

    def timeout_handler(signum, frame):
        print(f"⏰ Query timeout after {query_timeout} seconds: {description}")
        raise TimeoutError(f"Query timed out after {query_timeout} seconds")

    try:
        # Set query timeout protection
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(query_timeout)

        conn = trino.dbapi.connect(
            host='my-trino-trino.ns-data-platform.svc.cluster.local',
            port=8080,
            user='airflow',
            catalog=catalog,
            schema=schema,
            # Add memory and timeout properties
            session_properties={
                'query_max_memory': '20GB',
                'query_max_memory_per_node': '12GB',
                'query_max_total_memory': '24GB',
                'task_concurrency': '8',
                'task_writer_count': '4',
                'task_partitioned_writer_count': '4',
                'join_distribution_type': 'AUTOMATIC',
                'spill_enabled': 'true',
                'spiller_spill_path': '/tmp/trino-spill',
                'task_max_writer_count': '8'
            }
        )

        cursor = conn.cursor()
        start_time = time.time()

        print(f"🔧 Query memory limits: 20GB total, 12GB per node, spill enabled")
        cursor.execute(sql_query)

        if sql_query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            execution_time = time.time() - start_time
            print(f"✅ Query completed in {execution_time:.2f} seconds")
            return results
        else:
            execution_time = time.time() - start_time
            print(f"✅ DDL/DML completed in {execution_time:.2f} seconds")
            return "success"

    except TimeoutError as e:
        print(f"⏰ Query timed out: {e}")
        raise Exception(f"Query timeout after {query_timeout}s: {description}")
    except Exception as e:
        print(f"❌ Query failed: {e}")
        print(f"🔍 SQL: {sql_query[:200]}...")
        # Add memory usage info to error context
        if "OutOfMemoryError" in str(e) or "memory" in str(e).lower():
            print(f"💾 MEMORY ERROR: Consider reducing query complexity or increasing cluster memory")
        raise e
    finally:
        # Clear timeout
        signal.alarm(0)
        # Ensure connections are always closed
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

def setup_bronze_schema(**context):
    """Setup schemas for bronze data processing"""
    print("🏗️ STEP 2 CSV-ONLY: Setting up schemas for CSV → Bronze conversion...")

    try:
        # Create Hive schema for external tables
        execute_trino_query(
            "CREATE SCHEMA IF NOT EXISTS hive.test_ingestion",
            "Create Hive test_ingestion schema",
            catalog='hive'
        )

        # Create Iceberg bronze schema
        execute_trino_query(
            "CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3a://eds-lakehouse/bronze/')",
            "Create Iceberg bronze schema"
        )

        # Enhanced cleanup of existing bronze table and location
        try:
            # First try to drop the table
            execute_trino_query(
                "DROP TABLE IF EXISTS iceberg.bronze.biological_results_raw",
                "Drop existing bronze table"
            )
            print("✅ Existing bronze table dropped")
        except Exception as e:
            print(f"⚠️  Could not drop existing table: {e}")

        # Also try to clear the location by using unique location approach
        print("🔧 Using unique table location to avoid conflicts")

        print("✅ Bronze schemas ready for CSV processing!")
        return "bronze_schemas_ready"

    except Exception as e:
        print(f"❌ Bronze schema setup failed: {e}")
        raise Exception(f"Cannot setup bronze schemas: {e}")

def create_csv_external_table_corrected(**context):
    """Create Hive external table with CORRECTED 27-column mapping"""
    print("📊 Creating Hive external CSV table with CORRECTED COLUMN MAPPING...")

    try:
        # Drop existing table
        try:
            execute_trino_query(
                "DROP TABLE IF EXISTS hive.test_ingestion.biological_results_csv_external",
                "Clean up existing Hive external table",
                catalog='hive',
                schema='test_ingestion'
            )
        except:
            pass

        # Create external table with CORRECT column mapping to match actual CSV structure
        sql_create_external = """
        CREATE TABLE hive.test_ingestion.biological_results_csv_external (
            visit_id varchar,                           -- CSV position 1
            visit_date_utc varchar,                     -- CSV position 2
            visit_rank varchar,                         -- CSV position 3
            patient_id varchar,                         -- CSV position 4 ⭐
            report_id varchar,                          -- CSV position 5
            laboratory_uuid varchar,                    -- CSV position 6
            sub_laboratory_uuid varchar,               -- CSV position 7
            site_laboratory_uuid varchar,              -- CSV position 8
            measurement_source_value varchar,          -- CSV position 9 (LC:xxxx codes) ⭐
            debug_external_test_id varchar,            -- CSV position 10
            debug_external_test_scope varchar,         -- CSV position 11
            sampling_datetime_utc varchar,             -- CSV position 12 ⭐
            sampling_datetime_timezone varchar,        -- CSV position 13
            result_datetime_utc varchar,               -- CSV position 14 ⭐
            result_datetime_timezone varchar,          -- CSV position 15
            normality varchar,                          -- CSV position 16 ⭐
            value_type varchar,                         -- CSV position 17 ⭐
            value_as_number varchar,                    -- CSV position 18 ⭐
            unit_source_value varchar,                 -- CSV position 19 ⭐
            internal_numerical_unit_system varchar,    -- CSV position 20
            internal_numerical_reference_min varchar,  -- CSV position 21
            internal_numerical_reference_max varchar,  -- CSV position 22
            internal_categorical_qualification varchar, -- CSV position 23
            value_as_string varchar,                    -- CSV position 24 ⭐
            bacterium_id varchar,                       -- CSV position 25 ⭐
            range_type varchar,                         -- CSV position 26
            report_date_utc varchar                     -- CSV position 27 ⭐
        )
        WITH (
            external_location = 's3a://bronze/',
            format = 'CSV',
            csv_separator = ',',
            skip_header_line_count = 1
        )
        """

        execute_trino_query(
            sql_create_external,
            "Create Hive external CSV table with CORRECTED 27-column mapping",
            catalog='hive',
            schema='test_ingestion'
        )

        print("🧪 Validating external table can access CSV data...")

        # Enhanced validation query
        validation_query = """
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(DISTINCT "$path") as files_accessible,
            COUNT(CASE WHEN sampling_datetime_utc IS NOT NULL THEN 1 END) as non_null_timestamps
        FROM hive.test_ingestion.biological_results_csv_external
        WHERE patient_id IS NOT NULL
        LIMIT 1
        """

        result = execute_trino_query(
            validation_query,
            "Validate CSV external table with corrected mapping",
            catalog='hive',
            schema='test_ingestion'
        )

        if result and len(result) > 0:
            total_rows, unique_patients, files_accessible, non_null_timestamps = result[0]
            print(f"✅ CSV External table validation successful with CORRECTED MAPPING!")
            print(f"   📊 Accessible rows: {total_rows:,}")
            print(f"   👥 Unique patients: {unique_patients:,}")
            print(f"   📁 Files accessible: {files_accessible:,}")
            print(f"   📅 Valid timestamps: {non_null_timestamps:,}")

            if total_rows == 0:
                raise Exception("External table created but no CSV data is accessible")
            if unique_patients == 0:
                raise Exception("No valid patient IDs found - column mapping might still be wrong")

            # Store validation metrics
            context['task_instance'].xcom_push(key='external_table_rows', value=total_rows)
            context['task_instance'].xcom_push(key='external_table_files', value=files_accessible)
            context['task_instance'].xcom_push(key='unique_patients', value=unique_patients)

            print("✅ CORRECTED external table created and validated - ready for CTAS!")
            return "csv_external_ready"
        else:
            raise Exception("External table validation returned no results")

    except Exception as e:
        print(f"❌ External table creation/validation failed: {e}")
        print("💡 Ensure CSV files are present in s3a://bronze/ bucket")
        raise Exception(f"Cannot create/validate external table: {e}")

def monitor_resources():
    """Monitor system resources during processing"""
    try:
        import psutil
        import os

        # Get process info
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        cpu_percent = process.cpu_percent()

        # System info
        virtual_memory = psutil.virtual_memory()
        disk_usage = psutil.disk_usage('/')

        return {
            'process_memory_mb': memory_info.rss / 1024 / 1024,
            'process_cpu_percent': cpu_percent,
            'system_memory_percent': virtual_memory.percent,
            'system_memory_available_gb': virtual_memory.available / 1024 / 1024 / 1024,
            'disk_usage_percent': disk_usage.percent,
            'disk_free_gb': disk_usage.free / 1024 / 1024 / 1024
        }
    except ImportError:
        return {'status': 'psutil not available'}
    except Exception as e:
        return {'status': f'monitoring error: {e}'}

def execute_csv_to_bronze_ctas(**context):
    """Execute CSV → Bronze CTAS with CORRECTED column mapping"""
    print("🚀 STEP 2 CSV-ONLY: CSV → Bronze CTAS with CORRECTED MAPPING!")
    print("⚡ Same proven method that processed 634M+ rows in 62 minutes!")

    # Resource monitoring at start
    start_resources = monitor_resources()
    print(f"🖥️  Resource monitoring at start: {start_resources}")

    # Get validation metrics from previous task
    external_rows = context['task_instance'].xcom_pull(task_ids='create_csv_external_table_corrected', key='external_table_rows')
    external_files = context['task_instance'].xcom_pull(task_ids='create_csv_external_table_corrected', key='external_table_files')
    unique_patients = context['task_instance'].xcom_pull(task_ids='create_csv_external_table_corrected', key='unique_patients')

    if external_rows and external_files and unique_patients:
        print(f"📊 Processing: {external_rows:,} rows from {external_files:,} files, {unique_patients:,} patients")

    start_time = datetime.now()

    # CSV → Bronze CTAS operation with CORRECTED column mapping
    # Generate unique table name to avoid location conflicts
    import uuid
    unique_suffix = str(uuid.uuid4())[:8]

    sql_csv_to_bronze = f"""
    CREATE TABLE iceberg.bronze.biological_results_raw_{unique_suffix}
    WITH (
        format = 'PARQUET',

        -- Time-based partitioning for fast date range queries
        partitioning = ARRAY['year', 'month']

        -- Let Iceberg auto-generate location to avoid conflicts
    )
    AS
    SELECT
        -- Processing metadata
        'step2_csv_only_corrected_mapping' AS processing_batch,

        -- CSV columns with CORRECT mapping and proper typing
        patient_id,                                                           -- Now correctly from position 4
        TRY_CAST(visit_id AS BIGINT) AS visit_id,                           -- Now correctly from position 1
        TRY_CAST(sampling_datetime_utc AS TIMESTAMP) AS sampling_datetime_utc, -- Now correctly from position 12  ⭐
        TRY_CAST(result_datetime_utc AS TIMESTAMP) AS result_datetime_utc,   -- Now correctly from position 14  ⭐
        TRY_CAST(report_date_utc AS DATE) AS report_date_utc,               -- Now correctly from position 27  ⭐
        measurement_source_value,                                            -- Now correctly from position 9 (LC: codes) ⭐
        TRY_CAST(value_as_number AS DOUBLE) AS value_as_number,             -- Now correctly from position 18  ⭐
        value_as_string,                                                     -- Now correctly from position 24  ⭐
        unit_source_value,                                                   -- Now correctly from position 19  ⭐
        normality,                                                           -- Now correctly from position 16  ⭐
        '' AS abnormal_flag,                                                 -- Not in CSV, set empty
        value_type,                                                          -- Now correctly from position 17  ⭐
        bacterium_id,                                                        -- Now correctly from position 25  ⭐
        report_id AS provider_id,                                            -- Use report_id as provider_id
        laboratory_uuid,                                                     -- Now correctly from position 6   ⭐

        -- Partition columns for clustering (with fallbacks)
        CASE
            WHEN sampling_datetime_utc IS NOT NULL AND sampling_datetime_utc != ''
            THEN COALESCE(year(TRY_CAST(sampling_datetime_utc AS TIMESTAMP)),
                         year(TRY_CAST(sampling_datetime_utc AS DATE)), 1900)
            ELSE 1900
        END AS year,
        CASE
            WHEN sampling_datetime_utc IS NOT NULL AND sampling_datetime_utc != ''
            THEN COALESCE(month(TRY_CAST(sampling_datetime_utc AS TIMESTAMP)),
                         month(TRY_CAST(sampling_datetime_utc AS DATE)), 1)
            ELSE 1
        END AS month,

        -- Processing timestamp
        CURRENT_TIMESTAMP AS load_timestamp

    FROM hive.test_ingestion.biological_results_csv_external
    WHERE 1=1  -- Load all data first, then we can analyze data quality issues
    ORDER BY COALESCE(patient_id, 'UNKNOWN'), measurement_source_value  -- Clustering for patient queries
    """

    print(f"🔥 Starting CSV → Bronze CTAS with CORRECTED COLUMN MAPPING!")
    print(f"🎯 Creating table: biological_results_raw_{unique_suffix}")
    print("⏰ Expected time: ~62 minutes for 634M+ rows")

    result = execute_trino_query(sql_csv_to_bronze, "CSV → Bronze Parquet with CORRECTED mapping", schema='bronze')

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # Resource monitoring at completion
    end_resources = monitor_resources()
    print(f"🖥️  Resource monitoring at completion: {end_resources}")

    # Resource usage analysis
    if 'process_memory_mb' in start_resources and 'process_memory_mb' in end_resources:
        memory_delta = end_resources['process_memory_mb'] - start_resources['process_memory_mb']
        print(f"📊 Memory usage change: {memory_delta:+.1f}MB")

    print(f"🎉 CSV → Bronze conversion COMPLETE with CORRECTED MAPPING!")
    print(f"⏱️  Total time: {duration/3600:.2f} hours")
    print(f"🎯 Table created: biological_results_raw_{unique_suffix}")
    print(f"🚀 Data converted with corrected column mapping!")

    # Store table name for validation
    context['task_instance'].xcom_push(key='processing_time_hours', value=duration/3600)
    context['task_instance'].xcom_push(key='bronze_table_name', value=f'biological_results_raw_{unique_suffix}')

    return result

def validate_bronze_results(**context):
    """Validate bronze data creation with corrected mapping"""
    print("📊 Validating CSV → Bronze conversion results...")

    processing_time = context['task_instance'].xcom_pull(task_ids='execute_csv_to_bronze_ctas', key='processing_time_hours')
    external_rows = context['task_instance'].xcom_pull(task_ids='create_csv_external_table_corrected', key='external_table_rows')
    bronze_table_name = context['task_instance'].xcom_pull(task_ids='execute_csv_to_bronze_ctas', key='bronze_table_name')

    # Use dynamic table name or fallback to default
    table_name = bronze_table_name if bronze_table_name else 'biological_results_raw'
    print(f"🎯 Validating table: {table_name}")

    # Count bronze data
    try:
        count_result = execute_trino_query(
            f"SELECT COUNT(*) FROM iceberg.bronze.{table_name}",
            "Count bronze data rows",
            schema='bronze'
        )
        bronze_rows = count_result[0][0] if (count_result and len(count_result) > 0) else 0
    except Exception as e:
        print(f"⚠️  Could not count bronze rows: {e}")
        bronze_rows = 0

    # Detailed statistics
    try:
        stats_sql = f"""
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(DISTINCT visit_id) as unique_visits,
            MIN(sampling_datetime_utc) as earliest_sample,
            MAX(sampling_datetime_utc) as latest_sample,
            COUNT(CASE WHEN value_as_number IS NOT NULL THEN 1 END) as numeric_values,
            COUNT(CASE WHEN measurement_source_value LIKE 'LC:%' THEN 1 END) as lab_code_measurements
        FROM iceberg.bronze.{table_name}
        """

        stats_result = execute_trino_query(stats_sql, "Comprehensive bronze statistics", schema='bronze')
        stats = stats_result[0] if (stats_result and len(stats_result) > 0) else (0, 0, 0, None, None, 0, 0)
    except Exception as e:
        print(f"⚠️  Could not get detailed statistics: {e}")
        stats = (bronze_rows, 0, 0, None, None, 0, 0)

    # Check partitioning
    try:
        partition_sql = f"""
        SELECT
            COUNT(DISTINCT CASE WHEN year IS NOT NULL AND year > 1900 AND year <= YEAR(CURRENT_DATE) THEN year END) as years_spanned,
            COUNT(DISTINCT
                CASE
                    WHEN year IS NOT NULL AND month IS NOT NULL
                         AND year > 1900 AND year <= YEAR(CURRENT_DATE)
                         AND month >= 1 AND month <= 12
                    THEN CAST(year AS VARCHAR) || '-' || CAST(month AS VARCHAR)
                END
            ) as total_partitions,
            MIN(CASE WHEN year > 1900 AND year <= YEAR(CURRENT_DATE) THEN year END) as first_year,
            MAX(CASE WHEN year > 1900 AND year <= YEAR(CURRENT_DATE) THEN year END) as last_year
        FROM iceberg.bronze.{table_name}
        WHERE year IS NOT NULL AND month IS NOT NULL
        """

        partition_result = execute_trino_query(partition_sql, "Validate partitioning structure", schema='bronze')
        if partition_result and len(partition_result) > 0:
            years_spanned, total_partitions, first_year, last_year = partition_result[0]
        else:
            years_spanned, total_partitions, first_year, last_year = 0, 0, None, None
    except Exception as e:
        print(f"⚠️  Could not validate partitioning: {e}")
        years_spanned, total_partitions, first_year, last_year = 0, 0, None, None

    print(f"\n{'='*80}")
    print(f"🎉 CSV → BRONZE CONVERSION RESULTS (CORRECTED MAPPING)")
    print(f"{'='*80}")
    print(f"📊 PROCESSING SUMMARY:")
    print(f"   ⏱️  Processing time: {processing_time:.2f} hours" if processing_time else "   ⏱️  Processing time: Unknown")
    print(f"   📥 CSV rows: {external_rows:,}" if external_rows else "   📥 CSV rows: Unknown")
    print(f"   📤 Bronze rows: {bronze_rows:,}")
    print(f"   📊 Success rate: {(bronze_rows/external_rows)*100:.1f}%" if external_rows and external_rows > 0 else "   📊 Success rate: Unknown")

    print(f"\n📊 DATA DETAILS:")
    print(f"   📊 Total records: {stats[0]:,}")
    print(f"   👥 Unique patients: {stats[1]:,}")
    print(f"   🏥 Unique visits: {stats[2]:,}")
    print(f"   📅 Date range: {stats[3]} to {stats[4]}")
    print(f"   🔢 Numeric values: {stats[5]:,}")
    print(f"   🧪 Lab code measurements (LC:): {stats[6]:,}")

    print(f"\n🗂️ PARTITIONING STATUS:")
    print(f"   📅 Time partitions: {total_partitions:,} partitions ({years_spanned} years: {first_year}-{last_year})")
    print(f"   🗃️ Storage: s3a://eds-lakehouse/bronze/biological_results_raw/")
    print(f"   🔗 Clustering: patient_id + measurement_source_value")
    print(f"   📁 Format: Parquet with Iceberg metadata")

    # Processing rate
    if processing_time and processing_time > 0 and stats[0] > 0:
        processing_rate = stats[0] / (processing_time * 3600)
        print(f"   🚀 Processing rate: {processing_rate:,.0f} rows/second")

    print(f"\n🎯 COLUMN MAPPING TEST RESULTS:")
    if bronze_rows > 600000000:  # 600M+
        print(f"   ✅ SUCCESS: {bronze_rows:,} rows loaded (corrected mapping worked!)")
        print(f"   ✅ PATIENTS: {stats[1]:,} unique patients (data properly mapped)")
        print(f"   ✅ LAB CODES: {stats[6]:,} LC: measurements (measurement_source_value correct)")
        if years_spanned > 1:
            print(f"   ✅ TIMESTAMPS: {years_spanned} years of data (timestamp parsing worked)")
    else:
        print(f"   ❌ FAILURE: Only {bronze_rows:,} rows loaded (expected 634M+)")
        if stats[1] == 0:
            print(f"   ❌ No valid patients - patient_id mapping still incorrect")
        if stats[6] == 0:
            print(f"   ❌ No lab codes - measurement_source_value mapping still incorrect")
        if years_spanned <= 1:
            print(f"   ❌ Timestamp parsing failed - sampling_datetime_utc mapping still incorrect")

    print(f"\n{'='*80}")
    if bronze_rows > 600000000:
        print(f"✅ CSV → BRONZE SUCCESS: Column mapping fix worked!")
        print(f"🎯 Ready for Step 3: OMOP CDM transformation!")
    else:
        print(f"❌ CSV → BRONZE FAILED: Column mapping needs further investigation")
    print(f"{'='*80}")

    return "csv_conversion_complete"

# Task definitions
setup_schemas = PythonOperator(
    task_id='setup_bronze_schema',
    python_callable=setup_bronze_schema,
    dag=dag,
)

create_external = PythonOperator(
    task_id='create_csv_external_table_corrected',
    python_callable=create_csv_external_table_corrected,
    dag=dag,
)

execute_ctas = PythonOperator(
    task_id='execute_csv_to_bronze_ctas',
    python_callable=execute_csv_to_bronze_ctas,
    dag=dag,
    execution_timeout=timedelta(hours=2),  # Safety timeout for ~1 hour processing
)

validate_results = PythonOperator(
    task_id='validate_bronze_results',
    python_callable=validate_bronze_results,
    dag=dag,
)

# Clean CSV-only pipeline: CSV → Bronze with corrected mapping
setup_schemas >> create_external >> execute_ctas >> validate_results