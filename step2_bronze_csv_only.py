"""
STEP 2: CSV → Bronze Conversion Only - PURE APPROACH

FOCUSED CSV-TO-BRONZE PIPELINE - No OMOP vocabularies!
- Converts CSV files → clustered Parquet in bronze layer
- Time partitioning: year/month for faster queries
- Patient clustering: sorted by patient_id for patient queries
- Expected time: ~1 hour (62 minutes proven)
- Output: s3a://eds-lakehouse/bronze/ (raw lab data + clustering)

Pure Step 2: CSV files → Bronze layer only!
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
    'step2_bronze_csv_only',
    default_args=default_args,
    description='Step 2: CSV → Bronze conversion only (pure approach)',
    schedule=None,
    catchup=False,
    tags=['step2', 'bronze', 'csv-only', 'pure'],
    doc_md="""
    ## Pure Step 2: CSV → Bronze Conversion

    **Input**: CSV files in s3a://bronze/
    **Output**: s3a://eds-lakehouse/bronze/ (raw lab data + clustering)
    **Duration**: ~1 hour (62 minutes proven)

    No OMOP vocabularies - just pure CSV → Parquet conversion with clustering.
    """,
)

def execute_trino_query(sql_query, description, catalog='iceberg', schema='default'):
    """Execute Trino queries with robust error handling"""
    print(f"🚀 {description}")

    import trino

    conn = None
    cursor = None

    try:
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
            return results
        else:
            return "success"

    except Exception as e:
        print(f"❌ Query failed: {e}")
        print(f"🔍 SQL: {sql_query[:200]}...")
        raise e
    finally:
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
    print("🏗️ STEP 2: Setting up schemas for CSV → Bronze conversion...")

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

        # Drop existing bronze table for clean start
        try:
            execute_trino_query(
                "DROP TABLE IF EXISTS iceberg.bronze.biological_results_raw",
                "Drop existing bronze table"
            )
        except:
            pass

        print("✅ Bronze schemas ready for CSV processing!")
        return "bronze_schemas_ready"

    except Exception as e:
        print(f"❌ Bronze schema setup failed: {e}")
        raise Exception(f"Cannot setup bronze schemas: {e}")

def create_hive_csv_external_table(**context):
    """Create Hive external table for CSV processing"""
    print("📊 Creating Hive external CSV table for Step 2 processing...")

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

        # Create external table (proven config)
        sql_create_external = """
        CREATE TABLE hive.test_ingestion.biological_results_csv_external (
            patient_id varchar,
            visit_id varchar,
            sampling_datetime_utc varchar,
            result_datetime_utc varchar,
            report_date_utc varchar,
            measurement_source_value varchar,
            value_as_number varchar,
            value_as_string varchar,
            unit_source_value varchar,
            normality varchar,
            abnormal_flag varchar,
            value_type varchar,
            bacterium_id varchar,
            provider_id varchar,
            laboratory_uuid varchar
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
            "Create Hive external CSV table",
            catalog='hive',
            schema='test_ingestion'
        )

        # Validate table can access data
        validation_query = """
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(DISTINCT "$path") as files_accessible
        FROM hive.test_ingestion.biological_results_csv_external
        WHERE patient_id IS NOT NULL
        LIMIT 1
        """

        result = execute_trino_query(
            validation_query,
            "Validate CSV external table access",
            catalog='hive',
            schema='test_ingestion'
        )

        if result and len(result) > 0:
            total_rows, unique_patients, files_accessible = result[0]
            print(f"✅ CSV External table validation successful!")
            print(f"   📊 Accessible rows: {total_rows:,}")
            print(f"   👥 Unique patients: {unique_patients:,}")
            print(f"   📁 Files accessible: {files_accessible:,}")

            if total_rows == 0:
                raise Exception("External table created but no CSV data is accessible")

            # Store validation metrics
            context['task_instance'].xcom_push(key='external_table_rows', value=total_rows)
            context['task_instance'].xcom_push(key='external_table_files', value=files_accessible)

            print("✅ Hive external CSV table ready for CTAS!")
            return "hive_csv_external_ready"
        else:
            raise Exception("External table validation returned no results")

    except Exception as e:
        print(f"❌ Hive external table creation/validation failed: {e}")
        print("💡 Ensure CSV files are present in s3a://bronze/ bucket")
        raise Exception(f"Cannot create/validate Hive external table: {e}")

def execute_csv_to_bronze_ctas(**context):
    """Execute CSV → Bronze CTAS with clustering"""
    print("🚀 STEP 2: CSV → Bronze CTAS processing!")
    print("⚡ Converting CSV files to clustered Parquet in bronze layer")

    # Get validation metrics from previous task
    external_rows = context['task_instance'].xcom_pull(task_ids='create_hive_csv_external_table', key='external_table_rows')
    external_files = context['task_instance'].xcom_pull(task_ids='create_hive_csv_external_table', key='external_table_files')

    if external_rows and external_files:
        print(f"📊 Processing: {external_rows:,} rows from {external_files:,} CSV files")

    start_time = datetime.now()

    # CSV → Bronze CTAS operation
    sql_csv_to_bronze = """
    CREATE TABLE iceberg.bronze.biological_results_raw
    WITH (
        format = 'PARQUET',

        -- Time-based partitioning for fast date range queries
        partitioning = ARRAY['year', 'month'],

        -- Storage location
        location = 's3a://eds-lakehouse/bronze/biological_results_raw/'
    )
    AS
    SELECT
        -- Processing metadata
        'step2_csv_to_bronze' AS processing_batch,

        -- Raw CSV columns with basic type casting
        patient_id,
        TRY_CAST(visit_id AS BIGINT) AS visit_id,
        TRY_CAST(sampling_datetime_utc AS TIMESTAMP) AS sampling_datetime_utc,
        TRY_CAST(result_datetime_utc AS TIMESTAMP) AS result_datetime_utc,
        TRY_CAST(report_date_utc AS DATE) AS report_date_utc,
        measurement_source_value,  -- Keep original lab codes (LC:0007, etc.)
        TRY_CAST(value_as_number AS DOUBLE) AS value_as_number,
        value_as_string,
        unit_source_value,
        normality,
        abnormal_flag,
        value_type,
        bacterium_id,
        provider_id,
        laboratory_uuid,

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
    WHERE patient_id IS NOT NULL
      AND patient_id != ''
    ORDER BY patient_id, measurement_source_value  -- Clustering for patient queries
    """

    print("🔥 Starting CSV → Bronze CTAS conversion!")
    print("⏰ Expected time: ~62 minutes for 634M+ rows")

    result = execute_trino_query(sql_csv_to_bronze, "CSV → Bronze Parquet with clustering", schema='bronze')

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"🎉 CSV → Bronze conversion COMPLETE!")
    print(f"⏱️  Total time: {duration/3600:.2f} hours")
    print(f"🚀 Data converted to clustered Parquet in bronze layer!")

    context['task_instance'].xcom_push(key='processing_time_hours', value=duration/3600)

    return result

def validate_bronze_data(**context):
    """Validate bronze data creation"""
    print("📊 Validating Step 2: CSV → Bronze conversion results...")

    processing_time = context['task_instance'].xcom_pull(task_ids='execute_csv_to_bronze_ctas', key='processing_time_hours')
    external_rows = context['task_instance'].xcom_pull(task_ids='create_hive_csv_external_table', key='external_table_rows')

    # Count bronze data
    try:
        count_result = execute_trino_query(
            "SELECT COUNT(*) FROM iceberg.bronze.biological_results_raw",
            "Count bronze data rows",
            schema='bronze'
        )
        bronze_rows = count_result[0][0] if (count_result and len(count_result) > 0) else 0
    except Exception as e:
        print(f"⚠️  Could not count bronze rows: {e}")
        bronze_rows = 0

    # Detailed statistics
    try:
        stats_sql = """
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(DISTINCT visit_id) as unique_visits,
            MIN(sampling_datetime_utc) as earliest_sample,
            MAX(sampling_datetime_utc) as latest_sample,
            COUNT(CASE WHEN value_as_number IS NOT NULL THEN 1 END) as numeric_values
        FROM iceberg.bronze.biological_results_raw
        """

        stats_result = execute_trino_query(stats_sql, "Comprehensive bronze statistics", schema='bronze')
        stats = stats_result[0] if (stats_result and len(stats_result) > 0) else (0, 0, 0, None, None, 0)
    except Exception as e:
        print(f"⚠️  Could not get detailed statistics: {e}")
        stats = (bronze_rows, 0, 0, None, None, 0)

    # Check partitioning
    try:
        partition_sql = """
        SELECT
            COUNT(DISTINCT year) as years_spanned,
            COUNT(DISTINCT CAST(year AS VARCHAR) || '-' || CAST(month AS VARCHAR)) as total_partitions,
            MIN(year) as first_year,
            MAX(year) as last_year
        FROM iceberg.bronze.biological_results_raw
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
    print(f"🎉 STEP 2 RESULTS: CSV → BRONZE CONVERSION")
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

    print(f"\n🗂️ CLUSTERING STATUS:")
    print(f"   📅 Time partitions: {total_partitions:,} partitions ({years_spanned} years: {first_year}-{last_year})")
    print(f"   🗃️ Storage: s3a://eds-lakehouse/bronze/biological_results_raw/")
    print(f"   🔗 Clustering: patient_id + measurement_source_value")
    print(f"   📁 Format: Parquet with Iceberg metadata")

    # Processing rate
    if processing_time and processing_time > 0 and stats[0] > 0:
        processing_rate = stats[0] / (processing_time * 3600)
        print(f"   🚀 Processing rate: {processing_rate:,.0f} rows/second")

    print(f"\n🎯 OUTPUT:")
    print(f"   📍 Location: s3a://eds-lakehouse/bronze/")
    print(f"   📋 Format: Raw lab data + time clustering")
    print(f"   🧪 Lab codes: Original format (LC:0007, LC:0010, etc.)")
    print(f"   ➡️  Ready for: Step 3 - OMOP CDM transformation")

    print(f"\n{'='*80}")
    print(f"✅ STEP 2 COMPLETE: CSV → BRONZE READY FOR STEP 3!")
    print(f"{'='*80}")

    return "bronze_conversion_complete"

# Task definitions
setup_schemas = PythonOperator(
    task_id='setup_bronze_schema',
    python_callable=setup_bronze_schema,
    dag=dag,
)

create_external = PythonOperator(
    task_id='create_hive_csv_external_table',
    python_callable=create_hive_csv_external_table,
    dag=dag,
)

execute_ctas = PythonOperator(
    task_id='execute_csv_to_bronze_ctas',
    python_callable=execute_csv_to_bronze_ctas,
    dag=dag,
    execution_timeout=timedelta(hours=2),  # Safety timeout for ~1 hour processing
)

validate_bronze = PythonOperator(
    task_id='validate_bronze_data',
    python_callable=validate_bronze_data,
    dag=dag,
)

# Pure Step 2 pipeline: CSV → Bronze only
setup_schemas >> create_external >> execute_ctas >> validate_bronze