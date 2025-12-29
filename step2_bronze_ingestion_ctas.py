"""
STEP 2: Bronze CSV Ingestion - Direct CTAS Approach

Uses Trino's native read_files() function to process ALL 998 CSV files (200GB)
in a single SQL operation with parallel processing across Trino workers.

APPROACH: Direct Create Table As Select (CTAS)
- Single SQL statement processes all CSV files
- Trino workers handle parallel file processing
- Native S3 → Parquet conversion
- Optimal performance for 200GB scale

Expected processing time: 2-4 hours (vs 16+ hours with Python approach)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'omop-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'step2_bronze_ingestion_ctas',
    default_args=default_args,
    description='Step 2: Direct CTAS ingestion of ALL 998 CSV files - v7',
    schedule=None,  # Manual trigger after Step 1
    catchup=False,
    tags=['step2', 'bronze', 'ingestion', 'ctas', 'native', 'v7'],
)

def execute_trino_bronze(sql_query, description):
    """Execute Trino queries for bronze operations"""
    print(f"🚀 {description}")
    print(f"📝 SQL Preview: {sql_query[:300]}{'...' if len(sql_query) > 300 else ''}")

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
        print("✅ Connected to Trino (iceberg.bronze)")

        # Execute query
        cursor.execute(sql_query)

        if sql_query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            for row in results[:10]:
                print(f"📊 {row}")
            if len(results) > 10:
                print(f"... and {len(results) - 10} more rows")
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
    """Prepare bronze schema and clean environment"""
    print("🏗️  STEP 2: Preparing bronze environment for Direct CTAS...")

    # Create bronze schema
    sql_schema = "CREATE SCHEMA IF NOT EXISTS iceberg.bronze"
    execute_trino_bronze(sql_schema, "Create bronze schema")

    # Drop existing tables for fresh start
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

    print("✅ Bronze environment ready for Direct CTAS processing")
    return "environment_ready"

def test_read_files_availability(**context):
    """Test if read_files function is available and working"""
    print("🧪 Testing read_files function availability...")

    try:
        # Test 1: Check if read_files function exists
        sql_functions = "SHOW FUNCTIONS FROM system LIKE '%read%'"
        functions_result = execute_trino_bronze(sql_functions, "Check available read functions")

        # Test 2: Try reading a single CSV file to validate approach
        print("\\n📄 Testing single CSV file reading...")
        sql_test_single = """
        SELECT COUNT(*) as row_count
        FROM TABLE(
            system.table_function.read_files(
                path => 's3://bronze/biological_results_0000.csv',
                format => 'CSV',
                header => true
            )
        )
        """

        test_result = execute_trino_bronze(sql_test_single, "Test single CSV file")

        if test_result and test_result[0][0] > 0:
            print(f"✅ read_files function works! Found {test_result[0][0]:,} rows in test file")
            return "read_files_available"
        else:
            print("⚠️  read_files returned 0 rows - may have issues")
            return "read_files_available_but_empty"

    except Exception as e:
        print(f"❌ read_files test failed: {e}")
        print("🔧 This means Direct CTAS approach won't work")
        raise Exception(f"read_files function not available: {e}")

def create_bronze_table_direct_ctas(**context):
    """Create bronze table using Direct CTAS from ALL CSV files"""
    print("🚀 STEP 2: Creating bronze table with Direct CTAS from ALL 998 CSV files...")
    print("⚡ This is the high-performance approach for 200GB processing!")

    # The main CTAS operation - processes all 998 CSV files in parallel
    sql_ctas = """
    CREATE TABLE iceberg.bronze.biological_results_raw
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['bucket(patient_id, 32)', 'year(date_parse(sampling_datetime_utc, '%Y-%m-%d %H:%i:%s'))'],
        location = 's3://eds-lakehouse/bronze/biological_results/'
    )
    AS
    SELECT
        -- Add source file tracking
        regexp_extract("$path", '[^/]+$') AS source_file,

        -- Original CSV columns with proper typing
        patient_id,
        TRY_CAST(visit_id AS BIGINT) AS visit_id,
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

        -- Add processing metadata
        CURRENT_TIMESTAMP AS load_timestamp,
        'step2_bronze_ctas_v7' AS processing_batch

    FROM TABLE(
        system.table_function.read_files(
            path => 's3://bronze/*.csv',
            format => 'CSV',
            schema => 'patient_id VARCHAR, visit_id VARCHAR, sampling_datetime_utc VARCHAR, result_datetime_utc VARCHAR, report_date_utc VARCHAR, measurement_source_value VARCHAR, value_as_number VARCHAR, value_as_string VARCHAR, unit_source_value VARCHAR, normality VARCHAR, abnormal_flag VARCHAR, value_type VARCHAR, bacterium_id VARCHAR, provider_id VARCHAR, laboratory_uuid VARCHAR',
            header => true
        )
    )
    WHERE patient_id IS NOT NULL
      AND patient_id != ''
    """

    # Execute the CTAS operation
    result = execute_trino_bronze(sql_ctas, "Direct CTAS from ALL 998 CSV files")

    print("✅ Direct CTAS completed!")
    print("🎉 All 998 CSV files processed into Iceberg Parquet table!")

    return result

def validate_ctas_results(**context):
    """Validate the Direct CTAS results"""
    print("📊 STEP 2: Validating Direct CTAS results...")

    # Count total rows
    sql_count = "SELECT COUNT(*) as total_rows FROM iceberg.bronze.biological_results_raw"
    count_result = execute_trino_bronze(sql_count, "Count total rows from CTAS")

    if count_result:
        total_rows = count_result[0][0]
        print(f"📈 Total rows processed: {total_rows:,}")

    # Check file distribution
    sql_files = """
    SELECT
        source_file,
        COUNT(*) as row_count,
        MIN(load_timestamp) as processed_at
    FROM iceberg.bronze.biological_results_raw
    GROUP BY source_file
    ORDER BY source_file
    LIMIT 10
    """
    execute_trino_bronze(sql_files, "File distribution check")

    # Validate data quality
    sql_quality = """
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT patient_id) as unique_patients,
        COUNT(DISTINCT visit_id) as unique_visits,
        COUNT(DISTINCT source_file) as files_processed,
        MIN(sampling_datetime_utc) as earliest_date,
        MAX(sampling_datetime_utc) as latest_date
    FROM iceberg.bronze.biological_results_raw
    """
    quality_result = execute_trino_bronze(sql_quality, "Data quality summary")

    # Sample data
    sql_sample = """
    SELECT patient_id, visit_id, measurement_source_value, value_as_number, source_file
    FROM iceberg.bronze.biological_results_raw
    LIMIT 5
    """
    execute_trino_bronze(sql_sample, "Sample processed data")

    if quality_result:
        stats = quality_result[0]
        print(f"\\n🎯 DIRECT CTAS VALIDATION SUMMARY:")
        print(f"   ✅ Total records: {stats[0]:,}")
        print(f"   ✅ Unique patients: {stats[1]:,}")
        print(f"   ✅ Unique visits: {stats[2]:,}")
        print(f"   ✅ Files processed: {stats[3]:,}")
        print(f"   ✅ Date range: {stats[4]} to {stats[5]}")

        if stats[3] == 998:
            print("\\n🎉 SUCCESS: All 998 CSV files processed!")
            print("🚀 Direct CTAS approach worked perfectly for 200GB!")
        else:
            print(f"\\n⚠️  Only {stats[3]} files processed (expected 998)")

    print("\\n✅ STEP 2 DIRECT CTAS COMPLETED!")
    print("🔄 Ready for Step 3: Data Quality & Silver Layer")

    return quality_result

def create_enhanced_bronze_table(**context):
    """Create enhanced bronze table with OMOP-ready transformations"""
    print("⚡ Creating enhanced bronze table with OMOP transformations...")

    sql_enhanced = """
    CREATE TABLE iceberg.bronze.biological_results_enhanced
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['bucket(person_id_hash, 16)', 'measurement_year']
    )
    AS
    SELECT
        -- All raw columns
        *,

        -- OMOP-ready transformations
        abs(xxhash64(to_utf8(patient_id))) AS person_id_hash,
        substr(to_hex(sha256(to_utf8(patient_id))), 1, 32) AS person_id_anonymized,

        -- Date extractions
        TRY(date_parse(sampling_datetime_utc, '%Y-%m-%d %H:%i:%s')) AS sampling_datetime_parsed,
        TRY(year(date_parse(sampling_datetime_utc, '%Y-%m-%d %H:%i:%s'))) AS measurement_year,
        TRY(date(date_parse(sampling_datetime_utc, '%Y-%m-%d %H:%i:%s'))) AS measurement_date,

        -- Value processing
        TRY_CAST(value_as_number AS DOUBLE) AS value_as_number_parsed,
        COALESCE(value_as_string, CAST(value_as_number AS VARCHAR)) AS value_source_value,

        -- Data quality scoring
        CASE
            WHEN value_as_number IS NOT NULL AND unit_source_value IS NOT NULL THEN 1.0
            WHEN value_as_string IS NOT NULL AND length(trim(value_as_string)) > 0 THEN 0.8
            ELSE 0.5
        END AS data_quality_score,

        -- Generate sequential IDs
        ROW_NUMBER() OVER (ORDER BY sampling_datetime_utc, patient_id, visit_id) AS measurement_id

    FROM iceberg.bronze.biological_results_raw
    WHERE patient_id IS NOT NULL
    """

    result = execute_trino_bronze(sql_enhanced, "Create enhanced bronze table")

    # Validate enhanced table
    sql_enhanced_count = "SELECT COUNT(*) FROM iceberg.bronze.biological_results_enhanced"
    enhanced_result = execute_trino_bronze(sql_enhanced_count, "Count enhanced table rows")

    if enhanced_result:
        enhanced_rows = enhanced_result[0][0]
        print(f"✅ Enhanced table created with {enhanced_rows:,} rows")

    return result

# Task definitions
prepare_env = PythonOperator(
    task_id='prepare_bronze_environment',
    python_callable=prepare_bronze_environment,
    dag=dag,
)

test_read_files = PythonOperator(
    task_id='test_read_files_availability',
    python_callable=test_read_files_availability,
    dag=dag,
)

create_ctas = PythonOperator(
    task_id='create_bronze_table_direct_ctas',
    python_callable=create_bronze_table_direct_ctas,
    dag=dag,
)

validate_results = PythonOperator(
    task_id='validate_ctas_results',
    python_callable=validate_ctas_results,
    dag=dag,
)

create_enhanced = PythonOperator(
    task_id='create_enhanced_bronze_table',
    python_callable=create_enhanced_bronze_table,
    dag=dag,
)

# Dependencies - optimized for Direct CTAS approach
prepare_env >> test_read_files >> create_ctas >> validate_results >> create_enhanced