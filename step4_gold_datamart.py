"""
STEP 4: Silver → Gold Clinical Datamart Creation

GOLD DATAMART PIPELINE FOR CLINICAL RESEARCH
- Source: Silver OMOP CDM measurement_eb135b0d (634M+ measurements, 4.7M+ patients)
- Target: Gold clinical research tables optimized for analytics
- Focus: Patient summaries, measurement trends, laboratory analytics
- Expected time: ~20-30 minutes for datamart creation
- Output: s3a://eds-lakehouse/gold/ (Research-ready tables)

Step 4: OMOP CDM silver → Gold clinical datamart for research analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'clinical-datamart',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,  # No retries - fail fast for debugging
    'on_failure_callback': None,
    'on_retry_callback': None,
}

dag = DAG(
    'step4_gold_datamart',
    default_args=default_args,
    description='Step 4: Silver → Gold Clinical Datamart (v1 - Research Analytics)',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['step4', 'gold', 'datamart', 'clinical-research', 'v1', 'analytics'],
    doc_md="""
    ## Silver → Gold Clinical Datamart v1 - Research Analytics

    🎯 FOCUS: Transform 634M+ OMOP measurements to clinical research datamart
    🔧 SOURCE: measurement_eb135b0d (silver OMOP CDM)
    ⚡ TARGET: Gold clinical research tables (patient summaries, trends, analytics)
    🏥 OPTIMIZED: Fast clinical queries and research analytics
    📊 TABLES: Patient summary, measurement trends, lab analytics, cohort views

    Built on successful Step 3 OMOP transformation with 4.7M+ patients.
    Optimized for clinical research queries and longitudinal analysis.

    Version: gold-v1 (Dec 30, 2025) - Clinical Research Datamart
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
            # Optimized session properties for datamart creation
            session_properties={
                'query_max_memory': '40GB',           # Proven working memory limit
                'query_max_memory_per_node': '12GB',  # Within Helm limits
                'task_concurrency': '4',              # Memory-efficient concurrency
                'join_distribution_type': 'AUTOMATIC'
            }
        )

        cursor = conn.cursor()
        start_time = time.time()

        print(f"🔧 Query memory limits optimized for Gold datamart creation")
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
        # Add datamart-specific error context
        if "memory" in str(e).lower():
            print(f"💾 MEMORY ERROR: Consider optimizing datamart query")
        if "partition" in str(e).lower():
            print(f"🗂️ PARTITION ERROR: Check partitioning strategy")
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

def setup_gold_schemas(**context):
    """Setup Gold datamart schemas for clinical research"""
    print("🏗️ STEP 4 GOLD: Setting up Gold schemas for clinical research datamart...")

    try:
        # Create gold schema for clinical datamart tables
        execute_trino_query(
            "CREATE SCHEMA IF NOT EXISTS iceberg.gold WITH (location = 's3a://eds-lakehouse/gold/')",
            "Create Iceberg gold schema for clinical research datamart"
        )

        # Create analytics schema for aggregated views
        execute_trino_query(
            "CREATE SCHEMA IF NOT EXISTS iceberg.analytics WITH (location = 's3a://eds-lakehouse/analytics/')",
            "Create Iceberg analytics schema for research views"
        )

        print("✅ Gold datamart schemas ready for clinical research!")
        return "gold_schemas_ready"

    except Exception as e:
        print(f"❌ Gold schema setup failed: {e}")
        raise Exception(f"Cannot setup Gold schemas: {e}")

def create_patient_summary(**context):
    """Create patient summary table for clinical research"""
    print("👥 Creating patient summary table with measurement statistics...")

    try:
        # Source table from Step 3
        silver_table = "measurement_eb135b0d"  # From successful Step 3

        # Generate unique table name for gold
        import uuid
        unique_suffix = str(uuid.uuid4())[:8]
        patient_summary_table = f"patient_summary_{unique_suffix}"

        print(f"📊 Source: iceberg.silver.{silver_table}")
        print(f"🎯 Target: iceberg.gold.{patient_summary_table}")

        start_time = datetime.now()

        # Patient summary aggregation for clinical research
        patient_summary_sql = f"""
        CREATE TABLE iceberg.gold.{patient_summary_table}
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['patient_year_of_first_measurement']
        )
        AS
        SELECT
            -- Patient identifiers
            person_id,

            -- Measurement summary statistics
            COUNT(*) as total_measurements,
            COUNT(DISTINCT measurement_date) as unique_measurement_dates,
            COUNT(CASE WHEN value_as_number IS NOT NULL THEN 1 END) as numeric_measurements,
            COUNT(CASE WHEN unit_concept_id > 0 THEN 1 END) as measurements_with_units,

            -- Temporal coverage
            MIN(measurement_date) as first_measurement_date,
            MAX(measurement_date) as last_measurement_date,
            DATE_DIFF('day', MIN(measurement_date), MAX(measurement_date)) as measurement_span_days,

            -- Laboratory test diversity
            COUNT(DISTINCT measurement_source_value) as unique_lab_tests,

            -- Abnormal results tracking
            COUNT(CASE WHEN measurement_event_id = 4135493 THEN 1 END) as abnormal_results,
            COUNT(CASE WHEN measurement_event_id = 4328749 THEN 1 END) as high_results,
            COUNT(CASE WHEN measurement_event_id = 4124457 THEN 1 END) as normal_low_results,

            -- Statistical measures for numeric values
            AVG(value_as_number) as avg_numeric_value,
            MIN(value_as_number) as min_numeric_value,
            MAX(value_as_number) as max_numeric_value,
            STDDEV(value_as_number) as stddev_numeric_value,

            -- Quality indicators
            COUNT(CASE WHEN value_as_number IS NULL AND value_source_value IS NULL THEN 1 END) as missing_value_count,

            -- Patient activity patterns
            COUNT(DISTINCT YEAR(measurement_date)) as active_years,
            COUNT(DISTINCT MONTH(measurement_date)) as active_months,

            -- Partitioning column
            YEAR(MIN(measurement_date)) as patient_year_of_first_measurement

        FROM iceberg.silver.{silver_table}
        WHERE person_id IS NOT NULL
        GROUP BY person_id
        """

        print(f"🚀 Starting Patient Summary creation...")
        print(f"⏰ Expected processing time: ~8-12 minutes for 4.7M+ patients")

        result = execute_trino_query(
            patient_summary_sql,
            "Create patient summary table for clinical research"
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"🎉 Patient Summary creation complete!")
        print(f"⏱️  Processing time: {duration/60:.2f} minutes")
        print(f"🎯 Created table: iceberg.gold.{patient_summary_table}")

        # Store results for next tasks
        context['task_instance'].xcom_push(key='patient_summary_table', value=patient_summary_table)
        context['task_instance'].xcom_push(key='processing_time_minutes', value=duration/60)

        return "patient_summary_created"

    except Exception as e:
        print(f"❌ Patient Summary creation failed: {e}")
        print("💡 Check Silver OMOP data availability and memory limits")
        raise Exception(f"Cannot create Patient Summary: {e}")

def create_measurement_trends(**context):
    """Create measurement trends table for longitudinal analysis"""
    print("📈 Creating measurement trends table for longitudinal clinical analysis...")

    try:
        # Source table from Step 3
        silver_table = "measurement_eb135b0d"

        # Generate unique table name for trends
        import uuid
        unique_suffix = str(uuid.uuid4())[:8]
        trends_table = f"measurement_trends_{unique_suffix}"

        print(f"📊 Source: iceberg.silver.{silver_table}")
        print(f"🎯 Target: iceberg.gold.{trends_table}")

        start_time = datetime.now()

        # Measurement trends aggregation for longitudinal analysis
        trends_sql = f"""
        CREATE TABLE iceberg.gold.{trends_table}
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['measurement_year', 'measurement_month']
        )
        AS
        SELECT
            -- Temporal grouping
            measurement_year,
            measurement_month,
            measurement_date,

            -- Test identification
            measurement_source_value,
            unit_source_value,

            -- Daily aggregations
            COUNT(*) as daily_test_count,
            COUNT(DISTINCT person_id) as daily_unique_patients,

            -- Statistical measures
            AVG(value_as_number) as daily_avg_value,
            APPROX_PERCENTILE(value_as_number, 0.5) as daily_median_value,
            APPROX_PERCENTILE(value_as_number, 0.25) as daily_p25_value,
            APPROX_PERCENTILE(value_as_number, 0.75) as daily_p75_value,
            MIN(value_as_number) as daily_min_value,
            MAX(value_as_number) as daily_max_value,
            STDDEV(value_as_number) as daily_stddev_value,

            -- Abnormal result tracking
            COUNT(CASE WHEN measurement_event_id = 4135493 THEN 1 END) as daily_abnormal_count,
            COUNT(CASE WHEN measurement_event_id = 4328749 THEN 1 END) as daily_high_count,
            COUNT(CASE WHEN measurement_event_id = 4124457 THEN 1 END) as daily_normal_low_count,

            -- Percentage calculations
            ROUND(100.0 * COUNT(CASE WHEN measurement_event_id = 4135493 THEN 1 END) / COUNT(*), 2) as pct_abnormal,
            ROUND(100.0 * COUNT(CASE WHEN value_as_number IS NOT NULL THEN 1 END) / COUNT(*), 2) as pct_numeric,

            -- Data quality indicators
            COUNT(CASE WHEN value_as_number IS NULL AND value_source_value IS NULL THEN 1 END) as daily_missing_values

        FROM iceberg.silver.{silver_table}
        WHERE
            measurement_date IS NOT NULL
            AND measurement_source_value IS NOT NULL
        GROUP BY
            measurement_year,
            measurement_month,
            measurement_date,
            measurement_source_value,
            unit_source_value
        """

        print(f"🚀 Starting Measurement Trends creation...")
        print(f"⏰ Expected processing time: ~10-15 minutes for time-series aggregation")

        result = execute_trino_query(
            trends_sql,
            "Create measurement trends table for longitudinal analysis"
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"🎉 Measurement Trends creation complete!")
        print(f"⏱️  Processing time: {duration/60:.2f} minutes")
        print(f"🎯 Created table: iceberg.gold.{trends_table}")

        # Store results for validation
        context['task_instance'].xcom_push(key='trends_table', value=trends_table)
        context['task_instance'].xcom_push(key='trends_processing_time_minutes', value=duration/60)

        return "measurement_trends_created"

    except Exception as e:
        print(f"❌ Measurement Trends creation failed: {e}")
        print("💡 Check temporal data quality and aggregation complexity")
        raise Exception(f"Cannot create Measurement Trends: {e}")

def create_laboratory_analytics(**context):
    """Create laboratory analytics table for common lab test analysis"""
    print("🔬 Creating laboratory analytics table for common lab test research...")

    try:
        # Source table from Step 3
        silver_table = "measurement_eb135b0d"

        # Generate unique table name for lab analytics
        import uuid
        unique_suffix = str(uuid.uuid4())[:8]
        lab_analytics_table = f"laboratory_analytics_{unique_suffix}"

        print(f"📊 Source: iceberg.silver.{silver_table}")
        print(f"🎯 Target: iceberg.gold.{lab_analytics_table}")

        start_time = datetime.now()

        # Laboratory analytics aggregation for research
        lab_analytics_sql = f"""
        CREATE TABLE iceberg.gold.{lab_analytics_table}
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['test_category']
        )
        AS
        SELECT
            -- Test identification
            measurement_source_value as lab_test_code,
            unit_source_value as lab_unit,

            -- Test categorization (simplified for now)
            CASE
                WHEN unit_source_value IN ('mg/dL', 'mmol/L', 'nmol/L', 'g/dL') THEN 'Chemistry'
                WHEN unit_source_value IN ('U/L') THEN 'Enzymes'
                WHEN unit_source_value IN ('cells/µL', 'cells/uL', '/uL', '/µL') THEN 'Hematology'
                WHEN unit_source_value IN ('log copies/mL', 'copies/mL') THEN 'Virology'
                WHEN unit_source_value IN ('mOsm/kg') THEN 'Osmolality'
                WHEN unit_source_value IN ('%') THEN 'Percentage'
                ELSE 'Other'
            END as test_category,

            -- Volume statistics
            COUNT(*) as total_test_count,
            COUNT(DISTINCT person_id) as unique_patients_tested,
            COUNT(DISTINCT measurement_date) as unique_test_dates,

            -- Temporal coverage
            MIN(measurement_date) as earliest_test_date,
            MAX(measurement_date) as latest_test_date,
            DATE_DIFF('day', MIN(measurement_date), MAX(measurement_date)) as test_span_days,

            -- Numeric value analysis (where available)
            COUNT(CASE WHEN value_as_number IS NOT NULL THEN 1 END) as numeric_result_count,
            AVG(value_as_number) as avg_numeric_result,
            APPROX_PERCENTILE(value_as_number, 0.5) as median_numeric_result,
            MIN(value_as_number) as min_numeric_result,
            MAX(value_as_number) as max_numeric_result,
            STDDEV(value_as_number) as stddev_numeric_result,

            -- Reference range analysis (percentiles as proxy)
            APPROX_PERCENTILE(value_as_number, 0.025) as p2_5_value,
            APPROX_PERCENTILE(value_as_number, 0.975) as p97_5_value,

            -- Abnormal result analysis
            COUNT(CASE WHEN measurement_event_id = 4135493 THEN 1 END) as abnormal_result_count,
            COUNT(CASE WHEN measurement_event_id = 4328749 THEN 1 END) as high_result_count,
            COUNT(CASE WHEN measurement_event_id = 4124457 THEN 1 END) as normal_low_result_count,

            -- Percentage calculations
            ROUND(100.0 * COUNT(CASE WHEN measurement_event_id = 4135493 THEN 1 END) / COUNT(*), 2) as pct_abnormal,
            ROUND(100.0 * COUNT(CASE WHEN value_as_number IS NOT NULL THEN 1 END) / COUNT(*), 2) as pct_numeric,

            -- Data quality metrics
            COUNT(CASE WHEN value_as_number IS NULL AND value_source_value IS NULL THEN 1 END) as missing_value_count,
            ROUND(100.0 * COUNT(CASE WHEN value_as_number IS NULL AND value_source_value IS NULL THEN 1 END) / COUNT(*), 2) as pct_missing,

            -- Test frequency analysis
            COUNT(*) / COUNT(DISTINCT person_id) as avg_tests_per_patient,
            COUNT(*) / NULLIF(DATE_DIFF('day', MIN(measurement_date), MAX(measurement_date)), 0) as avg_tests_per_day

        FROM iceberg.silver.{silver_table}
        WHERE
            measurement_source_value IS NOT NULL
            AND measurement_source_value != ''
        GROUP BY
            measurement_source_value,
            unit_source_value
        HAVING COUNT(*) >= 1000  -- Focus on frequently used tests for analytics
        """

        print(f"🚀 Starting Laboratory Analytics creation...")
        print(f"⏰ Expected processing time: ~8-12 minutes for lab test aggregation")

        result = execute_trino_query(
            lab_analytics_sql,
            "Create laboratory analytics table for research"
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"🎉 Laboratory Analytics creation complete!")
        print(f"⏱️  Processing time: {duration/60:.2f} minutes")
        print(f"🎯 Created table: iceberg.gold.{lab_analytics_table}")

        # Store results for validation
        context['task_instance'].xcom_push(key='lab_analytics_table', value=lab_analytics_table)
        context['task_instance'].xcom_push(key='lab_processing_time_minutes', value=duration/60)

        return "laboratory_analytics_created"

    except Exception as e:
        print(f"❌ Laboratory Analytics creation failed: {e}")
        print("💡 Check lab test data availability and aggregation logic")
        raise Exception(f"Cannot create Laboratory Analytics: {e}")

def validate_gold_datamart(**context):
    """Validate Gold datamart creation results"""
    print("🔍 Validating Gold datamart creation results...")

    try:
        # Get table names from previous tasks
        patient_summary_table = context['task_instance'].xcom_pull(task_ids='create_patient_summary', key='patient_summary_table')
        trends_table = context['task_instance'].xcom_pull(task_ids='create_measurement_trends', key='trends_table')
        lab_analytics_table = context['task_instance'].xcom_pull(task_ids='create_laboratory_analytics', key='lab_analytics_table')

        # Get processing times
        patient_time = context['task_instance'].xcom_pull(task_ids='create_patient_summary', key='processing_time_minutes')
        trends_time = context['task_instance'].xcom_pull(task_ids='create_measurement_trends', key='trends_processing_time_minutes')
        lab_time = context['task_instance'].xcom_pull(task_ids='create_laboratory_analytics', key='lab_processing_time_minutes')

        if not all([patient_summary_table, trends_table, lab_analytics_table]):
            raise Exception("Missing datamart tables from previous tasks")

        print(f"🎯 Validating Gold datamart tables:")
        print(f"   👥 Patient Summary: iceberg.gold.{patient_summary_table}")
        print(f"   📈 Measurement Trends: iceberg.gold.{trends_table}")
        print(f"   🔬 Laboratory Analytics: iceberg.gold.{lab_analytics_table}")

        # Count records in each table
        patient_count_result = execute_trino_query(
            f"SELECT COUNT(*) FROM iceberg.gold.{patient_summary_table}",
            "Count patient summary records"
        )
        patient_count = patient_count_result[0][0] if patient_count_result else 0

        trends_count_result = execute_trino_query(
            f"SELECT COUNT(*) FROM iceberg.gold.{trends_table}",
            "Count measurement trends records"
        )
        trends_count = trends_count_result[0][0] if trends_count_result else 0

        lab_count_result = execute_trino_query(
            f"SELECT COUNT(*) FROM iceberg.gold.{lab_analytics_table}",
            "Count laboratory analytics records"
        )
        lab_count = lab_count_result[0][0] if lab_count_result else 0

        # Sample patient summary statistics
        patient_stats_sql = f"""
        SELECT
            COUNT(*) as total_patients,
            AVG(total_measurements) as avg_measurements_per_patient,
            AVG(measurement_span_days) as avg_measurement_span_days,
            AVG(unique_lab_tests) as avg_lab_tests_per_patient,
            SUM(abnormal_results) as total_abnormal_results,
            COUNT(CASE WHEN total_measurements >= 10 THEN 1 END) as patients_with_10plus_measurements
        FROM iceberg.gold.{patient_summary_table}
        """

        patient_stats_result = execute_trino_query(patient_stats_sql, "Patient summary statistics")
        patient_stats = patient_stats_result[0] if patient_stats_result else (0,)*6

        # Laboratory analytics summary
        lab_summary_sql = f"""
        SELECT
            COUNT(*) as unique_lab_test_types,
            COUNT(DISTINCT test_category) as test_categories,
            SUM(total_test_count) as total_lab_tests,
            SUM(unique_patients_tested) as total_patients_with_tests,
            AVG(pct_abnormal) as avg_abnormal_percentage
        FROM iceberg.gold.{lab_analytics_table}
        """

        lab_summary_result = execute_trino_query(lab_summary_sql, "Laboratory analytics summary")
        lab_summary = lab_summary_result[0] if lab_summary_result else (0,)*5

        total_processing_time = (patient_time or 0) + (trends_time or 0) + (lab_time or 0)

        print(f"\n{'='*80}")
        print(f"🎉 GOLD DATAMART CREATION RESULTS")
        print(f"{'='*80}")
        print(f"📊 PROCESSING SUMMARY:")
        print(f"   ⏱️  Total processing time: {total_processing_time:.2f} minutes")
        print(f"   📊 Patient Summary time: {patient_time:.2f} minutes" if patient_time else "   📊 Patient Summary time: Unknown")
        print(f"   📈 Trends creation time: {trends_time:.2f} minutes" if trends_time else "   📈 Trends creation time: Unknown")
        print(f"   🔬 Lab Analytics time: {lab_time:.2f} minutes" if lab_time else "   🔬 Lab Analytics time: Unknown")

        print(f"\n📊 DATAMART TABLES:")
        print(f"   👥 Patient Summary records: {patient_count:,}")
        print(f"   📈 Measurement Trends records: {trends_count:,}")
        print(f"   🔬 Laboratory Analytics records: {lab_count:,}")

        print(f"\n📊 CLINICAL RESEARCH INSIGHTS:")
        print(f"   👥 Total patients: {patient_stats[0]:,}")
        print(f"   📊 Avg measurements per patient: {patient_stats[1]:.1f}")
        print(f"   📅 Avg measurement span: {patient_stats[2]:.1f} days")
        print(f"   🧪 Avg lab tests per patient: {patient_stats[3]:.1f}")
        print(f"   ⚠️  Total abnormal results: {patient_stats[4]:,}")
        print(f"   📈 Patients with 10+ measurements: {patient_stats[5]:,}")

        print(f"\n🔬 LABORATORY ANALYTICS:")
        print(f"   🧪 Unique lab test types: {lab_summary[0]:,}")
        print(f"   📋 Test categories: {lab_summary[1]:,}")
        print(f"   🔢 Total lab tests analyzed: {lab_summary[2]:,}")
        print(f"   👥 Patients with lab tests: {lab_summary[3]:,}")
        print(f"   ⚠️  Average abnormal rate: {lab_summary[4]:.1f}%")

        print(f"\n🎯 DATAMART QUALITY ASSESSMENT:")
        if patient_count >= 4000000:  # 4M+ patients
            print(f"   ✅ EXCELLENT: {patient_count:,} patients in datamart")

        if total_processing_time <= 30:
            print(f"   ✅ EXCELLENT: Processing completed in {total_processing_time:.1f} minutes")
        elif total_processing_time <= 45:
            print(f"   ✅ GOOD: Processing completed in {total_processing_time:.1f} minutes")

        if lab_summary[0] >= 100:
            print(f"   ✅ EXCELLENT: {lab_summary[0]:,} unique lab test types")

        print(f"\n💡 READY FOR CLINICAL RESEARCH:")
        print(f"   📊 Patient cohort analysis with longitudinal tracking")
        print(f"   📈 Time-series analysis of laboratory trends")
        print(f"   🔬 Laboratory test reference range analysis")
        print(f"   🏥 Clinical outcome prediction and risk stratification")

        print(f"\n{'='*80}")
        if patient_count > 4000000 and lab_summary[0] > 50:
            print(f"✅ GOLD DATAMART CREATION SUCCESS!")
            print(f"🎯 Ready for clinical research and advanced analytics!")
        else:
            print(f"⚠️  Partial datamart creation - investigate data quality")
        print(f"{'='*80}")

        return "gold_datamart_complete"

    except Exception as e:
        print(f"❌ Gold datamart validation failed: {e}")
        raise Exception(f"Cannot validate Gold datamart: {e}")

# Task definitions
setup_schemas = PythonOperator(
    task_id='setup_gold_schemas',
    python_callable=setup_gold_schemas,
    dag=dag,
)

create_patient_summary_task = PythonOperator(
    task_id='create_patient_summary',
    python_callable=create_patient_summary,
    dag=dag,
    execution_timeout=timedelta(hours=1),  # Allow time for patient aggregation
)

create_trends_task = PythonOperator(
    task_id='create_measurement_trends',
    python_callable=create_measurement_trends,
    dag=dag,
    execution_timeout=timedelta(hours=1),  # Allow time for time-series aggregation
)

create_lab_analytics_task = PythonOperator(
    task_id='create_laboratory_analytics',
    python_callable=create_laboratory_analytics,
    dag=dag,
    execution_timeout=timedelta(hours=1),  # Allow time for lab test aggregation
)

validate_datamart = PythonOperator(
    task_id='validate_gold_datamart',
    python_callable=validate_gold_datamart,
    dag=dag,
)

# Gold datamart pipeline: Setup → [Patient Summary, Trends, Lab Analytics] → Validate
setup_schemas >> [create_patient_summary_task, create_trends_task, create_lab_analytics_task] >> validate_datamart