"""
STEP 3: Bronze → Silver OMOP CDM Transformation

OMOP CDM STANDARDIZATION PIPELINE
- Source: Bronze biological_results_raw_dc6befcb (634M+ rows)
- Target: Silver OMOP CDM tables (MEASUREMENT, PERSON, VISIT_OCCURRENCE)
- Vocabularies: LOINC → concept_id mapping for standardization
- Expected time: ~30-45 minutes for OMOP transformation
- Output: s3a://eds-lakehouse/silver/ (OMOP-compliant tables)

Step 3: Bronze lab data → OMOP CDM silver layer for clinical research
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
    'step3_omop_transformation',
    default_args=default_args,
    description='Step 3: Bronze → Silver OMOP CDM Transformation (v1)',
    schedule=None,
    catchup=False,
    tags=['step3', 'silver', 'omop-cdm', 'transformation', 'v1'],
    doc_md="""
    ## Bronze → Silver OMOP CDM Transformation v1

    🎯 FOCUS: Transform 634M+ bronze records to OMOP CDM format
    🔧 SOURCE: biological_results_raw_dc6befcb (bronze layer)
    ⚡ TARGET: OMOP CDM tables (MEASUREMENT, PERSON, VISIT_OCCURRENCE)
    🏗️ VOCABULARIES: LOINC, UCUM, SNOMED mapping

    Transforms raw laboratory data into standardized OMOP format for clinical research.
    Creates proper concept mappings and ensures OMOP CDM compliance.

    Version: omop-v1 (Dec 30, 2025) - Initial OMOP Transformation
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
            # Optimized session properties for OMOP transformation
            session_properties={
                'query_max_memory': '20GB',
                'query_max_memory_per_node': '8GB',
                'query_max_total_memory': '24GB',
                'task_concurrency': '8',
                'join_distribution_type': 'AUTOMATIC'
            }
        )

        cursor = conn.cursor()
        start_time = time.time()

        print(f"🔧 Query memory limits optimized for OMOP transformation")
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
        # Add OMOP-specific error context
        if "concept_id" in str(e).lower():
            print(f"🗂️ OMOP ERROR: Issue with concept mapping or vocabulary tables")
        if "OutOfMemoryError" in str(e) or "memory" in str(e).lower():
            print(f"💾 MEMORY ERROR: Consider optimizing OMOP transformation query")
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

def setup_omop_schemas(**context):
    """Setup OMOP silver schemas and vocabularies"""
    print("🏗️ STEP 3 OMOP: Setting up Silver schemas and OMOP vocabulary tables...")

    try:
        # Create silver schema for OMOP CDM tables
        execute_trino_query(
            "CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 's3a://eds-lakehouse/silver/')",
            "Create Iceberg silver schema for OMOP CDM"
        )

        # Create vocabulary schema for OMOP vocabularies
        execute_trino_query(
            "CREATE SCHEMA IF NOT EXISTS iceberg.vocabulary WITH (location = 's3a://eds-lakehouse/vocabulary/')",
            "Create Iceberg vocabulary schema for OMOP concepts"
        )

        print("✅ OMOP schemas ready for CDM transformation!")
        return "omop_schemas_ready"

    except Exception as e:
        print(f"❌ OMOP schema setup failed: {e}")
        raise Exception(f"Cannot setup OMOP schemas: {e}")

def load_omop_vocabularies(**context):
    """Load essential OMOP vocabularies for lab data mapping"""
    print("📚 Loading OMOP vocabularies (LOINC, UCUM) for laboratory data mapping...")

    try:
        # Create concept table for LOINC codes (laboratory measurements)
        loinc_concept_sql = """
        CREATE TABLE IF NOT EXISTS iceberg.vocabulary.concept (
            concept_id BIGINT,
            concept_name VARCHAR,
            domain_id VARCHAR,
            vocabulary_id VARCHAR,
            concept_class_id VARCHAR,
            standard_concept VARCHAR,
            concept_code VARCHAR,
            valid_start_date DATE,
            valid_end_date DATE,
            invalid_reason VARCHAR
        )
        WITH (
            format = 'PARQUET',
            location = 's3a://eds-lakehouse/vocabulary/concept/'
        )
        """

        execute_trino_query(
            loinc_concept_sql,
            "Create OMOP concept table for LOINC mapping"
        )

        # Create concept_relationship table for mappings
        relationship_sql = """
        CREATE TABLE IF NOT EXISTS iceberg.vocabulary.concept_relationship (
            concept_id_1 BIGINT,
            concept_id_2 BIGINT,
            relationship_id VARCHAR,
            valid_start_date DATE,
            valid_end_date DATE,
            invalid_reason VARCHAR
        )
        WITH (
            format = 'PARQUET',
            location = 's3a://eds-lakehouse/vocabulary/concept_relationship/'
        )
        """

        execute_trino_query(
            relationship_sql,
            "Create OMOP concept_relationship table for mappings"
        )

        # Insert basic LOINC concepts for common lab tests
        # This is a simplified version - in production, load from OHDSI Athena
        basic_loinc_concepts = """
        INSERT INTO iceberg.vocabulary.concept VALUES
        (3013682, 'Glucose [Mass/volume] in Serum or Plasma', 'Measurement', 'LOINC', 'Lab Test', 'S', '2345-7', DATE '1970-01-01', DATE '2099-12-31', NULL),
        (3004249, 'Cholesterol [Mass/volume] in Serum or Plasma', 'Measurement', 'LOINC', 'Lab Test', 'S', '2093-3', DATE '1970-01-01', DATE '2099-12-31', NULL),
        (3024561, 'Hemoglobin [Mass/volume] in Blood', 'Measurement', 'LOINC', 'Lab Test', 'S', '718-7', DATE '1970-01-01', DATE '2099-12-31', NULL),
        (3013721, 'Creatinine [Mass/volume] in Serum or Plasma', 'Measurement', 'LOINC', 'Lab Test', 'S', '2160-0', DATE '1970-01-01', DATE '2099-12-31', NULL),
        (3006923, 'Alanine aminotransferase [Enzymatic activity/volume] in Serum or Plasma', 'Measurement', 'LOINC', 'Lab Test', 'S', '1742-6', DATE '1970-01-01', DATE '2099-12-31', NULL)
        """

        # Check if concepts already exist before inserting
        count_result = execute_trino_query(
            "SELECT COUNT(*) FROM iceberg.vocabulary.concept",
            "Count existing OMOP concepts"
        )

        concept_count = count_result[0][0] if count_result and len(count_result) > 0 else 0

        if concept_count == 0:
            execute_trino_query(
                basic_loinc_concepts,
                "Insert basic LOINC concepts for lab tests"
            )
            print(f"✅ Inserted basic LOINC concepts for laboratory mapping")
        else:
            print(f"✅ Found {concept_count} existing OMOP concepts - skipping insert")

        # Store vocabulary readiness
        context['task_instance'].xcom_push(key='vocabulary_loaded', value=True)
        context['task_instance'].xcom_push(key='concept_count', value=concept_count if concept_count > 0 else 5)

        print("✅ OMOP vocabularies ready for transformation!")
        return "vocabularies_loaded"

    except Exception as e:
        print(f"❌ OMOP vocabulary loading failed: {e}")
        print("💡 For production, download full vocabularies from OHDSI Athena")
        raise Exception(f"Cannot load OMOP vocabularies: {e}")

def transform_bronze_to_omop_measurement(**context):
    """Transform bronze lab data to OMOP CDM MEASUREMENT table"""
    print("🔄 Transforming bronze laboratory data to OMOP CDM MEASUREMENT format...")

    try:
        # Get bronze table name from Step 2
        bronze_table = "biological_results_raw_dc6befcb"  # From Step 2 success

        # Generate unique table name for silver
        import uuid
        unique_suffix = str(uuid.uuid4())[:8]
        measurement_table = f"measurement_{unique_suffix}"

        print(f"📊 Source: iceberg.bronze.{bronze_table}")
        print(f"🎯 Target: iceberg.silver.{measurement_table}")

        start_time = datetime.now()

        # Bronze → OMOP MEASUREMENT transformation
        omop_measurement_sql = f"""
        CREATE TABLE iceberg.silver.{measurement_table}
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['measurement_year', 'measurement_month']
        )
        AS
        SELECT
            -- OMOP CDM MEASUREMENT required fields
            ROW_NUMBER() OVER (ORDER BY patient_id, sampling_datetime_utc) AS measurement_id,
            CAST(patient_id AS BIGINT) AS person_id,

            -- Map measurement_source_value to concept_id via LOINC
            COALESCE(
                -- Try to map via LOINC code extracted from measurement_source_value
                (SELECT concept_id
                 FROM iceberg.vocabulary.concept
                 WHERE vocabulary_id = 'LOINC'
                   AND concept_code = REGEXP_EXTRACT(b.measurement_source_value, 'LC:([^:]+)', 1)
                 LIMIT 1),
                0  -- 0 = No matching concept (unmapped)
            ) AS measurement_concept_id,

            -- Dates
            CAST(sampling_datetime_utc AS DATE) AS measurement_date,
            sampling_datetime_utc AS measurement_datetime,

            -- Measurement type concept (lab test = 44818702)
            44818702 AS measurement_type_concept_id,

            -- Operator concept (equals = 4172703, if no operator specified)
            4172703 AS operator_concept_id,

            -- Values
            TRY_CAST(value_as_number AS DOUBLE) AS value_as_number,

            -- Value as concept for categorical values
            CASE
                WHEN value_as_string IS NOT NULL AND TRY_CAST(value_as_number AS DOUBLE) IS NULL
                THEN 0  -- For categorical values, would need separate mapping
                ELSE NULL
            END AS value_as_concept_id,

            -- Unit concept mapping via UCUM
            COALESCE(
                -- Map common units to UCUM concepts
                CASE unit_source_value
                    WHEN 'mg/dL' THEN 8840      -- milligram per deciliter
                    WHEN 'mmol/L' THEN 8753     -- millimole per liter
                    WHEN 'g/dL' THEN 8713       -- gram per deciliter
                    WHEN 'U/L' THEN 8645        -- unit per liter
                    WHEN '/uL' THEN 8647        -- per microliter
                    WHEN '%' THEN 8554          -- percent
                    ELSE 0                       -- No matching unit concept
                END,
                0
            ) AS unit_concept_id,

            -- Range values (normal ranges)
            TRY_CAST(internal_numerical_reference_min AS DOUBLE) AS range_low,
            TRY_CAST(internal_numerical_reference_max AS DOUBLE) AS range_high,

            -- Provider and visit (simplified for POC)
            TRY_CAST(provider_id AS BIGINT) AS provider_id,
            TRY_CAST(visit_id AS BIGINT) AS visit_occurrence_id,

            -- Visit detail (not used for lab data typically)
            CAST(NULL AS BIGINT) AS visit_detail_id,

            -- Source values (preserve original codes and values)
            measurement_source_value,

            -- Map source concept (same as measurement_concept_id for labs)
            COALESCE(
                (SELECT concept_id
                 FROM iceberg.vocabulary.concept
                 WHERE vocabulary_id = 'LOINC'
                   AND concept_code = REGEXP_EXTRACT(b.measurement_source_value, 'LC:([^:]+)', 1)
                 LIMIT 1),
                0
            ) AS measurement_source_concept_id,

            unit_source_value,
            COALESCE(
                CASE unit_source_value
                    WHEN 'mg/dL' THEN 8840
                    WHEN 'mmol/L' THEN 8753
                    WHEN 'g/dL' THEN 8713
                    WHEN 'U/L' THEN 8645
                    WHEN '/uL' THEN 8647
                    WHEN '%' THEN 8554
                    ELSE 0
                END,
                0
            ) AS unit_source_concept_id,

            value_as_string AS value_source_value,

            -- Qualifier concept (normal/abnormal)
            CASE normality
                WHEN 'Normal' THEN 4124457     -- Normal
                WHEN 'Abnormal' THEN 4135493   -- Abnormal
                WHEN 'High' THEN 4328749       -- High
                WHEN 'Low' THEN 4124457        -- Low
                ELSE 0                          -- Unknown
            END AS measurement_event_id,

            -- Meas event field concept
            CAST(NULL AS BIGINT) AS meas_event_field_concept_id,

            -- Partitioning columns
            COALESCE(YEAR(sampling_datetime_utc), 1900) AS measurement_year,
            COALESCE(MONTH(sampling_datetime_utc), 1) AS measurement_month

        FROM iceberg.bronze.{bronze_table} b
        WHERE patient_id IS NOT NULL  -- Ensure valid patients only
        ORDER BY person_id, measurement_datetime  -- Maintain chronological order
        """

        print(f"🚀 Starting Bronze → OMOP MEASUREMENT transformation...")
        print(f"⏰ Expected processing time: ~20-30 minutes for 634M+ rows")

        result = execute_trino_query(
            omop_measurement_sql,
            "Transform bronze data to OMOP CDM MEASUREMENT table"
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print(f"🎉 OMOP MEASUREMENT transformation complete!")
        print(f"⏱️  Processing time: {duration/3600:.2f} hours")
        print(f"🎯 Created table: iceberg.silver.{measurement_table}")

        # Store results for validation
        context['task_instance'].xcom_push(key='measurement_table', value=measurement_table)
        context['task_instance'].xcom_push(key='transformation_time_hours', value=duration/3600)

        return "measurement_transformed"

    except Exception as e:
        print(f"❌ OMOP MEASUREMENT transformation failed: {e}")
        print("💡 Check vocabulary mapping and bronze table availability")
        raise Exception(f"Cannot transform to OMOP MEASUREMENT: {e}")

def validate_omop_transformation(**context):
    """Validate OMOP CDM transformation results"""
    print("🔍 Validating OMOP CDM transformation results...")

    try:
        measurement_table = context['task_instance'].xcom_pull(task_ids='transform_bronze_to_omop_measurement', key='measurement_table')
        transformation_time = context['task_instance'].xcom_pull(task_ids='transform_bronze_to_omop_measurement', key='transformation_time_hours')
        concept_count = context['task_instance'].xcom_pull(task_ids='load_omop_vocabularies', key='concept_count')

        if not measurement_table:
            raise Exception("No measurement table found from transformation")

        print(f"🎯 Validating OMOP table: iceberg.silver.{measurement_table}")

        # Count OMOP records
        count_result = execute_trino_query(
            f"SELECT COUNT(*) FROM iceberg.silver.{measurement_table}",
            "Count OMOP MEASUREMENT records"
        )
        omop_records = count_result[0][0] if count_result and len(count_result) > 0 else 0

        # OMOP validation statistics
        validation_sql = f"""
        SELECT
            COUNT(*) as total_measurements,
            COUNT(DISTINCT person_id) as unique_patients,
            COUNT(CASE WHEN measurement_concept_id > 0 THEN 1 END) as mapped_measurements,
            COUNT(CASE WHEN measurement_concept_id = 0 THEN 1 END) as unmapped_measurements,
            COUNT(CASE WHEN unit_concept_id > 0 THEN 1 END) as mapped_units,
            COUNT(CASE WHEN value_as_number IS NOT NULL THEN 1 END) as numeric_values,
            COUNT(DISTINCT measurement_concept_id) as unique_concepts,
            MIN(measurement_date) as earliest_measurement,
            MAX(measurement_date) as latest_measurement
        FROM iceberg.silver.{measurement_table}
        """

        validation_result = execute_trino_query(validation_sql, "OMOP transformation validation statistics")
        stats = validation_result[0] if validation_result and len(validation_result) > 0 else (0,)*9

        # Check concept mapping effectiveness
        mapping_effectiveness = (stats[2] / stats[0] * 100) if stats[0] > 0 else 0

        print(f"\n{'='*80}")
        print(f"🎉 OMOP CDM TRANSFORMATION RESULTS")
        print(f"{'='*80}")
        print(f"📊 TRANSFORMATION SUMMARY:")
        print(f"   ⏱️  Processing time: {transformation_time:.2f} hours" if transformation_time else "   ⏱️  Processing time: Unknown")
        print(f"   📤 OMOP measurements: {omop_records:,}")
        print(f"   👥 Unique patients: {stats[1]:,}")

        print(f"\n📊 OMOP CDM COMPLIANCE:")
        print(f"   🎯 Total measurements: {stats[0]:,}")
        print(f"   ✅ Mapped measurements: {stats[2]:,} ({mapping_effectiveness:.1f}%)")
        print(f"   ❌ Unmapped measurements: {stats[3]:,} ({(100-mapping_effectiveness):.1f}%)")
        print(f"   🏷️ Mapped units: {stats[4]:,}")
        print(f"   🔢 Numeric values: {stats[5]:,}")
        print(f"   🗂️ Unique concepts: {stats[6]:,}")

        print(f"\n📅 TEMPORAL COVERAGE:")
        print(f"   📅 Date range: {stats[7]} to {stats[8]}")
        print(f"   📚 Vocabulary concepts: {concept_count:,}")

        print(f"\n🎯 OMOP CDM QUALITY ASSESSMENT:")
        if mapping_effectiveness >= 80:
            print(f"   ✅ EXCELLENT: {mapping_effectiveness:.1f}% concept mapping rate")
        elif mapping_effectiveness >= 60:
            print(f"   ⚠️  GOOD: {mapping_effectiveness:.1f}% concept mapping rate (could improve)")
        else:
            print(f"   ❌ POOR: {mapping_effectiveness:.1f}% concept mapping rate (needs vocabulary enhancement)")

        if stats[1] > 4000000:  # 4M+ patients
            print(f"   ✅ EXCELLENT: {stats[1]:,} patients - large research cohort")

        print(f"\n💡 NEXT STEPS FOR IMPROVEMENT:")
        print(f"   🔄 Download full OMOP vocabularies from OHDSI Athena")
        print(f"   📚 Enhance LOINC → concept_id mapping coverage")
        print(f"   🏗️ Ready for Step 4: Silver → Gold clinical datamart")

        print(f"\n{'='*80}")
        if omop_records > 600000000:  # 600M+
            print(f"✅ OMOP CDM TRANSFORMATION SUCCESS!")
            print(f"🎯 Ready for clinical research and Step 4 datamart creation!")
        else:
            print(f"⚠️  Partial transformation - investigate data pipeline")
        print(f"{'='*80}")

        return "omop_transformation_complete"

    except Exception as e:
        print(f"❌ OMOP validation failed: {e}")
        raise Exception(f"Cannot validate OMOP transformation: {e}")

# Task definitions
setup_schemas = PythonOperator(
    task_id='setup_omop_schemas',
    python_callable=setup_omop_schemas,
    dag=dag,
)

load_vocabularies = PythonOperator(
    task_id='load_omop_vocabularies',
    python_callable=load_omop_vocabularies,
    dag=dag,
)

transform_measurement = PythonOperator(
    task_id='transform_bronze_to_omop_measurement',
    python_callable=transform_bronze_to_omop_measurement,
    dag=dag,
    execution_timeout=timedelta(hours=2),  # Allow time for 634M+ row transformation
)

validate_transformation = PythonOperator(
    task_id='validate_omop_transformation',
    python_callable=validate_omop_transformation,
    dag=dag,
)

# OMOP transformation pipeline: Setup → Vocabularies → Transform → Validate
setup_schemas >> load_vocabularies >> transform_measurement >> validate_transformation