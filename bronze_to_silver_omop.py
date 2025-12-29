"""
Bronze to Silver OMOP DAG - Production Ready

Transforms laboratory data from bronze to OMOP CDM v6.0 silver tables.
Uses Python Trino connector for reliable execution in Airflow containers.

Schedule: Weekly on Sunday at 2 AM
Strategy: Complete rebuild for data consistency
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'email_on_failure': True,
    'execution_timeout': timedelta(hours=4),
}

dag = DAG(
    'bronze_to_silver_omop',
    default_args=default_args,
    description='Transform bronze laboratory data to OMOP CDM v6.0 silver tables',
    schedule='0 2 * * 0',  # Weekly Sunday at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['omop', 'bronze-to-silver', 'production'],
)

def execute_trino_sql(sql_query, description="SQL execution"):
    """Execute SQL query using Trino Python connector"""
    try:
        import trino

        # Connect using same settings as in Dockerfile
        conn = trino.dbapi.connect(
            host='my-trino-trino.ns-data-platform.svc.cluster.local',
            port=8080,
            user='airflow',
            catalog='iceberg',
            schema='silver'
        )

        cursor = conn.cursor()
        print(f"🚀 {description}")

        # Execute the query
        cursor.execute(sql_query)

        # If it's a SELECT, show results
        if sql_query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            for row in results:
                print(f"📊 {row}")

        cursor.close()
        conn.close()
        print(f"✅ {description} completed successfully")

    except Exception as e:
        print(f"❌ Error in {description}: {str(e)}")
        raise

def start_pipeline(**context):
    """Initialize pipeline run"""
    print("🚀 Starting Bronze to Silver OMOP Transformation")
    print(f"📅 Execution Date: {context.get('execution_date', 'unknown')}")

    # Generate processing batch ID for audit trail
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_id = f"omop_bronze_to_silver_{timestamp}"

    context['task_instance'].xcom_push(key='batch_id', value=batch_id)
    print(f"🏷️  Batch ID: {batch_id}")

    return batch_id

def validate_bronze_data(**context):
    """Validate that bronze data exists and is accessible"""
    sql = """
    SELECT
        COUNT(*) as total_records,
        COUNT(DISTINCT person_id_hash) as unique_patients,
        MIN(measurement_date) as earliest_date,
        MAX(measurement_date) as latest_date
    FROM iceberg.bronze.biological_results_enhanced
    """

    execute_trino_sql(sql, "Bronze data validation")
    return "validated"

def create_omop_person(**context):
    """Create OMOP Person table with GDPR-compliant anonymization"""
    sql = """
    CREATE OR REPLACE TABLE iceberg.silver.omop_person AS
    SELECT
        -- Generate deterministic person_id from hash
        abs(hash(person_id_hash)) % 2147483647 AS person_id,

        -- Gender assignment (randomized for privacy)
        CASE (abs(hash(person_id_hash)) % 3)
            WHEN 0 THEN 8507  -- Male
            WHEN 1 THEN 8532  -- Female
            ELSE 8551         -- Unknown
        END AS gender_concept_id,

        -- Birth year estimation (privacy-preserving)
        CASE
            WHEN first_measurement_date >= DATE '2000-01-01'
            THEN year(first_measurement_date) - (abs(hash(person_id_hash)) % 80)
            ELSE 1950
        END AS year_of_birth,

        0 AS month_of_birth,  -- Privacy protection
        0 AS day_of_birth,    -- Privacy protection
        CAST(NULL AS TIMESTAMP) AS birth_datetime,

        -- Standard OMOP values for privacy
        8527 AS race_concept_id,      -- 'Other race'
        38003564 AS ethnicity_concept_id,  -- 'Not Hispanic or Latino'

        -- Nullable fields
        CAST(NULL AS INTEGER) AS location_id,
        CAST(NULL AS INTEGER) AS provider_id,
        CAST(NULL AS INTEGER) AS care_site_id,

        -- Anonymized source value
        substr(to_hex(sha256(to_utf8(person_id_hash))), 1, 16) AS person_source_value,

        -- Source concept values
        'Unknown' AS gender_source_value,
        0 AS gender_source_concept_id,
        'Other' AS race_source_value,
        0 AS race_source_concept_id,
        'Not Hispanic or Latino' AS ethnicity_source_value,
        0 AS ethnicity_source_concept_id

    FROM (
        SELECT
            person_id_hash,
            MIN(measurement_date) AS first_measurement_date,
            COUNT(*) AS total_measurements
        FROM iceberg.bronze.biological_results_enhanced
        WHERE person_id_hash IS NOT NULL
        GROUP BY person_id_hash
    )
    """

    execute_trino_sql(sql, "OMOP Person table creation")

def create_omop_visit_occurrence(**context):
    """Create OMOP Visit Occurrence table"""
    sql = """
    CREATE OR REPLACE TABLE iceberg.silver.omop_visit_occurrence AS
    SELECT
        visit_id AS visit_occurrence_id,
        abs(hash(person_id_hash)) % 2147483647 AS person_id,

        -- Visit type concepts
        9202 AS visit_concept_id,  -- Outpatient Visit
        32817 AS visit_type_concept_id,  -- EHR

        -- Visit dates
        measurement_date AS visit_start_date,
        measurement_date AS visit_end_date,
        CAST(measurement_datetime AS TIMESTAMP) AS visit_start_datetime,
        CAST(measurement_datetime AS TIMESTAMP) AS visit_end_datetime,

        -- Nullable provider/care site
        CAST(NULL AS INTEGER) AS provider_id,
        CAST(NULL AS INTEGER) AS care_site_id,

        -- Source values
        CAST(visit_id AS VARCHAR) AS visit_source_value,
        0 AS visit_source_concept_id,

        -- Admission/discharge (not applicable for outpatient)
        0 AS admitted_from_concept_id,
        CAST(NULL AS VARCHAR) AS admitted_from_source_value,
        0 AS discharged_to_concept_id,
        CAST(NULL AS VARCHAR) AS discharged_to_source_value,
        CAST(NULL AS INTEGER) AS preceding_visit_occurrence_id

    FROM (
        SELECT DISTINCT
            visit_id,
            person_id_hash,
            measurement_date,
            measurement_datetime
        FROM iceberg.bronze.biological_results_enhanced
        WHERE visit_id IS NOT NULL
          AND person_id_hash IS NOT NULL
          AND measurement_date IS NOT NULL
    )
    """

    execute_trino_sql(sql, "OMOP Visit Occurrence table creation")

def create_omop_measurement(**context):
    """Create OMOP Measurement table with concept mapping"""
    sql = """
    CREATE OR REPLACE TABLE iceberg.silver.omop_measurement AS
    SELECT
        measurement_id,
        abs(hash(person_id_hash)) % 2147483647 AS person_id,

        -- Laboratory concept mapping (LC codes to LOINC)
        CASE
            WHEN measurement_source_value LIKE '%hemoglobin%' OR measurement_source_value LIKE '%LC:0007%' THEN 4182210  -- Hemoglobin
            WHEN measurement_source_value LIKE '%glucose%' OR measurement_source_value LIKE '%LC:0001%' THEN 4263235     -- Glucose
            WHEN measurement_source_value LIKE '%white blood%' OR measurement_source_value LIKE '%LC:0010%' THEN 4143345 -- WBC
            WHEN measurement_source_value LIKE '%creatinine%' OR measurement_source_value LIKE '%LC:0012%' THEN 4023381  -- Creatinine
            WHEN measurement_source_value LIKE '%cholesterol%' OR measurement_source_value LIKE '%LC:0015%' THEN 4267147 -- Cholesterol
            WHEN measurement_source_value LIKE '%albumin%' OR measurement_source_value LIKE '%LC:0020%' THEN 4124362     -- Albumin
            ELSE 4124662  -- Generic laboratory test
        END AS measurement_concept_id,

        -- Dates and times
        measurement_date,
        CAST(measurement_datetime AS TIMESTAMP) AS measurement_datetime,

        -- Standard measurement type
        44818701 AS measurement_type_concept_id,  -- Lab test

        -- Operator concept (equals for numeric values)
        CASE
            WHEN value_as_number IS NOT NULL THEN 4172703  -- '=' operator
            ELSE CAST(NULL AS INTEGER)
        END AS operator_concept_id,

        -- Values
        value_as_number,
        CAST(NULL AS INTEGER) AS value_as_concept_id,  -- Would need concept mapping for categorical

        -- Unit concepts (common laboratory units)
        CASE
            WHEN unit_source_value = '%' THEN 8554
            WHEN unit_source_value = 'U/L' THEN 8645
            WHEN unit_source_value = 'nmol/L' THEN 8723
            WHEN unit_source_value = 'cells/µL' THEN 8961
            WHEN unit_source_value = 'mEq/L' THEN 8753
            WHEN unit_source_value = 'mg/dL' THEN 8840
            WHEN unit_source_value = 'g/dL' THEN 8713
            ELSE 0  -- Unknown unit
        END AS unit_concept_id,

        -- Range values (not available in source)
        CAST(NULL AS DOUBLE) AS range_low,
        CAST(NULL AS DOUBLE) AS range_high,

        -- Provider and visit
        CAST(NULL AS INTEGER) AS provider_id,
        visit_id AS visit_occurrence_id,
        CAST(NULL AS INTEGER) AS visit_detail_id,

        -- Source values
        measurement_source_value,
        0 AS measurement_source_concept_id,
        unit_source_value,
        value_source_value,

        -- Event tracking (not used)
        0 AS measurement_event_id,
        0 AS meas_event_field_concept_id

    FROM iceberg.bronze.biological_results_enhanced
    WHERE measurement_id IS NOT NULL
      AND person_id_hash IS NOT NULL
      AND measurement_date IS NOT NULL
    """

    execute_trino_sql(sql, "OMOP Measurement table creation")

def create_omop_observation(**context):
    """Create OMOP Observation table for qualitative results"""
    sql = """
    CREATE OR REPLACE TABLE iceberg.silver.omop_observation AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY measurement_id) AS observation_id,
        abs(hash(person_id_hash)) % 2147483647 AS person_id,

        -- Observation concepts
        4124662 AS observation_concept_id,  -- Laboratory test observation
        44818701 AS observation_type_concept_id,  -- Lab test

        -- Dates
        measurement_date AS observation_date,
        CAST(measurement_datetime AS TIMESTAMP) AS observation_datetime,

        -- Values
        value_as_string AS value_as_string,
        CAST(NULL AS INTEGER) AS value_as_concept_id,
        CAST(NULL AS DOUBLE) AS value_as_number,

        -- Qualifier concepts (normal/abnormal flags)
        CASE
            WHEN abnormal_flag = 'ABNORMAL' THEN 4135493  -- Abnormal
            WHEN abnormal_flag = 'NORMAL' THEN 4069590    -- Normal
            ELSE CAST(NULL AS INTEGER)
        END AS qualifier_concept_id,

        -- Units and provider
        CAST(NULL AS INTEGER) AS unit_concept_id,
        CAST(NULL AS INTEGER) AS provider_id,

        -- Visit information
        visit_id AS visit_occurrence_id,
        CAST(NULL AS INTEGER) AS visit_detail_id,

        -- Source values
        measurement_source_value AS observation_source_value,
        0 AS observation_source_concept_id,
        unit_source_value,
        value_qualifier AS qualifier_source_value,

        -- Event tracking
        0 AS observation_event_id,
        0 AS obs_event_field_concept_id,
        CAST(NULL AS TIMESTAMP) AS value_as_datetime

    FROM iceberg.bronze.biological_results_enhanced
    WHERE value_as_string IS NOT NULL
      AND value_as_string != ''
      AND person_id_hash IS NOT NULL
      AND measurement_date IS NOT NULL
    """

    execute_trino_sql(sql, "OMOP Observation table creation")

def validate_silver_tables(**context):
    """Validate all created OMOP silver tables"""
    tables = [
        ('omop_person', 'Person'),
        ('omop_visit_occurrence', 'Visit Occurrence'),
        ('omop_measurement', 'Measurement'),
        ('omop_observation', 'Observation')
    ]

    for table_name, display_name in tables:
        sql = f"SELECT COUNT(*) as record_count FROM iceberg.silver.{table_name}"
        execute_trino_sql(sql, f"{display_name} table validation")

    # Data quality check
    sql = """
    SELECT
        'Quality Check' as check_type,
        COUNT(DISTINCT p.person_id) as unique_persons,
        COUNT(DISTINCT v.visit_occurrence_id) as unique_visits,
        COUNT(m.measurement_id) as total_measurements,
        COUNT(o.observation_id) as total_observations
    FROM iceberg.silver.omop_person p
    LEFT JOIN iceberg.silver.omop_visit_occurrence v ON p.person_id = v.person_id
    LEFT JOIN iceberg.silver.omop_measurement m ON p.person_id = m.person_id
    LEFT JOIN iceberg.silver.omop_observation o ON p.person_id = o.person_id
    """

    execute_trino_sql(sql, "OMOP data quality validation")

    print("🎉 Bronze to Silver OMOP transformation completed successfully!")
    return "validated"

# Define tasks
start_task = PythonOperator(
    task_id='start_pipeline',
    python_callable=start_pipeline,
    dag=dag,
)

validate_bronze_task = PythonOperator(
    task_id='validate_bronze_data',
    python_callable=validate_bronze_data,
    dag=dag,
)

create_person_task = PythonOperator(
    task_id='create_omop_person',
    python_callable=create_omop_person,
    dag=dag,
)

create_visits_task = PythonOperator(
    task_id='create_omop_visit_occurrence',
    python_callable=create_omop_visit_occurrence,
    dag=dag,
)

create_measurements_task = PythonOperator(
    task_id='create_omop_measurement',
    python_callable=create_omop_measurement,
    dag=dag,
)

create_observations_task = PythonOperator(
    task_id='create_omop_observation',
    python_callable=create_omop_observation,
    dag=dag,
)

validate_silver_task = PythonOperator(
    task_id='validate_silver_tables',
    python_callable=validate_silver_tables,
    dag=dag,
)

# Task dependencies - Parallel where possible for performance
start_task >> validate_bronze_task >> [create_person_task, create_visits_task]
create_person_task >> create_measurements_task
create_visits_task >> create_measurements_task
create_measurements_task >> create_observations_task >> validate_silver_task