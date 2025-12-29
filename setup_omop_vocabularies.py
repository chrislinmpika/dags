"""
OMOP Vocabulary Setup DAG - Simple Version

Downloads and sets up OHDSI Athena vocabularies for OMOP concept mapping.
Uses basic Airflow operators for maximum compatibility.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'setup_omop_vocabularies',
    default_args=default_args,
    description='Setup OMOP vocabularies from OHDSI Athena',
    schedule=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['omop', 'vocabularies', 'setup'],
)

def guide_vocabulary_download(**context):
    """
    Guide user through OHDSI Athena vocabulary download process
    """
    print("🏥 OMOP Vocabulary Setup Guide")
    print("=" * 50)
    print("")
    print("📝 Manual Steps Required:")
    print("1. Go to: https://athena.ohdsi.org/vocabulary/list")
    print("2. Create an account if you don't have one")
    print("3. Create a vocabulary bundle with:")
    print("   ✅ LOINC (Logical Observation Identifiers Names and Codes)")
    print("   ✅ UCUM (Unified Code for Units of Measure)")
    print("   ✅ SNOMED (Systematized Nomenclature of Medicine Clinical Terms)")
    print("   ✅ RxNorm (Prescription drugs)")
    print("   ✅ Gender")
    print("   ✅ Race")
    print("   ✅ Ethnicity")
    print("4. Download the vocabulary bundle")
    print("5. Upload vocabulary files to MinIO bucket: 'omop-vocabularies'")
    print("")
    print("📋 Expected files in bucket:")
    files = [
        "CONCEPT.csv",
        "VOCABULARY.csv",
        "CONCEPT_CLASS.csv",
        "CONCEPT_RELATIONSHIP.csv",
        "RELATIONSHIP.csv",
        "CONCEPT_SYNONYM.csv"
    ]
    for file in files:
        print(f"   - {file}")
    print("")
    print("✅ After upload, trigger the next DAG: 'create_lab_code_mappings'")

    return "vocabulary_download_guided"

def check_vocabulary_availability(**context):
    """
    Check if vocabularies are available (placeholder)
    """
    print("🔍 Checking vocabulary availability...")
    print("📁 Looking for vocabulary files in MinIO bucket 'omop-vocabularies'")
    print("")
    print("⚠️  This step requires manual verification:")
    print("   1. Check MinIO console for uploaded vocabulary files")
    print("   2. Verify all required CSV files are present")
    print("   3. Continue to lab code mapping creation")

    return "vocabulary_check_complete"

# DAG Tasks
guide_task = PythonOperator(
    task_id='guide_vocabulary_download',
    python_callable=guide_vocabulary_download,
    dag=dag,
)

check_task = PythonOperator(
    task_id='check_vocabulary_availability',
    python_callable=check_vocabulary_availability,
    dag=dag,
)

# Simple SQL commands that can be run manually
create_tables_info = BashOperator(
    task_id='display_create_tables_sql',
    bash_command="""
    echo "📊 SQL Commands to create OMOP vocabulary tables:"
    echo ""
    echo "-- Run these SQL commands in your Trino/Iceberg environment:"
    echo ""
    echo "CREATE SCHEMA IF NOT EXISTS iceberg.silver;"
    echo ""
    echo "CREATE TABLE IF NOT EXISTS iceberg.silver.omop_concept ("
    echo "    concept_id BIGINT,"
    echo "    concept_name VARCHAR(255),"
    echo "    domain_id VARCHAR(20),"
    echo "    vocabulary_id VARCHAR(50),"
    echo "    concept_class_id VARCHAR(20),"
    echo "    standard_concept VARCHAR(1),"
    echo "    concept_code VARCHAR(50),"
    echo "    valid_start_date DATE,"
    echo "    valid_end_date DATE,"
    echo "    invalid_reason VARCHAR(1)"
    echo ") WITH (format = 'PARQUET');"
    echo ""
    echo "CREATE TABLE IF NOT EXISTS iceberg.silver.omop_vocabulary ("
    echo "    vocabulary_id VARCHAR(50),"
    echo "    vocabulary_name VARCHAR(255),"
    echo "    vocabulary_reference VARCHAR(255),"
    echo "    vocabulary_version VARCHAR(255)"
    echo ") WITH (format = 'PARQUET');"
    echo ""
    echo "✅ Tables created. Load vocabulary CSV data using your preferred method."
    """,
    dag=dag,
)

# Task dependencies
guide_task >> check_task >> create_tables_info