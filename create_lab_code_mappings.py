"""
Laboratory Code Mapping DAG
Creates mapping tables for LC:xxxx codes to LOINC concepts for OMOP compliance.

This DAG analyzes the source data and creates mappings from local laboratory codes
to standardized LOINC concepts for proper OMOP CDM compliance.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.trino.operators.trino import TrinoOperator
import pandas as pd
from minio import Minio
from trino.dbapi import connect
import hashlib
import re

# Configuration
MINIO_ENDPOINT = "minio-api.ns-data-platform.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
TRINO_HOST = "my-trino-trino.ns-data-platform.svc.cluster.local"
TRINO_PORT = 8080
TRINO_USER = "trino"
BRONZE_BUCKET = "bronze"

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

dag = DAG(
    'create_lab_code_mappings',
    default_args=default_args,
    description='Create laboratory code to LOINC mappings for OMOP compliance',
    schedule=None,  # Manual trigger after vocabulary setup
    catchup=False,
    tags=['omop', 'mapping', 'laboratory'],
)

def analyze_laboratory_codes(**context):
    """
    Analyze bronze data to identify unique laboratory codes and their usage patterns.
    Creates a comprehensive inventory for manual mapping curation.
    """
    print("🔍 Analyzing laboratory codes from bronze data...")

    minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    # Get list of CSV files
    objects = minio_client.list_objects(BRONZE_BUCKET, prefix="biological_results_", recursive=True)
    csv_files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]

    print(f"📁 Found {len(csv_files)} CSV files to analyze")

    lab_code_stats = {}
    value_type_stats = {}
    unit_stats = {}

    # Analyze first 10 files for code patterns (representative sample)
    sample_files = csv_files[:10]

    for file_name in sample_files:
        try:
            print(f"📊 Analyzing file: {file_name}")

            obj = minio_client.get_object(BRONZE_BUCKET, file_name)

            # Read file in chunks to avoid memory issues
            chunk_count = 0
            for chunk_df in pd.read_csv(obj, chunksize=5000):
                chunk_count += 1

                # Analyze laboratory codes
                for _, row in chunk_df.iterrows():
                    lab_code = row.get('internal_test_id', '').strip()
                    value_type = row.get('value_type', '').strip()
                    unit = row.get('internal_numerical_unit', '').strip()
                    normality = row.get('normality', '').strip()

                    if lab_code:
                        if lab_code not in lab_code_stats:
                            lab_code_stats[lab_code] = {
                                'count': 0,
                                'value_types': set(),
                                'units': set(),
                                'normality_flags': set(),
                                'sample_values': []
                            }

                        lab_code_stats[lab_code]['count'] += 1
                        lab_code_stats[lab_code]['value_types'].add(value_type)
                        lab_code_stats[lab_code]['units'].add(unit)
                        lab_code_stats[lab_code]['normality_flags'].add(normality)

                        # Store sample values for mapping hints
                        if len(lab_code_stats[lab_code]['sample_values']) < 5:
                            if value_type == 'NUMERIC':
                                sample_val = row.get('internal_numerical_value', '')
                            elif value_type == 'CATEGORICAL':
                                sample_val = row.get('internal_categorical_qualification', '')
                            else:
                                sample_val = ''

                            if sample_val:
                                lab_code_stats[lab_code]['sample_values'].append(str(sample_val))

                # Count value types and units globally
                value_type_stats[value_type] = value_type_stats.get(value_type, 0) + len(chunk_df)
                for unit in chunk_df['internal_numerical_unit'].dropna().unique():
                    unit_stats[unit] = unit_stats.get(unit, 0) + 1

                if chunk_count >= 5:  # Limit chunks per file for analysis
                    break

        except Exception as e:
            print(f"⚠️ Error analyzing file {file_name}: {e}")
            continue

    # Convert sets to lists for JSON serialization
    for code, stats in lab_code_stats.items():
        stats['value_types'] = list(stats['value_types'])
        stats['units'] = list(stats['units'])
        stats['normality_flags'] = list(stats['normality_flags'])

    # Sort by frequency for prioritization
    sorted_codes = dict(sorted(lab_code_stats.items(), key=lambda x: x[1]['count'], reverse=True))

    print(f"📈 Analysis Results:")
    print(f"  - Unique lab codes: {len(lab_code_stats)}")
    print(f"  - Value types: {list(value_type_stats.keys())}")
    print(f"  - Common units: {list(sorted(unit_stats.keys(), key=unit_stats.get, reverse=True)[:10])}")

    # Store results for next task
    context['task_instance'].xcom_push(key='lab_code_analysis', value=sorted_codes)
    context['task_instance'].xcom_push(key='value_type_stats', value=value_type_stats)
    context['task_instance'].xcom_push(key='unit_stats', value=unit_stats)

    return len(lab_code_stats)

def create_initial_mappings(**context):
    """
    Create initial laboratory code mappings based on pattern matching and common LOINC codes.
    This provides a starting point for manual curation.
    """
    print("🗺️ Creating initial laboratory code mappings...")

    # Get analysis results
    lab_code_analysis = context['task_instance'].xcom_pull(key='lab_code_analysis', task_ids='analyze_laboratory_codes')

    if not lab_code_analysis:
        raise Exception("❌ No lab code analysis found. Run analysis task first.")

    # Connect to vocabularies to find matching LOINC codes
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="iceberg", schema="vocabularies")
    cursor = conn.cursor()

    # Get common laboratory LOINC concepts
    cursor.execute("""
        SELECT concept_id, concept_name, concept_code, standard_concept
        FROM concept
        WHERE vocabulary_id = 'LOINC'
          AND domain_id = 'Measurement'
          AND standard_concept = 'S'
          AND (
              LOWER(concept_name) LIKE '%blood%'
              OR LOWER(concept_name) LIKE '%serum%'
              OR LOWER(concept_name) LIKE '%plasma%'
              OR LOWER(concept_name) LIKE '%urine%'
              OR LOWER(concept_name) LIKE '%hemoglobin%'
              OR LOWER(concept_name) LIKE '%glucose%'
              OR LOWER(concept_name) LIKE '%cholesterol%'
              OR LOWER(concept_name) LIKE '%white blood%'
              OR LOWER(concept_name) LIKE '%platelet%'
          )
        ORDER BY concept_name
        LIMIT 1000
    """)

    loinc_concepts = cursor.fetchall()
    print(f"📋 Found {len(loinc_concepts)} potential LOINC concepts")

    # Create mapping records
    mapping_records = []

    # Common laboratory test patterns (rule-based initial mapping)
    lab_patterns = {
        'LC:0007': {'likely_test': 'hemoglobin', 'default_concept': 4182210},  # Hemoglobin
        'LC:0010': {'likely_test': 'white blood cells', 'default_concept': 4143345},  # WBC count
        'LC:0001': {'likely_test': 'glucose', 'default_concept': 4263235},  # Glucose
        'LC:0012': {'likely_test': 'sodium', 'default_concept': 4264334},  # Sodium
        'LC:0005': {'likely_test': 'alanine aminotransferase', 'default_concept': 4144235},  # ALT
        'LC:2077': {'likely_test': 'platelets', 'default_concept': 4267147},  # Platelet count
        'LC:0241': {'likely_test': 'bacteria', 'default_concept': 4124662},  # Generic lab test
        'LC:0004': {'likely_test': 'protein', 'default_concept': 4124662},  # Generic lab test
        'LC:1806': {'likely_test': 'enzyme', 'default_concept': 4124662},  # Generic lab test
        'LC:0003': {'likely_test': 'microscopy', 'default_concept': 4124662},  # Generic lab test
        'LC:2302': {'likely_test': 'enzyme', 'default_concept': 4124662},  # Generic lab test
        'LC:0415': {'likely_test': 'viral load', 'default_concept': 4124662},  # Generic lab test
        'LC:0032': {'likely_test': 'enzyme', 'default_concept': 4124662},  # Generic lab test
    }

    for lab_code, stats in lab_code_analysis.items():
        # Get pattern match if available
        pattern_match = lab_patterns.get(lab_code, {})

        # Default to generic laboratory test concept
        target_concept_id = pattern_match.get('default_concept', 4124662)  # LOINC generic lab test

        # Determine mapping confidence based on pattern match
        if lab_code in lab_patterns:
            confidence = 0.8  # High confidence for known patterns
            mapping_status = 'PATTERN_MATCH'
        else:
            confidence = 0.3  # Low confidence for unknown codes
            mapping_status = 'UNMAPPED'

        # Create mapping record
        mapping_record = {
            'source_code': lab_code,
            'source_vocabulary_id': 'Local',
            'target_concept_id': target_concept_id,
            'target_vocabulary_id': 'LOINC',
            'mapping_confidence': confidence,
            'mapping_status': mapping_status,
            'usage_count': stats['count'],
            'value_types': ','.join(stats['value_types']),
            'units': ','.join(stats['units']),
            'sample_values': ','.join(stats['sample_values'][:3]),
            'manual_review_required': 'YES' if confidence < 0.7 else 'NO'
        }

        mapping_records.append(mapping_record)

    conn.close()

    # Store mapping records for loading
    context['task_instance'].xcom_push(key='mapping_records', value=mapping_records)

    print(f"✅ Created {len(mapping_records)} initial mapping records")
    print(f"📊 Mapping quality: {len([r for r in mapping_records if r['mapping_confidence'] >= 0.7])} high-confidence, {len([r for r in mapping_records if r['mapping_confidence'] < 0.7])} need review")

    return len(mapping_records)

def load_mappings_to_iceberg(**context):
    """Load laboratory code mappings into Iceberg table for use by the pipeline."""

    print("💾 Loading mappings into Iceberg table...")

    # Get mapping records
    mapping_records = context['task_instance'].xcom_pull(key='mapping_records', task_ids='create_initial_mappings')

    if not mapping_records:
        raise Exception("❌ No mapping records found.")

    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="iceberg", schema="silver")
    cursor = conn.cursor()

    try:
        # Clear existing mappings for fresh start
        cursor.execute("DELETE FROM concept_map_laboratory")

        # Insert new mappings
        insert_sql = """
            INSERT INTO concept_map_laboratory (
                source_code, source_vocabulary_id, target_concept_id, target_vocabulary_id,
                mapping_confidence, mapping_status, usage_count, value_types, units,
                sample_values, manual_review_required, load_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """

        for record in mapping_records:
            cursor.execute(insert_sql, (
                record['source_code'],
                record['source_vocabulary_id'],
                record['target_concept_id'],
                record['target_vocabulary_id'],
                record['mapping_confidence'],
                record['mapping_status'],
                record['usage_count'],
                record['value_types'],
                record['units'],
                record['sample_values'],
                record['manual_review_required']
            ))

        print(f"✅ Loaded {len(mapping_records)} mapping records")

        # Generate mapping summary
        cursor.execute("""
            SELECT
                mapping_status,
                COUNT(*) as count,
                AVG(mapping_confidence) as avg_confidence,
                SUM(usage_count) as total_usage
            FROM concept_map_laboratory
            GROUP BY mapping_status
            ORDER BY count DESC
        """)

        summary = cursor.fetchall()
        print("📊 Mapping Summary:")
        for row in summary:
            print(f"  {row[0]}: {row[1]} codes ({row[2]:.2f} avg confidence, {row[3]} total usage)")

    except Exception as e:
        print(f"❌ Error loading mappings: {e}")
        raise
    finally:
        conn.close()

# Task definitions
create_mapping_schema = TrinoOperator(
    task_id='create_mapping_schema',
    conn_id='trino_default',
    sql="""
        CREATE SCHEMA IF NOT EXISTS iceberg.silver
        WITH (location = 's3://eds-lakehouse/silver/')
    """,
    dag=dag,
)

create_mapping_table = TrinoOperator(
    task_id='create_mapping_table',
    conn_id='trino_default',
    sql="""
        CREATE TABLE IF NOT EXISTS iceberg.silver.concept_map_laboratory (
            source_code VARCHAR,
            source_vocabulary_id VARCHAR,
            target_concept_id INTEGER,
            target_vocabulary_id VARCHAR,
            mapping_confidence DOUBLE,
            mapping_status VARCHAR,
            usage_count BIGINT,
            value_types VARCHAR,
            units VARCHAR,
            sample_values VARCHAR,
            manual_review_required VARCHAR,
            load_timestamp TIMESTAMP(3) WITH TIME ZONE,
            valid_start_date DATE DEFAULT DATE '2025-01-01',
            valid_end_date DATE DEFAULT DATE '2099-12-31'
        ) WITH (
            format = 'PARQUET',
            partitioning = ARRAY['mapping_status'],
            sorted_by = ARRAY['usage_count DESC', 'source_code'],
            location = 's3://eds-lakehouse/silver/concept_map_laboratory/'
        )
    """,
    dag=dag,
)

analyze_lab_codes = PythonOperator(
    task_id='analyze_laboratory_codes',
    python_callable=analyze_laboratory_codes,
    dag=dag,
)

create_mappings = PythonOperator(
    task_id='create_initial_mappings',
    python_callable=create_initial_mappings,
    dag=dag,
)

load_mappings = PythonOperator(
    task_id='load_mappings_to_iceberg',
    python_callable=load_mappings_to_iceberg,
    dag=dag,
)

validate_mappings = TrinoOperator(
    task_id='validate_mappings',
    conn_id='trino_default',
    sql="""
        -- Validation query for mapping completeness
        WITH mapping_summary AS (
            SELECT
                COUNT(*) as total_codes,
                COUNT(CASE WHEN mapping_confidence >= 0.7 THEN 1 END) as high_confidence_codes,
                COUNT(CASE WHEN manual_review_required = 'YES' THEN 1 END) as needs_review,
                SUM(usage_count) as total_usage,
                SUM(CASE WHEN mapping_confidence >= 0.7 THEN usage_count ELSE 0 END) as confident_usage
            FROM iceberg.silver.concept_map_laboratory
        )
        SELECT
            total_codes,
            high_confidence_codes,
            needs_review,
            ROUND(100.0 * high_confidence_codes / total_codes, 2) as confidence_coverage_pct,
            ROUND(100.0 * confident_usage / total_usage, 2) as usage_coverage_pct
        FROM mapping_summary
    """,
    dag=dag,
)

# Dependencies
create_mapping_schema >> create_mapping_table >> analyze_lab_codes >> create_mappings >> load_mappings >> validate_mappings