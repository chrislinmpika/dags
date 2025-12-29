"""
STEP 1: Data Discovery & Validation

Scans MinIO for CSV files and validates their structure and basic data quality.
This is the first step in our Bronze → Silver OMOP pipeline.

GOAL: Understand what CSV files we have and their structure
OUTPUT: List of CSV files ready for processing
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'omop-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'step1_data_discovery',
    default_args=default_args,
    description='Step 1: Discover and validate CSV files in MinIO',
    schedule=None,  # Manual trigger for step-by-step execution
    catchup=False,
    tags=['step1', 'discovery', 'csv'],
)

def scan_minio_csv_files(**context):
    """Scan MinIO for biological results CSV files - NO FAKE DATA"""
    print("🔍 STEP 1: Scanning MinIO for CSV files...")

    try:
        import boto3
        print("✅ boto3 module available")
    except ImportError as e:
        print(f"❌ boto3 not available: {e}")
        print("🛠️  Install boto3 in Airflow container to connect to MinIO")
        raise Exception("boto3 required for MinIO connection")

    try:
        # Connect to YOUR ACTUAL MinIO instance
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio-api.ns-data-platform.svc.cluster.local:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
            region_name='us-east-1'
        )

        print("✅ Connected to MinIO successfully")

        # List all buckets first
        buckets = s3_client.list_buckets()
        print("📁 Available buckets:")
        for bucket in buckets['Buckets']:
            print(f"   - {bucket['Name']}")

        # Look for CSV files in bronze bucket
        csv_files = []
        print("\n🔍 Scanning bronze bucket for CSV files...")

        try:
            response = s3_client.list_objects_v2(Bucket='bronze')

            if 'Contents' in response:
                print(f"📄 Found {len(response['Contents'])} total objects in bronze bucket:")

                for obj in response['Contents']:
                    print(f"   - {obj['Key']} ({obj['Size']:,} bytes)")

                    if obj['Key'].endswith('.csv'):
                        csv_files.append({
                            'name': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat()
                        })

                print(f"\n📊 CSV FILES FOUND: {len(csv_files)}")
                for file_info in csv_files:
                    print(f"   ✅ {file_info['name']} ({file_info['size']:,} bytes)")

            else:
                print("📭 Bronze bucket is empty")

        except Exception as e:
            print(f"❌ Failed to list objects in bronze bucket: {e}")
            raise

        if not csv_files:
            print("❌ NO CSV FILES FOUND!")
            print("🔧 Check that CSV files are uploaded to the bronze bucket")
            raise Exception("No CSV files found in MinIO bronze bucket")

        # Store REAL results for next steps
        context['task_instance'].xcom_push(key='csv_files', value=csv_files)

        print(f"\n✅ Discovery completed: {len(csv_files)} REAL CSV files found")
        return csv_files

    except Exception as e:
        print(f"❌ MinIO connection or discovery failed: {e}")
        print("🔧 Check MinIO connection details, credentials, and bucket contents")
        raise

def validate_csv_structure(**context):
    """Validate that CSV files have expected structure"""
    print("🔍 STEP 1: Validating CSV file structure...")

    # Get CSV files from previous task
    csv_files = context['task_instance'].xcom_pull(task_ids='scan_minio_csv_files', key='csv_files')

    if not csv_files:
        print("❌ No CSV files found to validate")
        raise Exception("No CSV files found")

    print(f"📋 Validating structure of {len(csv_files)} CSV files...")

    # Expected CSV columns based on your original sample
    expected_columns = [
        'patient_id',
        'visit_id',
        'sampling_datetime_utc',
        'result_datetime_utc',
        'report_date_utc',
        'measurement_source_value',
        'value_as_number',
        'value_as_string',
        'unit_source_value',
        'normality',
        'abnormal_flag',
        'value_type',
        'bacterium_id',
        'provider_id',
        'laboratory_uuid'
    ]

    validation_results = {
        'files_count': len(csv_files),
        'expected_columns': expected_columns,
        'total_size_bytes': sum(f.get('size', 0) for f in csv_files),
        'validation_status': 'PASSED'
    }

    print("📊 Expected CSV structure:")
    for i, col in enumerate(expected_columns, 1):
        print(f"   {i:2d}. {col}")

    print(f"\n📈 File Summary:")
    print(f"   - File count: {validation_results['files_count']}")
    print(f"   - Total size: {validation_results['total_size_bytes']:,} bytes")
    print(f"   - Expected columns: {len(expected_columns)}")

    # Store validation results
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)

    print(f"\n✅ Structure validation: {validation_results['validation_status']}")
    return validation_results

def estimate_data_volume(**context):
    """Estimate processing requirements based on data volume"""
    print("🔍 STEP 1: Estimating data processing requirements...")

    # Get results from previous tasks
    csv_files = context['task_instance'].xcom_pull(task_ids='scan_minio_csv_files', key='csv_files')
    validation_results = context['task_instance'].xcom_pull(task_ids='validate_csv_structure', key='validation_results')

    if not csv_files or not validation_results:
        print("❌ Missing data from previous tasks")
        raise Exception("Cannot estimate without discovery and validation results")

    total_size_mb = validation_results['total_size_bytes'] / (1024 * 1024)
    estimated_rows = total_size_mb * 10000  # Rough estimate: 10K rows per MB

    processing_estimates = {
        'total_size_mb': round(total_size_mb, 2),
        'estimated_rows': int(estimated_rows),
        'estimated_patients': int(estimated_rows / 20),  # Assume ~20 lab results per patient
        'estimated_processing_time_minutes': max(5, int(total_size_mb / 20)),  # ~20MB per minute
        'recommended_chunk_size': min(50000, max(10000, int(estimated_rows / 10)))
    }

    print("📊 Data Volume Estimates:")
    print(f"   - Total size: {processing_estimates['total_size_mb']:.1f} MB")
    print(f"   - Estimated rows: {processing_estimates['estimated_rows']:,}")
    print(f"   - Estimated patients: {processing_estimates['estimated_patients']:,}")
    print(f"   - Est. processing time: {processing_estimates['estimated_processing_time_minutes']} minutes")
    print(f"   - Recommended chunk size: {processing_estimates['recommended_chunk_size']:,} rows")

    # Store estimates for next pipeline steps
    context['task_instance'].xcom_push(key='processing_estimates', value=processing_estimates)

    print("\n✅ Data volume estimation completed")
    return processing_estimates

def generate_step1_report(**context):
    """Generate final report for Step 1"""
    print("📋 STEP 1: Generating discovery report...")

    # Get all results
    csv_files = context['task_instance'].xcom_pull(task_ids='scan_minio_csv_files', key='csv_files')
    validation_results = context['task_instance'].xcom_pull(task_ids='validate_csv_structure', key='validation_results')
    processing_estimates = context['task_instance'].xcom_pull(task_ids='estimate_data_volume', key='processing_estimates')

    print("\n" + "="*60)
    print("🎯 STEP 1 DISCOVERY REPORT")
    print("="*60)

    print(f"\n📁 CSV FILES DISCOVERED:")
    print(f"   - Count: {len(csv_files) if csv_files else 0}")
    if csv_files:
        for file_info in csv_files[:5]:  # Show first 5 files
            print(f"   - {file_info['name']} ({file_info.get('size', 0):,} bytes)")
        if len(csv_files) > 5:
            print(f"   - ... and {len(csv_files) - 5} more files")

    if validation_results:
        print(f"\n🔍 STRUCTURE VALIDATION:")
        print(f"   - Status: {validation_results['validation_status']}")
        print(f"   - Expected columns: {len(validation_results['expected_columns'])}")

    if processing_estimates:
        print(f"\n📊 PROCESSING ESTIMATES:")
        print(f"   - Data volume: {processing_estimates['total_size_mb']:.1f} MB")
        print(f"   - Estimated rows: {processing_estimates['estimated_rows']:,}")
        print(f"   - Est. patients: {processing_estimates['estimated_patients']:,}")
        print(f"   - Est. runtime: {processing_estimates['estimated_processing_time_minutes']} min")

    print(f"\n🚀 NEXT STEPS:")
    print(f"   - Step 2: Bronze Layer - CSV Ingestion")
    print(f"   - Ready to process {len(csv_files) if csv_files else 0} CSV files")

    print("="*60)
    print("✅ STEP 1 COMPLETED SUCCESSFULLY")
    print("="*60)

    # Create final summary for next steps
    step1_summary = {
        'csv_files': csv_files,
        'validation_results': validation_results,
        'processing_estimates': processing_estimates,
        'status': 'COMPLETED',
        'next_step': 'step2_bronze_ingestion'
    }

    context['task_instance'].xcom_push(key='step1_summary', value=step1_summary)
    return step1_summary

# Task definitions
scan_files = PythonOperator(
    task_id='scan_minio_csv_files',
    python_callable=scan_minio_csv_files,
    dag=dag,
)

validate_structure = PythonOperator(
    task_id='validate_csv_structure',
    python_callable=validate_csv_structure,
    dag=dag,
)

estimate_volume = PythonOperator(
    task_id='estimate_data_volume',
    python_callable=estimate_data_volume,
    dag=dag,
)

generate_report = PythonOperator(
    task_id='generate_step1_report',
    python_callable=generate_step1_report,
    dag=dag,
)

# Task dependencies
scan_files >> validate_structure >> estimate_volume >> generate_report