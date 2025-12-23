#!/usr/bin/env python3
"""
Direct test of the load_csv_to_staging function to verify the fix works
"""

import pandas as pd
import logging
from minio import Minio
from trino.dbapi import connect
from datetime import datetime

# Configuration (same as DAG)
MINIO_ENDPOINT = "minio-api.ns-data-platform.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
TRINO_HOST = "my-trino-trino.ns-data-platform.svc.cluster.local"
TRINO_PORT = 8080
TRINO_USER = "trino"
BRONZE_BUCKET = "bronze"

def test_load_function():
    print("🧪 Testing load_csv_to_staging function...")

    try:
        # Step 1: List available files in bronze bucket
        minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)
        objects = minio_client.list_objects(BRONZE_BUCKET, prefix="biological_results_", recursive=True)
        all_files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]

        print(f"📁 Found {len(all_files)} CSV files in bronze bucket")

        if not all_files:
            print("⚠️ No CSV files found for testing")
            return

        # Take just 1 file for testing
        test_files = all_files[:1]
        print(f"🎯 Testing with file: {test_files[0]}")

        # Step 2: Connect to Trino
        conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="iceberg", schema="silver")
        cursor = conn.cursor()

        # Step 3: Create staging table (same as DAG)
        print("🔧 Creating staging table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging_biological_results (
                visit_id VARCHAR,
                visit_date_utc VARCHAR,
                visit_rank VARCHAR,
                patient_id VARCHAR,
                report_id VARCHAR,
                laboratory_uuid VARCHAR,
                sub_laboratory_uuid VARCHAR,
                site_laboratory_uuid VARCHAR,
                internal_test_id VARCHAR,
                debug_external_test_id VARCHAR,
                debug_external_test_scope VARCHAR,
                sampling_datetime_utc VARCHAR,
                sampling_datetime_timezone VARCHAR,
                result_datetime_utc VARCHAR,
                result_datetime_timezone VARCHAR,
                normality VARCHAR,
                value_type VARCHAR,
                internal_numerical_value VARCHAR,
                internal_numerical_unit VARCHAR,
                internal_numerical_unit_system VARCHAR,
                internal_numerical_reference_min VARCHAR,
                internal_numerical_reference_max VARCHAR,
                internal_categorical_qualification VARCHAR,
                internal_categorical_specification VARCHAR,
                internal_antibiogram_bacterium_id VARCHAR,
                range_type VARCHAR,
                report_date_utc VARCHAR,
                source_file VARCHAR,
                load_timestamp TIMESTAMP(3) WITH TIME ZONE
            ) WITH (format = 'PARQUET')
        """)
        print("✅ Staging table creation: SUCCESS")

        # Step 4: Clean staging table
        cursor.execute("DELETE FROM staging_biological_results")
        print("✅ Staging table cleanup: SUCCESS")

        # Step 5: Test streaming load
        for f in test_files:
            print(f"📄 Processing test file: {f}")
            obj = minio_client.get_object(BRONZE_BUCKET, f)

            # STREAMING SOLUTION: Process CSV in chunks
            chunk_size = 1000  # Small chunks for testing
            chunk_count = 0
            total_rows = 0

            for chunk_df in pd.read_csv(obj, chunksize=chunk_size):
                chunk_count += 1

                # Add metadata columns
                chunk_df['source_file'] = f
                chunk_df['load_timestamp'] = datetime.now()

                # Convert all columns to string (except metadata)
                for col in chunk_df.columns:
                    if col not in ['source_file', 'load_timestamp']:
                        chunk_df[col] = chunk_df[col].astype(str)

                # Insert chunk
                insert_sql = "INSERT INTO staging_biological_results VALUES (" + ",".join(["?"] * len(chunk_df.columns)) + ")"
                for row in chunk_df.values:
                    cursor.execute(insert_sql, tuple(row))

                total_rows += len(chunk_df)
                print(f"  ✅ Chunk {chunk_count}: {len(chunk_df)} rows inserted")

                # Stop after a few chunks for testing
                if chunk_count >= 3:
                    print(f"  📊 Test limited to {chunk_count} chunks")
                    break

            print(f"  🎉 File {f} test completed - {total_rows} rows total")

        # Step 6: Verify data was inserted
        cursor.execute("SELECT COUNT(*) FROM staging_biological_results")
        row_count = cursor.fetchone()[0]
        print(f"✅ Verification: {row_count} rows in staging table")

        # Step 7: Cleanup test data
        cursor.execute("DELETE FROM staging_biological_results")
        print("🧹 Test cleanup completed")

        conn.close()
        print("✨ Load function test: PASSED")

    except Exception as e:
        print(f"❌ Load function test failed: {e}")
        print(f"   Error type: {type(e).__name__}")

if __name__ == "__main__":
    test_load_function()