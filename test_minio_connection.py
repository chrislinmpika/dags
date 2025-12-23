#!/usr/bin/env python3
"""
Simple test script to diagnose MinIO connectivity issues
"""

from minio import Minio
from minio.error import S3Error
import sys

# Configuration (same as DAG)
MINIO_ENDPOINT = "minio-api.ns-data-platform.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

def test_minio_connection():
    try:
        print("🔗 Testing MinIO connection...")
        client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

        # Test 1: List buckets
        buckets = client.list_buckets()
        bucket_names = [bucket.name for bucket in buckets]
        print(f"✅ Available buckets: {bucket_names}")

        # Test 2: Check if eds-lakehouse bucket exists
        eds_bucket_exists = client.bucket_exists("eds-lakehouse")
        print(f"✅ eds-lakehouse bucket exists: {eds_bucket_exists}")

        # Test 3: Test write permission to eds-lakehouse
        try:
            test_object = "test_write_permission.txt"
            test_data = b"Hello, this is a test file to verify write permissions"

            # Upload test file
            client.put_object("eds-lakehouse", test_object,
                            io.BytesIO(test_data), len(test_data))
            print("✅ Write permission test: SUCCESS")

            # Download test file to verify
            response = client.get_object("eds-lakehouse", test_object)
            downloaded_data = response.read()
            if downloaded_data == test_data:
                print("✅ Read verification: SUCCESS")
            else:
                print("⚠️ Read verification: Data mismatch")

            # Cleanup test file
            client.remove_object("eds-lakehouse", test_object)
            print("✅ Cleanup test object: SUCCESS")

        except S3Error as e:
            print(f"❌ Write permission test failed: {e}")
            print(f"   Error code: {e.code}")

        # Test 4: Try to list objects in eds-lakehouse
        try:
            objects = list(client.list_objects("eds-lakehouse", recursive=True))
            print(f"✅ Objects in eds-lakehouse: {len(objects)} files")
            if objects:
                for i, obj in enumerate(objects[:5]):  # Show first 5
                    print(f"   {i+1}. {obj.object_name}")
                if len(objects) > 5:
                    print(f"   ... and {len(objects) - 5} more")
        except S3Error as e:
            print(f"❌ List objects failed: {e}")

        # Test 5: Check specific folder structure that Iceberg might use
        try:
            iceberg_objects = list(client.list_objects("eds-lakehouse", prefix="silver/", recursive=True))
            print(f"✅ Objects in eds-lakehouse/silver/: {len(iceberg_objects)} files")
        except S3Error as e:
            print(f"⚠️ List silver objects failed: {e}")

    except Exception as e:
        print(f"❌ MinIO connection failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        sys.exit(1)

if __name__ == "__main__":
    import io
    test_minio_connection()