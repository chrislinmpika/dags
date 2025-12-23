#!/usr/bin/env python3
"""
Cleanup script to remove old staging table files from MinIO/Iceberg
"""

from minio import Minio
from minio.error import S3Error
import sys

# Configuration
MINIO_ENDPOINT = "minio-api.ns-data-platform.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_NAME = "eds-lakehouse"

def cleanup_staging_tables():
    try:
        print("🧹 Starting MinIO cleanup...")
        client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

        # List all objects in silver schema that match staging table pattern
        staging_objects = list(client.list_objects(
            BUCKET_NAME,
            prefix="silver/staging_biological_results-",
            recursive=True
        ))

        print(f"📁 Found {len(staging_objects)} staging table files to clean up")

        if staging_objects:
            # Delete staging table objects one by one
            deleted_count = 0
            error_count = 0

            print(f"🗑️ Starting deletion of {len(staging_objects)} files...")

            for i, obj in enumerate(staging_objects):
                try:
                    client.remove_object(BUCKET_NAME, obj.object_name)
                    deleted_count += 1

                    if i % 1000 == 0:  # Progress update every 1000 files
                        print(f"   Progress: {i+1}/{len(staging_objects)} ({deleted_count} deleted)")

                except S3Error as e:
                    error_count += 1
                    if error_count <= 10:  # Show first 10 errors only
                        print(f"⚠️ Error deleting {obj.object_name}: {e}")

            print(f"✅ Total deleted: {deleted_count} staging files")
            print(f"⚠️ Total errors: {error_count} files")

        # Check remaining storage usage
        total_objects = list(client.list_objects(BUCKET_NAME, recursive=True))
        print(f"📊 Remaining objects in {BUCKET_NAME}: {len(total_objects)}")

    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        sys.exit(1)

if __name__ == "__main__":
    cleanup_staging_tables()