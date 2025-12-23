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
            # Delete staging table objects in batches
            batch_size = 1000
            deleted_count = 0

            for i in range(0, len(staging_objects), batch_size):
                batch = staging_objects[i:i+batch_size]
                object_names = [obj.object_name for obj in batch]

                try:
                    errors = client.remove_objects(BUCKET_NAME, object_names)
                    error_count = len(list(errors))

                    if error_count > 0:
                        print(f"⚠️ Batch {i//batch_size + 1}: {error_count} errors")
                    else:
                        deleted_count += len(object_names)
                        print(f"✅ Batch {i//batch_size + 1}: Deleted {len(object_names)} files")

                except Exception as e:
                    print(f"❌ Error in batch {i//batch_size + 1}: {e}")

            print(f"✅ Total deleted: {deleted_count} staging files")

        # Check remaining storage usage
        total_objects = list(client.list_objects(BUCKET_NAME, recursive=True))
        print(f"📊 Remaining objects in {BUCKET_NAME}: {len(total_objects)}")

    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        sys.exit(1)

if __name__ == "__main__":
    cleanup_staging_tables()