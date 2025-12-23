#!/usr/bin/env python3
"""
Simple test script to diagnose Iceberg connectivity and table creation issues
"""

from trino.dbapi import connect
import sys

# Configuration (same as DAG)
TRINO_HOST = "my-trino-trino.ns-data-platform.svc.cluster.local"
TRINO_PORT = 8080
TRINO_USER = "trino"

def test_connection():
    try:
        print("🔗 Testing Trino connection...")
        conn = connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="iceberg", schema="silver")
        cursor = conn.cursor()

        # Test 1: Check connection
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"✅ Basic connection: {result}")

        # Test 2: Check catalog
        cursor.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in cursor.fetchall()]
        print(f"✅ Available catalogs: {catalogs}")

        # Test 3: Check schema
        cursor.execute("SHOW SCHEMAS FROM iceberg")
        schemas = [row[0] for row in cursor.fetchall()]
        print(f"✅ Available schemas in iceberg: {schemas}")

        # Test 4: Try creating schema if it doesn't exist
        try:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS silver")
            print("✅ Schema creation: OK")
        except Exception as e:
            print(f"⚠️ Schema creation failed: {e}")

        # Test 5: Check existing tables
        try:
            cursor.execute("SHOW TABLES FROM iceberg.silver")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"✅ Existing tables: {tables}")
        except Exception as e:
            print(f"⚠️ Failed to show tables: {e}")

        # Test 6: Try simple table creation
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS iceberg.silver.test_table (
                    id VARCHAR,
                    name VARCHAR
                ) WITH (format = 'PARQUET')
            """)
            print("✅ Simple table creation: OK")

            # Clean up test table
            cursor.execute("DROP TABLE IF EXISTS iceberg.silver.test_table")
            print("✅ Table cleanup: OK")

        except Exception as e:
            print(f"❌ Table creation failed: {e}")
            print(f"   Error type: {type(e).__name__}")

        conn.close()

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print(f"   Error type: {type(e).__name__}")
        sys.exit(1)

if __name__ == "__main__":
    test_connection()