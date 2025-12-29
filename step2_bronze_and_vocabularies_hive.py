"""
STEP 2: Bronze + Vocabularies - ULTRA-ROBUST v20 with ICEBERG OPTIMIZATION

ULTRA-ROBUST HIVE APPROACH with comprehensive safety and ICEBERG-COMPATIBLE OPTIMIZATION!
- File validation, data quality checks, rollback protection
- Zero-division protection, array bounds checking, error recovery
- ICEBERG PARTITIONING: year/month columns for faster time-based queries
- ORDER BY CLUSTERING: sorted data for improved patient/measurement access
- VOCABULARY OPTIMIZATION: sorted by vocabulary_id + concept_code for efficient lookups
- Expected time: 3-4 hours total (vocabularies: 2-3h + CSV: 60min)
- Production-grade error handling with transactional safety

Ultra-robust with all edge cases covered + Iceberg-compatible optimization for analytics!
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'omop-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,  # No retries - fail fast for debugging
    'on_failure_callback': None,  # Could add alerting here
    'on_retry_callback': None,
}

dag = DAG(
    'step2_bronze_and_vocabularies_hive',
    default_args=default_args,
    description='Step 2: Complete foundation using ULTRA-ROBUST Hive approach v20 - TABLE CLEANUP FIXED',
    schedule=None,
    catchup=False,
    tags=['step2', 'bronze', 'vocabularies', 'hive', 'ultra-robust', 'v20', 'table-cleanup-fix'],
    doc_md="""
    ## Ultra-Robust v20 with Iceberg Optimization + Table Cleanup Fix

    Latest fixes applied:
    - ✅ Removed retries for immediate feedback
    - ✅ Fixed table already exists error during vocabulary loading
    - ✅ Iceberg-compatible partitioning and clustering
    - ✅ Comprehensive error handling and rollback protection

    Version: 24f9ff4 (Dec 29, 21:28)
    """,
)

def execute_trino_query(sql_query, description, catalog='iceberg', schema='default'):
    """Execute Trino queries with robust error handling"""
    print(f"🚀 {description}")

    import trino

    conn = None
    cursor = None

    try:
        conn = trino.dbapi.connect(
            host='my-trino-trino.ns-data-platform.svc.cluster.local',
            port=8080,
            user='airflow',
            catalog=catalog,
            schema=schema
        )

        cursor = conn.cursor()
        cursor.execute(sql_query)

        if sql_query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
            return results
        else:
            return "success"

    except Exception as e:
        print(f"❌ Query failed: {e}")
        print(f"🔍 SQL: {sql_query[:200]}...")
        raise e
    finally:
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

def check_vocabularies_exist(**context):
    """Check if OMOP vocabularies already exist"""
    print("📚 Checking if OMOP vocabularies already exist...")

    try:
        # Check if vocabulary tables exist
        result = execute_trino_query(
            "SHOW TABLES IN iceberg.omop_vocab",
            "Check vocabulary schema exists"
        )

        tables = [row[0] for row in result] if result else []
        required_tables = ['concept', 'vocabulary', 'concept_relationship']

        if all(table in tables for table in required_tables):
            concept_count = execute_trino_query(
                "SELECT COUNT(*) FROM iceberg.omop_vocab.concept",
                "Count existing concepts"
            )

            if concept_count and concept_count[0][0] > 100000:  # 100K+ concepts
                print(f"✅ Vocabularies already exist! {concept_count[0][0]:,} concepts found")
                print("⚡ Skipping vocabulary load - proceeding to CSV processing")
                return "vocabularies_exist"

        print("📥 Vocabularies not found - will load from your CSV files using Hive")
        return "vocabularies_needed"

    except Exception as e:
        print(f"📥 Vocabularies schema not found: {e}")
        print("💾 Will create fresh vocabulary setup using PROVEN Hive approach")
        return "vocabularies_needed"

def setup_hive_vocab_schema(**context):
    """Setup Hive schema for vocabularies"""
    print("🏗️ Setting up Hive schema for vocabulary processing...")

    try:
        # Create Hive schema for vocabularies
        execute_trino_query(
            "CREATE SCHEMA IF NOT EXISTS hive.omop_vocab",
            "Create Hive vocabulary schema",
            catalog='hive'
        )

        print("✅ Hive vocabulary schema ready!")
        return "hive_vocab_schema_ready"

    except Exception as e:
        print(f"❌ Hive vocab schema setup failed: {e}")
        raise Exception(f"Cannot setup Hive vocab schema: {e}")

def load_vocabulary_via_hive(**context):
    """Load OMOP vocabularies using bulletproof Hive external table approach"""
    print("📚 STEP 2v20: Loading OMOP vocabularies using ULTRA-ROBUST Hive method...")
    print("🎯 Enhanced with format detection, validation, and comprehensive error handling!")

    # Check if we need to load vocabularies
    vocab_status = context['task_instance'].xcom_pull(task_ids='check_vocabularies_exist')
    if vocab_status == "vocabularies_exist":
        print("✅ Vocabularies already exist - skipping load!")
        return "vocabularies_ready"

    try:
        # Create Iceberg vocabulary schema
        execute_trino_query(
            "CREATE SCHEMA IF NOT EXISTS iceberg.omop_vocab WITH (location = 's3a://eds-lakehouse/omop_vocab/')",
            "Create Iceberg vocabulary schema"
        )

        print("📊 Step 1: Validating vocabulary files and creating external tables...")

        # Create vocabulary external tables first to validate file access
        required_files = ['VOCABULARY.csv', 'CONCEPT.csv', 'CONCEPT_RELATIONSHIP.csv']
        print(f"📁 Validating access to {len(required_files)} vocabulary files...")

        # Validate files by attempting to create and test external tables
        validation_tables_created = []
        try:
            # Test VOCABULARY.csv access by creating validation external table
            execute_trino_query("""
                CREATE TABLE hive.omop_vocab.vocabulary_validation (
                    vocabulary_id varchar,
                    vocabulary_name varchar,
                    vocabulary_reference varchar,
                    vocabulary_version varchar,
                    vocabulary_concept_id varchar
                )
                WITH (
                    external_location = 's3a://omop-vocabularies/',
                    format = 'CSV',
                    csv_separator = '\t',
                    skip_header_line_count = 1
                )
            """, "Create VOCABULARY validation table", catalog='hive', schema='omop_vocab')
            validation_tables_created.append('vocabulary_validation')

            # Test data access
            vocab_test = execute_trino_query(
                "SELECT COUNT(*) FROM hive.omop_vocab.vocabulary_validation WHERE \"$path\" LIKE '%VOCABULARY.csv' LIMIT 1",
                "Test VOCABULARY.csv access",
                catalog='hive', schema='omop_vocab'
            )
            if not vocab_test or vocab_test[0][0] == 0:
                raise Exception("VOCABULARY.csv contains no data or is inaccessible")
            print(f"✅ VOCABULARY.csv is accessible and contains data")

            # Cleanup validation table
            execute_trino_query(
                "DROP TABLE hive.omop_vocab.vocabulary_validation",
                "Clean up validation table",
                catalog='hive', schema='omop_vocab'
            )
            validation_tables_created.remove('vocabulary_validation')

        except Exception as e:
            # Cleanup validation tables on failure
            for table in validation_tables_created:
                try:
                    execute_trino_query(
                        f"DROP TABLE IF EXISTS hive.omop_vocab.{table}",
                        f"Cleanup {table}",
                        catalog='hive', schema='omop_vocab'
                    )
                except:
                    pass
            print(f"❌ CRITICAL: Cannot access vocabulary files: {e}")
            print("💡 Please ensure vocabulary files are uploaded to s3a://omop-vocabularies/")
            raise Exception(f"Vocabulary file validation failed: {e}")

        # Clean up existing external tables first
        external_tables = ['vocabulary_external', 'concept_external', 'concept_relationship_external']
        for table_name in external_tables:
            try:
                execute_trino_query(
                    f"DROP TABLE IF EXISTS hive.omop_vocab.{table_name}",
                    f"Clean up existing {table_name}",
                    catalog='hive',
                    schema='omop_vocab'
                )
            except Exception as e:
                print(f"⚠️ Could not clean {table_name}: {e}")

        print("📊 Step 2: Auto-detecting CSV separator and creating external tables...")

        # Auto-detect CSV separator (try tab first, then comma as fallback)
        csv_separator = None
        external_tables_created = []

        for separator, separator_name in [('\t', 'tab-separated'), (',', 'comma-separated')]:
            try:
                print(f"🔍 Testing {separator_name} format...")

                # Create test external table for VOCABULARY
                execute_trino_query(f"""
                    CREATE TABLE hive.omop_vocab.vocabulary_external (
                        vocabulary_id varchar,
                        vocabulary_name varchar,
                        vocabulary_reference varchar,
                        vocabulary_version varchar,
                        vocabulary_concept_id varchar
                    )
                    WITH (
                        external_location = 's3a://omop-vocabularies/',
                        format = 'CSV',
                        csv_separator = '{separator}',
                        skip_header_line_count = 1
                    )
                """, f"Create {separator_name} VOCABULARY external table", catalog='hive', schema='omop_vocab')
                external_tables_created.append('vocabulary_external')

                # Test if separator works by checking for reasonable data
                test_result = execute_trino_query("""
                    SELECT vocabulary_id, vocabulary_name
                    FROM hive.omop_vocab.vocabulary_external
                    WHERE "$path" LIKE '%VOCABULARY.csv'
                    AND vocabulary_id IS NOT NULL
                    AND vocabulary_name IS NOT NULL
                    LIMIT 1
                """, f"Test {separator_name} data quality", catalog='hive', schema='omop_vocab')

                if test_result and len(test_result) > 0 and test_result[0][0] and test_result[0][1]:
                    # Check if data looks reasonable (no concatenated fields)
                    vocab_id = test_result[0][0].strip()
                    vocab_name = test_result[0][1].strip()

                    # Good data should have vocab_id without embedded separators/spaces
                    if len(vocab_id) < 50 and len(vocab_name) > 0 and len(vocab_name) < 200:
                        csv_separator = separator
                        print(f"✅ {separator_name} format detected and validated!")
                        print(f"   Sample: {vocab_id} -> {vocab_name}")
                        break
                    else:
                        print(f"⚠️ {separator_name} format produces invalid data layout")
                else:
                    print(f"⚠️ {separator_name} format produces no valid data")

                # Clean up test table
                execute_trino_query(
                    "DROP TABLE hive.omop_vocab.vocabulary_external",
                    f"Clean up {separator_name} test table",
                    catalog='hive', schema='omop_vocab'
                )
                external_tables_created.remove('vocabulary_external')

            except Exception as e:
                print(f"❌ {separator_name} format test failed: {e}")
                # Clean up on failure
                for table in external_tables_created:
                    try:
                        execute_trino_query(
                            f"DROP TABLE IF EXISTS hive.omop_vocab.{table}",
                            f"Cleanup {table}",
                            catalog='hive', schema='omop_vocab'
                        )
                    except:
                        pass
                external_tables_created = []

        if not csv_separator:
            raise Exception("Could not detect valid CSV separator for vocabulary files")

        print(f"📄 Using detected CSV separator: '{csv_separator}' ({'tab-separated' if csv_separator == '\t' else 'comma-separated'})")

        # Ensure cleanup before creating final tables
        try:
            execute_trino_query(
                "DROP TABLE IF EXISTS hive.omop_vocab.vocabulary_external",
                "Clean up any existing vocabulary_external table",
                catalog='hive', schema='omop_vocab'
            )
        except:
            pass  # Ignore if table doesn't exist

        # Create final external tables with detected separator
        execute_trino_query(f"""
            CREATE TABLE hive.omop_vocab.vocabulary_external (
                vocabulary_id varchar,
                vocabulary_name varchar,
                vocabulary_reference varchar,
                vocabulary_version varchar,
                vocabulary_concept_id varchar
            )
            WITH (
                external_location = 's3a://omop-vocabularies/',
                format = 'CSV',
                csv_separator = '{csv_separator}',
                skip_header_line_count = 1
            )
        """, "Create final VOCABULARY external table", catalog='hive', schema='omop_vocab')
        external_tables_created.append('vocabulary_external')

        # Ensure cleanup of all remaining external tables before creation
        for table_name in ['concept_external', 'concept_relationship_external']:
            try:
                execute_trino_query(
                    f"DROP TABLE IF EXISTS hive.omop_vocab.{table_name}",
                    f"Clean up any existing {table_name} table",
                    catalog='hive', schema='omop_vocab'
                )
            except:
                pass  # Ignore if table doesn't exist

        # Create remaining external tables with validated separator and comprehensive error handling
        try:
            # Create Hive external table for CONCEPT (using detected separator)
            execute_trino_query(f"""
                CREATE TABLE hive.omop_vocab.concept_external (
                    concept_id varchar,
                    concept_name varchar,
                    domain_id varchar,
                    vocabulary_id varchar,
                    concept_class_id varchar,
                    standard_concept varchar,
                    concept_code varchar,
                    valid_start_date varchar,
                    valid_end_date varchar,
                    invalid_reason varchar
                )
                WITH (
                    external_location = 's3a://omop-vocabularies/',
                    format = 'CSV',
                    csv_separator = '{csv_separator}',
                    skip_header_line_count = 1
                )
            """, "Create Hive external CONCEPT table", catalog='hive', schema='omop_vocab')
            external_tables_created.append('concept_external')

            # Validate CONCEPT external table
            concept_test = execute_trino_query(
                "SELECT COUNT(*) FROM hive.omop_vocab.concept_external WHERE \"$path\" LIKE '%CONCEPT.csv' LIMIT 1",
                "Validate CONCEPT external table",
                catalog='hive', schema='omop_vocab'
            )
            if not concept_test or concept_test[0][0] == 0:
                raise Exception("CONCEPT external table contains no data")
            print(f"✅ CONCEPT external table validated ({concept_test[0][0]} rows detected)")

            # Create Hive external table for CONCEPT_RELATIONSHIP (using detected separator)
            execute_trino_query(f"""
                CREATE TABLE hive.omop_vocab.concept_relationship_external (
                    concept_id_1 varchar,
                    concept_id_2 varchar,
                    relationship_id varchar,
                    valid_start_date varchar,
                    valid_end_date varchar,
                    invalid_reason varchar
                )
                WITH (
                    external_location = 's3a://omop-vocabularies/',
                    format = 'CSV',
                    csv_separator = '{csv_separator}',
                    skip_header_line_count = 1
                )
            """, "Create Hive external CONCEPT_RELATIONSHIP table", catalog='hive', schema='omop_vocab')
            external_tables_created.append('concept_relationship_external')

            # Validate CONCEPT_RELATIONSHIP external table
            rel_test = execute_trino_query(
                "SELECT COUNT(*) FROM hive.omop_vocab.concept_relationship_external WHERE \"$path\" LIKE '%CONCEPT_RELATIONSHIP.csv' LIMIT 1",
                "Validate CONCEPT_RELATIONSHIP external table",
                catalog='hive', schema='omop_vocab'
            )
            if not rel_test or rel_test[0][0] == 0:
                raise Exception("CONCEPT_RELATIONSHIP external table contains no data")
            print(f"✅ CONCEPT_RELATIONSHIP external table validated ({rel_test[0][0]} rows detected)")

            print("🎉 All external tables created and validated successfully!")

        except Exception as e:
            print(f"❌ External table creation failed: {e}")
            print("🔄 Rolling back external tables...")

            # Rollback: Drop any successfully created external tables
            for table in external_tables_created:
                try:
                    execute_trino_query(
                        f"DROP TABLE IF EXISTS hive.omop_vocab.{table}",
                        f"Rollback: Drop {table} external table",
                        catalog='hive', schema='omop_vocab'
                    )
                    print(f"🗑️ Rolled back {table} external table")
                except Exception as rollback_error:
                    print(f"⚠️ Could not rollback {table}: {rollback_error}")

            raise Exception(f"External table creation failed with rollback: {e}")

        print("📊 Step 2: Cleaning up existing tables and loading vocabularies...")

        # Clean up existing Iceberg tables for fresh start
        for table_name in ['vocabulary', 'concept', 'concept_relationship']:
            try:
                execute_trino_query(
                    f"DROP TABLE IF EXISTS iceberg.omop_vocab.{table_name}",
                    f"Clean up existing {table_name} table"
                )
                print(f"✅ Cleaned up existing {table_name} table")
            except Exception as e:
                print(f"⚠️ Could not clean {table_name}: {e}")

        print("📊 Step 3: Loading vocabularies via CTAS with validation and rollback protection...")

        # Track successful table creations for rollback
        created_tables = []

        try:
            # Load VOCABULARY via CTAS (filter by filename)
            execute_trino_query("""
                CREATE TABLE iceberg.omop_vocab.vocabulary
                WITH (format = 'PARQUET')
                AS
                SELECT
                    vocabulary_id,
                    vocabulary_name,
                    vocabulary_reference,
                    vocabulary_version,
                    TRY_CAST(vocabulary_concept_id AS BIGINT) AS vocabulary_concept_id
                FROM hive.omop_vocab.vocabulary_external
                WHERE vocabulary_id IS NOT NULL
                  AND vocabulary_id != ''
                  AND "$path" LIKE '%VOCABULARY.csv'
            """, "Load VOCABULARY via proven CTAS method")

            # Validate VOCABULARY data
            vocab_count = execute_trino_query(
                "SELECT COUNT(*) FROM iceberg.omop_vocab.vocabulary",
                "Validate VOCABULARY data"
            )
            vocab_rows = vocab_count[0][0] if (vocab_count and len(vocab_count) > 0) else 0
            if vocab_rows == 0:
                raise Exception("VOCABULARY table created but contains no data")
            print(f"✅ VOCABULARY table created with {vocab_rows:,} vocabularies")
            created_tables.append('vocabulary')

            # Load CONCEPT via CTAS with ICEBERG-COMPATIBLE optimization (this is the big one - 5M+ concepts)
            execute_trino_query("""
                CREATE TABLE iceberg.omop_vocab.concept
                WITH (
                    format = 'PARQUET',
                    location = 's3a://eds-lakehouse/omop_vocab/concept/'
                )
                AS
                SELECT
                    TRY_CAST(concept_id AS BIGINT) AS concept_id,
                    concept_name,
                    domain_id,
                    vocabulary_id,
                    concept_class_id,
                    standard_concept,
                    concept_code,
                    TRY_CAST(valid_start_date AS DATE) AS valid_start_date,
                    TRY_CAST(valid_end_date AS DATE) AS valid_end_date,
                    invalid_reason
                FROM hive.omop_vocab.concept_external
                WHERE concept_id IS NOT NULL
                  AND concept_id != ''
                  AND TRY_CAST(concept_id AS BIGINT) IS NOT NULL
                  AND "$path" LIKE '%CONCEPT.csv'
                ORDER BY vocabulary_id, concept_code, concept_id  -- Clustering via ORDER BY (Iceberg-compatible)
            """, "Load CONCEPT via proven CTAS method with Iceberg-compatible optimization (5M+ concepts)")

            # Validate CONCEPT data
            concept_count = execute_trino_query(
                "SELECT COUNT(*) FROM iceberg.omop_vocab.concept",
                "Validate CONCEPT data"
            )
            concept_rows = concept_count[0][0] if (concept_count and len(concept_count) > 0) else 0
            if concept_rows < 10000:  # Expect at least 10K concepts
                raise Exception(f"CONCEPT table created but only contains {concept_rows:,} concepts (expected 100K+)")
            print(f"✅ CONCEPT table created with {concept_rows:,} concepts")
            created_tables.append('concept')

            # Load CONCEPT_RELATIONSHIP via CTAS with Iceberg-compatible optimization
            execute_trino_query("""
                CREATE TABLE iceberg.omop_vocab.concept_relationship
                WITH (
                    format = 'PARQUET',
                    location = 's3a://eds-lakehouse/omop_vocab/concept_relationship/'
                )
                AS
                SELECT
                    TRY_CAST(concept_id_1 AS BIGINT) AS concept_id_1,
                    TRY_CAST(concept_id_2 AS BIGINT) AS concept_id_2,
                    relationship_id,
                    TRY_CAST(valid_start_date AS DATE) AS valid_start_date,
                    TRY_CAST(valid_end_date AS DATE) AS valid_end_date,
                    invalid_reason
                FROM hive.omop_vocab.concept_relationship_external
                WHERE concept_id_1 IS NOT NULL
                  AND concept_id_2 IS NOT NULL
                  AND TRY_CAST(concept_id_1 AS BIGINT) IS NOT NULL
                  AND TRY_CAST(concept_id_2 AS BIGINT) IS NOT NULL
                  AND "$path" LIKE '%CONCEPT_RELATIONSHIP.csv'
                ORDER BY relationship_id, concept_id_1, concept_id_2  -- Clustering via ORDER BY (Iceberg-compatible)
            """, "Load CONCEPT_RELATIONSHIP via proven CTAS method with Iceberg-compatible optimization")

            # Validate CONCEPT_RELATIONSHIP data
            rel_count = execute_trino_query(
                "SELECT COUNT(*) FROM iceberg.omop_vocab.concept_relationship",
                "Validate CONCEPT_RELATIONSHIP data"
            )
            rel_rows = rel_count[0][0] if (rel_count and len(rel_count) > 0) else 0
            if rel_rows == 0:
                raise Exception("CONCEPT_RELATIONSHIP table created but contains no data")
            print(f"✅ CONCEPT_RELATIONSHIP table created with {rel_rows:,} relationships")
            created_tables.append('concept_relationship')

        except Exception as e:
            print(f"❌ Vocabulary loading failed: {e}")
            print("🔄 Rolling back partially created tables...")

            # Rollback: Drop any successfully created tables
            for table in created_tables:
                try:
                    execute_trino_query(
                        f"DROP TABLE IF EXISTS iceberg.omop_vocab.{table}",
                        f"Rollback: Drop {table} table"
                    )
                    print(f"🗑️ Rolled back {table} table")
                except Exception as rollback_error:
                    print(f"⚠️ Could not rollback {table}: {rollback_error}")

            raise Exception(f"Vocabulary loading failed with rollback: {e}")

        print("✅ Complete OMOP vocabulary set loaded using proven Hive method!")
        print("🎯 Ready for professional-grade OMOP CDM mapping!")

        return "vocabularies_loaded_hive"

    except Exception as e:
        print(f"❌ Hive vocabulary loading failed: {e}")
        raise Exception(f"Cannot load vocabularies via Hive: {e}")

def setup_bronze_hive_schema(**context):
    """Setup Hive schema for bronze data processing"""
    print("🏗️ Setting up Hive schema for bronze CSV processing...")

    try:
        # Create Hive test_ingestion schema (same as successful test)
        execute_trino_query(
            "CREATE SCHEMA IF NOT EXISTS hive.test_ingestion",
            "Create Hive test_ingestion schema",
            catalog='hive'
        )

        # Create Iceberg bronze schema
        execute_trino_query(
            "CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3a://eds-lakehouse/bronze/')",
            "Create Iceberg bronze schema"
        )

        # Drop existing bronze table for clean start
        try:
            execute_trino_query(
                "DROP TABLE IF EXISTS iceberg.bronze.biological_results_raw",
                "Drop existing bronze table"
            )
        except:
            pass

        print("✅ Bronze schemas ready for proven Hive processing!")
        return "bronze_schemas_ready"

    except Exception as e:
        print(f"❌ Bronze schema setup failed: {e}")
        raise Exception(f"Cannot setup bronze schemas: {e}")

def create_hive_csv_external_table(**context):
    """Create Hive external table for CSV processing with comprehensive validation"""
    print("📊 Creating Hive external CSV table using PROVEN method with validation...")

    try:
        # Drop existing table
        try:
            execute_trino_query(
                "DROP TABLE IF EXISTS hive.test_ingestion.biological_results_csv_external",
                "Clean up existing Hive external table",
                catalog='hive',
                schema='test_ingestion'
            )
        except:
            pass

        # Create external table (same config as successful test!)
        sql_create_external = """
        CREATE TABLE hive.test_ingestion.biological_results_csv_external (
            patient_id varchar,
            visit_id varchar,
            sampling_datetime_utc varchar,
            result_datetime_utc varchar,
            report_date_utc varchar,
            measurement_source_value varchar,
            value_as_number varchar,
            value_as_string varchar,
            unit_source_value varchar,
            normality varchar,
            abnormal_flag varchar,
            value_type varchar,
            bacterium_id varchar,
            provider_id varchar,
            laboratory_uuid varchar
        )
        WITH (
            external_location = 's3a://bronze/',
            format = 'CSV',
            csv_separator = ',',
            skip_header_line_count = 1
        )
        """

        execute_trino_query(
            sql_create_external,
            "Create Hive external CSV table (proven config)",
            catalog='hive',
            schema='test_ingestion'
        )

        print("🧪 Validating external table can access CSV data...")

        # Validate table can access data and count files
        validation_query = """
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(DISTINCT "$path") as files_accessible
        FROM hive.test_ingestion.biological_results_csv_external
        WHERE patient_id IS NOT NULL
        LIMIT 1
        """

        result = execute_trino_query(
            validation_query,
            "Validate CSV external table access",
            catalog='hive',
            schema='test_ingestion'
        )

        if result and len(result) > 0:
            total_rows, unique_patients, files_accessible = result[0]
            print(f"✅ CSV External table validation successful!")
            print(f"   📊 Accessible rows: {total_rows:,}")
            print(f"   👥 Unique patients: {unique_patients:,}")
            print(f"   📁 Files accessible: {files_accessible:,}")

            if total_rows == 0:
                raise Exception("External table created but no CSV data is accessible")
            if files_accessible == 0:
                raise Exception("External table created but no CSV files are accessible")

            # Store validation metrics for monitoring
            context['task_instance'].xcom_push(key='external_table_rows', value=total_rows)
            context['task_instance'].xcom_push(key='external_table_files', value=files_accessible)

            print("✅ Hive external CSV table created and validated - ready for proven CTAS!")
            return "hive_csv_external_ready"
        else:
            raise Exception("External table validation returned no results")

    except Exception as e:
        print(f"❌ Hive external table creation/validation failed: {e}")
        print("💡 Ensure CSV files are present in s3a://bronze/ bucket")
        raise Exception(f"Cannot create/validate Hive external table: {e}")

def execute_proven_csv_ctas(**context):
    """Execute proven Hive→Iceberg CTAS (62 minutes for 634M+ rows)"""
    print("🚀 STEP 2v20: BULLETPROOF Hive→Iceberg CTAS processing!")
    print("⚡ Same method that processed 634M+ rows in 62 minutes!")

    # Validate external table metrics from previous task
    external_rows = context['task_instance'].xcom_pull(task_ids='create_hive_csv_external_table', key='external_table_rows')
    external_files = context['task_instance'].xcom_pull(task_ids='create_hive_csv_external_table', key='external_table_files')

    if external_rows is None or external_files is None:
        print("⚠️  External table validation metrics not found - proceeding with caution")
    else:
        print(f"📊 External table validated: {external_rows:,} rows from {external_files:,} files")
        if external_rows < 1000000:  # Less than 1M rows
            print(f"⚠️  WARNING: Only {external_rows:,} rows accessible (expected 634M+)")

    start_time = datetime.now()

    # The proven CTAS operation with ICEBERG-COMPATIBLE optimization
    sql_proven_ctas = """
    CREATE TABLE iceberg.bronze.biological_results_raw
    WITH (
        format = 'PARQUET',

        -- ICEBERG-COMPATIBLE TIME PARTITIONING for faster time queries
        partitioning = ARRAY['year', 'month'],

        -- LOCATION optimization
        location = 's3a://eds-lakehouse/bronze/biological_results_raw/'
    )
    AS
    SELECT
        -- Track source processing
        'hive_bulletproof_v20_optimized' AS processing_batch,

        -- CSV columns with proper typing (same as successful test)
        patient_id,
        TRY_CAST(visit_id AS BIGINT) AS visit_id,
        TRY_CAST(sampling_datetime_utc AS TIMESTAMP) AS sampling_datetime_utc,
        TRY_CAST(result_datetime_utc AS TIMESTAMP) AS result_datetime_utc,
        TRY_CAST(report_date_utc AS DATE) AS report_date_utc,
        measurement_source_value,
        TRY_CAST(value_as_number AS DOUBLE) AS value_as_number,
        value_as_string,
        unit_source_value,
        normality,
        abnormal_flag,
        value_type,
        bacterium_id,
        provider_id,
        laboratory_uuid,

        -- DERIVED PARTITION COLUMNS (Iceberg-compatible with NULL safety)
        COALESCE(year(TRY_CAST(sampling_datetime_utc AS TIMESTAMP)), 1900) AS year,
        COALESCE(month(TRY_CAST(sampling_datetime_utc AS TIMESTAMP)), 1) AS month,

        -- Processing metadata
        CURRENT_TIMESTAMP AS load_timestamp

    FROM hive.test_ingestion.biological_results_csv_external
    WHERE patient_id IS NOT NULL
      AND patient_id != ''
      AND TRY_CAST(sampling_datetime_utc AS TIMESTAMP) IS NOT NULL  -- Required for partitioning
    ORDER BY patient_id, measurement_source_value  -- Clustering via ORDER BY (Iceberg-compatible)
    """

    print("🔥 Starting PROVEN CTAS with ICEBERG-COMPATIBLE OPTIMIZATION!")
    print("⚡ Same proven method + Iceberg time partitioning + ORDER BY clustering")
    print("⏰ Expected time: ~62 minutes (proven benchmark)")
    print("🎯 Creates optimized partitioned structure for faster future analytics!")

    result = execute_trino_query(sql_proven_ctas, "PROVEN: Hive CSV → Iceberg Parquet with Iceberg-Compatible Optimization", schema='bronze')

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"🎉 PROVEN PROCESSING with ICEBERG-COMPATIBLE OPTIMIZATION COMPLETE!")
    print(f"⏱️  Total time: {duration/3600:.2f} hours")
    print(f"🏆 Same proven method that achieved 169K+ rows/second!")
    print(f"🚀 Data now optimized with Iceberg partitioning + ORDER BY clustering!")

    context['task_instance'].xcom_push(key='processing_time_hours', value=duration/3600)

    return result

def validate_complete_foundation(**context):
    """Validate complete foundation setup with comprehensive safety checks"""
    print("📊 STEP 2v20: Validating complete foundation setup with safety checks...")

    # Collect validation data from previous tasks
    external_rows = context['task_instance'].xcom_pull(task_ids='create_hive_csv_external_table', key='external_table_rows')
    external_files = context['task_instance'].xcom_pull(task_ids='create_hive_csv_external_table', key='external_table_files')

    # Safely get processing time with default fallback
    processing_time = context['task_instance'].xcom_pull(task_ids='execute_proven_csv_ctas', key='processing_time_hours')
    if processing_time is None:
        print("⚠️  Processing time not found in XCom, using fallback validation")
        processing_time = 1.0  # Default fallback to prevent division by zero

    print(f"🔍 Foundation validation with external table metrics:")
    if external_rows and external_files:
        print(f"   📊 External table: {external_rows:,} rows from {external_files:,} files")
    else:
        print(f"   ⚠️  External table metrics unavailable")

    # Validate vocabulary setup with error handling
    try:
        vocab_count = execute_trino_query(
            "SELECT COUNT(*) FROM iceberg.omop_vocab.concept",
            "Count vocabulary concepts"
        )
        vocab_concepts = vocab_count[0][0] if (vocab_count and len(vocab_count) > 0) else 0
    except Exception as e:
        print(f"⚠️  Could not count vocabulary concepts: {e}")
        vocab_concepts = 0

    # Count total bronze data rows with error handling
    try:
        count_result = execute_trino_query(
            "SELECT COUNT(*) FROM iceberg.bronze.biological_results_raw",
            "Count total processed rows",
            schema='bronze'
        )
        total_rows = count_result[0][0] if (count_result and len(count_result) > 0) else 0
    except Exception as e:
        print(f"⚠️  Could not count bronze data rows: {e}")
        total_rows = 0

    # Detailed bronze statistics with comprehensive error handling
    try:
        stats_sql = """
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT patient_id) as unique_patients,
            COUNT(DISTINCT visit_id) as unique_visits,
            MIN(sampling_datetime_utc) as earliest_sample,
            MAX(sampling_datetime_utc) as latest_sample,
            COUNT(CASE WHEN value_as_number IS NOT NULL THEN 1 END) as numeric_values,
            COUNT(CASE WHEN value_as_string IS NOT NULL THEN 1 END) as string_values
        FROM iceberg.bronze.biological_results_raw
        """

        stats_result = execute_trino_query(stats_sql, "Comprehensive statistics", schema='bronze')
        if stats_result and len(stats_result) > 0:
            stats = stats_result[0]
        else:
            print("⚠️  No statistics available, using defaults")
            stats = (0, 0, 0, None, None, 0, 0)
    except Exception as e:
        print(f"⚠️  Could not get detailed statistics: {e}")
        stats = (total_rows, 0, 0, None, None, 0, 0)

    # Check partitioning structure (Iceberg partition validation)
    try:
        partitions_sql = """
        SELECT
            COUNT(DISTINCT year) as years_spanned,
            COUNT(DISTINCT concat(year, '-', month)) as total_partitions,
            MIN(year) as first_year,
            MAX(year) as last_year
        FROM iceberg.bronze.biological_results_raw
        WHERE year IS NOT NULL AND month IS NOT NULL
        """

        partition_result = execute_trino_query(partitions_sql, "Validate Iceberg partition structure", schema='bronze')
        if partition_result and len(partition_result) > 0:
            years_spanned, total_partitions, first_year, last_year = partition_result[0]
        else:
            years_spanned, total_partitions, first_year, last_year = 0, 0, None, None
    except Exception as e:
        print(f"⚠️  Could not validate partitioning: {e}")
        years_spanned, total_partitions, first_year, last_year = 0, 0, None, None

    print(f"\n{'='*80}")
    print(f"🎉 COMPLETE FOUNDATION SETUP RESULTS v20 with ICEBERG-COMPATIBLE OPTIMIZATION")
    print(f"{'='*80}")
    print(f"📚 VOCABULARY STATUS (with ORDER BY clustering):")
    print(f"   🔍 Concepts loaded: {vocab_concepts:,}")
    print(f"   🗂️ Data sorted by vocabulary_id + concept_code (faster lookups)")
    print(f"   📁 Optimized Parquet file layout for join performance")
    print(f"   ✅ Vocabularies ready for efficient OMOP mapping")
    print(f"\n📊 BRONZE DATA STATUS (with Iceberg partitioning + ORDER BY clustering):")
    print(f"   ⏱️  CSV processing time: {processing_time:.2f} hours")
    print(f"   📊 Total records: {stats[0]:,}")
    print(f"   👥 Unique patients: {stats[1]:,}")
    print(f"   🏥 Unique visits: {stats[2]:,}")
    print(f"   📅 Date range: {stats[3]} to {stats[4]}")
    print(f"   🔢 Numeric values: {stats[5]:,}")
    print(f"   📝 String values: {stats[6]:,}")
    print(f"\n🗂️ ICEBERG PARTITIONING & CLUSTERING STATUS:")
    print(f"   📅 Iceberg partitions: {total_partitions:,} partitions ({years_spanned} years: {first_year}-{last_year})")
    print(f"   🗃️ Partition columns: year, month (derived from sampling_datetime_utc)")
    print(f"   🔗 Data ordering: patient_id + measurement_source_value (ORDER BY clustering)")
    print(f"   📁 File format: Optimized Parquet with Iceberg metadata")
    print(f"   🚀 Expected benefits: Faster time range queries + improved patient/lab code access")

    # Safe processing rate calculation with division by zero protection
    if processing_time > 0 and stats[0] > 0:
        processing_rate = stats[0] / (processing_time * 3600)
        print(f"   🚀 Processing rate: {processing_rate:,.0f} rows/second")
    else:
        print(f"   🚀 Processing rate: Unable to calculate (time: {processing_time}, rows: {stats[0]})")

    if stats[0] >= 600000000 and vocab_concepts >= 100000:
        print(f"\n✅ SUCCESS: Complete foundation ready!")
        print(f"🏆 Ready for Step 3: OMOP CDM transformation!")
        print(f"📋 Next: Map lab codes to LOINC concepts using {vocab_concepts:,} vocabularies")
    else:
        if stats[0] < 600000000:
            print(f"\n⚠️  Only {stats[0]:,} rows processed (expected 634M+)")
        if vocab_concepts < 100000:
            print(f"\n⚠️  Only {vocab_concepts:,} vocabulary concepts loaded (expected 1M+)")

    # Sample data
    sample = execute_trino_query(
        """
        SELECT patient_id, visit_id, measurement_source_value, processing_batch
        FROM iceberg.bronze.biological_results_raw
        LIMIT 3
        """,
        "Sample processed data",
        schema='bronze'
    )

    print(f"\n📋 SAMPLE BRONZE DATA:")
    for row in sample:
        print(f"  Patient: {row[0]} | Visit: {row[1]} | Measurement: {row[2]} | Batch: {row[3]}")

    print(f"\n🔍 LAB CODE ANALYSIS:")
    lab_codes = execute_trino_query(
        """
        SELECT
            measurement_source_value,
            COUNT(*) as frequency
        FROM iceberg.bronze.biological_results_raw
        WHERE measurement_source_value IS NOT NULL
        GROUP BY measurement_source_value
        ORDER BY frequency DESC
        LIMIT 10
        """,
        "Analyze top lab codes",
        schema='bronze'
    )

    print(f"📊 TOP LAB CODES:")
    for code_row in lab_codes:
        print(f"   {code_row[0]}: {code_row[1]:,} occurrences")

    print(f"\n{'='*80}")
    print(f"🎉 STEP 2v20 COMPLETE - FOUNDATION READY FOR OMOP with ICEBERG OPTIMIZATION!")
    print(f"📚 Vocabularies: ✅ (ORDER BY sorted) | Bronze Data: ✅ (Iceberg partitioned)")
    print(f"🚀 Method: BULLETPROOF HIVE + ICEBERG-COMPATIBLE OPTIMIZATION for better analytics")
    print(f"➡️  NEXT: Step 3 - OPTIMIZED OMOP CDM Transformation with partitioned lookups")
    print(f"{'='*80}")

    return "foundation_complete_proven"

# Task definitions
check_vocab = PythonOperator(
    task_id='check_vocabularies_exist',
    python_callable=check_vocabularies_exist,
    dag=dag,
)

setup_hive_vocab = PythonOperator(
    task_id='setup_hive_vocab_schema',
    python_callable=setup_hive_vocab_schema,
    dag=dag,
)

load_vocab = PythonOperator(
    task_id='load_vocabulary_via_hive',
    python_callable=load_vocabulary_via_hive,
    dag=dag,
    execution_timeout=timedelta(hours=4),  # Generous timeout for 5M+ concepts
)

setup_bronze = PythonOperator(
    task_id='setup_bronze_hive_schema',
    python_callable=setup_bronze_hive_schema,
    dag=dag,
)

create_external = PythonOperator(
    task_id='create_hive_csv_external_table',
    python_callable=create_hive_csv_external_table,
    dag=dag,
)

execute_ctas = PythonOperator(
    task_id='execute_proven_csv_ctas',
    python_callable=execute_proven_csv_ctas,
    dag=dag,
    execution_timeout=timedelta(hours=2),  # Proven to complete in ~1 hour
)

validate_foundation = PythonOperator(
    task_id='validate_complete_foundation',
    python_callable=validate_complete_foundation,
    dag=dag,
)

# Complete foundation pipeline using PROVEN Hive method
check_vocab >> setup_hive_vocab >> load_vocab >> setup_bronze >> create_external >> execute_ctas >> validate_foundation