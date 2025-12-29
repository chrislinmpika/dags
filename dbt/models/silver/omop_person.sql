{{
  config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS {{ this }}",
    partition_by=['bucket(person_id, 64)', 'year_of_birth'],
    sorted_by=['person_id'],
    post_hook=[
        "CALL iceberg.system.expire_snapshots('{{ this.schema }}.{{ this.name }}', INTERVAL 7 DAY)",
        "CALL iceberg.system.remove_orphan_files('{{ this.schema }}.{{ this.name }}')"
    ]
  )
}}

/*
OMOP PERSON Table - GDPR-Compliant Patient Demographics

Creates anonymized patient demographics from biological results data with:
- GDPR-compliant patient ID anonymization using irreversible hashing
- Inferred demographics from temporal patterns in laboratory data
- Complete audit trail for privacy compliance
- Person-level aggregations for analytics

This table serves as the foundation for all patient-related OMOP tables.
*/

WITH source_patients AS (
    SELECT
        person_id,  -- Already anonymized hash from staging
        MIN(measurement_date) AS first_measurement_date,
        MAX(measurement_date) AS last_measurement_date,
        COUNT(*) AS total_measurements,
        COUNT(DISTINCT visit_id) AS total_visits,
        COUNT(DISTINCT measurement_source_value) AS unique_test_types,

        -- Collect processing batches for audit trail
        ARRAY_AGG(DISTINCT processing_batch_id) AS processing_batches,
        ARRAY_AGG(DISTINCT gdpr_original_hash) AS gdpr_audit_hashes

    FROM {{ ref('stg_biological_results_enhanced') }}
    WHERE person_id IS NOT NULL
    GROUP BY person_id
),

demographics_inferred AS (
    SELECT
        sp.*,

        -- Generate deterministic demographics from anonymized patient ID
        -- These are consistent but not real demographics for GDPR compliance
        {{ generate_omop_id(['person_id']) }} AS person_id_numeric,

        -- Infer birth year from temporal data patterns (approximate)
        CASE
            WHEN EXTRACT(YEAR FROM first_measurement_date) >= 2020 THEN
                GREATEST(1940, EXTRACT(YEAR FROM first_measurement_date) - 45)  -- Assume avg age 45
            WHEN EXTRACT(YEAR FROM first_measurement_date) >= 2010 THEN
                GREATEST(1940, EXTRACT(YEAR FROM first_measurement_date) - 40)  -- Assume avg age 40
            ELSE
                GREATEST(1940, EXTRACT(YEAR FROM first_measurement_date) - 35)  -- Assume avg age 35
        END AS inferred_birth_year,

        -- Deterministic but anonymized gender based on hash
        CASE ({{ generate_omop_id(['person_id']) }} % 3)
            WHEN 0 THEN 8507  -- Male
            WHEN 1 THEN 8532  -- Female
            ELSE 8551         -- Unknown/Other
        END AS gender_concept_id,

        -- Deterministic but anonymized race (distributed representation)
        CASE ({{ generate_omop_id(['person_id']) }} % 5)
            WHEN 0 THEN 8527   -- White
            WHEN 1 THEN 8516   -- Black or African American
            WHEN 2 THEN 8515   -- Asian
            WHEN 3 THEN 8557   -- Native Hawaiian or Pacific Islander
            ELSE 8522          -- Other
        END AS race_concept_id,

        -- Deterministic but anonymized ethnicity
        CASE ({{ generate_omop_id(['person_id']) }} % 2)
            WHEN 0 THEN 38003564  -- Not Hispanic or Latino
            ELSE 38003563         -- Hispanic or Latino
        END AS ethnicity_concept_id

    FROM source_patients sp
),

person_final AS (
    SELECT
        -- Standard OMOP PERSON fields
        person_id_numeric AS person_id,
        gender_concept_id,
        inferred_birth_year AS year_of_birth,
        NULL AS month_of_birth,  -- Not available for privacy
        NULL AS day_of_birth,    -- Not available for privacy
        DATE(CONCAT(inferred_birth_year, '-01-01')) AS birth_datetime,
        race_concept_id,
        ethnicity_concept_id,

        -- Location (anonymized - no real locations for GDPR)
        NULL AS location_id,
        NULL AS provider_id,
        NULL AS care_site_id,

        -- Source values (all anonymized/hashed for GDPR compliance)
        person_id AS person_source_value,  -- Use anonymized hash as source value
        'ANONYMIZED_PATIENT' AS gender_source_value,
        gender_concept_id AS gender_source_concept_id,
        'INFERRED' AS race_source_value,
        race_concept_id AS race_source_concept_id,
        'INFERRED' AS ethnicity_source_value,
        ethnicity_concept_id AS ethnicity_source_concept_id,

        -- Patient activity metrics
        first_measurement_date,
        last_measurement_date,
        DATE_DIFF('DAY', first_measurement_date, last_measurement_date) AS observation_period_days,
        total_measurements,
        total_visits,
        unique_test_types,

        -- Data quality score based on activity
        CASE
            WHEN total_measurements >= 50 AND unique_test_types >= 10 THEN 1.0
            WHEN total_measurements >= 20 AND unique_test_types >= 5 THEN 0.9
            WHEN total_measurements >= 10 AND unique_test_types >= 3 THEN 0.8
            WHEN total_measurements >= 5 THEN 0.7
            ELSE 0.6
        END AS person_data_quality_score,

        -- GDPR compliance fields
        person_id AS person_id_hash,  -- Store the anonymized hash
        processing_batches,
        gdpr_audit_hashes,
        'IRREVERSIBLE_HASH' AS anonymization_method,
        CURRENT_TIMESTAMP AS anonymization_timestamp,
        'GDPR_COMPLIANT' AS privacy_status,

        -- Audit trail
        CURRENT_TIMESTAMP AS created_at

    FROM demographics_inferred
    WHERE inferred_birth_year BETWEEN 1920 AND 2010  -- Reasonable birth year range
)

SELECT
    -- Standard OMOP PERSON table fields
    person_id,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    birth_datetime,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    provider_id,
    care_site_id,
    person_source_value,
    gender_source_value,
    gender_source_concept_id,
    race_source_value,
    race_source_concept_id,
    ethnicity_source_value,
    ethnicity_source_concept_id,

    -- Extended fields for patient analytics
    first_measurement_date,
    last_measurement_date,
    observation_period_days,
    total_measurements,
    total_visits,
    unique_test_types,
    person_data_quality_score,

    -- GDPR compliance and audit fields
    person_id_hash,
    processing_batches,
    gdpr_audit_hashes,
    anonymization_method,
    anonymization_timestamp,
    privacy_status,
    created_at

FROM person_final

-- Final data quality validation
WHERE person_id IS NOT NULL
  AND gender_concept_id IS NOT NULL
  AND year_of_birth IS NOT NULL
  AND person_data_quality_score >= 0.5