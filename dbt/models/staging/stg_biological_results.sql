-- models/staging/stg_biological_results_fixed.sql
{{
    config(
        materialized='table',
        partition_by=['measurement_date'],
        format='iceberg'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('staging', 'biological_results_enhanced') }}
),

cleaned AS (
    SELECT
        -- IDs
        visit_id,
        person_id_hash,
        measurement_id,

        -- Dates
        measurement_datetime,
        measurement_date,
        visit_date,

        -- Measurement details
        measurement_source_value,
        value_as_number,
        value_as_string,
        value_qualifier,
        unit_source_value,
        normality,
        abnormal_flag,
        value_type,
        bacterium_id,

        -- Context
        provider_id,
        laboratory_uuid,
        data_quality_score,
        omop_target_domain,

        -- GDPR audit
        processing_batch_id,
        gdpr_original_hash,
        source_file,
        load_timestamp

    FROM source

    -- Filter invalid rows
    WHERE visit_id IS NOT NULL
      AND person_id_hash IS NOT NULL
      AND measurement_datetime IS NOT NULL
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY visit_id, person_id_hash, measurement_id
            ORDER BY load_timestamp DESC
        ) AS row_num
    FROM cleaned
)

SELECT
    visit_id,
    person_id_hash,
    measurement_id,
    measurement_datetime,
    measurement_date,
    visit_date,
    measurement_source_value,
    value_as_number,
    value_as_string,
    value_qualifier,
    unit_source_value,
    normality,
    abnormal_flag,
    value_type,
    bacterium_id,
    provider_id,
    laboratory_uuid,
    data_quality_score,
    omop_target_domain,
    processing_batch_id,
    gdpr_original_hash,
    source_file,
    load_timestamp
FROM deduplicated
WHERE row_num = 1