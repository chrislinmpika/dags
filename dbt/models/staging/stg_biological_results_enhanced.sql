{{
  config(
    materialized='view',
    docs={
      "show": true
    }
  )
}}

/*
Enhanced Biological Results Staging Model

This staging model prepares the bronze biological results data for OMOP transformation by:
- Cleaning and validating data quality
- Adding GDPR compliance fields
- Generating unique identifiers
- Standardizing value types and units
- Calculating data quality scores

This model serves as the foundation for all downstream OMOP CDM tables.
*/

WITH source_data AS (
    SELECT *
    FROM {{ source('staging', 'biological_results_enhanced') }}
    WHERE load_timestamp >= CURRENT_DATE - INTERVAL 8 DAY  -- Last week's data for full rebuild
),

enhanced_data AS (
    SELECT
        -- Core identifiers
        measurement_id,
        person_id AS original_person_id,
        person_id_hash,  -- GDPR anonymized patient ID
        gdpr_original_hash,  -- For audit trail

        -- Measurement details
        measurement_source_value,
        measurement_datetime,
        measurement_date,

        -- Value fields with cleaning
        CASE
            WHEN value_as_number IS NOT NULL
                 AND value_as_number BETWEEN -1000000 AND 1000000
            THEN value_as_number
            ELSE NULL
        END AS value_as_number,

        CASE
            WHEN LENGTH(TRIM(value_as_string)) > 0
            THEN TRIM(value_as_string)
            ELSE NULL
        END AS value_as_string,

        CASE
            WHEN LENGTH(TRIM(value_qualifier)) > 0
            THEN TRIM(value_qualifier)
            ELSE NULL
        END AS value_qualifier,

        -- Unit standardization
        CASE
            WHEN TRIM(unit_source_value) IN ('', 'NULL', 'null') THEN NULL
            ELSE TRIM(unit_source_value)
        END AS unit_source_value,

        -- Normality and flags
        {{ standardize_normality('normality', 'value_as_number') }} AS normality,

        CASE
            WHEN UPPER(abnormal_flag) IN ('H', 'HIGH') THEN 'HIGH'
            WHEN UPPER(abnormal_flag) IN ('L', 'LOW') THEN 'LOW'
            WHEN UPPER(abnormal_flag) IN ('N', 'NORMAL') THEN 'NORMAL'
            ELSE abnormal_flag
        END AS abnormal_flag,

        -- Visit and provider context
        visit_id,
        visit_date,
        provider_id,
        laboratory_uuid,

        -- Enhanced classification
        value_type,
        bacterium_id,
        omop_target_domain,

        -- Data quality scoring (enhanced)
        GREATEST(0.0, LEAST(1.0,
            data_quality_score
            -- Boost quality for complete records
            + CASE WHEN unit_source_value IS NOT NULL AND value_as_number IS NOT NULL THEN 0.1 ELSE 0.0 END
            -- Penalize very old or future dates
            - CASE
                WHEN measurement_date < DATE '2000-01-01' OR measurement_date > CURRENT_DATE THEN 0.3
                ELSE 0.0
              END
        )) AS data_quality_score,

        -- Audit trail
        processing_batch_id,
        source_file,
        load_timestamp

    FROM source_data
    WHERE person_id_hash IS NOT NULL  -- Must have anonymized patient ID
      AND measurement_source_value IS NOT NULL  -- Must have measurement code
),

visit_enhanced AS (
    SELECT
        ed.*,

        -- Visit rank for patient (chronological ordering)
        ROW_NUMBER() OVER (
            PARTITION BY person_id_hash
            ORDER BY visit_date ASC, measurement_datetime ASC
        ) AS visit_rank,

        -- Measurement sequence within visit
        ROW_NUMBER() OVER (
            PARTITION BY person_id_hash, visit_id
            ORDER BY measurement_datetime ASC
        ) AS measurement_sequence,

        -- Daily measurement count per patient
        COUNT(*) OVER (
            PARTITION BY person_id_hash, measurement_date
        ) AS daily_measurement_count

    FROM enhanced_data ed
),

final AS (
    SELECT
        -- Core measurement identifiers
        measurement_id,
        person_id_hash AS person_id,  -- Use anonymized ID consistently
        measurement_source_value,
        measurement_datetime,
        measurement_date,

        -- Value fields
        value_as_number,
        value_as_string,
        value_qualifier,
        unit_source_value,
        normality,
        abnormal_flag,

        -- Visit context
        visit_id,
        visit_date,
        visit_rank,
        measurement_sequence,
        daily_measurement_count,

        -- Provider and location
        provider_id,
        laboratory_uuid,

        -- Enhanced fields
        value_type,
        bacterium_id,
        omop_target_domain,
        data_quality_score,

        -- Range values (derived from normality and value)
        CASE
            WHEN value_type = 'NUMERIC' AND normality = 'NORMAL' AND value_as_number IS NOT NULL THEN
                value_as_number * 0.8  -- Estimate low range as 80% of normal value
            ELSE NULL
        END AS estimated_range_low,

        CASE
            WHEN value_type = 'NUMERIC' AND normality = 'NORMAL' AND value_as_number IS NOT NULL THEN
                value_as_number * 1.2  -- Estimate high range as 120% of normal value
            ELSE NULL
        END AS estimated_range_high,

        -- GDPR and audit
        gdpr_original_hash,
        processing_batch_id,
        source_file,
        load_timestamp,

        -- Additional derived fields
        CASE
            WHEN measurement_date IS NOT NULL THEN
                EXTRACT(YEAR FROM measurement_date)
            ELSE NULL
        END AS measurement_year,

        CASE
            WHEN measurement_datetime IS NOT NULL THEN
                EXTRACT(HOUR FROM measurement_datetime)
            ELSE NULL
        END AS measurement_hour

    FROM visit_enhanced

    -- Data quality filter
    WHERE data_quality_score >= {{ var('min_data_quality_score', 0.6) }}
      AND measurement_date BETWEEN DATE '2000-01-01' AND CURRENT_DATE
      AND person_id IS NOT NULL
)

SELECT * FROM final