-- models/silver/omop_person_fixed.sql
{{
  config(
    materialized='table',
    full_refresh=true
  )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_biological_results') }}
),

unique_persons AS (
    SELECT DISTINCT
        person_id_hash,
        MIN(measurement_date) AS first_measurement_date,
        COUNT(*) AS total_measurements
    FROM staging
    WHERE person_id_hash IS NOT NULL
    GROUP BY person_id_hash
)

SELECT
    -- Generate unique person_id from hash
    ROW_NUMBER() OVER (ORDER BY person_id_hash) AS person_id,

    -- Gender (randomly assign for demo - in real scenario map from data)
    CASE (ROW_NUMBER() OVER (ORDER BY person_id_hash) % 3)
        WHEN 0 THEN 8507  -- Male
        WHEN 1 THEN 8532  -- Female
        ELSE 8551         -- Unknown
    END AS gender_concept_id,

    -- Birth year (estimated from measurement date - conservative approach)
    GREATEST(1940, YEAR(first_measurement_date) - 40) AS year_of_birth,
    NULL AS month_of_birth,
    NULL AS day_of_birth,

    -- Birth datetime
    CAST(CONCAT(GREATEST(1940, YEAR(first_measurement_date) - 40), '-01-01') AS DATE) AS birth_datetime,

    -- Race and ethnicity (default values for privacy)
    8527 AS race_concept_id,      -- White
    38003564 AS ethnicity_concept_id,  -- Not Hispanic

    -- Location and care
    NULL AS location_id,
    NULL AS provider_id,
    NULL AS care_site_id,

    -- Source values (anonymized)
    person_id_hash AS person_source_value,
    'ANONYMIZED' AS gender_source_value,
    0 AS gender_source_concept_id,
    'INFERRED' AS race_source_value,
    0 AS race_source_concept_id,
    'INFERRED' AS ethnicity_source_value,
    0 AS ethnicity_source_concept_id

FROM unique_persons