-- models/silver/omop_measurement_fixed.sql
{{
  config(
    materialized='table',
    full_refresh=true
  )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_biological_results') }}
),

persons AS (
    SELECT * FROM {{ ref('omop_person') }}
),

measurements AS (
    SELECT
        s.*,
        p.person_id
    FROM staging s
    JOIN persons p ON s.person_id_hash = p.person_source_value
    WHERE s.value_as_number IS NOT NULL  -- Only numeric measurements
       OR s.value_as_string IS NOT NULL
),

concept_mapped AS (
    SELECT
        m.*,

        -- Map measurement to LOINC concepts
        CASE
            WHEN m.measurement_source_value = 'LC:0007' THEN 4182210  -- Hemoglobin
            WHEN m.measurement_source_value = 'LC:0010' THEN 4143345  -- White blood cells
            WHEN m.measurement_source_value = 'LC:0001' THEN 4263235  -- Glucose
            WHEN m.measurement_source_value = 'LC:0012' THEN 4143876  -- Red blood cells
            WHEN m.measurement_source_value = 'LC:0024' THEN 4024659  -- Bacteria identification
            ELSE 4124662  -- Generic laboratory test
        END AS measurement_concept_id,

        -- Map units to concepts (simplified)
        CASE
            WHEN m.unit_source_value = '%' THEN 8554        -- percent
            WHEN m.unit_source_value = 'g/dL' THEN 8713     -- gram per deciliter
            WHEN m.unit_source_value = 'U/L' THEN 8645      -- unit per liter
            WHEN m.unit_source_value = '/μL' THEN 8647      -- per microliter
            WHEN m.unit_source_value = 'mg/dL' THEN 8840    -- milligram per deciliter
            ELSE 0
        END AS unit_concept_id,

        -- Measurement type concept
        32856 AS measurement_type_concept_id,  -- Lab result

        -- Operator concept for qualifiers
        CASE
            WHEN m.value_qualifier = '>' THEN 4171756   -- Greater than
            WHEN m.value_qualifier = '<' THEN 4171754   -- Less than
            WHEN m.value_qualifier = '>=' THEN 4171755  -- Greater than or equal to
            WHEN m.value_qualifier = '<=' THEN 4171753  -- Less than or equal to
            ELSE 0
        END AS operator_concept_id,

        -- Generate visit_occurrence_id
        ROW_NUMBER() OVER (
            PARTITION BY m.person_id_hash, m.visit_date
            ORDER BY m.measurement_datetime
        ) AS visit_occurrence_id

    FROM measurements m
)

SELECT
    -- Generate unique measurement_id
    ROW_NUMBER() OVER (ORDER BY person_id, measurement_datetime) AS measurement_id,

    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_type_concept_id,
    operator_concept_id,

    -- Values
    CASE
        WHEN value_as_number IS NOT NULL THEN value_as_number
        ELSE NULL
    END AS value_as_number,

    CASE
        WHEN value_as_string IS NOT NULL THEN 0  -- No concept for string values yet
        ELSE 0
    END AS value_as_concept_id,

    unit_concept_id,

    -- Reference ranges (null for now)
    NULL AS range_low,
    NULL AS range_high,

    -- Provider and visit
    NULL AS provider_id,
    visit_occurrence_id,
    NULL AS visit_detail_id,

    -- Source values
    measurement_source_value,
    0 AS measurement_source_concept_id,
    unit_source_value,
    COALESCE(value_as_string, CAST(value_as_number AS VARCHAR)) AS value_source_value

FROM concept_mapped
WHERE measurement_concept_id IS NOT NULL