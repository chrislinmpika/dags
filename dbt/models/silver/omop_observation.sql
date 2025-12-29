-- models/silver/omop_observation_fixed.sql
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

visits AS (
    SELECT * FROM {{ ref('omop_visit_occurrence') }}
),

observations AS (
    SELECT
        s.*,
        p.person_id,
        v.visit_occurrence_id
    FROM staging s
    JOIN persons p ON s.person_id_hash = p.person_source_value
    LEFT JOIN visits v ON p.person_id = v.person_id
                       AND s.visit_date = v.visit_start_date
    WHERE s.value_as_string IS NOT NULL  -- Categorical/qualitative results
       OR s.bacterium_id IS NOT NULL    -- Microbiology results
       OR s.normality IS NOT NULL       -- Normal/abnormal flags
),

concept_mapped AS (
    SELECT
        o.*,

        -- Map observation concepts
        CASE
            WHEN o.bacterium_id IS NOT NULL THEN 4024659      -- Bacteria identification
            WHEN o.normality = 'NORMAL' THEN 4069590          -- Normal finding
            WHEN o.normality = 'ABNORMAL' THEN 4135493        -- Abnormal finding
            WHEN o.abnormal_flag = 'H' THEN 4328749           -- High
            WHEN o.abnormal_flag = 'L' THEN 4267147           -- Low
            ELSE 4124662  -- Generic observation
        END AS observation_concept_id,

        -- Value concepts
        CASE
            WHEN o.normality = 'NORMAL' THEN 4069590
            WHEN o.normality = 'ABNORMAL' THEN 4135493
            WHEN o.bacterium_id IS NOT NULL THEN 0  -- Would need microbiology vocabulary
            ELSE 0
        END AS value_as_concept_id,

        -- Observation type concept
        32817 AS observation_type_concept_id  -- EHR

    FROM observations o
)

SELECT
    -- Generate unique observation_id
    ROW_NUMBER() OVER (ORDER BY person_id, measurement_datetime) AS observation_id,

    person_id,
    observation_concept_id,
    measurement_date AS observation_date,
    measurement_datetime AS observation_datetime,
    observation_type_concept_id,

    -- Values
    CASE
        WHEN value_as_number IS NOT NULL THEN value_as_number
        ELSE NULL
    END AS value_as_number,

    value_as_concept_id,

    -- Qualifiers
    CASE
        WHEN abnormal_flag IN ('H', 'L') THEN
            CASE abnormal_flag
                WHEN 'H' THEN 4328749  -- High
                WHEN 'L' THEN 4267147  -- Low
                ELSE 0
            END
        ELSE 0
    END AS qualifier_concept_id,

    -- Units (for numeric observations)
    CASE
        WHEN unit_source_value = '%' THEN 8554        -- percent
        WHEN unit_source_value = 'g/dL' THEN 8713     -- gram per deciliter
        ELSE 0
    END AS unit_concept_id,

    -- Provider and visit
    NULL AS provider_id,
    visit_occurrence_id,
    NULL AS visit_detail_id,

    -- Source values
    measurement_source_value AS observation_source_value,
    0 AS observation_source_concept_id,
    unit_source_value,
    COALESCE(value_as_string, bacterium_id, normality, abnormal_flag) AS value_source_value

FROM concept_mapped
WHERE observation_concept_id IS NOT NULL