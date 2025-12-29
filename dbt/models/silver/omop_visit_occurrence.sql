-- models/silver/omop_visit_occurrence_fixed.sql
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

visit_data AS (
    SELECT
        s.person_id_hash,
        p.person_id,
        s.visit_id,
        s.visit_date,
        MIN(s.measurement_datetime) AS visit_start_datetime,
        MAX(s.measurement_datetime) AS visit_end_datetime,
        COUNT(*) AS measurement_count,
        s.provider_id,
        s.laboratory_uuid
    FROM staging s
    JOIN persons p ON s.person_id_hash = p.person_source_value
    WHERE s.visit_date IS NOT NULL
    GROUP BY s.person_id_hash, p.person_id, s.visit_id, s.visit_date, s.provider_id, s.laboratory_uuid
)

SELECT
    -- Generate unique visit_occurrence_id
    ROW_NUMBER() OVER (ORDER BY person_id, visit_date, visit_start_datetime) AS visit_occurrence_id,

    person_id,

    -- Visit concept (outpatient visit for lab encounters)
    9202 AS visit_concept_id,  -- Outpatient visit

    -- Visit dates
    visit_date AS visit_start_date,
    CAST(visit_start_datetime AS TIMESTAMP) AS visit_start_datetime,
    visit_date AS visit_end_date,
    CAST(visit_end_datetime AS TIMESTAMP) AS visit_end_datetime,

    -- Visit type concept (EHR encounter record)
    32817 AS visit_type_concept_id,

    -- Provider and care site
    NULL AS provider_id,  -- Anonymized
    NULL AS care_site_id,

    -- Source values
    visit_id AS visit_source_value,
    0 AS visit_source_concept_id,

    -- Admission/discharge
    NULL AS admitted_from_concept_id,
    NULL AS admitted_from_source_value,
    NULL AS discharged_to_concept_id,
    NULL AS discharged_to_source_value,

    -- Preceding visit
    NULL AS preceding_visit_occurrence_id

FROM visit_data