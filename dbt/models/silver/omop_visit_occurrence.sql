-- models/silver/omop_visit_occurrence.sql
{{
    config(
        materialized='incremental',
        unique_key='visit_occurrence_id',
        incremental_strategy='merge',
        partition_by=['year(visit_start_date)', 'bucket(person_id, 16)'],
        sorted_by=['person_id', 'visit_start_date']
    )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_biological_results') }}
    
    {% if is_incremental() %}
    WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
    {% endif %}
),

visits AS (
    SELECT DISTINCT
        visit_id,
        patient_id,
        visit_datetime,
        site_laboratory_uuid,
        load_timestamp
    FROM staging
),

transformed AS (
    SELECT
        -- Visit ID
        visit_id AS visit_occurrence_id,
        
        -- Person
        patient_id AS person_id,
        
        -- Visit concept (9202 = Outpatient Visit)
        9202 AS visit_concept_id,
        
        -- Dates (start = end pour un résultat biologique)
        CAST(visit_datetime AS DATE) AS visit_start_date,
        visit_datetime AS visit_start_datetime,
        CAST(visit_datetime AS DATE) AS visit_end_date,
        visit_datetime AS visit_end_datetime,
        
        -- Type concept (32817 = EHR)
        32817 AS visit_type_concept_id,
        
        -- Provider & Care site
        CAST(NULL AS VARCHAR) AS provider_id,
        site_laboratory_uuid AS care_site_id,
        
        -- Source values
        visit_id AS visit_source_value,
        0 AS visit_source_concept_id,
        
        -- Metadata
        load_timestamp
        
    FROM visits
)

SELECT * FROM transformed
