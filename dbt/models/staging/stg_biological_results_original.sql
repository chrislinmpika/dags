-- models/staging/stg_biological_results.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('staging', 'biological_results_enhanced') }}
),

cleaned AS (
    SELECT
        -- IDs
        visit_id,
        patient_id,
        report_id,
        
        -- Dates (conversion string → timestamp)
        TRY_CAST(visit_date_utc AS TIMESTAMP(3)) AS visit_datetime,
        
        -- Rang de visite (conversion string → integer)
        TRY_CAST(visit_rank AS INTEGER) AS visit_rank,
        
        -- Laboratoires
        laboratory_uuid,
        sub_laboratory_uuid,
        site_laboratory_uuid,
        
        -- Metadata
        source_file,
        load_timestamp
        
    FROM source
    
    -- Filtrer les lignes invalides
    WHERE visit_id IS NOT NULL
      AND patient_id IS NOT NULL
      AND TRY_CAST(visit_date_utc AS TIMESTAMP(3)) IS NOT NULL
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY visit_id, patient_id, report_id 
            ORDER BY load_timestamp DESC
        ) AS row_num
    FROM cleaned
)

SELECT
    visit_id,
    patient_id,
    report_id,
    visit_datetime,
    visit_rank,
    laboratory_uuid,
    sub_laboratory_uuid,
    site_laboratory_uuid,
    source_file,
    load_timestamp
FROM deduplicated
WHERE row_num = 1
