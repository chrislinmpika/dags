-- models/silver/omop_measurement.sql
{{
    config(
        materialized='incremental',
        unique_key='measurement_id',
        incremental_strategy='merge',
        partition_by=['month(measurement_date)', 'bucket(person_id, 16)'],
        sorted_by=['person_id', 'measurement_date']
    )
}}

WITH staging AS (
    SELECT * FROM {{ ref('stg_biological_results') }}
    
    {% if is_incremental() %}
    -- Ne traiter que les nouvelles données
    WHERE load_timestamp > (SELECT MAX(load_timestamp) FROM {{ this }})
    {% endif %}
),

transformed AS (
    SELECT
        -- Générer measurement_id (hash MD5)
        MD5(CAST(CONCAT(visit_id, patient_id, COALESCE(report_id, '')) AS VARBINARY)) AS measurement_id,
        
        -- Person
        patient_id AS person_id,
        
        -- Measurement concept (0 = unmapped pour l'instant)
        0 AS measurement_concept_id,
        
        -- Dates
        CAST(visit_datetime AS DATE) AS measurement_date,
        visit_datetime AS measurement_datetime,
        
        -- Type concept (32856 = Lab)
        32856 AS measurement_type_concept_id,
        
        -- Operator (NULL pour l'instant)
        CAST(NULL AS INTEGER) AS operator_concept_id,
        
        -- Values (NULL pour l'instant, à enrichir plus tard)
        CAST(NULL AS DOUBLE) AS value_as_number,
        CAST(NULL AS INTEGER) AS value_as_concept_id,
        
        -- Unit (NULL pour l'instant)
        CAST(NULL AS INTEGER) AS unit_concept_id,
        CAST(NULL AS DOUBLE) AS range_low,
        CAST(NULL AS DOUBLE) AS range_high,
        
        -- Provider (laboratoire)
        laboratory_uuid AS provider_id,
        
        -- Visit
        visit_id AS visit_occurrence_id,
        CAST(NULL AS VARCHAR) AS visit_detail_id,
        
        -- Source values
        report_id AS measurement_source_value,
        0 AS measurement_source_concept_id,
        CAST(NULL AS VARCHAR) AS unit_source_value,
        CAST(NULL AS VARCHAR) AS value_source_value,
        
        -- Metadata
        load_timestamp
        
    FROM staging
)

SELECT * FROM transformed
