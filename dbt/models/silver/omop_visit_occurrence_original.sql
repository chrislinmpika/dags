{{
  config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS {{ this }}",
    partition_by=['year(visit_start_date)', 'bucket(person_id, 64)'],
    sorted_by=['person_id', 'visit_start_date'],
    post_hook=[
        "CALL iceberg.system.expire_snapshots('{{ this.schema }}.{{ this.name }}', INTERVAL 7 DAY)",
        "CALL iceberg.system.remove_orphan_files('{{ this.schema }}.{{ this.name }}')"
    ]
  )
}}

/*
OMOP VISIT_OCCURRENCE Table - Healthcare Visits

Creates visit records from laboratory data by grouping measurements
that occur within the same healthcare encounter.

Since laboratory data doesn't explicitly contain visit information,
visits are inferred by:
- Grouping by patient + date + laboratory location
- Assuming measurements on the same day at same lab = same visit
- Setting visit type to outpatient (most lab work is outpatient)
*/

WITH source_visits AS (
    SELECT
        person_id,
        visit_id,
        visit_date,
        visit_rank,
        provider_id,
        laboratory_uuid,
        processing_batch_id,

        -- Group measurements into visits by date and location
        MIN(measurement_datetime) AS visit_start_datetime,
        MAX(measurement_datetime) AS visit_end_datetime,
        COUNT(*) AS measurement_count

    FROM {{ ref('stg_biological_results_enhanced') }}
    WHERE person_id IS NOT NULL
      AND visit_id IS NOT NULL
      AND data_quality_score >= 0.8
    GROUP BY
        person_id,
        visit_id,
        visit_date,
        visit_rank,
        provider_id,
        laboratory_uuid,
        processing_batch_id
),

person_mapped AS (
    SELECT
        sv.*,
        p.person_id AS omop_person_id
    FROM source_visits sv
    JOIN {{ ref('omop_person') }} p
        ON sv.person_id = p.person_id_hash
),

visits_final AS (
    SELECT
        -- Core OMOP VISIT_OCCURRENCE fields
        {{ generate_omop_id(['visit_id', 'person_id']) }} AS visit_occurrence_id,
        omop_person_id AS person_id,

        -- Visit classification
        9202 AS visit_concept_id,      -- Outpatient Visit
        DATE(visit_start_datetime) AS visit_start_date,
        visit_start_datetime,
        DATE(visit_end_datetime) AS visit_end_date,
        visit_end_datetime,
        32817 AS visit_type_concept_id,  -- EHR encounter record

        -- Provider and care site (derived from laboratory)
        CASE
            WHEN provider_id IS NOT NULL THEN
                {{ generate_omop_id(['provider_id']) }}
            ELSE NULL
        END AS provider_id,

        CASE
            WHEN laboratory_uuid IS NOT NULL THEN
                {{ generate_omop_id(['laboratory_uuid']) }}
            ELSE NULL
        END AS care_site_id,

        -- Source values
        visit_id AS visit_source_value,
        0 AS visit_source_concept_id,

        -- Additional context
        NULL AS admitted_from_concept_id,
        NULL AS admitted_from_source_value,
        NULL AS discharged_to_concept_id,
        NULL AS discharged_to_source_value,
        NULL AS preceding_visit_occurrence_id,

        -- Visit characteristics derived from lab data
        measurement_count,
        visit_rank,
        laboratory_uuid AS source_laboratory,

        -- GDPR audit fields
        processing_batch_id AS gdpr_processing_batch,
        CURRENT_TIMESTAMP AS created_at

    FROM person_mapped
)

SELECT
    -- Standard OMOP VISIT_OCCURRENCE fields
    visit_occurrence_id,
    person_id,
    visit_concept_id,
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    visit_type_concept_id,
    provider_id,
    care_site_id,
    visit_source_value,
    visit_source_concept_id,
    admitted_from_concept_id,
    admitted_from_source_value,
    discharged_to_concept_id,
    discharged_to_source_value,
    preceding_visit_occurrence_id,

    -- Extended fields for laboratory context
    measurement_count,
    visit_rank,
    source_laboratory,

    -- GDPR audit
    gdpr_processing_batch,
    created_at

FROM visits_final

-- Data quality validation
WHERE visit_start_date IS NOT NULL
  AND person_id IS NOT NULL
  AND {{ validate_omop_dates('visit_start_date') }}
  AND visit_start_date <= visit_end_date