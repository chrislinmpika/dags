{{
  config(
    materialized='table',
    pre_hook="DROP TABLE IF EXISTS {{ this }}",
    partition_by=['year(observation_date)', 'bucket(person_id, 128)'],
    sorted_by=['person_id', 'observation_date', 'observation_concept_id'],
    post_hook=[
        "CALL iceberg.system.expire_snapshots('{{ this.schema }}.{{ this.name }}', INTERVAL 7 DAY)",
        "CALL iceberg.system.remove_orphan_files('{{ this.schema }}.{{ this.name }}')"
    ]
  )
}}

/*
OMOP OBSERVATION Table - Categorical Laboratory Results

Transforms categorical, qualitative, and descriptive laboratory results
into OMOP CDM v6.0 compliant format including:
- Microscopy results (cell morphology, bacteria identification)
- Categorical test results (positive/negative, reactive/non-reactive)
- Antibiogram results (bacterial sensitivities)
- Quality indicators and normality flags

This table complements the MEASUREMENT table by capturing
non-numeric laboratory findings.
*/

WITH source_observations AS (
    SELECT *
    FROM {{ ref('stg_biological_results_enhanced') }}
    WHERE omop_target_domain = 'OBSERVATION'  -- Categorical/qualitative results
      AND (
          value_as_string IS NOT NULL
          OR bacterium_id IS NOT NULL
          OR value_qualifier IS NOT NULL
      )
      AND data_quality_score >= 0.7  -- Allow slightly lower quality for observations
),

concept_mapped AS (
    SELECT
        so.*,

        -- Map observation to appropriate LOINC concept
        COALESCE(
            -- Use laboratory mapping if available
            CASE WHEN lm.mapping_confidence >= 0.7 THEN lm.target_concept_id END,
            -- Pattern-based mapping for common observations
            {{ map_to_loinc('measurement_source_value') }}
        ) AS observation_concept_id,

        -- Map categorical values to standard concepts
        {{ map_categorical_value('value_as_string', 'value_qualifier') }} AS value_as_concept_id,

        -- Observation type
        32856 AS observation_type_concept_id,  -- Lab result

        -- Qualifier concepts for additional context
        CASE
            WHEN normality = 'ABNORMAL' THEN 45878583  -- Abnormal
            WHEN normality = 'NORMAL' THEN 45884084    -- Normal
            ELSE 0
        END AS qualifier_concept_id,

        -- Unit concepts (rarely applicable for observations)
        0 AS unit_concept_id

    FROM source_observations so

    -- Join with laboratory code mappings
    LEFT JOIN {{ source('silver', 'concept_map_laboratory') }} lm
        ON so.measurement_source_value = lm.source_code
        AND lm.mapping_status = 'ACTIVE'
),

person_mapped AS (
    SELECT
        cm.*,
        p.person_id AS omop_person_id,
        vo.visit_occurrence_id
    FROM concept_mapped cm
    JOIN {{ ref('omop_person') }} p
        ON cm.person_id = p.person_id_hash
    LEFT JOIN {{ ref('omop_visit_occurrence') }} vo
        ON cm.visit_id = vo.visit_source_value
        AND p.person_id = vo.person_id
),

observations_final AS (
    SELECT
        -- Core OMOP OBSERVATION fields
        {{ generate_omop_id(['measurement_id']) }} AS observation_id,
        omop_person_id AS person_id,
        observation_concept_id,
        measurement_date AS observation_date,
        measurement_datetime AS observation_datetime,
        observation_type_concept_id,

        -- Value fields
        value_as_number,  -- Usually NULL for observations
        value_as_string,
        value_as_concept_id,

        -- Qualifier and unit
        qualifier_concept_id,
        unit_concept_id,

        -- Provider and visit context
        CASE
            WHEN provider_id IS NOT NULL THEN
                {{ generate_omop_id(['provider_id']) }}
            ELSE NULL
        END AS provider_id,

        visit_occurrence_id,
        NULL AS visit_detail_id,

        -- Source values for traceability
        measurement_source_value AS observation_source_value,
        0 AS observation_source_concept_id,
        value_as_string AS value_source_value,
        value_qualifier AS qualifier_source_value,
        unit_source_value AS unit_source_value,

        -- Event context
        NULL AS observation_event_id,
        NULL AS obs_event_field_concept_id,

        -- Quality and audit
        data_quality_score,
        normality AS result_normality,
        abnormal_flag,

        -- Special handling for microbiology results
        bacterium_id,
        CASE
            WHEN value_type = 'ANTIBIOGRAM' THEN 'ANTIBIOGRAM'
            WHEN value_type = 'CATEGORICAL' THEN 'CATEGORICAL'
            ELSE 'QUALITATIVE'
        END AS result_category,

        -- GDPR audit
        processing_batch_id AS gdpr_processing_batch,
        source_file AS data_source_file,
        load_timestamp AS source_load_timestamp

    FROM person_mapped
    WHERE observation_concept_id > 0  -- Must have valid concept mapping
)

SELECT
    -- Standard OMOP OBSERVATION table fields
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
    value_as_string,
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    observation_source_value,
    observation_source_concept_id,
    value_source_value,
    qualifier_source_value,
    unit_source_value,
    observation_event_id,
    obs_event_field_concept_id,

    -- Extended fields for laboratory observations
    data_quality_score,
    result_normality,
    abnormal_flag,
    bacterium_id,
    result_category,

    -- GDPR audit
    gdpr_processing_batch,
    data_source_file,
    source_load_timestamp,
    CURRENT_TIMESTAMP AS created_at

FROM observations_final

-- Data quality validation
WHERE observation_date BETWEEN DATE '2000-01-01' AND CURRENT_DATE
  AND person_id IS NOT NULL
  AND observation_concept_id > 0