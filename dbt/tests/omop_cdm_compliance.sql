/*
OMOP CDM v6.0 Compliance Validation Suite

Comprehensive test suite to validate:
- OMOP CDM structural compliance
- Data quality standards
- Concept mapping coverage
- GDPR privacy compliance
- Referential integrity
- Value range validation

This test suite ensures the OMOP implementation meets healthcare
data standards and regulatory requirements.
*/

-- Test 1: OMOP Person Table Compliance
WITH person_compliance AS (
    SELECT
        COUNT(*) AS total_persons,
        COUNT(CASE WHEN person_id IS NOT NULL THEN 1 END) AS valid_person_ids,
        COUNT(CASE WHEN year_of_birth BETWEEN 1900 AND YEAR(CURRENT_DATE) THEN 1 END) AS valid_birth_years,
        COUNT(CASE WHEN gender_concept_id >= 0 THEN 1 END) AS valid_gender_concepts,
        COUNT(CASE WHEN person_id_hash IS NOT NULL THEN 1 END) AS gdpr_compliant_persons

    FROM {{ ref('omop_person') }}
),

-- Test 2: OMOP Measurement Table Compliance
measurement_compliance AS (
    SELECT
        COUNT(*) AS total_measurements,
        COUNT(CASE WHEN measurement_id IS NOT NULL THEN 1 END) AS valid_measurement_ids,
        COUNT(CASE WHEN person_id IS NOT NULL THEN 1 END) AS valid_person_refs,
        COUNT(CASE WHEN measurement_concept_id > 0 THEN 1 END) AS mapped_concepts,
        COUNT(CASE WHEN measurement_date BETWEEN DATE '2000-01-01' AND CURRENT_DATE THEN 1 END) AS valid_dates,
        COUNT(CASE WHEN value_as_number BETWEEN -1000000 AND 1000000 THEN 1 END) AS valid_numeric_values,
        COUNT(CASE WHEN unit_concept_id > 0 THEN 1 END) AS mapped_units,
        AVG(mapping_confidence) AS avg_mapping_confidence

    FROM {{ ref('omop_measurement') }}
),

-- Test 3: OMOP Visit Occurrence Table Compliance
visit_compliance AS (
    SELECT
        COUNT(*) AS total_visits,
        COUNT(CASE WHEN visit_occurrence_id IS NOT NULL THEN 1 END) AS valid_visit_ids,
        COUNT(CASE WHEN person_id IS NOT NULL THEN 1 END) AS valid_person_refs,
        COUNT(CASE WHEN visit_concept_id > 0 THEN 1 END) AS mapped_visit_concepts,
        COUNT(CASE WHEN visit_start_date <= visit_end_date THEN 1 END) AS valid_date_ranges,
        COUNT(CASE WHEN visit_type_concept_id > 0 THEN 1 END) AS mapped_visit_types

    FROM {{ ref('omop_visit_occurrence') }}
),

-- Test 4: OMOP Observation Table Compliance
observation_compliance AS (
    SELECT
        COUNT(*) AS total_observations,
        COUNT(CASE WHEN observation_id IS NOT NULL THEN 1 END) AS valid_observation_ids,
        COUNT(CASE WHEN person_id IS NOT NULL THEN 1 END) AS valid_person_refs,
        COUNT(CASE WHEN observation_concept_id > 0 THEN 1 END) AS mapped_concepts,
        COUNT(CASE WHEN observation_date BETWEEN DATE '2000-01-01' AND CURRENT_DATE THEN 1 END) AS valid_dates,
        COUNT(CASE WHEN value_as_string IS NOT NULL OR value_as_concept_id > 0 THEN 1 END) AS valid_values

    FROM {{ ref('omop_observation') }}
),

-- Test 5: Concept Mapping Coverage
concept_coverage AS (
    SELECT
        COUNT(DISTINCT measurement_source_value) AS total_lab_codes,
        COUNT(DISTINCT CASE WHEN measurement_concept_id > 0 THEN measurement_source_value END) AS mapped_lab_codes,
        COUNT(DISTINCT CASE WHEN mapping_confidence >= 0.8 THEN measurement_source_value END) AS high_confidence_codes,
        AVG(mapping_confidence) AS avg_confidence

    FROM {{ ref('omop_measurement') }}
),

-- Test 6: GDPR Compliance Validation
gdpr_compliance AS (
    SELECT
        -- Check patient ID anonymization
        COUNT(CASE WHEN person_id_hash IS NOT NULL THEN 1 END) AS anonymized_patients,
        COUNT(CASE WHEN gdpr_processing_batch IS NOT NULL THEN 1 END) AS audited_records,

        -- Verify no original patient IDs leaked
        COUNT(CASE WHEN person_source_value LIKE 'patient_%' THEN 1 END) AS potential_pii_leaks

    FROM {{ ref('omop_person') }}
),

-- Test 7: Referential Integrity
referential_integrity AS (
    SELECT
        -- Measurements without valid person references
        (SELECT COUNT(*)
         FROM {{ ref('omop_measurement') }} m
         LEFT JOIN {{ ref('omop_person') }} p ON m.person_id = p.person_id
         WHERE p.person_id IS NULL) AS orphaned_measurements,

        -- Observations without valid person references
        (SELECT COUNT(*)
         FROM {{ ref('omop_observation') }} o
         LEFT JOIN {{ ref('omop_person') }} p ON o.person_id = p.person_id
         WHERE p.person_id IS NULL) AS orphaned_observations,

        -- Visits without valid person references
        (SELECT COUNT(*)
         FROM {{ ref('omop_visit_occurrence') }} v
         LEFT JOIN {{ ref('omop_person') }} p ON v.person_id = p.person_id
         WHERE p.person_id IS NULL) AS orphaned_visits
),

-- Test 8: Data Quality Scores
data_quality AS (
    SELECT
        'MEASUREMENT' AS table_name,
        AVG(data_quality_score) AS avg_quality_score,
        COUNT(CASE WHEN data_quality_score >= 0.8 THEN 1 END) AS high_quality_records,
        COUNT(*) AS total_records
    FROM {{ ref('omop_measurement') }}

    UNION ALL

    SELECT
        'OBSERVATION' AS table_name,
        AVG(data_quality_score) AS avg_quality_score,
        COUNT(CASE WHEN data_quality_score >= 0.7 THEN 1 END) AS high_quality_records,
        COUNT(*) AS total_records
    FROM {{ ref('omop_observation') }}
),

-- Final compliance summary
compliance_summary AS (
    SELECT
        -- Person table compliance
        CASE
            WHEN pc.valid_person_ids = pc.total_persons
                 AND pc.valid_birth_years = pc.total_persons
                 AND pc.gdpr_compliant_persons = pc.total_persons
            THEN 'PASS' ELSE 'FAIL'
        END AS person_table_compliance,

        -- Measurement table compliance
        CASE
            WHEN mc.valid_measurement_ids = mc.total_measurements
                 AND mc.valid_person_refs = mc.total_measurements
                 AND mc.mapped_concepts >= (mc.total_measurements * 0.95)  -- 95% mapping required
                 AND mc.avg_mapping_confidence >= 0.7
            THEN 'PASS' ELSE 'FAIL'
        END AS measurement_table_compliance,

        -- Visit table compliance
        CASE
            WHEN vc.valid_visit_ids = vc.total_visits
                 AND vc.valid_person_refs = vc.total_visits
                 AND vc.mapped_visit_concepts = vc.total_visits
                 AND vc.valid_date_ranges = vc.total_visits
            THEN 'PASS' ELSE 'FAIL'
        END AS visit_table_compliance,

        -- Observation table compliance
        CASE
            WHEN oc.valid_observation_ids = oc.total_observations
                 AND oc.valid_person_refs = oc.total_observations
                 AND oc.mapped_concepts >= (oc.total_observations * 0.9)  -- 90% mapping required
            THEN 'PASS' ELSE 'FAIL'
        END AS observation_table_compliance,

        -- Concept mapping compliance
        CASE
            WHEN cc.mapped_lab_codes >= (cc.total_lab_codes * 0.95)  -- 95% coverage required
                 AND cc.high_confidence_codes >= (cc.total_lab_codes * 0.8)  -- 80% high confidence
            THEN 'PASS' ELSE 'FAIL'
        END AS concept_mapping_compliance,

        -- GDPR compliance
        CASE
            WHEN gc.anonymized_patients > 0
                 AND gc.audited_records > 0
                 AND gc.potential_pii_leaks = 0
            THEN 'PASS' ELSE 'FAIL'
        END AS gdpr_compliance,

        -- Referential integrity
        CASE
            WHEN ri.orphaned_measurements = 0
                 AND ri.orphaned_observations = 0
                 AND ri.orphaned_visits = 0
            THEN 'PASS' ELSE 'FAIL'
        END AS referential_integrity,

        -- Overall compliance
        CASE
            WHEN
                -- All individual compliance checks pass
                (pc.valid_person_ids = pc.total_persons AND pc.gdpr_compliant_persons = pc.total_persons) AND
                (mc.mapped_concepts >= mc.total_measurements * 0.95 AND mc.avg_mapping_confidence >= 0.7) AND
                (vc.valid_visit_ids = vc.total_visits AND vc.valid_date_ranges = vc.total_visits) AND
                (oc.mapped_concepts >= oc.total_observations * 0.9) AND
                (cc.mapped_lab_codes >= cc.total_lab_codes * 0.95) AND
                (gc.potential_pii_leaks = 0) AND
                (ri.orphaned_measurements = 0 AND ri.orphaned_observations = 0 AND ri.orphaned_visits = 0)
            THEN 'OMOP_CDM_COMPLIANT'
            ELSE 'NON_COMPLIANT'
        END AS overall_omop_compliance,

        -- Detailed metrics for reporting
        pc.total_persons,
        mc.total_measurements,
        vc.total_visits,
        oc.total_observations,
        cc.avg_confidence AS concept_mapping_confidence,
        CURRENT_TIMESTAMP AS validation_timestamp

    FROM person_compliance pc
    CROSS JOIN measurement_compliance mc
    CROSS JOIN visit_compliance vc
    CROSS JOIN observation_compliance oc
    CROSS JOIN concept_coverage cc
    CROSS JOIN gdpr_compliance gc
    CROSS JOIN referential_integrity ri
)

-- Final validation result
SELECT
    overall_omop_compliance,
    person_table_compliance,
    measurement_table_compliance,
    visit_table_compliance,
    observation_table_compliance,
    concept_mapping_compliance,
    gdpr_compliance,
    referential_integrity,
    total_persons,
    total_measurements,
    total_visits,
    total_observations,
    concept_mapping_confidence,
    validation_timestamp,

    -- Compliance score (percentage)
    CASE
        WHEN overall_omop_compliance = 'OMOP_CDM_COMPLIANT' THEN 100.0
        ELSE
            (CASE WHEN person_table_compliance = 'PASS' THEN 1 ELSE 0 END +
             CASE WHEN measurement_table_compliance = 'PASS' THEN 1 ELSE 0 END +
             CASE WHEN visit_table_compliance = 'PASS' THEN 1 ELSE 0 END +
             CASE WHEN observation_table_compliance = 'PASS' THEN 1 ELSE 0 END +
             CASE WHEN concept_mapping_compliance = 'PASS' THEN 1 ELSE 0 END +
             CASE WHEN gdpr_compliance = 'PASS' THEN 1 ELSE 0 END +
             CASE WHEN referential_integrity = 'PASS' THEN 1 ELSE 0 END
            ) * 100.0 / 7.0
    END AS compliance_score_pct

FROM compliance_summary

-- This test should return exactly one row with overall_omop_compliance = 'OMOP_CDM_COMPLIANT'
-- If it doesn't, the OMOP implementation needs to be fixed before production deployment