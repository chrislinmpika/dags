/*
OMOP Concept Mapping Macros
Reusable functions for mapping source codes to OMOP concepts
*/

-- Generate consistent OMOP IDs
{% macro generate_omop_id(source_fields) %}
    -- Generate deterministic hash-based ID from source fields
    abs(hash(concat(
        {% for field in source_fields %}
            '{{ field }}', '_',
        {% endfor %}
        '{{ run_started_at.strftime("%Y%m%d") }}'
    ))) % 2147483647  -- Keep within bigint range
{% endmacro %}

-- Map laboratory codes to LOINC concepts
{% macro map_to_loinc(source_code_column, hint_text_column=none) %}
    CASE
        -- Use laboratory mapping table if available and confident
        WHEN lab_map.target_concept_id IS NOT NULL
             AND lab_map.mapping_confidence >= 0.7
        THEN lab_map.target_concept_id

        -- Pattern-based mapping for common laboratory codes
        WHEN {{ source_code_column }} = 'LC:0007' THEN 4182210  -- Hemoglobin [Mass/volume] in Blood
        WHEN {{ source_code_column }} = 'LC:0010' THEN 4143345  -- Leukocytes [#/volume] in Blood
        WHEN {{ source_code_column }} = 'LC:0001' THEN 4263235  -- Glucose [Mass/volume] in Serum or Plasma
        WHEN {{ source_code_column }} = 'LC:0012' THEN 4143876  -- Erythrocytes [#/volume] in Blood
        WHEN {{ source_code_column }} = 'LC:0024' THEN 4024659  -- Bacteria identified in Isolate

        -- Pattern matching for hint text if provided
        {% if hint_text_column %}
        WHEN LOWER({{ hint_text_column }}) LIKE '%glucose%' THEN 4263235
        WHEN LOWER({{ hint_text_column }}) LIKE '%hemoglobin%' OR LOWER({{ hint_text_column }}) LIKE '%hb%' THEN 4182210
        WHEN LOWER({{ hint_text_column }}) LIKE '%white%blood%cell%' OR LOWER({{ hint_text_column }}) LIKE '%wbc%' THEN 4143345
        {% endif %}

        -- Default to generic laboratory test
        ELSE 4124662  -- Laboratory test
    END
{% endmacro %}

-- Map units to UCUM standard
{% macro map_to_ucum(unit_column) %}
    CASE
        WHEN {{ unit_column }} = '%' THEN 8554           -- percent
        WHEN {{ unit_column }} = 'g/L' THEN 8713         -- gram per liter
        WHEN {{ unit_column }} = 'g/dL' THEN 8713        -- gram per deciliter
        WHEN {{ unit_column }} = 'mg/dL' THEN 8840       -- milligram per deciliter
        WHEN {{ unit_column }} = 'mmol/L' THEN 8753      -- millimole per liter
        WHEN {{ unit_column }} = 'nmol/L' THEN 8842      -- nanomole per liter
        WHEN {{ unit_column }} = 'U/L' THEN 8645         -- unit per liter
        WHEN {{ unit_column }} = 'cells/µL' THEN 8784    -- cells per microliter
        WHEN {{ unit_column }} = 'cells/uL' THEN 8784    -- cells per microliter
        WHEN {{ unit_column }} = 'mEq/L' THEN 8753       -- milliequivalent per liter
        WHEN {{ unit_column }} = 'log copies/mL' THEN 0  -- No standard UCUM for log copies
        WHEN {{ unit_column }} = 'copies/mL' THEN 0      -- No standard UCUM for copies
        ELSE 0  -- No unit mapping found
    END
{% endmacro %}

-- Map categorical values to standard concepts
{% macro map_categorical_value(value_column, qualifier_column=none) %}
    CASE
        -- Standard positive/negative results
        WHEN UPPER({{ value_column }}) = 'POSITIVE' THEN 4181412     -- Positive
        WHEN UPPER({{ value_column }}) = 'NEGATIVE' THEN 4132135     -- Negative
        WHEN UPPER({{ value_column }}) = 'NORMAL' THEN 4069590       -- Normal
        WHEN UPPER({{ value_column }}) = 'ABNORMAL' THEN 4135493     -- Abnormal

        -- Reactive/Non-reactive
        WHEN UPPER({{ value_column }}) = 'REACTIVE' THEN 4181412     -- Positive (reactive)
        WHEN UPPER({{ value_column }}) = 'NON-REACTIVE' THEN 4132135 -- Negative (non-reactive)
        WHEN UPPER({{ value_column }}) = 'NONREACTIVE' THEN 4132135  -- Negative (non-reactive)

        -- Present/Absent
        WHEN UPPER({{ value_column }}) = 'PRESENT' THEN 4118309      -- Present
        WHEN UPPER({{ value_column }}) = 'ABSENT' THEN 4124736       -- Absent

        -- Detected/Not detected
        WHEN UPPER({{ value_column }}) = 'DETECTED' THEN 4181412     -- Detected
        WHEN UPPER({{ value_column }}) = 'NOT DETECTED' THEN 4132135 -- Not detected
        WHEN UPPER({{ value_column }}) = 'UNDETECTED' THEN 4132135   -- Not detected

        -- High/Low/Normal ranges
        WHEN UPPER({{ value_column }}) = 'HIGH' THEN 4328749         -- High
        WHEN UPPER({{ value_column }}) = 'LOW' THEN 4124052          -- Low
        WHEN UPPER({{ value_column }}) = 'WITHIN NORMAL LIMITS' THEN 4069590  -- Normal

        {% if qualifier_column %}
        -- Use qualifier if value mapping fails
        WHEN {{ value_column }} IS NULL AND UPPER({{ qualifier_column }}) = 'POSITIVE' THEN 4181412
        WHEN {{ value_column }} IS NULL AND UPPER({{ qualifier_column }}) = 'NEGATIVE' THEN 4132135
        {% endif %}

        ELSE 0  -- No concept mapping found
    END
{% endmacro %}

-- Validate OMOP date ranges
{% macro validate_omop_dates(date_column) %}
    {{ date_column }} BETWEEN DATE '1900-01-01' AND CURRENT_DATE
{% endmacro %}

-- Calculate data quality score
{% macro calculate_quality_score(required_fields, optional_fields=[]) %}
    (
        -- Base score for required fields (70% weight)
        (
            {% for field in required_fields %}
                CASE WHEN {{ field }} IS NOT NULL THEN 1.0 ELSE 0.0 END
                {%- if not loop.last %} + {% endif %}
            {% endfor %}
        ) / {{ required_fields | length }} * 0.7

        -- Bonus points for optional fields (30% weight)
        {% if optional_fields %}
        + (
            {% for field in optional_fields %}
                CASE WHEN {{ field }} IS NOT NULL THEN 1.0 ELSE 0.0 END
                {%- if not loop.last %} + {% endif %}
            {% endfor %}
        ) / {{ optional_fields | length }} * 0.3
        {% else %}
        + 0.3  -- Full bonus if no optional fields specified
        {% endif %}
    )
{% endmacro %}

-- GDPR anonymization helper
{% macro gdpr_hash_patient_id(patient_id_column, salt='omop_salt_2025') %}
    SUBSTRING(
        TO_HEX(SHA256(TO_UTF8(CONCAT('{{ salt }}', '_', CAST({{ patient_id_column }} AS VARCHAR))))),
        1, 16
    )
{% endmacro %}

-- Map visit types for laboratory data
{% macro map_visit_type() %}
    CASE
        WHEN laboratory_uuid IS NOT NULL THEN 9202  -- Outpatient Visit (most lab work)
        WHEN provider_id IS NOT NULL THEN 9201      -- Inpatient Visit
        ELSE 32037  -- Non-hospital institution Visit
    END
{% endmacro %}

-- Generate concept mapping confidence score
{% macro mapping_confidence_score(mapping_method_column, source_frequency_column=none) %}
    CASE {{ mapping_method_column }}
        WHEN 'EXACT_MATCH' THEN 0.95
        WHEN 'PREDEFINED_PATTERN' THEN 0.9
        WHEN 'FUZZY_MATCH' THEN 0.8
        WHEN 'PATTERN_INFERENCE' THEN 0.7
        WHEN 'GENERIC_FALLBACK' THEN 0.5
        ELSE 0.3  -- Unknown method
    END
    {% if source_frequency_column %}
    -- Boost confidence for high-frequency codes
    + CASE
        WHEN {{ source_frequency_column }} >= 1000 THEN 0.05
        WHEN {{ source_frequency_column }} >= 100 THEN 0.03
        ELSE 0.0
    END
    {% endif %}
{% endmacro %}

-- Standardize laboratory result normality
{% macro standardize_normality(normality_column, value_column, range_low_column=none, range_high_column=none) %}
    CASE
        WHEN UPPER({{ normality_column }}) = 'NORMAL' THEN 'NORMAL'
        WHEN UPPER({{ normality_column }}) = 'ABNORMAL' THEN 'ABNORMAL'
        WHEN UPPER({{ normality_column }}) IN ('HIGH', 'LOW') THEN 'ABNORMAL'

        {% if range_low_column and range_high_column %}
        -- Derive normality from reference ranges if not provided
        WHEN {{ normality_column }} IS NULL AND {{ value_column }} IS NOT NULL THEN
            CASE
                WHEN {{ value_column }} BETWEEN {{ range_low_column }} AND {{ range_high_column }} THEN 'NORMAL'
                ELSE 'ABNORMAL'
            END
        {% endif %}

        ELSE 'UNKNOWN'
    END
{% endmacro %}

-- Convert measurement datetime with timezone handling
{% macro standardize_datetime(datetime_column, default_timezone='UTC') %}
    CASE
        WHEN {{ datetime_column }} IS NOT NULL THEN
            -- Convert to UTC if not already
            AT_TIMEZONE({{ datetime_column }}, '{{ default_timezone }}')
        ELSE NULL
    END
{% endmacro %}