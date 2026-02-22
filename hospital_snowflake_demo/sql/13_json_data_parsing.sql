-- =============================================================================
-- FILE: 13_json_data_parsing.sql
-- PURPOSE: Demonstrate JSON data loading and parsing in Snowflake
-- FEATURES SHOWCASED:
--   - VARIANT data type for semi-structured data
--   - PARSE_JSON function
--   - Dot notation for JSON navigation
--   - LATERAL FLATTEN for array expansion
--   - Type casting with :: operator
--   - Creating structured views from JSON
-- =============================================================================

USE ROLE DATA_ENGINEER;
USE WAREHOUSE HOSPITAL_LOAD_WH;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA RAW_DATA;

-- =============================================================================
-- STEP 1: Create table to store raw JSON data
-- =============================================================================
CREATE OR REPLACE TABLE RAW_DATA.PATIENT_VITALS_JSON_RAW (
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(255),
    raw_json VARIANT
);

-- =============================================================================
-- STEP 2: Create internal stage for JSON files
-- =============================================================================
CREATE OR REPLACE STAGE RAW_DATA.VITALS_JSON_STAGE
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

-- Upload the JSON file to stage using:
-- PUT file:///<path>/patient_vitals_monitoring.json @RAW_DATA.VITALS_JSON_STAGE;

-- =============================================================================
-- STEP 3: Load JSON data into raw table
-- =============================================================================
COPY INTO RAW_DATA.PATIENT_VITALS_JSON_RAW (source_file, raw_json)
FROM (
    SELECT 
        METADATA$FILENAME,
        $1
    FROM @RAW_DATA.VITALS_JSON_STAGE
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
ON_ERROR = 'CONTINUE';

-- =============================================================================
-- STEP 4: Parse JSON into structured view using dot notation and FLATTEN
-- =============================================================================
USE SCHEMA TRANSFORMED;

CREATE OR REPLACE VIEW TRANSFORMED.V_PATIENT_VITALS_PARSED AS
SELECT
    -- Core identifiers (using :: for type casting)
    raw_json:patient_id::VARCHAR AS patient_id,
    raw_json:monitoring_session::VARCHAR AS monitoring_session,
    raw_json:timestamp::TIMESTAMP_NTZ AS measurement_timestamp,
    raw_json:device_id::VARCHAR AS device_id,
    
    -- Location details (nested JSON navigation with dot notation)
    raw_json:location.department::VARCHAR AS department_code,
    raw_json:location.room::VARCHAR AS room_number,
    raw_json:location.bed::VARCHAR AS bed_id,
    
    -- Heart rate vitals (nested object access)
    raw_json:vitals.heart_rate.value::INTEGER AS heart_rate_value,
    raw_json:vitals.heart_rate.unit::VARCHAR AS heart_rate_unit,
    raw_json:vitals.heart_rate.status::VARCHAR AS heart_rate_status,
    
    -- Blood pressure vitals (multiple nested values)
    raw_json:vitals.blood_pressure.systolic::INTEGER AS bp_systolic,
    raw_json:vitals.blood_pressure.diastolic::INTEGER AS bp_diastolic,
    raw_json:vitals.blood_pressure.unit::VARCHAR AS bp_unit,
    raw_json:vitals.blood_pressure.status::VARCHAR AS bp_status,
    
    -- Oxygen saturation
    raw_json:vitals.oxygen_saturation.value::INTEGER AS oxygen_saturation,
    raw_json:vitals.oxygen_saturation.status::VARCHAR AS oxygen_status,
    
    -- Temperature
    raw_json:vitals.temperature.value::FLOAT AS temperature_value,
    raw_json:vitals.temperature.unit::VARCHAR AS temperature_unit,
    raw_json:vitals.temperature.status::VARCHAR AS temperature_status,
    
    -- Respiratory rate
    raw_json:vitals.respiratory_rate.value::INTEGER AS respiratory_rate,
    raw_json:vitals.respiratory_rate.status::VARCHAR AS respiratory_status,
    
    -- Specialty vitals (may be NULL if not present)
    raw_json:vitals.fetal_heart_rate.value::INTEGER AS fetal_heart_rate,
    raw_json:vitals.intracranial_pressure.value::FLOAT AS intracranial_pressure,
    raw_json:vitals.glasgow_coma_scale.total::INTEGER AS gcs_total,
    raw_json:vitals.nihss_score.value::INTEGER AS nihss_score,
    raw_json:vitals.troponin_level.value::FLOAT AS troponin_level,
    raw_json:vitals.pain_score.value::INTEGER AS pain_score,
    raw_json:vitals.ecg_rhythm.type::VARCHAR AS ecg_rhythm_type,
    
    -- Count of alerts
    ARRAY_SIZE(raw_json:alerts) AS alert_count,
    
    -- Raw JSON for reference
    raw_json AS raw_data
FROM RAW_DATA.PATIENT_VITALS_JSON_RAW;

-- =============================================================================
-- STEP 5: Use LATERAL FLATTEN to expand alerts array into rows
-- =============================================================================
CREATE OR REPLACE VIEW TRANSFORMED.V_PATIENT_VITALS_ALERTS AS
SELECT
    raw_json:patient_id::VARCHAR AS patient_id,
    raw_json:monitoring_session::VARCHAR AS monitoring_session,
    raw_json:timestamp::TIMESTAMP_NTZ AS measurement_timestamp,
    raw_json:location.department::VARCHAR AS department_code,
    
    -- Flattened alert data (one row per alert)
    alert.value:type::VARCHAR AS alert_type,
    alert.value:severity::VARCHAR AS alert_severity,
    alert.value:message::VARCHAR AS alert_message,
    alert.index AS alert_sequence
FROM RAW_DATA.PATIENT_VITALS_JSON_RAW,
    LATERAL FLATTEN(input => raw_json:alerts) AS alert
WHERE ARRAY_SIZE(raw_json:alerts) > 0;

-- =============================================================================
-- STEP 6: Create analytics views in ANALYTICS schema
-- =============================================================================
USE SCHEMA ANALYTICS;

-- View: Patient Vitals Summary with risk scoring
CREATE OR REPLACE VIEW ANALYTICS.V_PATIENT_VITALS_SUMMARY AS
SELECT
    patient_id,
    monitoring_session,
    measurement_timestamp,
    department_code,
    room_number,
    bed_id,
    
    -- Core vitals
    heart_rate_value,
    heart_rate_status,
    bp_systolic,
    bp_diastolic,
    bp_status,
    oxygen_saturation,
    oxygen_status,
    temperature_value,
    temperature_status,
    respiratory_rate,
    respiratory_status,
    
    -- Specialty vitals
    fetal_heart_rate,
    intracranial_pressure,
    gcs_total,
    nihss_score,
    troponin_level,
    pain_score,
    ecg_rhythm_type,
    
    -- Risk score calculation
    CASE 
        WHEN oxygen_saturation < 90 THEN 3
        WHEN oxygen_saturation < 94 THEN 2
        WHEN oxygen_saturation < 96 THEN 1
        ELSE 0
    END +
    CASE 
        WHEN bp_systolic > 160 OR bp_systolic < 90 THEN 3
        WHEN bp_systolic > 150 OR bp_systolic < 100 THEN 2
        WHEN bp_systolic > 140 OR bp_systolic < 110 THEN 1
        ELSE 0
    END +
    CASE 
        WHEN heart_rate_value > 120 OR heart_rate_value < 50 THEN 3
        WHEN heart_rate_value > 100 OR heart_rate_value < 60 THEN 2
        WHEN heart_rate_value > 90 OR heart_rate_value < 65 THEN 1
        ELSE 0
    END AS vital_risk_score,
    
    alert_count
FROM TRANSFORMED.V_PATIENT_VITALS_PARSED;

-- View: Alerts by severity for monitoring dashboard
CREATE OR REPLACE VIEW ANALYTICS.V_ALERTS_BY_SEVERITY AS
SELECT
    alert_severity,
    alert_type,
    COUNT(*) AS alert_count,
    LISTAGG(DISTINCT department_code, ', ') AS affected_departments
FROM TRANSFORMED.V_PATIENT_VITALS_ALERTS
GROUP BY alert_severity, alert_type
ORDER BY 
    CASE alert_severity 
        WHEN 'critical' THEN 1 
        WHEN 'warning' THEN 2 
        WHEN 'info' THEN 3 
        ELSE 4 
    END,
    alert_count DESC;

-- View: Department vitals overview
CREATE OR REPLACE VIEW ANALYTICS.V_DEPARTMENT_VITALS_OVERVIEW AS
SELECT
    department_code,
    COUNT(DISTINCT patient_id) AS patient_count,
    COUNT(*) AS total_measurements,
    
    -- Average vitals
    ROUND(AVG(heart_rate_value), 1) AS avg_heart_rate,
    ROUND(AVG(bp_systolic), 1) AS avg_systolic_bp,
    ROUND(AVG(bp_diastolic), 1) AS avg_diastolic_bp,
    ROUND(AVG(oxygen_saturation), 1) AS avg_oxygen_saturation,
    ROUND(AVG(temperature_value), 1) AS avg_temperature,
    ROUND(AVG(respiratory_rate), 1) AS avg_respiratory_rate,
    
    -- Risk metrics
    ROUND(AVG(vital_risk_score), 2) AS avg_risk_score,
    SUM(alert_count) AS total_alerts,
    SUM(CASE WHEN vital_risk_score >= 5 THEN 1 ELSE 0 END) AS high_risk_patients
FROM ANALYTICS.V_PATIENT_VITALS_SUMMARY
GROUP BY department_code
ORDER BY avg_risk_score DESC;

-- View: Critical alerts requiring immediate attention
CREATE OR REPLACE VIEW ANALYTICS.V_CRITICAL_ALERTS AS
SELECT
    va.patient_id,
    va.monitoring_session,
    va.measurement_timestamp,
    va.department_code,
    va.alert_type,
    va.alert_message,
    vs.heart_rate_value,
    vs.bp_systolic || '/' || vs.bp_diastolic AS blood_pressure,
    vs.oxygen_saturation,
    vs.vital_risk_score
FROM TRANSFORMED.V_PATIENT_VITALS_ALERTS va
JOIN ANALYTICS.V_PATIENT_VITALS_SUMMARY vs 
    ON va.patient_id = vs.patient_id 
    AND va.monitoring_session = vs.monitoring_session
WHERE va.alert_severity = 'critical'
ORDER BY va.measurement_timestamp DESC;

-- =============================================================================
-- STEP 7: Grant permissions to roles
-- =============================================================================
GRANT USAGE ON SCHEMA TRANSFORMED TO ROLE CLINICAL_ADMIN;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON ALL VIEWS IN SCHEMA TRANSFORMED TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS TO ROLE CLINICAL_ADMIN;

GRANT USAGE ON SCHEMA ANALYTICS TO ROLE PHYSICIAN;
GRANT SELECT ON ANALYTICS.V_PATIENT_VITALS_SUMMARY TO ROLE PHYSICIAN;
GRANT SELECT ON ANALYTICS.V_CRITICAL_ALERTS TO ROLE PHYSICIAN;

GRANT USAGE ON SCHEMA ANALYTICS TO ROLE NURSE;
GRANT SELECT ON ANALYTICS.V_PATIENT_VITALS_SUMMARY TO ROLE NURSE;
GRANT SELECT ON ANALYTICS.V_CRITICAL_ALERTS TO ROLE NURSE;

GRANT USAGE ON SCHEMA ANALYTICS TO ROLE ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS TO ROLE ANALYST;

-- =============================================================================
-- STEP 8: Sample queries demonstrating JSON parsing techniques
-- =============================================================================

-- Query 1: Basic JSON extraction with dot notation
SELECT 
    raw_json:patient_id::VARCHAR AS patient,
    raw_json:vitals.heart_rate.value::INT AS heart_rate
FROM RAW_DATA.PATIENT_VITALS_JSON_RAW
LIMIT 5;

-- Query 2: Deeply nested JSON with multiple levels
SELECT 
    raw_json:patient_id::VARCHAR AS patient,
    raw_json:vitals.blood_pressure.systolic::INT AS systolic,
    raw_json:vitals.blood_pressure.diastolic::INT AS diastolic,
    raw_json:vitals.blood_pressure.status::VARCHAR AS status
FROM RAW_DATA.PATIENT_VITALS_JSON_RAW;

-- Query 3: Working with arrays using ARRAY_SIZE and indexing
SELECT 
    raw_json:patient_id::VARCHAR AS patient,
    ARRAY_SIZE(raw_json:alerts) AS num_alerts,
    raw_json:alerts[0]:type::VARCHAR AS first_alert_type,
    raw_json:alerts[0]:severity::VARCHAR AS first_alert_severity
FROM RAW_DATA.PATIENT_VITALS_JSON_RAW
WHERE ARRAY_SIZE(raw_json:alerts) > 0;

-- Query 4: LATERAL FLATTEN for array expansion
SELECT 
    raw_json:patient_id::VARCHAR AS patient,
    f.value:type::VARCHAR AS alert_type,
    f.value:severity::VARCHAR AS severity,
    f.value:message::VARCHAR AS message
FROM RAW_DATA.PATIENT_VITALS_JSON_RAW,
    LATERAL FLATTEN(input => raw_json:alerts) f
WHERE ARRAY_SIZE(raw_json:alerts) > 0;

-- Query 5: Conditional extraction (handle optional fields)
SELECT 
    raw_json:patient_id::VARCHAR AS patient,
    raw_json:location.department::VARCHAR AS dept,
    COALESCE(raw_json:vitals.fetal_heart_rate.value::INT, 0) AS fetal_hr,
    COALESCE(raw_json:vitals.intracranial_pressure.value::FLOAT, 0) AS icp,
    COALESCE(raw_json:vitals.troponin_level.value::FLOAT, 0) AS troponin
FROM RAW_DATA.PATIENT_VITALS_JSON_RAW;

-- =============================================================================
-- VERIFICATION: Check parsed data
-- =============================================================================
SELECT 'Vitals Summary' AS view_name, COUNT(*) AS row_count FROM ANALYTICS.V_PATIENT_VITALS_SUMMARY
UNION ALL
SELECT 'Alerts by Severity', COUNT(*) FROM ANALYTICS.V_ALERTS_BY_SEVERITY
UNION ALL
SELECT 'Department Overview', COUNT(*) FROM ANALYTICS.V_DEPARTMENT_VITALS_OVERVIEW
UNION ALL
SELECT 'Critical Alerts', COUNT(*) FROM ANALYTICS.V_CRITICAL_ALERTS;
