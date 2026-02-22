-- ============================================================================
-- Hospital Snowflake Demo - RBAC and Data Governance
-- ============================================================================
-- This script demonstrates role-based access control and data governance features

USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;

-- 1. Create Data Classification Tags for HIPAA Compliance
CREATE OR REPLACE TAG HOSPITAL_DEMO.ANALYTICS.PII_LEVEL
ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED';

CREATE OR REPLACE TAG HOSPITAL_DEMO.ANALYTICS.PHI_INDICATOR
ALLOWED_VALUES 'YES', 'NO';

CREATE OR REPLACE TAG HOSPITAL_DEMO.ANALYTICS.DATA_CLASSIFICATION
ALLOWED_VALUES 'DEMOGRAPHIC', 'CLINICAL', 'FINANCIAL', 'OPERATIONAL';

-- 2. Apply Tags to Tables and Columns
-- Tag Patient Dimension
ALTER TABLE TRANSFORMED.DIM_PATIENT SET TAG 
    HOSPITAL_DEMO.ANALYTICS.PII_LEVEL = 'RESTRICTED',
    HOSPITAL_DEMO.ANALYTICS.PHI_INDICATOR = 'YES',
    HOSPITAL_DEMO.ANALYTICS.DATA_CLASSIFICATION = 'DEMOGRAPHIC';

-- Tag specific PII columns
ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN first_name 
SET TAG HOSPITAL_DEMO.ANALYTICS.PII_LEVEL = 'RESTRICTED';

ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN last_name 
SET TAG HOSPITAL_DEMO.ANALYTICS.PII_LEVEL = 'RESTRICTED';

ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN full_name 
SET TAG HOSPITAL_DEMO.ANALYTICS.PII_LEVEL = 'RESTRICTED';

ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN address 
SET TAG HOSPITAL_DEMO.ANALYTICS.PII_LEVEL = 'RESTRICTED';

-- Tag Admission Facts
ALTER TABLE TRANSFORMED.FACT_ADMISSIONS SET TAG 
    HOSPITAL_DEMO.ANALYTICS.PII_LEVEL = 'CONFIDENTIAL',
    HOSPITAL_DEMO.ANALYTICS.PHI_INDICATOR = 'YES',
    HOSPITAL_DEMO.ANALYTICS.DATA_CLASSIFICATION = 'CLINICAL';

-- Tag clinical data columns
ALTER TABLE TRANSFORMED.FACT_ADMISSIONS MODIFY COLUMN diagnosis_primary 
SET TAG HOSPITAL_DEMO.ANALYTICS.PHI_INDICATOR = 'YES';

ALTER TABLE TRANSFORMED.FACT_ADMISSIONS MODIFY COLUMN diagnosis_secondary 
SET TAG HOSPITAL_DEMO.ANALYTICS.PHI_INDICATOR = 'YES';

-- 3. Create Row Access Policies for Department-Based Access
-- Note: Since FACT_ADMISSIONS uses department_key, we'll apply the policy to views that include department_id

-- Remove policy from tables first, then drop and recreate
-- Use separate commands to handle cases where policy may not exist
BEGIN
    ALTER TABLE RAW_DATA.PATIENT_ADMISSIONS_RAW DROP ROW ACCESS POLICY HOSPITAL_DEMO.ANALYTICS.DEPARTMENT_ACCESS_POLICY;
EXCEPTION
    WHEN OTHER THEN
        SELECT 'Policy not currently applied to table' as cleanup_status;
END;

DROP ROW ACCESS POLICY IF EXISTS HOSPITAL_DEMO.ANALYTICS.DEPARTMENT_ACCESS_POLICY;

CREATE ROW ACCESS POLICY HOSPITAL_DEMO.ANALYTICS.DEPARTMENT_ACCESS_POLICY
AS (department_id STRING) RETURNS BOOLEAN ->
    CASE 
        WHEN CURRENT_ROLE() IN ('CLINICAL_ADMIN', 'SYSADMIN', 'ACCOUNTADMIN') THEN TRUE
        WHEN CURRENT_ROLE() = 'PHYSICIAN' AND department_id IN ('CARD', 'EMER', 'NEUR') THEN TRUE
        WHEN CURRENT_ROLE() = 'NURSE' AND department_id = 'CARD' THEN TRUE
        ELSE FALSE
    END;

-- Apply row access policy to raw admissions table (which has department_id)
ALTER TABLE RAW_DATA.PATIENT_ADMISSIONS_RAW ADD ROW ACCESS POLICY HOSPITAL_DEMO.ANALYTICS.DEPARTMENT_ACCESS_POLICY ON (department_id);

-- 4. Create Masking Policies for PII Protection

-- Remove existing masking policies from columns first
BEGIN
    ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN first_name UNSET MASKING POLICY;
    ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN last_name UNSET MASKING POLICY;
    ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN full_name UNSET MASKING POLICY;
    ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN address UNSET MASKING POLICY;
    ALTER TABLE TRANSFORMED.FACT_ADMISSIONS MODIFY COLUMN total_charges UNSET MASKING POLICY;
EXCEPTION
    WHEN OTHER THEN
        SELECT 'Some masking policies not currently applied' as cleanup_status;
END;

-- Drop existing masking policies
DROP MASKING POLICY IF EXISTS HOSPITAL_DEMO.ANALYTICS.MASK_PATIENT_NAME;
DROP MASKING POLICY IF EXISTS HOSPITAL_DEMO.ANALYTICS.MASK_ADDRESS;
DROP MASKING POLICY IF EXISTS HOSPITAL_DEMO.ANALYTICS.MASK_FINANCIAL;

-- Policy to mask patient names
CREATE MASKING POLICY HOSPITAL_DEMO.ANALYTICS.MASK_PATIENT_NAME
AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('CLINICAL_ADMIN', 'PHYSICIAN', 'SYSADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() = 'NURSE' THEN 
            CASE 
                WHEN val IS NOT NULL THEN LEFT(val, 1) || REPEAT('*', LENGTH(val) - 1)
                ELSE val
            END
        ELSE '***MASKED***'
    END;

-- Policy to mask addresses
CREATE MASKING POLICY HOSPITAL_DEMO.ANALYTICS.MASK_ADDRESS
AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('CLINICAL_ADMIN', 'SYSADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() IN ('PHYSICIAN', 'NURSE') THEN 
            REGEXP_REPLACE(val, '[0-9]+', 'XXX') -- Mask street numbers
        ELSE 'RESTRICTED'
    END;

-- Policy to mask financial data
CREATE MASKING POLICY HOSPITAL_DEMO.ANALYTICS.MASK_FINANCIAL
AS (val NUMBER) RETURNS NUMBER ->
    CASE 
        WHEN CURRENT_ROLE() IN ('CLINICAL_ADMIN', 'ANALYST', 'SYSADMIN', 'ACCOUNTADMIN') THEN val
        ELSE NULL
    END;

-- Apply masking policies
ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN first_name 
SET MASKING POLICY HOSPITAL_DEMO.ANALYTICS.MASK_PATIENT_NAME;

ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN last_name 
SET MASKING POLICY HOSPITAL_DEMO.ANALYTICS.MASK_PATIENT_NAME;

ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN full_name 
SET MASKING POLICY HOSPITAL_DEMO.ANALYTICS.MASK_PATIENT_NAME;

ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN address 
SET MASKING POLICY HOSPITAL_DEMO.ANALYTICS.MASK_ADDRESS;

ALTER TABLE TRANSFORMED.FACT_ADMISSIONS MODIFY COLUMN total_charges 
SET MASKING POLICY HOSPITAL_DEMO.ANALYTICS.MASK_FINANCIAL;

-- 5. Create Secure Views for Different User Types
USE SCHEMA ANALYTICS;

-- View for Physicians - Full clinical data, masked PII
CREATE OR REPLACE SECURE VIEW VW_PHYSICIAN_DASHBOARD AS
SELECT 
    fa.admission_id,
    dp.full_name as patient_name,
    dp.age,
    dp.gender,
    fa.admission_date_key,
    dd.department_name,
    fa.diagnosis_primary,
    fa.diagnosis_secondary,
    fa.length_of_stay_days,
    fa.is_emergency
FROM TRANSFORMED.FACT_ADMISSIONS fa
JOIN TRANSFORMED.DIM_PATIENT dp ON fa.patient_key = dp.patient_key
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
JOIN TRANSFORMED.DIM_DATE dt ON fa.admission_date_key = dt.date_key
WHERE dp.is_current = TRUE;

-- View for Nurses - Limited clinical data
CREATE OR REPLACE SECURE VIEW VW_NURSE_DASHBOARD AS
SELECT 
    fa.admission_id,
    dp.full_name as patient_name,
    dp.age_group,
    fa.room_number,
    fa.bed_number,
    dd.department_name,
    fa.chief_complaint,
    fa.is_emergency
FROM TRANSFORMED.FACT_ADMISSIONS fa
JOIN TRANSFORMED.DIM_PATIENT dp ON fa.patient_key = dp.patient_key
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
WHERE dp.is_current = TRUE;

-- View for Analysts - Aggregated data without PII
CREATE OR REPLACE SECURE VIEW VW_ANALYST_DASHBOARD AS
SELECT 
    dt.date_value as admission_date,
    dt.month_name,
    dt.day_name,
    dd.department_name,
    dd.specialization_type,
    COUNT(*) as admission_count,
    AVG(fa.length_of_stay_days) as avg_length_of_stay,
    SUM(fa.total_charges) as total_revenue,
    COUNT(CASE WHEN fa.is_emergency THEN 1 END) as emergency_admissions,
    dw.weather_condition,
    dw.temperature_range
FROM TRANSFORMED.FACT_ADMISSIONS fa
JOIN TRANSFORMED.DIM_DATE dt ON fa.admission_date_key = dt.date_key
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
LEFT JOIN TRANSFORMED.DIM_WEATHER dw ON fa.weather_key = dw.weather_key
GROUP BY dt.date_value, dt.month_name, dt.day_name, dd.department_name, dd.specialization_type, dw.weather_condition, dw.temperature_range;

-- 6. Grant Permissions on Views
GRANT SELECT ON VW_PHYSICIAN_DASHBOARD TO ROLE PHYSICIAN;
GRANT SELECT ON VW_NURSE_DASHBOARD TO ROLE NURSE;
GRANT SELECT ON VW_ANALYST_DASHBOARD TO ROLE ANALYST;

-- Grant broader access to clinical admin
GRANT SELECT ON ALL TABLES IN SCHEMA TRANSFORMED TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS TO ROLE CLINICAL_ADMIN;

-- 7. Create Audit Trail
CREATE OR REPLACE TABLE HOSPITAL_DEMO.ANALYTICS.AUDIT_LOG (
    audit_id INTEGER AUTOINCREMENT PRIMARY KEY,
    user_name STRING,
    role_name STRING,
    query_text STRING,
    table_accessed STRING,
    access_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    session_id STRING,
    client_ip STRING
);

-- Create stored procedure for audit logging
CREATE OR REPLACE PROCEDURE LOG_DATA_ACCESS(
    table_name STRING,
    access_type STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO HOSPITAL_DEMO.ANALYTICS.AUDIT_LOG (
        user_name, role_name, table_accessed, session_id
    ) VALUES (
        CURRENT_USER(),
        CURRENT_ROLE(),
        :table_name,
        CURRENT_SESSION()
    );
    RETURN 'Access logged for ' || :table_name;
END;
$$;

-- 8. Create Data Quality Rules
CREATE OR REPLACE TABLE HOSPITAL_DEMO.ANALYTICS.DATA_QUALITY_RULES (
    rule_id INTEGER AUTOINCREMENT PRIMARY KEY,
    table_name STRING,
    column_name STRING,
    rule_type STRING,
    rule_description STRING,
    rule_query STRING,
    is_active BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample data quality rules
INSERT INTO HOSPITAL_DEMO.ANALYTICS.DATA_QUALITY_RULES 
(table_name, column_name, rule_type, rule_description, rule_query) VALUES
('DIM_PATIENT', 'patient_id', 'UNIQUENESS', 'Patient ID must be unique', 
 'SELECT COUNT(*) - COUNT(DISTINCT patient_id) FROM DIM_PATIENT WHERE is_current = TRUE'),
('FACT_ADMISSIONS', 'total_charges', 'RANGE', 'Total charges must be positive', 
 'SELECT COUNT(*) FROM FACT_ADMISSIONS WHERE total_charges <= 0'),
('FACT_ADMISSIONS', 'length_of_stay_days', 'RANGE', 'Length of stay must be reasonable', 
 'SELECT COUNT(*) FROM FACT_ADMISSIONS WHERE length_of_stay_days < 0 OR length_of_stay_days > 365');

-- 9. Demonstrate Access Control Testing
-- Test different role permissions
USE ROLE PHYSICIAN;
SELECT 'Testing PHYSICIAN role access:' as test_message;
SELECT COUNT(*) as accessible_records FROM ANALYTICS.VW_PHYSICIAN_DASHBOARD;

USE ROLE NURSE;
SELECT 'Testing NURSE role access:' as test_message;
SELECT COUNT(*) as accessible_records FROM ANALYTICS.VW_NURSE_DASHBOARD;

USE ROLE ANALYST;
SELECT 'Testing ANALYST role access:' as test_message;
SELECT COUNT(*) as accessible_records FROM ANALYTICS.VW_ANALYST_DASHBOARD;

-- 10. Show Governance Objects
USE ROLE ACCOUNTADMIN;
SHOW TAGS IN ACCOUNT;
SHOW MASKING POLICIES IN ACCOUNT;
SHOW ROW ACCESS POLICIES IN ACCOUNT;

-- Show tag references
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES 
WHERE TAG_DATABASE = 'HOSPITAL_DEMO'
ORDER BY OBJECT_NAME, COLUMN_NAME;

SELECT 'RBAC and Data Governance setup completed successfully!' as status_message;
SELECT 'Healthcare data is now properly secured and compliant.' as next_step;
