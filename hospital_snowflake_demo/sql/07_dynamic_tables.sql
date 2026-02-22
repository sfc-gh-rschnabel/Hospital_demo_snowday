-- ============================================================================
-- Hospital Snowflake Demo - Dynamic Tables for Real-Time Analytics
-- ============================================================================
-- This script demonstrates Snowflake Dynamic Tables for declarative data 
-- pipelines that automatically refresh based on target lag settings.
--
-- Dynamic Tables Benefits:
-- - Declarative pipeline definition (no orchestration needed)
-- - Automatic incremental refreshes when possible
-- - Built-in dependency management
-- - Cost-effective near-real-time data freshness
--
-- IMPORTANT: Run this script as ACCOUNTADMIN or DATA_ENGINEER
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;

-- ============================================================================
-- 1. BRONZE LAYER - Dynamic Tables from Raw Data (Near Real-Time Cleansing)
-- ============================================================================
-- These dynamic tables cleanse and validate raw data as it arrives
-- Target lag: 1 minute for near-real-time processing

CREATE OR REPLACE DYNAMIC TABLE TRANSFORMED.DT_CLEAN_ADMISSIONS
TARGET_LAG = '1 minute'
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
SELECT 
    admission_id,
    patient_id,
    admission_date,
    admission_time,
    discharge_date,
    discharge_time,
    department_id,
    UPPER(TRIM(admission_type)) as admission_type,
    TRIM(chief_complaint) as chief_complaint,
    TRIM(diagnosis_primary) as diagnosis_primary,
    TRIM(diagnosis_secondary) as diagnosis_secondary,
    TRIM(attending_physician) as attending_physician,
    room_number,
    bed_number,
    COALESCE(total_charges, 0) as total_charges,
    UPPER(TRIM(weather_condition)) as weather_condition,
    temperature_f,
    DATEDIFF(day, admission_date, COALESCE(discharge_date, CURRENT_DATE())) as length_of_stay_days,
    CASE 
        WHEN admission_type ILIKE '%emergency%' THEN TRUE 
        ELSE FALSE 
    END as is_emergency,
    load_timestamp,
    source_file
FROM RAW_DATA.PATIENT_ADMISSIONS_RAW
WHERE admission_id IS NOT NULL 
  AND patient_id IS NOT NULL
  AND admission_date IS NOT NULL;

CREATE OR REPLACE DYNAMIC TABLE TRANSFORMED.DT_CLEAN_PROCEDURES
TARGET_LAG = '1 minute'
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
SELECT 
    procedure_id,
    admission_id,
    UPPER(TRIM(procedure_code)) as procedure_code,
    INITCAP(TRIM(procedure_name)) as procedure_name,
    procedure_date,
    procedure_time,
    INITCAP(TRIM(performing_physician)) as performing_physician,
    COALESCE(procedure_duration_minutes, 0) as procedure_duration_minutes,
    COALESCE(procedure_cost, 0) as procedure_cost,
    UPPER(TRIM(anesthesia_type)) as anesthesia_type,
    COALESCE(TRIM(complications), 'None') as complications,
    procedure_notes,
    CASE 
        WHEN TRIM(complications) IS NULL OR TRIM(complications) = '' OR TRIM(complications) = 'None' 
        THEN TRUE ELSE FALSE 
    END as is_successful,
    load_timestamp
FROM RAW_DATA.MEDICAL_PROCEDURES_RAW
WHERE procedure_id IS NOT NULL 
  AND admission_id IS NOT NULL;

CREATE OR REPLACE DYNAMIC TABLE TRANSFORMED.DT_CLEAN_MEDICATIONS
TARGET_LAG = '1 minute'
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
SELECT 
    order_id,
    admission_id,
    patient_id,
    UPPER(TRIM(medication_code)) as medication_code,
    INITCAP(TRIM(medication_name)) as medication_name,
    INITCAP(TRIM(prescribing_physician)) as prescribing_physician,
    order_date,
    order_time,
    COALESCE(quantity_ordered, 0) as quantity_ordered,
    UPPER(TRIM(frequency)) as frequency,
    COALESCE(duration_days, 1) as duration_days,
    UPPER(TRIM(route)) as route,
    UPPER(TRIM(priority)) as priority,
    UPPER(TRIM(order_status)) as order_status,
    COALESCE(allergies_checked, FALSE) as allergies_checked,
    COALESCE(interactions_checked, FALSE) as interactions_checked,
    load_timestamp
FROM RAW_DATA.MEDICATION_ORDERS_RAW
WHERE order_id IS NOT NULL 
  AND patient_id IS NOT NULL;

-- ============================================================================
-- 2. SILVER LAYER - Aggregated Dynamic Tables (5-minute refresh)
-- ============================================================================
-- These dynamic tables aggregate data for operational dashboards
-- Using DOWNSTREAM for some tables to create efficient refresh chains

-- Department Admission Summary (refreshes every 5 minutes)
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DT_DEPARTMENT_ADMISSION_SUMMARY
TARGET_LAG = '5 minutes'
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
SELECT 
    d.department_id,
    d.department_name,
    d.specialization_type,
    DATE_TRUNC('hour', a.admission_date) as admission_hour,
    COUNT(*) as admission_count,
    COUNT(CASE WHEN a.is_emergency THEN 1 END) as emergency_count,
    AVG(a.total_charges) as avg_charges,
    SUM(a.total_charges) as total_charges,
    AVG(a.length_of_stay_days) as avg_length_of_stay,
    MAX(a.load_timestamp) as last_updated
FROM TRANSFORMED.DT_CLEAN_ADMISSIONS a
JOIN RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d ON a.department_id = d.department_id
GROUP BY 
    d.department_id, 
    d.department_name, 
    d.specialization_type,
    DATE_TRUNC('hour', a.admission_date);

-- Physician Performance Summary
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DT_PHYSICIAN_PERFORMANCE
TARGET_LAG = '5 minutes'
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
SELECT 
    a.attending_physician as physician_name,
    d.department_name,
    DATE_TRUNC('day', a.admission_date) as report_date,
    COUNT(DISTINCT a.admission_id) as total_admissions,
    COUNT(DISTINCT p.procedure_id) as total_procedures,
    AVG(a.length_of_stay_days) as avg_patient_los,
    SUM(a.total_charges) as total_revenue,
    AVG(p.procedure_duration_minutes) as avg_procedure_duration,
    SUM(CASE WHEN p.is_successful THEN 1 ELSE 0 END) as successful_procedures,
    COUNT(p.procedure_id) as procedure_count,
    CASE 
        WHEN COUNT(p.procedure_id) > 0 
        THEN ROUND(SUM(CASE WHEN p.is_successful THEN 1 ELSE 0 END) * 100.0 / COUNT(p.procedure_id), 2)
        ELSE 100 
    END as success_rate_pct,
    MAX(a.load_timestamp) as last_updated
FROM TRANSFORMED.DT_CLEAN_ADMISSIONS a
JOIN RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d ON a.department_id = d.department_id
LEFT JOIN TRANSFORMED.DT_CLEAN_PROCEDURES p ON a.admission_id = p.admission_id
WHERE a.attending_physician IS NOT NULL
GROUP BY 
    a.attending_physician,
    d.department_name,
    DATE_TRUNC('day', a.admission_date);

-- Weather Impact Analysis (for predictive analytics)
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DT_WEATHER_ADMISSION_CORRELATION
TARGET_LAG = '5 minutes'
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
SELECT 
    a.weather_condition,
    CASE 
        WHEN a.temperature_f < 32 THEN 'Freezing (<32°F)'
        WHEN a.temperature_f BETWEEN 32 AND 50 THEN 'Cold (32-50°F)'
        WHEN a.temperature_f BETWEEN 51 AND 70 THEN 'Moderate (51-70°F)'
        WHEN a.temperature_f BETWEEN 71 AND 85 THEN 'Warm (71-85°F)'
        WHEN a.temperature_f > 85 THEN 'Hot (>85°F)'
        ELSE 'Unknown'
    END as temperature_range,
    DATE_TRUNC('day', a.admission_date) as admission_date,
    COUNT(*) as total_admissions,
    COUNT(CASE WHEN a.is_emergency THEN 1 END) as emergency_admissions,
    ROUND(COUNT(CASE WHEN a.is_emergency THEN 1 END) * 100.0 / COUNT(*), 2) as emergency_pct,
    AVG(a.length_of_stay_days) as avg_los,
    AVG(a.total_charges) as avg_charges
FROM TRANSFORMED.DT_CLEAN_ADMISSIONS a
WHERE a.weather_condition IS NOT NULL
GROUP BY 
    a.weather_condition,
    CASE 
        WHEN a.temperature_f < 32 THEN 'Freezing (<32°F)'
        WHEN a.temperature_f BETWEEN 32 AND 50 THEN 'Cold (32-50°F)'
        WHEN a.temperature_f BETWEEN 51 AND 70 THEN 'Moderate (51-70°F)'
        WHEN a.temperature_f BETWEEN 71 AND 85 THEN 'Warm (71-85°F)'
        WHEN a.temperature_f > 85 THEN 'Hot (>85°F)'
        ELSE 'Unknown'
    END,
    DATE_TRUNC('day', a.admission_date);

-- Medication Order Summary
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DT_MEDICATION_SUMMARY
TARGET_LAG = '5 minutes'
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
SELECT 
    m.medication_name,
    m.medication_code,
    DATE_TRUNC('day', m.order_date) as order_date,
    COUNT(*) as total_orders,
    SUM(m.quantity_ordered) as total_quantity,
    COUNT(DISTINCT m.patient_id) as unique_patients,
    COUNT(DISTINCT m.prescribing_physician) as prescribing_physicians,
    SUM(CASE WHEN m.priority = 'STAT' THEN 1 ELSE 0 END) as stat_orders,
    SUM(CASE WHEN m.priority = 'URGENT' THEN 1 ELSE 0 END) as urgent_orders,
    SUM(CASE WHEN m.allergies_checked THEN 1 ELSE 0 END) as allergy_checked_count,
    SUM(CASE WHEN m.interactions_checked THEN 1 ELSE 0 END) as interaction_checked_count,
    ROUND(SUM(CASE WHEN m.allergies_checked THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as allergy_check_compliance_pct,
    MAX(m.load_timestamp) as last_updated
FROM TRANSFORMED.DT_CLEAN_MEDICATIONS m
GROUP BY 
    m.medication_name,
    m.medication_code,
    DATE_TRUNC('day', m.order_date);

-- ============================================================================
-- 3. GOLD LAYER - Executive Dashboard Dynamic Tables (DOWNSTREAM refresh)
-- ============================================================================
-- These tables refresh when their upstream dependencies refresh
-- Perfect for executive dashboards that don't need independent refresh schedules

-- Hospital-Wide KPI Dashboard (refreshes when department summary refreshes)
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DT_HOSPITAL_KPIS
TARGET_LAG = DOWNSTREAM
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
SELECT 
    'Hospital-Wide' as scope,
    CURRENT_DATE() as report_date,
    SUM(admission_count) as total_admissions_today,
    SUM(emergency_count) as total_emergencies_today,
    ROUND(SUM(emergency_count) * 100.0 / NULLIF(SUM(admission_count), 0), 2) as emergency_rate_pct,
    ROUND(AVG(avg_length_of_stay), 2) as avg_length_of_stay,
    SUM(total_charges) as total_revenue_today,
    ROUND(AVG(avg_charges), 2) as avg_charges_per_admission,
    COUNT(DISTINCT department_id) as active_departments,
    MAX(last_updated) as data_freshness
FROM ANALYTICS.DT_DEPARTMENT_ADMISSION_SUMMARY
WHERE DATE_TRUNC('day', admission_hour) = CURRENT_DATE();

-- Department Ranking (for executive dashboards)
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DT_DEPARTMENT_RANKINGS
TARGET_LAG = DOWNSTREAM
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
SELECT 
    department_name,
    specialization_type,
    SUM(admission_count) as total_admissions,
    SUM(total_charges) as total_revenue,
    ROUND(AVG(avg_length_of_stay), 2) as avg_los,
    ROUND(SUM(emergency_count) * 100.0 / NULLIF(SUM(admission_count), 0), 2) as emergency_rate_pct,
    RANK() OVER (ORDER BY SUM(total_charges) DESC) as revenue_rank,
    RANK() OVER (ORDER BY SUM(admission_count) DESC) as volume_rank,
    RANK() OVER (ORDER BY AVG(avg_length_of_stay) ASC) as efficiency_rank,
    MAX(last_updated) as data_freshness
FROM ANALYTICS.DT_DEPARTMENT_ADMISSION_SUMMARY
GROUP BY department_name, specialization_type;

-- Top Physicians Performance Leaderboard
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DT_PHYSICIAN_LEADERBOARD
TARGET_LAG = DOWNSTREAM
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
SELECT 
    physician_name,
    department_name,
    SUM(total_admissions) as total_patients,
    SUM(total_procedures) as total_procedures,
    ROUND(AVG(avg_patient_los), 2) as avg_patient_los,
    SUM(total_revenue) as total_revenue,
    ROUND(AVG(success_rate_pct), 2) as avg_success_rate,
    RANK() OVER (ORDER BY SUM(total_revenue) DESC) as revenue_rank,
    RANK() OVER (ORDER BY SUM(total_procedures) DESC) as procedure_rank,
    RANK() OVER (ORDER BY AVG(success_rate_pct) DESC) as quality_rank,
    MAX(last_updated) as data_freshness
FROM ANALYTICS.DT_PHYSICIAN_PERFORMANCE
GROUP BY physician_name, department_name;

-- ============================================================================
-- 4. REAL-TIME ALERTS DYNAMIC TABLE
-- ============================================================================
-- This dynamic table identifies conditions requiring immediate attention
-- Refreshes every minute for near-real-time alerting

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DT_REALTIME_ALERTS
TARGET_LAG = '1 minute'
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
WITH department_stats AS (
    SELECT 
        department_name,
        SUM(admission_count) as hourly_admissions,
        SUM(emergency_count) as hourly_emergencies,
        AVG(avg_length_of_stay) as avg_los
    FROM ANALYTICS.DT_DEPARTMENT_ADMISSION_SUMMARY
    WHERE admission_hour >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    GROUP BY department_name
),
medication_alerts AS (
    SELECT 
        medication_name,
        SUM(stat_orders) as stat_count,
        SUM(total_orders) as total_orders,
        MIN(allergy_check_compliance_pct) as min_compliance
    FROM ANALYTICS.DT_MEDICATION_SUMMARY
    WHERE order_date = CURRENT_DATE()
    GROUP BY medication_name
    HAVING SUM(stat_orders) > 5 OR MIN(allergy_check_compliance_pct) < 80
)
SELECT 
    'DEPARTMENT_SURGE' as alert_type,
    'HIGH' as severity,
    department_name as entity_name,
    'High admission volume: ' || hourly_admissions || ' admissions in last hour' as alert_message,
    hourly_admissions as metric_value,
    CURRENT_TIMESTAMP() as alert_timestamp
FROM department_stats
WHERE hourly_admissions > 20

UNION ALL

SELECT 
    'EMERGENCY_SPIKE' as alert_type,
    'CRITICAL' as severity,
    department_name as entity_name,
    'Emergency surge: ' || hourly_emergencies || ' emergencies in last hour' as alert_message,
    hourly_emergencies as metric_value,
    CURRENT_TIMESTAMP() as alert_timestamp
FROM department_stats
WHERE hourly_emergencies > 10

UNION ALL

SELECT 
    'MEDICATION_COMPLIANCE' as alert_type,
    'WARNING' as severity,
    medication_name as entity_name,
    'Low allergy check compliance: ' || min_compliance || '%' as alert_message,
    min_compliance as metric_value,
    CURRENT_TIMESTAMP() as alert_timestamp
FROM medication_alerts
WHERE min_compliance < 80

UNION ALL

SELECT 
    'STAT_MEDICATION_VOLUME' as alert_type,
    'WARNING' as severity,
    medication_name as entity_name,
    'High STAT order volume: ' || stat_count || ' orders today' as alert_message,
    stat_count as metric_value,
    CURRENT_TIMESTAMP() as alert_timestamp
FROM medication_alerts
WHERE stat_count > 5;

-- ============================================================================
-- 5. GRANT PERMISSIONS ON DYNAMIC TABLES
-- ============================================================================

-- Bronze layer access for data engineers
GRANT SELECT ON DYNAMIC TABLE TRANSFORMED.DT_CLEAN_ADMISSIONS TO ROLE DATA_ENGINEER;
GRANT SELECT ON DYNAMIC TABLE TRANSFORMED.DT_CLEAN_PROCEDURES TO ROLE DATA_ENGINEER;
GRANT SELECT ON DYNAMIC TABLE TRANSFORMED.DT_CLEAN_MEDICATIONS TO ROLE DATA_ENGINEER;

-- Silver layer access for clinical admin and analysts
GRANT SELECT ON DYNAMIC TABLE ANALYTICS.DT_DEPARTMENT_ADMISSION_SUMMARY TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON DYNAMIC TABLE ANALYTICS.DT_DEPARTMENT_ADMISSION_SUMMARY TO ROLE ANALYST;
GRANT SELECT ON DYNAMIC TABLE ANALYTICS.DT_PHYSICIAN_PERFORMANCE TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON DYNAMIC TABLE ANALYTICS.DT_WEATHER_ADMISSION_CORRELATION TO ROLE ANALYST;
GRANT SELECT ON DYNAMIC TABLE ANALYTICS.DT_MEDICATION_SUMMARY TO ROLE CLINICAL_ADMIN;

-- Gold layer access for executives
GRANT SELECT ON DYNAMIC TABLE ANALYTICS.DT_HOSPITAL_KPIS TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON DYNAMIC TABLE ANALYTICS.DT_DEPARTMENT_RANKINGS TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON DYNAMIC TABLE ANALYTICS.DT_PHYSICIAN_LEADERBOARD TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON DYNAMIC TABLE ANALYTICS.DT_REALTIME_ALERTS TO ROLE CLINICAL_ADMIN;

-- ============================================================================
-- 6. MONITORING AND VALIDATION QUERIES
-- ============================================================================

-- View all dynamic tables and their refresh status
SHOW DYNAMIC TABLES IN DATABASE HOSPITAL_DEMO;

-- Check refresh history for dynamic tables
SELECT 
    name,
    state,
    state_message,
    refresh_trigger,
    refresh_action,
    data_timestamp,
    query_id
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE name LIKE 'DT_%'
ORDER BY data_timestamp DESC
LIMIT 20;

-- View dynamic table graph (dependency relationships)
SELECT 
    name,
    target_lag,
    warehouse,
    refresh_mode,
    scheduling_state
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
WHERE name LIKE 'DT_%';

-- ============================================================================
-- 7. SAMPLE QUERIES FOR DEMO
-- ============================================================================

-- Real-time hospital KPIs
SELECT 'Hospital-Wide KPIs (Auto-Refreshing)' as report_title;
SELECT * FROM ANALYTICS.DT_HOSPITAL_KPIS;

-- Department rankings
SELECT 'Department Rankings by Revenue' as report_title;
SELECT 
    department_name,
    specialization_type,
    total_admissions,
    total_revenue,
    revenue_rank,
    volume_rank,
    efficiency_rank
FROM ANALYTICS.DT_DEPARTMENT_RANKINGS
ORDER BY revenue_rank
LIMIT 10;

-- Physician leaderboard
SELECT 'Top Physicians by Quality Score' as report_title;
SELECT 
    physician_name,
    department_name,
    total_patients,
    total_procedures,
    avg_success_rate,
    quality_rank
FROM ANALYTICS.DT_PHYSICIAN_LEADERBOARD
ORDER BY quality_rank
LIMIT 10;

-- Weather correlation insights
SELECT 'Weather Impact on Admissions' as report_title;
SELECT 
    weather_condition,
    temperature_range,
    SUM(total_admissions) as total_admissions,
    ROUND(AVG(emergency_pct), 2) as avg_emergency_pct,
    ROUND(AVG(avg_los), 2) as avg_length_of_stay
FROM ANALYTICS.DT_WEATHER_ADMISSION_CORRELATION
GROUP BY weather_condition, temperature_range
ORDER BY total_admissions DESC;

-- Current alerts
SELECT 'Active Real-Time Alerts' as report_title;
SELECT 
    alert_type,
    severity,
    entity_name,
    alert_message,
    alert_timestamp
FROM ANALYTICS.DT_REALTIME_ALERTS
ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'WARNING' THEN 3 
        ELSE 4 
    END,
    alert_timestamp DESC;

-- ============================================================================
-- 8. DYNAMIC TABLE PIPELINE VISUALIZATION
-- ============================================================================
-- The following shows the dependency graph of our dynamic tables:
--
--  RAW DATA (Base Tables)
--       │
--       ▼
--  ┌─────────────────────────────────────────────────────────────┐
--  │  BRONZE LAYER (1-minute lag)                                │
--  │  ├── DT_CLEAN_ADMISSIONS                                    │
--  │  ├── DT_CLEAN_PROCEDURES                                    │
--  │  └── DT_CLEAN_MEDICATIONS                                   │
--  └─────────────────────────────────────────────────────────────┘
--       │
--       ▼
--  ┌─────────────────────────────────────────────────────────────┐
--  │  SILVER LAYER (5-minute lag)                                │
--  │  ├── DT_DEPARTMENT_ADMISSION_SUMMARY ◄─┐                    │
--  │  ├── DT_PHYSICIAN_PERFORMANCE          │                    │
--  │  ├── DT_WEATHER_ADMISSION_CORRELATION  │                    │
--  │  └── DT_MEDICATION_SUMMARY             │                    │
--  └─────────────────────────────────────────┼────────────────────┘
--       │                                    │
--       ▼                                    │
--  ┌─────────────────────────────────────────┼────────────────────┐
--  │  GOLD LAYER (DOWNSTREAM - refreshes when silver refreshes)  │
--  │  ├── DT_HOSPITAL_KPIS ─────────────────┘                    │
--  │  ├── DT_DEPARTMENT_RANKINGS                                 │
--  │  └── DT_PHYSICIAN_LEADERBOARD                               │
--  └─────────────────────────────────────────────────────────────┘
--       │
--       ▼
--  ┌─────────────────────────────────────────────────────────────┐
--  │  ALERTING (1-minute lag)                                    │
--  │  └── DT_REALTIME_ALERTS                                     │
--  └─────────────────────────────────────────────────────────────┘

SELECT 'Dynamic Tables pipeline created successfully!' as status_message;
SELECT 'Data will automatically refresh based on target lag settings.' as info;
SELECT 'Monitor refresh status in Snowsight: Monitoring > Dynamic Tables' as tip;
