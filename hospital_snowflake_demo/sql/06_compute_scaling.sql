-- ============================================================================
-- Hospital Snowflake Demo - Compute Scaling and Performance
-- ============================================================================
-- This script demonstrates Snowflake's compute scaling capabilities
-- IMPORTANT: Run this script as ACCOUNTADMIN for warehouse management privileges

-- Note: Warehouse operations require ACCOUNTADMIN privileges
USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA ANALYTICS;

-- 1. Create Different Warehouse Configurations for Various Workloads

-- Small warehouse for simple queries
CREATE OR REPLACE WAREHOUSE HOSPITAL_SMALL_WH
WITH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD'
COMMENT = 'Small warehouse for simple queries and lookups';

-- Medium warehouse for regular analytics
CREATE OR REPLACE WAREHOUSE HOSPITAL_MEDIUM_WH
WITH 
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD'
COMMENT = 'Medium warehouse for regular analytics workloads';

-- Large warehouse for complex analytics
CREATE OR REPLACE WAREHOUSE HOSPITAL_LARGE_WH
WITH 
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 600
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 5
    SCALING_POLICY = 'STANDARD'
COMMENT = 'Large warehouse for complex analytics and reporting';

-- Multi-cluster warehouse for high concurrency
CREATE OR REPLACE WAREHOUSE HOSPITAL_CONCURRENT_WH
WITH 
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 2
    MAX_CLUSTER_COUNT = 8
    SCALING_POLICY = 'ECONOMY'
COMMENT = 'Multi-cluster warehouse for high concurrency scenarios';

-- 2. Grant Warehouse Permissions
GRANT USAGE ON WAREHOUSE HOSPITAL_SMALL_WH TO ROLE NURSE;
GRANT USAGE ON WAREHOUSE HOSPITAL_MEDIUM_WH TO ROLE PHYSICIAN;
GRANT USAGE ON WAREHOUSE HOSPITAL_LARGE_WH TO ROLE ANALYST;
GRANT USAGE ON WAREHOUSE HOSPITAL_CONCURRENT_WH TO ROLE CLINICAL_ADMIN;

-- 3. Performance Testing Queries

-- Simple query - use small warehouse
USE WAREHOUSE HOSPITAL_SMALL_WH;

SELECT 'Simple Query Performance Test' as test_type;
SELECT 
    department_name,
    COUNT(*) as patient_count
FROM VW_ANALYST_DASHBOARD
GROUP BY department_name
ORDER BY patient_count DESC;

-- Check query performance
SELECT 
    QUERY_ID,
    QUERY_TEXT,
    WAREHOUSE_NAME,
    WAREHOUSE_SIZE,
    EXECUTION_TIME,
    COMPILATION_TIME,
    TOTAL_ELAPSED_TIME
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TEXT ILIKE '%department_name%'
ORDER BY START_TIME DESC
LIMIT 1;

-- Medium complexity query - use medium warehouse
USE WAREHOUSE HOSPITAL_MEDIUM_WH;

SELECT 'Medium Complexity Query Performance Test' as test_type;
WITH monthly_stats AS (
    SELECT 
        month_name,
        department_name,
        weather_condition,
        COUNT(*) as admissions,
        AVG(avg_length_of_stay) as avg_los,
        SUM(total_revenue) as revenue
    FROM VW_ANALYST_DASHBOARD
    GROUP BY month_name, department_name, weather_condition
)
SELECT 
    month_name,
    department_name,
    SUM(admissions) as total_admissions,
    AVG(avg_los) as overall_avg_los,
    SUM(revenue) as total_revenue,
    COUNT(DISTINCT weather_condition) as weather_variety
FROM monthly_stats
GROUP BY month_name, department_name
ORDER BY total_revenue DESC;

-- Complex analytical query - use large warehouse
USE WAREHOUSE HOSPITAL_LARGE_WH;

SELECT 'Complex Analytics Query Performance Test' as test_type;
WITH patient_journey AS (
    SELECT 
        fa.patient_key,
        dp.age_group,
        dp.insurance_provider,
        COUNT(*) as admission_count,
        AVG(fa.length_of_stay_days) as avg_los,
        SUM(fa.total_charges) as total_charges,
        MIN(dt.date_value) as first_admission,
        MAX(dt.date_value) as last_admission,
        COUNT(DISTINCT fa.department_key) as departments_visited,
        LISTAGG(DISTINCT dd.department_name, ', ') as departments_list
    FROM TRANSFORMED.FACT_ADMISSIONS fa
    JOIN TRANSFORMED.DIM_PATIENT dp ON fa.patient_key = dp.patient_key
    JOIN TRANSFORMED.DIM_DATE dt ON fa.admission_date_key = dt.date_key
    JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
    WHERE dp.is_current = TRUE
    GROUP BY fa.patient_key, dp.age_group, dp.insurance_provider
),
weather_impact AS (
    SELECT 
        dw.weather_condition,
        dw.temperature_range,
        COUNT(*) as weather_admissions,
        AVG(fa.length_of_stay_days) as weather_avg_los
    FROM TRANSFORMED.FACT_ADMISSIONS fa
    JOIN TRANSFORMED.DIM_WEATHER dw ON fa.weather_key = dw.weather_key
    GROUP BY dw.weather_condition, dw.temperature_range
)
SELECT 
    pj.age_group,
    pj.insurance_provider,
    COUNT(*) as patient_count,
    AVG(pj.admission_count) as avg_admissions_per_patient,
    AVG(pj.avg_los) as avg_length_of_stay,
    AVG(pj.total_charges) as avg_total_charges,
    AVG(pj.departments_visited) as avg_departments_per_patient,
    wi.weather_condition,
    wi.weather_avg_los
FROM patient_journey pj
CROSS JOIN weather_impact wi
GROUP BY pj.age_group, pj.insurance_provider, wi.weather_condition, wi.weather_avg_los
ORDER BY avg_total_charges DESC;

-- 4. Demonstrate Auto-Scaling with Concurrent Queries
USE WAREHOUSE HOSPITAL_CONCURRENT_WH;

-- Simple concurrent query examples (run these manually to show scaling)
SELECT 'Concurrent Query Example 1: Admission Count' as query_type;
SELECT COUNT(*) as total_admissions FROM TRANSFORMED.FACT_ADMISSIONS;

SELECT 'Concurrent Query Example 2: Department Summary' as query_type;
SELECT department_name, COUNT(*) as admission_count
FROM VW_ANALYST_DASHBOARD 
GROUP BY department_name
ORDER BY admission_count DESC;

SELECT 'Concurrent Query Example 3: Weather Analysis' as query_type;
SELECT weather_condition, AVG(total_revenue) as avg_revenue
FROM VW_ANALYST_DASHBOARD 
WHERE weather_condition IS NOT NULL
GROUP BY weather_condition
ORDER BY avg_revenue DESC;

-- 5. Warehouse Performance Monitoring
-- Note: Account usage views require ACCOUNTADMIN privileges
-- For demo purposes, we'll create a simplified monitoring view
CREATE OR REPLACE VIEW VW_WAREHOUSE_PERFORMANCE AS
SELECT 
    'HOSPITAL_SMALL_WH' as warehouse_name,
    'X-SMALL' as warehouse_size,
    'Optimized for simple queries and lookups' as usage_description,
    60 as auto_suspend_minutes,
    1 as min_clusters,
    1 as max_clusters
UNION ALL
SELECT 'HOSPITAL_MEDIUM_WH', 'MEDIUM', 'Regular analytics workloads', 300, 1, 3
UNION ALL
SELECT 'HOSPITAL_LARGE_WH', 'LARGE', 'Complex analytics and reporting', 600, 1, 5
UNION ALL
SELECT 'HOSPITAL_CONCURRENT_WH', 'MEDIUM', 'High concurrency scenarios', 300, 2, 8;

-- Alternative: Use INFORMATION_SCHEMA for current session queries (limited scope)
/*
CREATE OR REPLACE VIEW VW_RECENT_QUERY_PERFORMANCE AS
SELECT 
    query_id,
    query_text,
    warehouse_name,
    execution_time,
    total_elapsed_time,
    start_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE warehouse_name LIKE 'HOSPITAL_%'
  AND start_time >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY start_time DESC;
*/

-- 6. Cost Analysis by Warehouse
-- Note: Account usage views require ACCOUNTADMIN privileges
-- For demo purposes, we'll create estimated cost projections
CREATE OR REPLACE VIEW VW_WAREHOUSE_COSTS AS
SELECT 
    warehouse_name,
    warehouse_size,
    CURRENT_DATE() as usage_date,
    CASE 
        WHEN warehouse_size = 'X-SMALL' THEN 2.0
        WHEN warehouse_size = 'SMALL' THEN 4.0
        WHEN warehouse_size = 'MEDIUM' THEN 8.0
        WHEN warehouse_size = 'LARGE' THEN 16.0
        WHEN warehouse_size = 'X-LARGE' THEN 32.0
        ELSE 8.0
    END as estimated_hourly_credits,
    CASE 
        WHEN warehouse_size = 'X-SMALL' THEN 2.0 * 2.5
        WHEN warehouse_size = 'SMALL' THEN 4.0 * 2.5
        WHEN warehouse_size = 'MEDIUM' THEN 8.0 * 2.5
        WHEN warehouse_size = 'LARGE' THEN 16.0 * 2.5
        WHEN warehouse_size = 'X-LARGE' THEN 32.0 * 2.5
        ELSE 8.0 * 2.5
    END as estimated_hourly_cost_usd
FROM (
    SELECT 'HOSPITAL_SMALL_WH' as warehouse_name, 'X-SMALL' as warehouse_size
    UNION ALL
    SELECT 'HOSPITAL_MEDIUM_WH', 'MEDIUM'
    UNION ALL
    SELECT 'HOSPITAL_LARGE_WH', 'LARGE'
    UNION ALL
    SELECT 'HOSPITAL_CONCURRENT_WH', 'MEDIUM'
);

-- 7. Query Performance Recommendations
-- Note: Account usage views require ACCOUNTADMIN privileges
-- For demo purposes, we'll create performance guidelines
CREATE OR REPLACE VIEW VW_QUERY_PERFORMANCE_RECOMMENDATIONS AS
SELECT 
    'SIMPLE_QUERIES' as query_type,
    'X-SMALL to SMALL' as recommended_warehouse_size,
    'Patient lookups, basic reports' as use_case,
    '$2-4 per hour' as estimated_cost,
    'Use for nurse and basic physician queries' as recommendation
UNION ALL
SELECT 'ANALYTICAL_QUERIES', 'MEDIUM to LARGE', 'Department analytics, trending', '$8-16 per hour', 'Use for analyst and physician research'
UNION ALL
SELECT 'COMPLEX_ANALYTICS', 'LARGE to X-LARGE', 'Population health, ML models', '$16-32 per hour', 'Use for research and strategic analysis'
UNION ALL
SELECT 'HIGH_CONCURRENCY', 'MEDIUM with multi-cluster', 'Many simultaneous users', '$8+ per hour', 'Auto-scale based on user demand';

-- 8. Demonstrate Warehouse Resizing
-- Show current warehouse sizes
SHOW WAREHOUSES LIKE 'HOSPITAL_%';

-- Resize warehouse dynamically (example)
ALTER WAREHOUSE HOSPITAL_MEDIUM_WH SET WAREHOUSE_SIZE = 'LARGE';
SELECT 'Warehouse resized to LARGE' as scaling_action;

-- Resize back
ALTER WAREHOUSE HOSPITAL_MEDIUM_WH SET WAREHOUSE_SIZE = 'MEDIUM';
SELECT 'Warehouse resized back to MEDIUM' as scaling_action;

-- 9. Create Scaling Alerts (using stored procedure)
-- Note: Account usage views require ACCOUNTADMIN privileges
-- For demo purposes, we'll create a simplified utilization check
CREATE OR REPLACE PROCEDURE CHECK_WAREHOUSE_UTILIZATION()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    current_hour INTEGER;
    utilization_status STRING;
BEGIN
    SELECT HOUR(CURRENT_TIME()) INTO current_hour;
    
    -- Simulate utilization based on time of day
    IF (current_hour BETWEEN 8 AND 17) THEN
        LET utilization_status := 'PEAK HOURS: Monitor warehouse scaling closely during business hours';
    ELSEIF (current_hour BETWEEN 18 AND 22) THEN
        LET utilization_status := 'MODERATE: Evening shift - medium utilization expected';
    ELSE
        LET utilization_status := 'LOW: Off-hours - minimal utilization expected';
    END IF;
    
    RETURN 'Utilization Status: ' || utilization_status;
END;
$$;

-- Run utilization check
CALL CHECK_WAREHOUSE_UTILIZATION();

-- 10. Show Performance Results
SELECT 'Warehouse Performance Summary:' as summary;
SELECT * FROM VW_WAREHOUSE_PERFORMANCE;

SELECT 'Warehouse Cost Analysis:' as summary;
SELECT * FROM VW_WAREHOUSE_COSTS;

SELECT 'Performance Recommendations:' as summary;
SELECT * FROM VW_QUERY_PERFORMANCE_RECOMMENDATIONS;

SELECT 'Compute scaling demonstration completed successfully!' as status_message;
SELECT 'Warehouses configured for optimal performance at different scales.' as next_step;
