-- ============================================================================
-- Hospital Snowflake Demo - Bed Management Analytics
-- ============================================================================
-- This script creates analytics views for bed management and occupancy
-- IMPORTANT: Run this script as ACCOUNTADMIN for full access to all objects

-- Note: Using ACCOUNTADMIN for comprehensive access to all objects
USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA ANALYTICS;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;

-- 1. Bed Utilization Dashboard
CREATE OR REPLACE VIEW VW_BED_UTILIZATION_DASHBOARD AS
SELECT 
    dd.department_name,
    dd.specialization_type,
    COUNT(DISTINCT db.bed_key) as total_beds,
    SUM(CASE WHEN fba.is_occupied THEN 1 ELSE 0 END) as occupied_beds,
    SUM(CASE WHEN fba.is_available THEN 1 ELSE 0 END) as available_beds,
    SUM(CASE WHEN fba.is_maintenance THEN 1 ELSE 0 END) as maintenance_beds,
    ROUND(AVG(fba.utilization_rate) * 100, 2) as utilization_percentage,
    SUM(fba.revenue_potential) as daily_revenue_potential,
    dt.date_value as report_date
FROM TRANSFORMED.FACT_BED_AVAILABILITY fba
JOIN TRANSFORMED.DIM_BED db ON fba.bed_key = db.bed_key
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fba.department_key = dd.department_key
JOIN TRANSFORMED.DIM_DATE dt ON fba.date_key = dt.date_key
WHERE dt.date_value >= CURRENT_DATE - 30  -- Last 30 days
GROUP BY dd.department_name, dd.specialization_type, dt.date_value
ORDER BY dt.date_value DESC, utilization_percentage DESC;

-- 2. Real-time Bed Status View
CREATE OR REPLACE VIEW VW_CURRENT_BED_STATUS AS
SELECT 
    db.bed_id,
    dd.department_name,
    db.room_number,
    db.bed_number,
    db.bed_type,
    db.equipment,
    fba.status,
    fba.reserved_until,
    dp.full_name as current_patient,
    fbo.check_in_time,
    fbo.planned_checkout_date,
    fbo.total_nights,
    fbo.nightly_rate,
    fba.last_updated
FROM TRANSFORMED.DIM_BED db
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON db.department_id = dd.department_id
LEFT JOIN TRANSFORMED.FACT_BED_AVAILABILITY fba ON db.bed_key = fba.bed_key 
    AND fba.availability_date CURRENT_DATE - 90
LEFT JOIN TRANSFORMED.FACT_BED_OCCUPANCY fbo ON db.bed_key = fbo.bed_key 
    AND fbo.is_occupied = TRUE
LEFT JOIN TRANSFORMED.DIM_PATIENT dp ON fbo.patient_key = dp.patient_key 
    AND dp.is_current = TRUE
WHERE db.is_active = TRUE
ORDER BY dd.department_name, db.room_number, db.bed_number;

-- 3. Bed Revenue Analysis
CREATE OR REPLACE VIEW VW_BED_REVENUE_ANALYSIS AS
SELECT 
    dd.department_name,
    db.bed_type,
    COUNT(DISTINCT db.bed_key) as bed_count,
    AVG(db.daily_rate) as avg_daily_rate,
    SUM(fbo.total_bed_charges) as total_revenue,
    AVG(fbo.total_nights) as avg_length_of_stay,
    COUNT(fbo.booking_id) as total_bookings,
    SUM(fbo.total_bed_charges) / COUNT(DISTINCT db.bed_key) as revenue_per_bed,
    ROUND(COUNT(fbo.booking_id) / COUNT(DISTINCT db.bed_key), 2) as bookings_per_bed
FROM TRANSFORMED.DIM_BED db
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON db.department_id = dd.department_id
LEFT JOIN TRANSFORMED.FACT_BED_OCCUPANCY fbo ON db.bed_key = fbo.bed_key
GROUP BY dd.department_name, db.bed_type
ORDER BY total_revenue DESC;

-- 4. Bed Turnover Analysis
CREATE OR REPLACE VIEW VW_BED_TURNOVER_ANALYSIS AS
WITH bed_turnovers AS (
    SELECT 
        db.bed_key,
        dd.department_name,
        db.bed_type,
        COUNT(fbo.booking_id) as bookings_count,
        AVG(fbo.total_nights) as avg_stay_duration,
        MIN(fbo.occupancy_date) as first_booking,
        MAX(fbo.planned_checkout_date) as last_checkout,
        DATEDIFF(day, MIN(fbo.occupancy_date), MAX(fbo.planned_checkout_date)) as analysis_period_days
    FROM TRANSFORMED.DIM_BED db
    JOIN TRANSFORMED.DIM_DEPARTMENT dd ON db.department_id = dd.department_id
    JOIN TRANSFORMED.FACT_BED_OCCUPANCY fbo ON db.bed_key = fbo.bed_key
    GROUP BY db.bed_key, dd.department_name, db.bed_type
)
SELECT 
    department_name,
    bed_type,
    COUNT(*) as beds_analyzed,
    AVG(bookings_count) as avg_bookings_per_bed,
    AVG(avg_stay_duration) as avg_patient_stay_days,
    AVG(analysis_period_days) as avg_analysis_period,
    ROUND(AVG(bookings_count) / AVG(analysis_period_days) * 365, 2) as annual_turnover_rate,
    CASE 
        WHEN AVG(bookings_count) / AVG(analysis_period_days) * 365 > 100 THEN 'High Turnover'
        WHEN AVG(bookings_count) / AVG(analysis_period_days) * 365 > 50 THEN 'Medium Turnover'
        ELSE 'Low Turnover'
    END as turnover_category
FROM bed_turnovers
GROUP BY department_name, bed_type
ORDER BY annual_turnover_rate DESC;

-- 5. Bed Capacity Planning View
CREATE OR REPLACE VIEW VW_BED_CAPACITY_PLANNING AS
WITH daily_occupancy AS (
    SELECT 
        fba.availability_date,
        dd.department_name,
        COUNT(DISTINCT db.bed_key) as total_beds,
        SUM(CASE WHEN fba.is_occupied THEN 1 ELSE 0 END) as occupied_beds,
        SUM(CASE WHEN fba.is_available THEN 1 ELSE 0 END) as available_beds,
        SUM(CASE WHEN fba.is_maintenance THEN 1 ELSE 0 END) as maintenance_beds,
        ROUND(AVG(fba.utilization_rate) * 100, 2) as daily_utilization_pct
    FROM TRANSFORMED.FACT_BED_AVAILABILITY fba
    JOIN TRANSFORMED.DIM_BED db ON fba.bed_key = db.bed_key
    JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fba.department_key = dd.department_key
    JOIN TRANSFORMED.DIM_DATE dt ON fba.date_key = dt.date_key
    WHERE dt.date_value >= CURRENT_DATE - 90  -- Last 90 days
    GROUP BY fba.availability_date, dd.department_name
)
SELECT 
    department_name,
    AVG(total_beds) as avg_total_beds,
    AVG(occupied_beds) as avg_occupied_beds,
    AVG(available_beds) as avg_available_beds,
    AVG(maintenance_beds) as avg_maintenance_beds,
    AVG(daily_utilization_pct) as avg_utilization_percentage,
    MAX(daily_utilization_pct) as peak_utilization_percentage,
    MIN(daily_utilization_pct) as min_utilization_percentage,
    STDDEV(daily_utilization_pct) as utilization_volatility,
    CASE 
        WHEN AVG(daily_utilization_pct) > 85 THEN 'Over Capacity - Need More Beds'
        WHEN AVG(daily_utilization_pct) > 70 THEN 'High Utilization - Monitor Closely'
        WHEN AVG(daily_utilization_pct) > 50 THEN 'Good Utilization'
        ELSE 'Under Utilized - Consider Reallocation'
    END as capacity_recommendation
FROM daily_occupancy
GROUP BY department_name
ORDER BY avg_utilization_percentage DESC;

-- 6. Bed Assignment Optimization View
CREATE OR REPLACE VIEW VW_BED_ASSIGNMENT_OPTIMIZATION AS
SELECT 
    dd.department_name,
    db.bed_type,
    db.equipment,
    COUNT(DISTINCT db.bed_key) as beds_available,
    AVG(db.daily_rate) as avg_daily_rate,
    COUNT(fbo.booking_id) as historical_bookings,
    AVG(fbo.total_nights) as avg_booking_duration,
    SUM(fbo.total_bed_charges) as total_revenue_generated,
    COUNT(CASE WHEN fbo.special_requirements IS NOT NULL THEN 1 END) as special_requirements_count,
    ROUND(COUNT(fbo.booking_id) / COUNT(DISTINCT db.bed_key), 2) as bookings_per_bed_ratio,
    CASE 
        WHEN COUNT(fbo.booking_id) / COUNT(DISTINCT db.bed_key) > 50 THEN 'High Demand'
        WHEN COUNT(fbo.booking_id) / COUNT(DISTINCT db.bed_key) > 25 THEN 'Medium Demand'
        ELSE 'Low Demand'
    END as demand_level
FROM TRANSFORMED.DIM_BED db
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON db.department_id = dd.department_id
LEFT JOIN TRANSFORMED.FACT_BED_OCCUPANCY fbo ON db.bed_key = fbo.bed_key
WHERE db.is_active = TRUE
GROUP BY dd.department_name, db.bed_type, db.equipment
ORDER BY bookings_per_bed_ratio DESC;

-- 7. Patient Flow and Bed Management
CREATE OR REPLACE VIEW VW_PATIENT_FLOW_ANALYSIS AS
SELECT 
    dt.date_value,
    dt.day_name,
    dd.department_name,
    COUNT(DISTINCT fa.admission_id) as admissions,
    COUNT(DISTINCT fbo.booking_id) as bed_bookings,
    AVG(fa.length_of_stay_days) as avg_admission_los,
    AVG(fbo.total_nights) as avg_bed_nights,
    SUM(fa.total_charges) as total_admission_charges,
    SUM(fbo.total_bed_charges) as total_bed_charges,
    ROUND((SUM(fbo.total_bed_charges) / SUM(fa.total_charges)) * 100, 2) as bed_charge_percentage,
    COUNT(CASE WHEN fbo.is_overdue THEN 1 END) as overdue_discharges
FROM TRANSFORMED.DIM_DATE dt
LEFT JOIN TRANSFORMED.FACT_ADMISSIONS fa ON dt.date_key = fa.admission_date_key
LEFT JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
LEFT JOIN TRANSFORMED.FACT_BED_OCCUPANCY fbo ON fa.admission_key = fbo.bed_key -- Simplified join
WHERE dt.date_value >= CURRENT_DATE - 30
GROUP BY dt.date_value, dt.day_name, dd.department_name
HAVING COUNT(DISTINCT fa.admission_id) > 0
ORDER BY dt.date_value DESC, admissions DESC;

-- 8. Bed Management KPIs
CREATE OR REPLACE VIEW VW_BED_MANAGEMENT_KPIS AS
WITH current_metrics AS (
    SELECT 
        COUNT(DISTINCT db.bed_key) as total_beds,
        SUM(CASE WHEN fba.is_occupied THEN 1 ELSE 0 END) as occupied_beds,
        SUM(CASE WHEN fba.is_available THEN 1 ELSE 0 END) as available_beds,
        SUM(CASE WHEN fba.is_maintenance THEN 1 ELSE 0 END) as maintenance_beds,
        AVG(fba.utilization_rate) as avg_utilization_rate,
        SUM(fba.revenue_potential) as total_revenue_potential
    FROM TRANSFORMED.DIM_BED db
    LEFT JOIN TRANSFORMED.FACT_BED_AVAILABILITY fba ON db.bed_key = fba.bed_key 
        AND fba.availability_date <= CURRENT_DATE - 90 
    WHERE db.is_active = TRUE
),
monthly_metrics AS (
    SELECT 
        AVG(fbo.total_nights) as avg_length_of_stay,
        SUM(fbo.total_bed_charges) as monthly_bed_revenue,
        COUNT(fbo.booking_id) as monthly_bookings,
        COUNT(CASE WHEN fbo.is_overdue THEN 1 END) as overdue_discharges
    FROM TRANSFORMED.FACT_BED_OCCUPANCY fbo
    JOIN TRANSFORMED.DIM_DATE dt ON fbo.date_key = dt.date_key
    WHERE dt.date_value >= CURRENT_DATE - 30
)
SELECT 
    'Current Bed Utilization' as kpi_category,
    'Total Beds' as kpi_name,
    cm.total_beds as kpi_value,
    NULL as target_value,
    'Count' as unit
FROM current_metrics cm
UNION ALL
SELECT 'Current Bed Utilization', 'Occupied Beds', cm.occupied_beds, NULL, 'Count' FROM current_metrics cm
UNION ALL
SELECT 'Current Bed Utilization', 'Available Beds', cm.available_beds, NULL, 'Count' FROM current_metrics cm
UNION ALL
SELECT 'Current Bed Utilization', 'Utilization Rate', ROUND(cm.avg_utilization_rate * 100, 2), 75, 'Percentage' FROM current_metrics cm
UNION ALL
SELECT 'Monthly Performance', 'Average Length of Stay', ROUND(mm.avg_length_of_stay, 2), 4.5, 'Days' FROM monthly_metrics mm
UNION ALL
SELECT 'Monthly Performance', 'Bed Revenue', mm.monthly_bed_revenue, NULL, 'USD' FROM monthly_metrics mm
UNION ALL
SELECT 'Monthly Performance', 'Total Bookings', mm.monthly_bookings, NULL, 'Count' FROM monthly_metrics mm
UNION ALL
SELECT 'Monthly Performance', 'Overdue Discharges', mm.overdue_discharges, 0, 'Count' FROM monthly_metrics mm;

-- 9. Bed Allocation Recommendations
CREATE OR REPLACE VIEW VW_BED_ALLOCATION_RECOMMENDATIONS AS
WITH department_demand AS (
    SELECT 
        dd.department_name,
        COUNT(DISTINCT db.bed_key) as current_beds,
        AVG(fba.utilization_rate) as avg_utilization,
        COUNT(fbo.booking_id) as total_bookings,
        AVG(fbo.total_nights) as avg_stay_duration,
        SUM(fbo.total_bed_charges) as total_revenue
    FROM TRANSFORMED.DIM_DEPARTMENT dd
    LEFT JOIN TRANSFORMED.DIM_BED db ON dd.department_id = db.department_id
    LEFT JOIN TRANSFORMED.FACT_BED_AVAILABILITY fba ON db.bed_key = fba.bed_key
    LEFT JOIN TRANSFORMED.FACT_BED_OCCUPANCY fbo ON db.bed_key = fbo.bed_key
    WHERE db.is_active = TRUE
    GROUP BY dd.department_name
)
SELECT 
    department_name,
    current_beds,
    ROUND(avg_utilization * 100, 2) as utilization_percentage,
    total_bookings,
    ROUND(avg_stay_duration, 2) as avg_stay_days,
    total_revenue,
    CASE 
        WHEN avg_utilization > 0.9 THEN CEIL(current_beds * 0.2)  -- Add 20% more beds
        WHEN avg_utilization > 0.8 THEN CEIL(current_beds * 0.1)  -- Add 10% more beds
        WHEN avg_utilization < 0.5 THEN -FLOOR(current_beds * 0.1) -- Remove 10% of beds
        ELSE 0
    END as recommended_bed_change,
    CASE 
        WHEN avg_utilization > 0.9 THEN 'Increase Capacity - High Demand'
        WHEN avg_utilization > 0.8 THEN 'Monitor Closely - Near Capacity'
        WHEN avg_utilization > 0.6 THEN 'Optimal Utilization'
        WHEN avg_utilization > 0.4 THEN 'Consider Reallocation'
        ELSE 'Significant Under-Utilization'
    END as recommendation
FROM department_demand
WHERE current_beds > 0
ORDER BY avg_utilization DESC;

-- 10. Bed Management Alerts
CREATE OR REPLACE TABLE BED_MANAGEMENT_ALERTS (
    alert_id INTEGER AUTOINCREMENT PRIMARY KEY,
    alert_date DATE,
    department_name STRING,
    alert_type STRING,
    alert_severity STRING,
    alert_message STRING,
    current_utilization DECIMAL(5,2),
    recommended_action STRING,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Stored procedure to generate bed management alerts
CREATE OR REPLACE PROCEDURE GENERATE_BED_ALERTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    dept_cursor CURSOR FOR 
        SELECT department_name, utilization_percentage 
        FROM VW_BED_UTILIZATION_DASHBOARD 
        WHERE report_date = CURRENT_DATE();
    dept_name STRING;
    utilization DECIMAL(5,2);
    alert_count INTEGER := 0;
BEGIN
    -- Clear today's alerts
    DELETE FROM BED_MANAGEMENT_ALERTS WHERE alert_date = CURRENT_DATE();
    
    -- Generate new alerts
    FOR record IN dept_cursor DO
        dept_name := record.department_name;
        utilization := record.utilization_percentage;
        
        IF (utilization > 95) THEN
            INSERT INTO BED_MANAGEMENT_ALERTS (
                alert_date, department_name, alert_type, alert_severity,
                alert_message, current_utilization, recommended_action
            ) VALUES (
                CURRENT_DATE(), dept_name, 'Over Capacity', 'CRITICAL',
                'Department over 95% capacity - immediate action required',
                utilization, 'Add temporary beds or transfer patients'
            );
            LET alert_count := alert_count + 1;
        ELSEIF (utilization > 85) THEN
            INSERT INTO BED_MANAGEMENT_ALERTS (
                alert_date, department_name, alert_type, alert_severity,
                alert_message, current_utilization, recommended_action
            ) VALUES (
                CURRENT_DATE(), dept_name, 'High Utilization', 'WARNING',
                'Department approaching capacity - monitor closely',
                utilization, 'Prepare discharge plans and monitor admissions'
            );
            LET alert_count := alert_count + 1;
        ELSEIF (utilization < 30) THEN
            INSERT INTO BED_MANAGEMENT_ALERTS (
                alert_date, department_name, alert_type, alert_severity,
                alert_message, current_utilization, recommended_action
            ) VALUES (
                CURRENT_DATE(), dept_name, 'Low Utilization', 'INFO',
                'Department significantly under-utilized',
                utilization, 'Consider reallocating beds to high-demand departments'
            );
            LET alert_count := alert_count + 1;
        END IF;
    END FOR;
    
    RETURN 'Generated ' || alert_count || ' bed management alerts for ' || CURRENT_DATE();
END;
$$;

-- 11. Grant Permissions for Bed Management Views
GRANT SELECT ON VW_BED_UTILIZATION_DASHBOARD TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_CURRENT_BED_STATUS TO ROLE PHYSICIAN;
GRANT SELECT ON VW_CURRENT_BED_STATUS TO ROLE NURSE;
GRANT SELECT ON VW_BED_REVENUE_ANALYSIS TO ROLE ANALYST;
GRANT SELECT ON VW_BED_TURNOVER_ANALYSIS TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_BED_CAPACITY_PLANNING TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_BED_ALLOCATION_RECOMMENDATIONS TO ROLE CLINICAL_ADMIN;

-- 12. Sample Queries for Demo

-- Current bed status summary
SELECT 'Current Bed Status Summary' as report_title;
SELECT 
    status,
    COUNT(*) as bed_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) as percentage
FROM VW_CURRENT_BED_STATUS
GROUP BY status
ORDER BY bed_count DESC;

-- Department utilization summary
SELECT 'Department Utilization Summary' as report_title;
SELECT 
    department_name,
    avg_utilization_percentage,
    capacity_recommendation
FROM VW_BED_CAPACITY_PLANNING
ORDER BY avg_utilization_percentage DESC;

-- Revenue analysis summary
SELECT 'Bed Revenue Analysis Summary' as report_title;
SELECT 
    department_name,
    bed_type,
    bed_count,
    total_revenue,
    revenue_per_bed
FROM VW_BED_REVENUE_ANALYSIS
WHERE total_revenue > 0
ORDER BY revenue_per_bed DESC
LIMIT 10;

-- Generate and show alerts
CALL GENERATE_BED_ALERTS();
SELECT 'Current Bed Management Alerts' as report_title;
SELECT * FROM BED_MANAGEMENT_ALERTS WHERE alert_date = CURRENT_DATE() ORDER BY alert_severity, department_name;

SELECT 'Bed management analytics setup completed successfully!' as status_message;
SELECT 'Comprehensive bed utilization and revenue tracking ready.' as next_step;
