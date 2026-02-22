-- ============================================================================
-- Hospital Snowflake Demo - Australian Healthcare Semantic Views
-- ============================================================================
-- This script creates semantic views enriched with authentic Australian healthcare terminology
-- for use by clinicians, health administrators, and analysts in Queensland Health and beyond
--
-- Key Australian Healthcare Terms Used:
-- - "Presentation" (not admission) for ED arrivals
-- - "Admitted patient" (inpatient services)
-- - "Non-admitted patient" (outpatient/ED services)
-- - "NEAT" (National Emergency Access Target - 4 hours)
-- - "ALOS" (Average Length of Stay)
-- - "Occupied Bed Days" (OBD)
-- - "Available Bed Days" (ABD)
-- - "Category" for triage categories (1-5)
-- - "Theatre" (not OR - Operating Room)
-- - "Ward" (not unit)
-- - "Discharge planning" and "NDIS" considerations
-- - "Medicare" and "DVA" (Department of Veterans' Affairs)
-- - "Allied Health" services (Physio, OT, etc.)
-- - "Medical Officer" (MO), "Registrar", "Consultant"
-- - "RMO" (Resident Medical Officer)
-- - "Specialty" (not department in clinical context)
-- - "Elective surgery waitlist"
-- - "Casemix" and "DRG" (Diagnosis Related Group)
-- - "KPI" aligned with AIHW and Queensland Health standards
--
-- IMPORTANT: Run this as ACCOUNTADMIN or DATA_ENGINEER
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA ANALYTICS;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;

-- ============================================================================
-- 1. ADMITTED PATIENT ACTIVITY - Core Queensland Health Reporting
-- ============================================================================

CREATE OR REPLACE VIEW VW_ADMITTED_PATIENT_ACTIVITY AS
WITH patient_episodes AS (
    SELECT 
        fa.admission_id as episode_id,
        fa.admission_key,
        dp.patient_id as ur_number,  -- Unit Record Number (Australian term)
        dp.full_name as patient_name,
        dp.age,
        dp.age_group,
        dp.gender,
        dp.insurance_provider as funding_source,
        
        -- Department/Specialty
        dd.department_name as specialty,
        dd.specialization_type,
        dd.department_head as consultant,
        
        -- Admission details
        fa.admission_type as separation_mode,  -- Emergency/Elective/Urgent
        CASE 
            WHEN fa.admission_type = 'Emergency' THEN 'Emergency'
            WHEN fa.admission_type = 'Elective' THEN 'Elective'
            ELSE 'Urgent'
        END as patient_election_status,
        
        fa.chief_complaint as presenting_problem,
        fa.diagnosis_primary as principal_diagnosis,
        fa.diagnosis_secondary as additional_diagnosis,
        
        -- Dates and times
        d_admit.date_value as admission_date,
        t_admit.time_value as admission_time,
        d_admit.day_name as admission_day_of_week,
        d_admit.is_weekend as admitted_on_weekend,
        
        d_discharge.date_value as separation_date,  -- Australian term for discharge
        t_discharge.time_value as separation_time,
        
        -- Length of stay calculations
        fa.length_of_stay_days as alos_days,  -- ALOS = Average Length of Stay
        fa.length_of_stay_hours as alos_hours,
        
        -- Bed management
        fa.room_number as ward_room,
        fa.bed_number as bed_position,
        
        -- Financial
        fa.total_charges as episode_cost,
        fa.is_emergency as is_emergency_presentation,
        fa.is_readmission as unplanned_readmission,  -- 28-day readmission KPI
        
        -- Physician details
        dph.physician_name as treating_medical_officer,  -- MO or Consultant
        dph.specialty as medical_specialty,
        
        -- Weather impact (relevant for ED presentations)
        dw.weather_condition,
        dw.temperature_range,
        dw.season as seasonal_period,
        
        -- Date dimension attributes
        d_admit.month_name as admission_month,
        d_admit.quarter as admission_quarter,
        d_admit.year as financial_year_start,  -- Could be adjusted to July-June for Aus FY
        
        -- KPI flags
        CASE WHEN fa.length_of_stay_days > 21 THEN TRUE ELSE FALSE END as long_stay_patient,  -- >21 days
        CASE WHEN fa.is_readmission THEN TRUE ELSE FALSE END as readmission_within_28_days,
        
        fa.created_timestamp as record_created
        
    FROM TRANSFORMED.FACT_ADMISSIONS fa
    JOIN TRANSFORMED.DIM_PATIENT dp ON fa.patient_key = dp.patient_key
    JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
    LEFT JOIN TRANSFORMED.DIM_PHYSICIAN dph ON fa.physician_key = dph.physician_key
    LEFT JOIN TRANSFORMED.DIM_WEATHER dw ON fa.weather_key = dw.weather_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_admit ON fa.admission_date_key = d_admit.date_key
    LEFT JOIN TRANSFORMED.DIM_TIME t_admit ON fa.admission_time_key = t_admit.time_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_discharge ON fa.discharge_date_key = d_discharge.date_key
    LEFT JOIN TRANSFORMED.DIM_TIME t_discharge ON fa.discharge_time_key = t_discharge.time_key
)
SELECT * FROM patient_episodes;

COMMENT ON VIEW VW_ADMITTED_PATIENT_ACTIVITY IS 
'Admitted patient activity aligned with AIHW and Queensland Health reporting standards. 
Includes ALOS, separation modes, unplanned readmissions, and long-stay patient flags.';

-- ============================================================================
-- 2. OCCUPIED BED DAYS (OBD) & AVAILABLE BED DAYS (ABD) - Critical Hospital KPI
-- ============================================================================

CREATE OR REPLACE VIEW VW_OBD_ABD_ANALYSIS AS
WITH daily_bed_metrics AS (
    SELECT 
        fba.availability_date as service_date,
        dd.department_name as specialty_ward,
        dd.specialization_type,
        
        -- Bed counts
        COUNT(DISTINCT db.bed_key) as total_bed_complement,  -- Total beds in ward
        SUM(CASE WHEN fba.is_occupied THEN 1 ELSE 0 END) as occupied_beds,
        SUM(CASE WHEN fba.is_available THEN 1 ELSE 0 END) as available_beds,
        SUM(CASE WHEN fba.is_maintenance THEN 1 ELSE 0 END) as beds_unavailable,
        
        -- Occupied Bed Days (OBD) - count occupied beds as 1 day each
        SUM(CASE WHEN fba.is_occupied THEN 1 ELSE 0 END) as occupied_bed_days_obd,
        
        -- Available Bed Days (ABD) - theoretical bed days if all available
        COUNT(DISTINCT db.bed_key) as available_bed_days_abd,
        
        -- Bed occupancy rate (OBD / ABD)
        ROUND(
            (SUM(CASE WHEN fba.is_occupied THEN 1 ELSE 0 END)::FLOAT / 
             COUNT(DISTINCT db.bed_key)::FLOAT) * 100, 
            2
        ) as bed_occupancy_rate_percent,
        
        -- Utilization rate
        ROUND(AVG(fba.utilization_rate) * 100, 2) as utilization_rate_percent,
        
        -- Revenue potential
        SUM(fba.revenue_potential) as daily_revenue_potential,
        SUM(db.daily_rate) as ward_bed_value,
        
        -- Date attributes
        dt.day_name as day_of_week,
        dt.is_weekend,
        dt.month_name,
        dt.quarter,
        dt.year
        
    FROM TRANSFORMED.FACT_BED_AVAILABILITY fba
    JOIN TRANSFORMED.DIM_BED db ON fba.bed_key = db.bed_key
    JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fba.department_key = dd.department_key
    JOIN TRANSFORMED.DIM_DATE dt ON fba.date_key = dt.date_key
    WHERE dt.date_value >= CURRENT_DATE - 365  -- Last 12 months
    GROUP BY 
        fba.availability_date, 
        dd.department_name, 
        dd.specialization_type,
        dt.day_name,
        dt.is_weekend,
        dt.month_name,
        dt.quarter,
        dt.year
)
SELECT 
    service_date,
    specialty_ward,
    specialization_type,
    total_bed_complement,
    occupied_beds,
    available_beds,
    beds_unavailable,
    occupied_bed_days_obd,
    available_bed_days_abd,
    bed_occupancy_rate_percent,
    utilization_rate_percent,
    
    -- KPI thresholds (Queensland Health targets)
    CASE 
        WHEN bed_occupancy_rate_percent > 92 THEN 'Over capacity - Critical'  -- >92% is unsafe
        WHEN bed_occupancy_rate_percent > 85 THEN 'High occupancy - Monitor'  -- 85-92% target range
        WHEN bed_occupancy_rate_percent > 70 THEN 'Optimal occupancy'
        WHEN bed_occupancy_rate_percent > 50 THEN 'Adequate capacity'
        ELSE 'Low occupancy - Review'
    END as occupancy_status,
    
    daily_revenue_potential,
    ward_bed_value,
    day_of_week,
    is_weekend,
    month_name,
    quarter,
    year
FROM daily_bed_metrics
ORDER BY service_date DESC, bed_occupancy_rate_percent DESC;

COMMENT ON VIEW VW_OBD_ABD_ANALYSIS IS 
'Occupied Bed Days (OBD) and Available Bed Days (ABD) analysis for Queensland Health reporting. 
Monitors bed occupancy rates against safe capacity thresholds (85-92% target).';

-- ============================================================================
-- 3. EMERGENCY DEPARTMENT (ED) PRESENTATIONS - NEAT Compliance
-- ============================================================================

CREATE OR REPLACE VIEW VW_ED_PRESENTATIONS_NEAT AS
WITH ed_activity AS (
    SELECT 
        fa.admission_id as presentation_id,
        dp.patient_id as ur_number,
        dp.full_name as patient_name,
        dp.age,
        dp.age_group,
        
        -- Presentation details
        d_admit.date_value as presentation_date,
        t_admit.time_value as presentation_time,
        t_admit.shift as presentation_shift,  -- Day/Evening/Night
        
        fa.chief_complaint as presenting_complaint,
        fa.diagnosis_primary as ed_diagnosis,
        
        -- Triage category (simulated based on admission type)
        CASE 
            WHEN fa.chief_complaint ILIKE '%cardiac%' OR fa.chief_complaint ILIKE '%stroke%' THEN 'Category 1 - Resuscitation'
            WHEN fa.chief_complaint ILIKE '%severe%' OR fa.chief_complaint ILIKE '%acute%' THEN 'Category 2 - Emergency'
            WHEN fa.admission_type = 'Emergency' THEN 'Category 3 - Urgent'
            ELSE 'Category 4 - Semi-urgent'
        END as triage_category,
        
        -- Time in ED (simplified - using admission to discharge as proxy)
        fa.length_of_stay_hours as time_in_ed_hours,
        
        -- NEAT compliance (National Emergency Access Target = 4 hours)
        CASE 
            WHEN fa.length_of_stay_hours <= 4 THEN TRUE 
            ELSE FALSE 
        END as neat_compliant,  -- Should be within 4 hours
        
        CASE 
            WHEN fa.length_of_stay_hours <= 4 THEN 'Within NEAT target'
            WHEN fa.length_of_stay_hours <= 8 THEN 'Extended stay (4-8 hours)'
            WHEN fa.length_of_stay_hours <= 24 THEN 'Prolonged stay (8-24 hours)'
            ELSE 'Access block (>24 hours)'  -- Patient ready for admission but no bed
        END as neat_status,
        
        -- Separation mode from ED
        CASE 
            WHEN fa.admission_type = 'Emergency' AND fa.length_of_stay_days >= 1 THEN 'Admitted to ward'
            WHEN fa.length_of_stay_hours < 4 THEN 'Discharged from ED'
            ELSE 'Other (DAMA, Transfer, etc.)'  -- DAMA = Discharged Against Medical Advice
        END as separation_from_ed,
        
        -- Specialty that patient was referred to (if admitted)
        dd.department_name as admitting_specialty,
        dph.physician_name as treating_officer,
        
        -- Weather impact on ED presentations
        dw.weather_condition,
        dw.temperature_range,
        dw.season as seasonal_impact,
        
        -- Date attributes
        d_admit.day_name as day_of_week,
        d_admit.is_weekend as weekend_presentation,
        d_admit.month_name,
        d_admit.quarter
        
    FROM TRANSFORMED.FACT_ADMISSIONS fa
    JOIN TRANSFORMED.DIM_PATIENT dp ON fa.patient_key = dp.patient_key
    JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
    LEFT JOIN TRANSFORMED.DIM_PHYSICIAN dph ON fa.physician_key = dph.physician_key
    LEFT JOIN TRANSFORMED.DIM_WEATHER dw ON fa.weather_key = dw.weather_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_admit ON fa.admission_date_key = d_admit.date_key
    LEFT JOIN TRANSFORMED.DIM_TIME t_admit ON fa.admission_time_key = t_admit.time_key
    WHERE fa.admission_type = 'Emergency'  -- ED presentations only
)
SELECT * FROM ed_activity;

COMMENT ON VIEW VW_ED_PRESENTATIONS_NEAT IS 
'Emergency Department presentations with NEAT (National Emergency Access Target) compliance tracking. 
Target: 90% of ED patients should be admitted, discharged, or transferred within 4 hours.';

-- ============================================================================
-- 4. ELECTIVE SURGERY ACTIVITY & WAITLIST PERFORMANCE
-- ============================================================================

CREATE OR REPLACE VIEW VW_ELECTIVE_SURGERY_PERFORMANCE AS
WITH elective_procedures AS (
    SELECT 
        fp.procedure_id,
        dp.patient_id as ur_number,
        dp.full_name as patient_name,
        dp.age,
        dp.age_group,
        dp.insurance_provider as funding_source,
        
        -- Procedure details
        dpr.procedure_code,
        dpr.procedure_name as procedure_description,
        dpr.procedure_category,
        dpr.is_surgical as is_surgical_procedure,
        
        -- Urgency category (Australian elective surgery categories)
        CASE 
            WHEN fp.procedure_duration_minutes > 240 THEN 'Category 1 - Urgent (30 days)'
            WHEN fp.procedure_duration_minutes > 120 THEN 'Category 2 - Semi-urgent (90 days)'
            ELSE 'Category 3 - Non-urgent (365 days)'
        END as elective_surgery_category,
        
        -- Theatre details (not "OR" - we say Theatre in Australia)
        dd.department_name as theatre_specialty,
        dph.physician_name as operating_surgeon,
        dph.specialty as surgeon_specialty,
        
        -- Procedure timing
        d_proc.date_value as procedure_date,
        t_proc.time_value as procedure_time,
        t_proc.shift as theatre_shift,
        
        fp.procedure_duration_minutes as theatre_time_minutes,
        ROUND(fp.procedure_duration_minutes / 60.0, 2) as theatre_time_hours,
        
        -- Anaesthetic details
        fp.anesthesia_type as anaesthetic_type,
        CASE 
            WHEN fp.anesthesia_type = 'General' THEN 'GA - General Anaesthetic'
            WHEN fp.anesthesia_type = 'Spinal' THEN 'Spinal/Regional'
            WHEN fp.anesthesia_type = 'Local' THEN 'Local anaesthetic'
            ELSE 'No anaesthetic'
        END as anaesthetic_description,
        
        -- Outcome
        fp.is_successful as procedure_successful,
        fp.complications as post_operative_complications,
        CASE 
            WHEN fp.complications = 'None' THEN 'No complications'
            WHEN fp.complications IS NULL THEN 'No complications'
            ELSE 'Complication recorded'
        END as complication_status,
        
        -- Financial
        fp.procedure_cost as theatre_cost,
        fa.total_charges as total_episode_cost,
        
        -- Admission details
        fa.admission_type as admission_mode,
        fa.length_of_stay_days as post_op_los_days,
        
        -- Date attributes
        d_proc.day_name as day_of_week,
        d_proc.is_weekend as weekend_theatre,
        d_proc.month_name,
        d_proc.quarter,
        d_proc.year
        
    FROM TRANSFORMED.FACT_PROCEDURES fp
    JOIN TRANSFORMED.DIM_PROCEDURE dpr ON fp.procedure_key = dpr.procedure_key
    LEFT JOIN TRANSFORMED.FACT_ADMISSIONS fa ON fp.admission_key = fa.admission_key
    LEFT JOIN TRANSFORMED.DIM_PATIENT dp ON fp.patient_key = dp.patient_key
    LEFT JOIN TRANSFORMED.DIM_PHYSICIAN dph ON fp.physician_key = dph.physician_key
    LEFT JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_proc ON fp.procedure_date_key = d_proc.date_key
    LEFT JOIN TRANSFORMED.DIM_TIME t_proc ON fp.procedure_time_key = t_proc.time_key
    WHERE dpr.is_surgical = TRUE  -- Surgical procedures only
)
SELECT * FROM elective_procedures;

COMMENT ON VIEW VW_ELECTIVE_SURGERY_PERFORMANCE IS 
'Elective surgery activity with Australian clinical terminology (theatre, anaesthetic, categories). 
Tracks performance against elective surgery waiting time targets (Category 1: 30 days, Category 2: 90 days, Category 3: 365 days).';

-- ============================================================================
-- 5. CASEMIX & DRG REPORTING (Simplified)
-- ============================================================================

CREATE OR REPLACE VIEW VW_CASEMIX_DRG_SUMMARY AS
WITH episode_classification AS (
    SELECT 
        fa.admission_id as episode_number,
        dp.patient_id as ur_number,
        dp.age_group,
        
        -- Casemix classification (simplified DRG grouping)
        dd.specialization_type as service_category,
        dd.department_name as specialty,
        
        -- Principal diagnosis as proxy for DRG
        fa.diagnosis_primary as principal_diagnosis,
        
        -- Complexity indicators
        fa.length_of_stay_days as alos,
        CASE 
            WHEN fa.length_of_stay_days > 21 THEN 'Long stay'
            WHEN fa.length_of_stay_days > 7 THEN 'Extended stay'
            WHEN fa.length_of_stay_days > 3 THEN 'Short stay'
            ELSE 'Same day or overnight'
        END as los_category,
        
        -- Co-morbidities proxy
        CASE 
            WHEN fa.diagnosis_secondary IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END as has_comorbidities,
        
        -- Procedure complexity
        COUNT(fp.procedure_id) as procedure_count,
        SUM(fp.procedure_cost) as total_procedure_cost,
        
        -- Episode cost (used for casemix funding)
        fa.total_charges as episode_cost,
        
        -- Funding source
        dp.insurance_provider as funding_source,
        CASE 
            WHEN dp.insurance_provider = 'Medicare' THEN 'Public patient'
            WHEN dp.insurance_provider IN ('Medibank Private', 'BUPA', 'NIB') THEN 'Private patient'
            ELSE 'Other funding'
        END as patient_funding_type,
        
        -- Admission details
        fa.admission_type as separation_mode,
        d_admit.date_value as admission_date,
        d_discharge.date_value as separation_date,
        
        -- Readmission flag (28-day unplanned readmission)
        fa.is_readmission as unplanned_readmission_28_days,
        
        -- Date attributes
        d_admit.month_name,
        d_admit.quarter,
        d_admit.year as financial_year
        
    FROM TRANSFORMED.FACT_ADMISSIONS fa
    JOIN TRANSFORMED.DIM_PATIENT dp ON fa.patient_key = dp.patient_key
    JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
    LEFT JOIN TRANSFORMED.FACT_PROCEDURES fp ON fa.admission_key = fp.admission_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_admit ON fa.admission_date_key = d_admit.date_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_discharge ON fa.discharge_date_key = d_discharge.date_key
    GROUP BY 
        fa.admission_id,
        dp.patient_id,
        dp.age_group,
        dd.specialization_type,
        dd.department_name,
        fa.diagnosis_primary,
        fa.diagnosis_secondary,
        fa.length_of_stay_days,
        fa.total_charges,
        dp.insurance_provider,
        fa.admission_type,
        fa.is_readmission,
        d_admit.date_value,
        d_discharge.date_value,
        d_admit.month_name,
        d_admit.quarter,
        d_admit.year
)
SELECT * FROM episode_classification;

COMMENT ON VIEW VW_CASEMIX_DRG_SUMMARY IS 
'Casemix and DRG (Diagnosis Related Group) reporting for activity-based funding. 
Classifies episodes by complexity, comorbidities, procedures, and funding source (public/private mix).';

-- ============================================================================
-- 6. QUEENSLAND HEALTH KPI DASHBOARD
-- ============================================================================

CREATE OR REPLACE VIEW VW_QLD_HEALTH_KPI_DASHBOARD AS
WITH kpi_metrics AS (
    -- ALOS (Average Length of Stay)
    SELECT 
        'ALOS - Average Length of Stay' as kpi_name,
        'Admitted Patient Activity' as kpi_category,
        ROUND(AVG(length_of_stay_days), 2) as actual_value,
        4.5 as target_value,  -- Example target
        'Days' as unit_of_measure,
        CASE 
            WHEN AVG(length_of_stay_days) <= 4.5 THEN 'Meeting target'
            WHEN AVG(length_of_stay_days) <= 5.5 THEN 'Within tolerance'
            ELSE 'Not meeting target'
        END as performance_status
    FROM TRANSFORMED.FACT_ADMISSIONS
    WHERE admission_date_key >= TO_NUMBER(TO_CHAR(CURRENT_DATE - 90, 'YYYYMMDD'))  -- Last 90 days
    
    UNION ALL
    
    -- Bed occupancy rate
    SELECT 
        'Bed Occupancy Rate' as kpi_name,
        'Bed Management' as kpi_category,
        ROUND(AVG(CASE WHEN is_occupied THEN 100 ELSE 0 END), 2) as actual_value,
        85 as target_value,  -- 85% target (safe occupancy)
        'Percent' as unit_of_measure,
        CASE 
            WHEN AVG(CASE WHEN is_occupied THEN 100 ELSE 0 END) BETWEEN 75 AND 92 THEN 'Optimal range'
            WHEN AVG(CASE WHEN is_occupied THEN 100 ELSE 0 END) > 92 THEN 'Over capacity - Critical'
            ELSE 'Under-utilized'
        END as performance_status
    FROM TRANSFORMED.FACT_BED_AVAILABILITY
    WHERE date_key >= TO_NUMBER(TO_CHAR(CURRENT_DATE - 30, 'YYYYMMDD'))  -- Last 30 days
    
    UNION ALL
    
    -- NEAT compliance (ED 4-hour target)
    SELECT 
        'NEAT Compliance (4-hour ED target)' as kpi_name,
        'Emergency Department' as kpi_category,
        ROUND(
            AVG(CASE WHEN length_of_stay_hours <= 4 THEN 100 ELSE 0 END), 
            2
        ) as actual_value,
        90 as target_value,  -- 90% target
        'Percent' as unit_of_measure,
        CASE 
            WHEN AVG(CASE WHEN length_of_stay_hours <= 4 THEN 100 ELSE 0 END) >= 90 THEN 'Meeting target'
            WHEN AVG(CASE WHEN length_of_stay_hours <= 4 THEN 100 ELSE 0 END) >= 80 THEN 'Close to target'
            ELSE 'Not meeting target'
        END as performance_status
    FROM TRANSFORMED.FACT_ADMISSIONS
    WHERE admission_type = 'Emergency'
      AND admission_date_key >= TO_NUMBER(TO_CHAR(CURRENT_DATE - 30, 'YYYYMMDD'))  -- Last 30 days
    
    UNION ALL
    
    -- Unplanned readmissions (28-day)
    SELECT 
        'Unplanned Readmissions (28-day)' as kpi_name,
        'Quality & Safety' as kpi_category,
        ROUND(
            AVG(CASE WHEN is_readmission THEN 100 ELSE 0 END), 
            2
        ) as actual_value,
        10 as target_value,  -- <10% target
        'Percent' as unit_of_measure,
        CASE 
            WHEN AVG(CASE WHEN is_readmission THEN 100 ELSE 0 END) <= 10 THEN 'Meeting target'
            WHEN AVG(CASE WHEN is_readmission THEN 100 ELSE 0 END) <= 15 THEN 'Review required'
            ELSE 'Not meeting target'
        END as performance_status
    FROM TRANSFORMED.FACT_ADMISSIONS
    WHERE admission_date_key >= TO_NUMBER(TO_CHAR(CURRENT_DATE - 90, 'YYYYMMDD'))  -- Last 90 days
    
    UNION ALL
    
    -- Elective surgery on-time performance
    SELECT 
        'Elective Surgery - On-time Performance' as kpi_name,
        'Elective Surgery' as kpi_category,
        ROUND(
            AVG(CASE WHEN complications = 'None' THEN 100 ELSE 0 END), 
            2
        ) as actual_value,
        95 as target_value,  -- 95% procedures without complications
        'Percent' as unit_of_measure,
        CASE 
            WHEN AVG(CASE WHEN complications = 'None' THEN 100 ELSE 0 END) >= 95 THEN 'Meeting target'
            WHEN AVG(CASE WHEN complications = 'None' THEN 100 ELSE 0 END) >= 90 THEN 'Close to target'
            ELSE 'Not meeting target'
        END as performance_status
    FROM TRANSFORMED.FACT_PROCEDURES
    WHERE procedure_date_key >= TO_NUMBER(TO_CHAR(CURRENT_DATE - 90, 'YYYYMMDD'))  -- Last 90 days
)
SELECT * FROM kpi_metrics;

COMMENT ON VIEW VW_QLD_HEALTH_KPI_DASHBOARD IS 
'Queensland Health Key Performance Indicators (KPIs) dashboard aligned with AIHW reporting standards. 
Includes ALOS, bed occupancy, NEAT compliance, unplanned readmissions, and elective surgery performance.';

-- ============================================================================
-- 7. CLINICAL HANDOVER SUMMARY (Ward Rounds View)
-- ============================================================================

CREATE OR REPLACE VIEW VW_CLINICAL_HANDOVER_SUMMARY AS
WITH current_inpatients AS (
    SELECT 
        dp.patient_id as ur_number,
        dp.full_name as patient_name,
        dp.age,
        dp.gender,
        
        -- Admission details
        fa.admission_id as current_episode,
        dd.department_name as current_ward,
        fa.room_number as room,
        fa.bed_number as bed,
        
        -- Clinical details
        fa.chief_complaint as presenting_complaint,
        fa.diagnosis_primary as working_diagnosis,
        dph.physician_name as consultant_in_charge,
        dph.specialty as consultant_specialty,
        
        -- Length of stay
        d_admit.date_value as date_of_admission,
        DATEDIFF(day, d_admit.date_value, CURRENT_DATE) as current_los_days,
        
        -- Expected discharge
        d_discharge.date_value as expected_discharge_date,
        CASE 
            WHEN d_discharge.date_value IS NOT NULL 
            THEN DATEDIFF(day, CURRENT_DATE, d_discharge.date_value)
            ELSE NULL
        END as days_until_discharge,
        
        -- Flags for handover
        CASE 
            WHEN DATEDIFF(day, d_admit.date_value, CURRENT_DATE) > 21 THEN TRUE 
            ELSE FALSE 
        END as long_stay_patient,
        
        CASE 
            WHEN fa.diagnosis_secondary IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END as has_comorbidities,
        
        fa.is_readmission as previous_admission_within_28_days,
        
        -- Procedure activity (for handover awareness)
        COUNT(fp.procedure_id) as procedures_performed,
        MAX(d_proc.date_value) as last_procedure_date,
        
        -- Funding/insurance
        dp.insurance_provider as funding_source
        
    FROM TRANSFORMED.FACT_ADMISSIONS fa
    JOIN TRANSFORMED.DIM_PATIENT dp ON fa.patient_key = dp.patient_key
    JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
    LEFT JOIN TRANSFORMED.DIM_PHYSICIAN dph ON fa.physician_key = dph.physician_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_admit ON fa.admission_date_key = d_admit.date_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_discharge ON fa.discharge_date_key = d_discharge.date_key
    LEFT JOIN TRANSFORMED.FACT_PROCEDURES fp ON fa.admission_key = fp.admission_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_proc ON fp.procedure_date_key = d_proc.date_key
    WHERE d_discharge.date_value IS NULL OR d_discharge.date_value >= CURRENT_DATE  -- Current inpatients
    GROUP BY 
        dp.patient_id,
        dp.full_name,
        dp.age,
        dp.gender,
        fa.admission_id,
        dd.department_name,
        fa.room_number,
        fa.bed_number,
        fa.chief_complaint,
        fa.diagnosis_primary,
        fa.diagnosis_secondary,
        dph.physician_name,
        dph.specialty,
        d_admit.date_value,
        d_discharge.date_value,
        fa.is_readmission,
        dp.insurance_provider
)
SELECT * FROM current_inpatients
ORDER BY current_ward, room, bed;

COMMENT ON VIEW VW_CLINICAL_HANDOVER_SUMMARY IS 
'Clinical handover summary for ward rounds and shift handover. 
Lists current inpatients with key clinical details, LOS, expected discharge, and flags for long-stay or complex patients.';

-- ============================================================================
-- 8. ALLIED HEALTH SERVICES (Simulated - Extended from Procedures)
-- ============================================================================

CREATE OR REPLACE VIEW VW_ALLIED_HEALTH_SERVICES AS
WITH allied_services AS (
    SELECT 
        fp.procedure_id as service_id,
        dp.patient_id as ur_number,
        dp.full_name as patient_name,
        dp.age,
        dp.age_group,
        
        -- Service classification (simulated from procedure data)
        CASE 
            WHEN dpr.procedure_name ILIKE '%physical%' THEN 'Physiotherapy'
            WHEN dpr.procedure_name ILIKE '%rehab%' THEN 'Occupational Therapy'
            WHEN dpr.procedure_name ILIKE '%speech%' THEN 'Speech Pathology'
            WHEN dpr.procedure_name ILIKE '%nutrition%' THEN 'Dietetics'
            WHEN dpr.procedure_name ILIKE '%social%' THEN 'Social Work'
            WHEN dpr.procedure_name ILIKE '%psych%' THEN 'Psychology'
            ELSE 'Other Allied Health'
        END as allied_health_discipline,
        
        dpr.procedure_name as service_description,
        
        -- Service details
        d_proc.date_value as service_date,
        fp.procedure_duration_minutes as service_duration_minutes,
        
        -- Location
        dd.department_name as service_location,
        
        -- Associated admission
        fa.admission_id as related_episode,
        fa.diagnosis_primary as patient_diagnosis,
        
        -- Outcome tracking
        fp.is_successful as service_completed,
        fp.complications as service_notes,
        
        -- Financial
        fp.procedure_cost as service_cost,
        
        -- Date attributes
        d_proc.day_name,
        d_proc.is_weekend,
        d_proc.month_name,
        d_proc.quarter
        
    FROM TRANSFORMED.FACT_PROCEDURES fp
    JOIN TRANSFORMED.DIM_PROCEDURE dpr ON fp.procedure_key = dpr.procedure_key
    LEFT JOIN TRANSFORMED.FACT_ADMISSIONS fa ON fp.admission_key = fa.admission_key
    LEFT JOIN TRANSFORMED.DIM_PATIENT dp ON fp.patient_key = dp.patient_key
    LEFT JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_proc ON fp.procedure_date_key = d_proc.date_key
    WHERE dpr.is_surgical = FALSE  -- Non-surgical procedures = Allied Health services
)
SELECT * FROM allied_services;

COMMENT ON VIEW VW_ALLIED_HEALTH_SERVICES IS 
'Allied Health services activity including Physiotherapy, Occupational Therapy, Speech Pathology, Dietetics, Social Work, and Psychology. 
Critical for discharge planning and NDIS coordination.';

-- ============================================================================
-- 9. DISCHARGE PLANNING & FLOW MANAGEMENT
-- ============================================================================

CREATE OR REPLACE VIEW VW_DISCHARGE_PLANNING_DASHBOARD AS
WITH discharge_cohort AS (
    SELECT 
        dp.patient_id as ur_number,
        dp.full_name as patient_name,
        dp.age,
        dp.age_group,
        
        -- Admission details
        fa.admission_id as episode_number,
        dd.department_name as current_ward,
        fa.room_number,
        fa.bed_number,
        
        -- Clinical details
        fa.diagnosis_primary as principal_diagnosis,
        dph.physician_name as consultant,
        
        -- Dates
        d_admit.date_value as admission_date,
        d_discharge.date_value as planned_discharge_date,
        DATEDIFF(day, d_admit.date_value, CURRENT_DATE) as current_los,
        DATEDIFF(day, CURRENT_DATE, d_discharge.date_value) as days_to_discharge,
        
        -- Discharge planning flags
        CASE 
            WHEN dp.age >= 75 THEN TRUE 
            ELSE FALSE 
        END as elderly_patient_ndis_consider,  -- NDIS = National Disability Insurance Scheme
        
        CASE 
            WHEN DATEDIFF(day, d_admit.date_value, CURRENT_DATE) > 14 THEN TRUE 
            ELSE FALSE 
        END as requires_aged_care_assessment,
        
        CASE 
            WHEN fa.diagnosis_secondary IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END as complex_discharge_planning,
        
        -- Allied health needs (from procedures)
        COUNT(DISTINCT CASE 
            WHEN dpr.procedure_name ILIKE '%physical%' THEN fp.procedure_id 
        END) as physio_sessions,
        
        COUNT(DISTINCT CASE 
            WHEN dpr.procedure_name ILIKE '%rehab%' THEN fp.procedure_id 
        END) as ot_sessions,
        
        -- Funding considerations
        dp.insurance_provider as funding_source,
        CASE 
            WHEN dp.insurance_provider = 'Medicare' THEN 'Public patient - Medicare'
            WHEN dp.insurance_provider = 'DVA Health' THEN 'DVA Gold Card - Enhanced services'
            WHEN dp.insurance_provider IN ('Medibank Private', 'BUPA', 'NIB') THEN 'Private patient'
            ELSE 'Other funding'
        END as discharge_funding_pathway,
        
        -- Readmission risk
        fa.is_readmission as previous_readmission_flag
        
    FROM TRANSFORMED.FACT_ADMISSIONS fa
    JOIN TRANSFORMED.DIM_PATIENT dp ON fa.patient_key = dp.patient_key
    JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
    LEFT JOIN TRANSFORMED.DIM_PHYSICIAN dph ON fa.physician_key = dph.physician_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_admit ON fa.admission_date_key = d_admit.date_key
    LEFT JOIN TRANSFORMED.DIM_DATE d_discharge ON fa.discharge_date_key = d_discharge.date_key
    LEFT JOIN TRANSFORMED.FACT_PROCEDURES fp ON fa.admission_key = fp.admission_key
    LEFT JOIN TRANSFORMED.DIM_PROCEDURE dpr ON fp.procedure_key = dpr.procedure_key
    WHERE d_discharge.date_value IS NULL OR d_discharge.date_value >= CURRENT_DATE  -- Current or planned
    GROUP BY 
        dp.patient_id,
        dp.full_name,
        dp.age,
        dp.age_group,
        fa.admission_id,
        dd.department_name,
        fa.room_number,
        fa.bed_number,
        fa.diagnosis_primary,
        fa.diagnosis_secondary,
        dph.physician_name,
        d_admit.date_value,
        d_discharge.date_value,
        dp.insurance_provider,
        fa.is_readmission
)
SELECT * FROM discharge_cohort
WHERE days_to_discharge IS NOT NULL
ORDER BY days_to_discharge ASC, current_los DESC;

COMMENT ON VIEW VW_DISCHARGE_PLANNING_DASHBOARD IS 
'Discharge planning dashboard with Australian health system considerations including NDIS, DVA, Medicare, aged care assessments, and allied health coordination. 
Prioritizes patients by discharge date and complexity.';

-- ============================================================================
-- 10. AIHW (Australian Institute of Health and Welfare) REPORTING SUMMARY
-- ============================================================================

CREATE OR REPLACE VIEW VW_AIHW_REPORTING_SUMMARY AS
SELECT 
    'Admitted patient separations' as aihw_metric,
    'Admitted Patient Care' as aihw_category,
    COUNT(DISTINCT fa.admission_id) as metric_count,
    d.year as reporting_year,
    d.quarter as reporting_quarter
FROM TRANSFORMED.FACT_ADMISSIONS fa
JOIN TRANSFORMED.DIM_DATE d ON fa.admission_date_key = d.date_key
WHERE d.date_value >= CURRENT_DATE - 365
GROUP BY d.year, d.quarter

UNION ALL

SELECT 
    'Occupied bed days (OBD)' as aihw_metric,
    'Hospital Resources' as aihw_category,
    SUM(CASE WHEN is_occupied THEN 1 ELSE 0 END) as metric_count,
    dt.year as reporting_year,
    dt.quarter as reporting_quarter
FROM TRANSFORMED.FACT_BED_AVAILABILITY fba
JOIN TRANSFORMED.DIM_DATE dt ON fba.date_key = dt.date_key
WHERE dt.date_value >= CURRENT_DATE - 365
GROUP BY dt.year, dt.quarter

UNION ALL

SELECT 
    'Average length of stay (ALOS)' as aihw_metric,
    'Efficiency Indicators' as aihw_category,
    ROUND(AVG(fa.length_of_stay_days), 2) as metric_count,
    d.year as reporting_year,
    d.quarter as reporting_quarter
FROM TRANSFORMED.FACT_ADMISSIONS fa
JOIN TRANSFORMED.DIM_DATE d ON fa.admission_date_key = d.date_key
WHERE d.date_value >= CURRENT_DATE - 365
GROUP BY d.year, d.quarter

UNION ALL

SELECT 
    'Emergency presentations' as aihw_metric,
    'Emergency Department' as aihw_category,
    COUNT(DISTINCT fa.admission_id) as metric_count,
    d.year as reporting_year,
    d.quarter as reporting_quarter
FROM TRANSFORMED.FACT_ADMISSIONS fa
JOIN TRANSFORMED.DIM_DATE d ON fa.admission_date_key = d.date_key
WHERE fa.admission_type = 'Emergency'
  AND d.date_value >= CURRENT_DATE - 365
GROUP BY d.year, d.quarter

UNION ALL

SELECT 
    'Elective surgery procedures' as aihw_metric,
    'Elective Surgery' as aihw_category,
    COUNT(DISTINCT fp.procedure_id) as metric_count,
    d.year as reporting_year,
    d.quarter as reporting_quarter
FROM TRANSFORMED.FACT_PROCEDURES fp
JOIN TRANSFORMED.DIM_PROCEDURE dpr ON fp.procedure_key = dpr.procedure_key
JOIN TRANSFORMED.DIM_DATE d ON fp.procedure_date_key = d.date_key
WHERE dpr.is_surgical = TRUE
  AND d.date_value >= CURRENT_DATE - 365
GROUP BY d.year, d.quarter

ORDER BY reporting_year DESC, reporting_quarter DESC, aihw_category, aihw_metric;

COMMENT ON VIEW VW_AIHW_REPORTING_SUMMARY IS 
'AIHW (Australian Institute of Health and Welfare) standard reporting metrics for national healthcare data collection. 
Quarterly summary of key activity indicators aligned with METeOR metadata standards.';

-- ============================================================================
-- 11. GRANT PERMISSIONS TO ROLES (Australian Clinical Roles)
-- ============================================================================

-- Clinical Admin - Full access to all semantic views
GRANT SELECT ON VW_ADMITTED_PATIENT_ACTIVITY TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_OBD_ABD_ANALYSIS TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_ED_PRESENTATIONS_NEAT TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_ELECTIVE_SURGERY_PERFORMANCE TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_CASEMIX_DRG_SUMMARY TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_QLD_HEALTH_KPI_DASHBOARD TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_CLINICAL_HANDOVER_SUMMARY TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_ALLIED_HEALTH_SERVICES TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_DISCHARGE_PLANNING_DASHBOARD TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_AIHW_REPORTING_SUMMARY TO ROLE CLINICAL_ADMIN;

-- Physician - Clinical views relevant for patient care
GRANT SELECT ON VW_ADMITTED_PATIENT_ACTIVITY TO ROLE PHYSICIAN;
GRANT SELECT ON VW_ED_PRESENTATIONS_NEAT TO ROLE PHYSICIAN;
GRANT SELECT ON VW_ELECTIVE_SURGERY_PERFORMANCE TO ROLE PHYSICIAN;
GRANT SELECT ON VW_CLINICAL_HANDOVER_SUMMARY TO ROLE PHYSICIAN;
GRANT SELECT ON VW_ALLIED_HEALTH_SERVICES TO ROLE PHYSICIAN;
GRANT SELECT ON VW_DISCHARGE_PLANNING_DASHBOARD TO ROLE PHYSICIAN;

-- Nurse - Ward management and patient flow views
GRANT SELECT ON VW_OBD_ABD_ANALYSIS TO ROLE NURSE;
GRANT SELECT ON VW_CLINICAL_HANDOVER_SUMMARY TO ROLE NURSE;
GRANT SELECT ON VW_DISCHARGE_PLANNING_DASHBOARD TO ROLE NURSE;

-- Analyst - All views for reporting and analytics
GRANT SELECT ON VW_ADMITTED_PATIENT_ACTIVITY TO ROLE ANALYST;
GRANT SELECT ON VW_OBD_ABD_ANALYSIS TO ROLE ANALYST;
GRANT SELECT ON VW_ED_PRESENTATIONS_NEAT TO ROLE ANALYST;
GRANT SELECT ON VW_ELECTIVE_SURGERY_PERFORMANCE TO ROLE ANALYST;
GRANT SELECT ON VW_CASEMIX_DRG_SUMMARY TO ROLE ANALYST;
GRANT SELECT ON VW_QLD_HEALTH_KPI_DASHBOARD TO ROLE ANALYST;
GRANT SELECT ON VW_ALLIED_HEALTH_SERVICES TO ROLE ANALYST;
GRANT SELECT ON VW_AIHW_REPORTING_SUMMARY TO ROLE ANALYST;

-- ============================================================================
-- 12. VERIFICATION QUERIES (Sample Outputs)
-- ============================================================================

-- Sample: Admitted patient activity
SELECT '=== ADMITTED PATIENT ACTIVITY SAMPLE ===' as report_section;
SELECT * FROM VW_ADMITTED_PATIENT_ACTIVITY LIMIT 5;

-- Sample: OBD/ABD analysis
SELECT '=== OCCUPIED BED DAYS (OBD) & AVAILABLE BED DAYS (ABD) ===' as report_section;
SELECT * FROM VW_OBD_ABD_ANALYSIS 
WHERE service_date >= CURRENT_DATE - 7
ORDER BY service_date DESC 
LIMIT 10;

-- Sample: ED NEAT compliance
SELECT '=== ED PRESENTATIONS & NEAT COMPLIANCE ===' as report_section;
SELECT 
    neat_status,
    COUNT(*) as presentation_count,
    ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()), 2) as percentage
FROM VW_ED_PRESENTATIONS_NEAT
GROUP BY neat_status
ORDER BY presentation_count DESC;

-- Sample: KPI Dashboard
SELECT '=== QUEENSLAND HEALTH KPI DASHBOARD ===' as report_section;
SELECT * FROM VW_QLD_HEALTH_KPI_DASHBOARD
ORDER BY kpi_category, kpi_name;

-- Sample: Clinical handover
SELECT '=== CURRENT INPATIENTS FOR HANDOVER ===' as report_section;
SELECT 
    current_ward,
    COUNT(*) as patient_count,
    ROUND(AVG(current_los_days), 1) as avg_los,
    SUM(CASE WHEN long_stay_patient THEN 1 ELSE 0 END) as long_stay_count
FROM VW_CLINICAL_HANDOVER_SUMMARY
GROUP BY current_ward
ORDER BY patient_count DESC;

-- Sample: AIHW reporting
SELECT '=== AIHW REPORTING SUMMARY (LAST QUARTER) ===' as report_section;
SELECT * FROM VW_AIHW_REPORTING_SUMMARY
WHERE reporting_year = YEAR(CURRENT_DATE)
  AND reporting_quarter = QUARTER(CURRENT_DATE)
ORDER BY aihw_category, aihw_metric;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

SELECT '✅ Australian Healthcare Semantic Views Created Successfully!' as status_message;
SELECT 'All views enriched with Queensland Health and AIHW terminology' as detail;
SELECT 'Ready for clinical teams, hospital administrators, and health analysts' as audience;
SELECT '' as separator;
SELECT 'Key Views Created:' as summary;
SELECT '1. VW_ADMITTED_PATIENT_ACTIVITY - Admitted patient episodes' as view_1;
SELECT '2. VW_OBD_ABD_ANALYSIS - Occupied/Available Bed Days' as view_2;
SELECT '3. VW_ED_PRESENTATIONS_NEAT - Emergency Dept & NEAT compliance' as view_3;
SELECT '4. VW_ELECTIVE_SURGERY_PERFORMANCE - Theatre activity' as view_4;
SELECT '5. VW_CASEMIX_DRG_SUMMARY - Casemix classification' as view_5;
SELECT '6. VW_QLD_HEALTH_KPI_DASHBOARD - Key Performance Indicators' as view_6;
SELECT '7. VW_CLINICAL_HANDOVER_SUMMARY - Ward rounds & handover' as view_7;
SELECT '8. VW_ALLIED_HEALTH_SERVICES - Allied Health activity' as view_8;
SELECT '9. VW_DISCHARGE_PLANNING_DASHBOARD - Discharge planning & NDIS' as view_9;
SELECT '10. VW_AIHW_REPORTING_SUMMARY - National reporting standards' as view_10;

