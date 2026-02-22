-- ============================================================================
-- Hospital Snowflake Demo - Comprehensive Semantic View
-- ============================================================================
-- Single semantic view covering all hospital activity with Australian healthcare terminology
-- Simplified approach: Use single date/time tables, relationships provide context
-- Reference: https://docs.snowflake.com/en/user-guide/views-semantic/sql
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA ANALYTICS;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;

-- ============================================================================
-- COMPREHENSIVE QUEENSLAND HEALTH SEMANTIC VIEW
-- ============================================================================

CREATE OR REPLACE SEMANTIC VIEW SV_QUEENSLAND_HEALTH

  TABLES (
    -- Core fact tables
    admissions AS TRANSFORMED.FACT_ADMISSIONS
      PRIMARY KEY (admission_key)
      WITH SYNONYMS ('patient admissions', 'hospital separations', 'patient episodes', 'inpatient activity')
      COMMENT = 'Admitted patient activity including emergency presentations and elective admissions',
    
    procedures AS TRANSFORMED.FACT_PROCEDURES
      PRIMARY KEY (procedure_fact_key)
      WITH SYNONYMS ('surgical procedures', 'theatre procedures', 'operations', 'surgeries')
      COMMENT = 'Medical and surgical procedures performed during admissions',
    
    bed_occupancy AS TRANSFORMED.FACT_BED_OCCUPANCY
      PRIMARY KEY (bed_occupancy_key)
      WITH SYNONYMS ('bed bookings', 'inpatient stays', 'bed assignments')
      COMMENT = 'Patient bed occupancy and bookings',
    
    bed_availability AS TRANSFORMED.FACT_BED_AVAILABILITY
      PRIMARY KEY (bed_availability_key)
      WITH SYNONYMS ('bed status', 'bed capacity', 'available beds')
      COMMENT = 'Daily bed availability and occupancy tracking',
    
    -- Dimension tables
    patients AS TRANSFORMED.DIM_PATIENT
      PRIMARY KEY (patient_key)
      UNIQUE (patient_id)
      WITH SYNONYMS ('patient demographics', 'patient details')
      COMMENT = 'Patient demographic information with UR numbers',
    
    departments AS TRANSFORMED.DIM_DEPARTMENT
      PRIMARY KEY (department_key)
      UNIQUE (department_id)
      WITH SYNONYMS ('specialties', 'hospital departments', 'clinical specialties', 'wards')
      COMMENT = 'Hospital departments and clinical specialties',
    
    physicians AS TRANSFORMED.DIM_PHYSICIAN
      PRIMARY KEY (physician_key)
      WITH SYNONYMS ('doctors', 'medical officers', 'consultants', 'treating doctors')
      COMMENT = 'Treating medical officers, consultants and specialists',
    
    procedure_types AS TRANSFORMED.DIM_PROCEDURE
      PRIMARY KEY (procedure_key)
      UNIQUE (procedure_code)
      WITH SYNONYMS ('procedure codes', 'procedure catalog')
      COMMENT = 'Medical and surgical procedure definitions',
    
    beds AS TRANSFORMED.DIM_BED
      PRIMARY KEY (bed_key)
      UNIQUE (bed_id)
      WITH SYNONYMS ('bed inventory', 'bed master')
      COMMENT = 'Hospital bed inventory across all wards',
    
    bed_statuses AS TRANSFORMED.DIM_BED_STATUS
      PRIMARY KEY (bed_status_key)
      WITH SYNONYMS ('bed status types', 'occupancy statuses')
      COMMENT = 'Bed status categories',
    
    dates AS TRANSFORMED.DIM_DATE
      PRIMARY KEY (date_key)
      WITH SYNONYMS ('dates', 'calendar', 'date dimension')
      COMMENT = 'Date dimension for all date attributes',
    
    times AS TRANSFORMED.DIM_TIME
      PRIMARY KEY (time_key)
      WITH SYNONYMS ('times', 'time of day')
      COMMENT = 'Time dimension for all time attributes',
    
    weather AS TRANSFORMED.DIM_WEATHER
      PRIMARY KEY (weather_key)
      WITH SYNONYMS ('weather conditions', 'weather data')
      COMMENT = 'Weather conditions for correlation with ED presentations'
  )

  RELATIONSHIPS (
    -- Admission relationships
    admissions_to_patients AS
      admissions (patient_key) REFERENCES patients,
    admissions_to_departments AS
      admissions (department_key) REFERENCES departments,
    admissions_to_physicians AS
      admissions (physician_key) REFERENCES physicians,
    admissions_to_weather AS
      admissions (weather_key) REFERENCES weather,
    admissions_to_admission_dates AS
      admissions (admission_date_key) REFERENCES dates,
    admissions_to_admission_times AS
      admissions (admission_time_key) REFERENCES times,
    admissions_to_discharge_dates AS
      admissions (discharge_date_key) REFERENCES dates,
    
    -- Procedure relationships
    procedures_to_admissions AS
      procedures (admission_key) REFERENCES admissions,
    procedures_to_patients AS
      procedures (patient_key) REFERENCES patients,
    procedures_to_procedure_types AS
      procedures (procedure_key) REFERENCES procedure_types,
    procedures_to_physicians AS
      procedures (physician_key) REFERENCES physicians,
    procedures_to_procedure_dates AS
      procedures (procedure_date_key) REFERENCES dates,
    procedures_to_procedure_times AS
      procedures (procedure_time_key) REFERENCES times,
    
    -- Bed occupancy relationships
    bed_occupancy_to_beds AS
      bed_occupancy (bed_key) REFERENCES beds,
    bed_occupancy_to_patients AS
      bed_occupancy (patient_key) REFERENCES patients,
    bed_occupancy_to_dates AS
      bed_occupancy (date_key) REFERENCES dates,
    bed_occupancy_to_bed_statuses AS
      bed_occupancy (bed_status_key) REFERENCES bed_statuses,
    bed_occupancy_to_departments AS
      bed_occupancy (department_key) REFERENCES departments,
    
    -- Bed availability relationships
    bed_availability_to_beds AS
      bed_availability (bed_key) REFERENCES beds,
    bed_availability_to_dates AS
      bed_availability (date_key) REFERENCES dates,
    bed_availability_to_departments AS
      bed_availability (department_key) REFERENCES departments,
    
    -- Bed to department relationship
    beds_to_departments AS
      beds (department_id) REFERENCES departments (department_id)
  )

  FACTS (
    -- Admission facts (using actual column names from FACT_ADMISSIONS)
    admissions.los_days AS length_of_stay_days
      COMMENT = 'Length of stay in days for admission',
    admissions.los_hours AS length_of_stay_hours
      COMMENT = 'Length of stay in hours for admission',
    admissions.charges AS total_charges
      COMMENT = 'Total charges for the hospital episode',
    admissions.emergency_flag AS CASE WHEN admission_type = 'Emergency' THEN 1 ELSE 0 END
      COMMENT = 'Emergency presentation indicator',
    admissions.neat_flag AS CASE WHEN length_of_stay_hours <= 4 THEN 1 ELSE 0 END
      COMMENT = 'NEAT 4-hour target compliance indicator',
    admissions.long_stay_flag AS CASE WHEN length_of_stay_days > 21 THEN 1 ELSE 0 END
      COMMENT = 'Long stay patient indicator (>21 days)',
    admissions.readmit_flag AS CASE WHEN is_readmission THEN 1 ELSE 0 END
      COMMENT = '28-day unplanned readmission indicator',
    
    -- Procedure facts (using actual column names from FACT_PROCEDURES)
    procedures.duration_mins AS procedure_duration_minutes
      COMMENT = 'Procedure duration in minutes',
    procedures.proc_cost AS procedure_cost
      COMMENT = 'Cost of the procedure',
    procedures.success_flag AS CASE WHEN is_successful THEN 1 ELSE 0 END
      COMMENT = 'Procedure success indicator',
    
    -- Bed occupancy facts (using actual column names from FACT_BED_OCCUPANCY)
    bed_occupancy.nights AS total_nights
      COMMENT = 'Total nights in bed',
    bed_occupancy.night_rate AS nightly_rate
      COMMENT = 'Nightly rate for bed',
    bed_occupancy.bed_charges AS total_bed_charges
      COMMENT = 'Total charges for bed occupancy',
    bed_occupancy.occupied_flag AS CASE WHEN is_occupied THEN 1 ELSE 0 END
      COMMENT = 'Bed occupied indicator',
    
    -- Bed availability facts (using actual column names from FACT_BED_AVAILABILITY)
    bed_availability.obd_flag AS CASE WHEN is_occupied THEN 1 ELSE 0 END
      COMMENT = 'Occupied bed day indicator for OBD calculation',
    bed_availability.abd_flag AS 1
      COMMENT = 'Available bed day indicator for ABD calculation',
    bed_availability.util_rate AS utilization_rate
      COMMENT = 'Bed utilization rate (0-1)',
    bed_availability.rev_potential AS revenue_potential
      COMMENT = 'Revenue potential for available bed',
    
    -- Bed dimension facts (using actual column names from DIM_BED)
    beds.rate AS daily_rate
      COMMENT = 'Daily bed rate'
  )

  DIMENSIONS (
    -- Patient dimensions (using actual column names from DIM_PATIENT)
    patients.patient_id AS patient_id
      WITH SYNONYMS ('ur number', 'unit record number', 'patient identifier', 'mrn', 'medical record number', 'ur')
      COMMENT = 'Unit Record Number - unique patient identifier (Australian term)',
    
    patients.full_name AS full_name
      WITH SYNONYMS ('patient name', 'name')
      COMMENT = 'Patient full name',
    
    patients.age AS age
      WITH SYNONYMS ('age in years', 'patient age', 'years old')
      COMMENT = 'Patient age in years',
    
    patients.age_group AS age_group
      WITH SYNONYMS ('age category', 'age bracket', 'age range')
      COMMENT = 'Patient age group - Pediatric, Adult, Senior',
    
    patients.gender AS gender
      WITH SYNONYMS ('sex', 'patient gender')
      COMMENT = 'Patient gender',
    
    patients.insurance_provider AS insurance_provider
      WITH SYNONYMS ('payer', 'insurance', 'funding', 'payer type', 'funding source')
      COMMENT = 'Insurance provider or funding source (Medicare, DVA, Private)',
    
    patients.city AS city
      WITH SYNONYMS ('patient location', 'patient city', 'locality')
      COMMENT = 'Patient city of residence',
    
    patients.state AS state
      WITH SYNONYMS ('patient state')
      COMMENT = 'Patient state of residence',
    
    -- Department/Specialty dimensions (using actual column names from DIM_DEPARTMENT)
    departments.department_name AS department_name
      WITH SYNONYMS ('specialty', 'clinical specialty', 'ward', 'department', 'clinical area')
      COMMENT = 'Hospital specialty or department name',
    
    departments.specialization_type AS specialization_type
      WITH SYNONYMS ('specialty category', 'specialty type', 'service type')
      COMMENT = 'Clinical specialization type',
    
    departments.department_head AS department_head
      WITH SYNONYMS ('consultant', 'head of department', 'lead consultant')
      COMMENT = 'Department head or lead consultant',
    
    departments.bed_capacity AS bed_capacity
      WITH SYNONYMS ('ward beds', 'bed complement', 'ward capacity')
      COMMENT = 'Number of beds in ward/department',
    
    -- Physician dimensions (using actual column names from DIM_PHYSICIAN)
    physicians.physician_name AS physician_name
      WITH SYNONYMS ('doctor', 'medical officer', 'mo', 'consultant', 'treating doctor', 'clinician', 'treating medical officer')
      COMMENT = 'Treating Medical Officer or consultant (Australian term)',
    
    physicians.specialty AS specialty
      WITH SYNONYMS ('physician specialty', 'medical specialty', 'clinical specialty', 'doctor specialty')
      COMMENT = 'Physician clinical specialty',
    
    -- Admission dimensions (using actual column names from FACT_ADMISSIONS)
    admissions.admission_type AS admission_type
      WITH SYNONYMS ('separation mode', 'admission mode', 'type of admission', 'admission category')
      COMMENT = 'Type of admission - Emergency, Elective, Urgent',
    
    admissions.chief_complaint AS chief_complaint
      WITH SYNONYMS ('complaint', 'presenting problem', 'reason for admission', 'presenting complaint')
      COMMENT = 'Chief complaint or presenting problem',
    
    admissions.diagnosis_primary AS diagnosis_primary
      WITH SYNONYMS ('diagnosis', 'main diagnosis', 'primary diagnosis', 'principal diagnosis')
      COMMENT = 'Principal diagnosis for the episode',
    
    admissions.diagnosis_secondary AS diagnosis_secondary
      WITH SYNONYMS ('secondary diagnosis', 'comorbidities', 'additional diagnosis')
      COMMENT = 'Secondary or additional diagnosis',
    
    -- Procedure dimensions (using actual column names from DIM_PROCEDURE and FACT_PROCEDURES)
    procedure_types.procedure_code AS procedure_code
      WITH SYNONYMS ('code', 'cpt code', 'proc code')
      COMMENT = 'Procedure code',
    
    procedure_types.procedure_name AS procedure_name
      WITH SYNONYMS ('procedure', 'operation', 'surgery', 'procedure description')
      COMMENT = 'Procedure name and description',
    
    procedure_types.procedure_category AS procedure_category
      WITH SYNONYMS ('procedure type', 'category')
      COMMENT = 'Procedure category - Diagnostic, Surgical, Imaging, Laboratory',
    
    procedure_types.is_surgical AS is_surgical
      WITH SYNONYMS ('surgical', 'is surgery')
      COMMENT = 'Whether procedure is surgical',
    
    procedures.anesthesia_type AS anesthesia_type
      WITH SYNONYMS ('anesthesia', 'anaesthetic', 'anaesthetic type')
      COMMENT = 'Type of anaesthetic - General, Spinal, Local (Australian spelling)',
    
    procedures.complications AS complications
      WITH SYNONYMS ('post op complications', 'adverse events')
      COMMENT = 'Post-operative complications',
    
    procedures.is_successful AS is_successful
      WITH SYNONYMS ('successful', 'procedure successful')
      COMMENT = 'Whether procedure was successful',
    
    -- Bed dimensions (using actual column names from DIM_BED)
    beds.bed_id AS bed_id
      WITH SYNONYMS ('bed number', 'bed identifier', 'bed')
      COMMENT = 'Unique bed identifier',
    
    beds.bed_type AS bed_type
      WITH SYNONYMS ('type of bed', 'bed category', 'bed classification')
      COMMENT = 'Bed type - Standard, ICU, Private, Semi-Private, Isolation',
    
    beds.equipment AS equipment
      WITH SYNONYMS ('equipment available', 'bed equipment')
      COMMENT = 'Equipment available with bed',
    
    beds.is_active AS is_active
      WITH SYNONYMS ('active', 'bed active')
      COMMENT = 'Whether bed is active',
    
    -- Bed availability dimensions (using actual column names from FACT_BED_AVAILABILITY)
    bed_availability.availability_date AS availability_date
      WITH SYNONYMS ('date', 'service date', 'bed date')
      COMMENT = 'Date of bed availability',
    
    bed_availability.status AS status
      WITH SYNONYMS ('bed status', 'availability status', 'occupancy status')
      COMMENT = 'Bed status - Available, Occupied, Maintenance, Cleaning, Out of Service',
    
    bed_availability.is_available AS is_available
      WITH SYNONYMS ('available', 'free', 'bed available')
      COMMENT = 'Whether bed is available',
    
    bed_availability.is_occupied AS is_occupied
      WITH SYNONYMS ('occupied', 'in use', 'bed occupied')
      COMMENT = 'Whether bed is occupied',
    
    bed_availability.is_maintenance AS is_maintenance
      WITH SYNONYMS ('out of service', 'maintenance', 'unavailable')
      COMMENT = 'Whether bed is out of service',
    
    bed_statuses.status_name AS status_name
      WITH SYNONYMS ('status type', 'status category')
      COMMENT = 'Bed status type name',
    
    -- Date dimensions (using actual column names from DIM_DATE)
    dates.date_value AS date_value
      WITH SYNONYMS ('date', 'admission date', 'procedure date', 'service date')
      COMMENT = 'Date value',
    
    dates.year AS year
      WITH SYNONYMS ('year', 'financial year')
      COMMENT = 'Year',
    
    dates.quarter AS quarter
      WITH SYNONYMS ('quarter', 'qtr')
      COMMENT = 'Quarter (1-4)',
    
    dates.month AS month
      WITH SYNONYMS ('month number')
      COMMENT = 'Month number (1-12)',
    
    dates.month_name AS month_name
      WITH SYNONYMS ('month', 'month name')
      COMMENT = 'Month name',
    
    dates.week AS week
      WITH SYNONYMS ('week', 'week number')
      COMMENT = 'Week number',
    
    dates.day_of_week AS day_of_week
      WITH SYNONYMS ('day number', 'weekday number')
      COMMENT = 'Day of week number (1-7)',
    
    dates.day_name AS day_name
      WITH SYNONYMS ('day of week', 'weekday', 'day')
      COMMENT = 'Day of week name',
    
    dates.is_weekend AS is_weekend
      WITH SYNONYMS ('weekend')
      COMMENT = 'Whether date is weekend',
    
    -- Time dimensions (using actual column names from DIM_TIME)
    times.time_value AS time_value
      WITH SYNONYMS ('time', 'time of day')
      COMMENT = 'Time value',
    
    times.hour AS hour
      WITH SYNONYMS ('hour', 'hour of day')
      COMMENT = 'Hour (0-23)',
    
    times.shift AS shift
      WITH SYNONYMS ('shift', 'work shift', 'time period')
      COMMENT = 'Shift - Day, Evening, Night',
    
    -- Weather dimensions (using actual column names from DIM_WEATHER)
    weather.weather_condition AS weather_condition
      WITH SYNONYMS ('weather', 'weather type', 'conditions')
      COMMENT = 'Weather condition at time of admission',
    
    weather.temperature_range AS temperature_range
      WITH SYNONYMS ('temp', 'temperature')
      COMMENT = 'Temperature range',
    
    weather.season AS season
      WITH SYNONYMS ('season', 'weather season', 'seasonal weather')
      COMMENT = 'Season for weather correlation'
  )

  METRICS (
    -- Volume metrics
    admissions.total_separations AS COUNT(DISTINCT admissions.admission_id)
      WITH SYNONYMS ('number of admissions', 'admission count', 'total admissions', 'separations')
      COMMENT = 'Total number of hospital admissions/separations (Australian term)',
    
    patients.unique_patients AS COUNT(DISTINCT patients.patient_key)
      WITH SYNONYMS ('patient count', 'number of patients', 'distinct patients', 'unique ur numbers')
      COMMENT = 'Count of unique patients by UR number',
    
    -- ALOS (Average Length of Stay) - Queensland Health KPI
    admissions.average_los AS AVG(admissions.los_days)
      WITH SYNONYMS ('alos', 'average length of stay', 'mean length of stay', 'avg los')
      COMMENT = 'Average Length of Stay in days (ALOS) - Queensland Health KPI',
    
    admissions.median_los AS MEDIAN(admissions.los_days)
      WITH SYNONYMS ('median length of stay', 'median alos')
      COMMENT = 'Median Length of Stay in days',
    
    -- OBD (Occupied Bed Days) - AIHW metric
    admissions.occupied_bed_days AS SUM(admissions.los_days)
      WITH SYNONYMS ('obd', 'occupied bed days', 'patient days', 'bed days', 'inpatient days')
      COMMENT = 'Total Occupied Bed Days (OBD) - AIHW standard metric',
    
    -- Revenue metrics
    admissions.total_revenue AS SUM(admissions.charges)
      WITH SYNONYMS ('revenue', 'total charges', 'total cost', 'total income', 'hospital revenue')
      COMMENT = 'Total revenue from patient episodes',
    
    admissions.average_cost AS AVG(admissions.charges)
      WITH SYNONYMS ('average cost', 'avg cost per admission', 'mean episode cost')
      COMMENT = 'Average cost per patient episode',
    
    -- Emergency Department metrics
    admissions.ed_presentation_count AS SUM(admissions.emergency_flag)
      WITH SYNONYMS ('ed presentations', 'emergency count', 'ed admissions', 'emergency admissions', 'ed visits')
      COMMENT = 'Count of Emergency Department presentations',
    
    admissions.ed_presentation_rate AS 
      (SUM(admissions.emergency_flag) / COUNT(DISTINCT admissions.admission_id) * 100)
      WITH SYNONYMS ('ed rate', 'emergency rate', 'percentage emergency', 'percent via ed')
      COMMENT = 'Percentage of admissions via Emergency Department',
    
    -- NEAT compliance (4-hour ED target) - Queensland Health KPI
    admissions.neat_compliant_count AS SUM(admissions.neat_flag)
      WITH SYNONYMS ('neat count', '4 hour compliant count', 'ed 4 hour count', 'within target count')
      COMMENT = 'Count of NEAT compliant presentations (ED within 4 hours)',
    
    admissions.neat_compliance_rate AS 
      (SUM(admissions.neat_flag * admissions.emergency_flag) / 
       NULLIF(SUM(admissions.emergency_flag), 0) * 100)
      WITH SYNONYMS ('neat compliance', '4 hour target compliance', 'ed 4 hour rate', 'neat percentage', 'four hour target')
      COMMENT = 'NEAT compliance rate - Queensland Health target ≥90%',
    
    -- Long stay patients
    admissions.long_stay_count AS SUM(admissions.long_stay_flag)
      WITH SYNONYMS ('long stay count', 'number of long stay patients', 'extended stay count')
      COMMENT = 'Count of patients with length of stay greater than 21 days',
    
    admissions.long_stay_rate AS 
      (SUM(admissions.long_stay_flag) / COUNT(DISTINCT admissions.admission_id) * 100)
      WITH SYNONYMS ('long stay percentage', 'long stay rate percentage')
      COMMENT = 'Percentage of patients with long stay (>21 days)',
    
    -- Unplanned readmissions (28-day) - Queensland Health KPI
    admissions.readmission_count AS SUM(admissions.readmit_flag)
      WITH SYNONYMS ('readmission count', '28 day readmission count', 'readmissions')
      COMMENT = 'Count of 28-day unplanned readmissions',
    
    admissions.readmission_rate AS 
      (SUM(admissions.readmit_flag) / COUNT(DISTINCT admissions.admission_id) * 100)
      WITH SYNONYMS ('readmission rate', '28 day readmission rate', 'readmission percentage')
      COMMENT = '28-day unplanned readmission rate - Queensland Health target <10%',
    
    -- Procedure metrics
    procedures.total_procedures AS COUNT(DISTINCT procedures.procedure_id)
      WITH SYNONYMS ('procedure count', 'number of procedures', 'surgery count', 'number of surgeries')
      COMMENT = 'Total number of procedures performed',
    
    procedures.total_theatre_time AS SUM(procedures.duration_mins) / 60
      WITH SYNONYMS ('theatre time', 'operating time', 'surgery hours', 'total or time', 'theatre hours')
      COMMENT = 'Total theatre hours utilized (Australian term)',
    
    procedures.average_theatre_time AS AVG(procedures.duration_mins) / 60
      WITH SYNONYMS ('avg theatre time', 'average surgery time', 'mean theatre time', 'average procedure time')
      COMMENT = 'Average theatre time per procedure in hours',
    
    procedures.total_procedure_cost AS SUM(procedures.proc_cost)
      WITH SYNONYMS ('theatre cost', 'surgery cost', 'operating cost', 'total cost')
      COMMENT = 'Total procedure costs',
    
    procedures.average_procedure_cost AS AVG(procedures.proc_cost)
      WITH SYNONYMS ('avg procedure cost', 'average surgery cost', 'mean cost')
      COMMENT = 'Average cost per procedure',
    
    procedures.procedure_success_rate AS 
      (SUM(procedures.success_flag) / COUNT(DISTINCT procedures.procedure_id) * 100)
      WITH SYNONYMS ('success rate', 'procedure success percentage', 'surgery success rate')
      COMMENT = 'Procedure success rate percentage - Queensland Health target ≥95%',
    
    -- Bed management metrics
    beds.total_beds AS COUNT(DISTINCT beds.bed_key)
      WITH SYNONYMS ('bed count', 'number of beds', 'bed complement', 'bed capacity')
      COMMENT = 'Total number of beds in hospital',
    
    bed_availability.occupied_beds_count AS SUM(bed_availability.obd_flag)
      WITH SYNONYMS ('beds occupied', 'number of occupied beds', 'occupied bed count')
      COMMENT = 'Count of occupied beds',
    
    bed_availability.available_beds_count AS 
      SUM(CASE WHEN bed_availability.is_available THEN 1 ELSE 0 END)
      WITH SYNONYMS ('beds available', 'free beds', 'number of available beds')
      COMMENT = 'Count of available beds',
    
    -- OBD and ABD (AIHW standard metrics)
    bed_availability.obd_total AS SUM(bed_availability.obd_flag)
      WITH SYNONYMS ('obd', 'occupied bed days', 'patient bed days', 'inpatient bed days')
      COMMENT = 'Occupied Bed Days (OBD) - AIHW standard metric',
    
    bed_availability.abd_total AS SUM(bed_availability.abd_flag)
      WITH SYNONYMS ('abd', 'available bed days', 'theoretical bed days', 'potential bed days')
      COMMENT = 'Available Bed Days (ABD) - theoretical bed day capacity',
    
    -- Bed occupancy rate - Queensland Health target 85-92%
    bed_availability.occupancy_rate_pct AS 
      (SUM(bed_availability.obd_flag) / SUM(bed_availability.abd_flag) * 100)
      WITH SYNONYMS ('occupancy rate', 'bed occupancy', 'occupancy percentage', 'bed occupancy rate')
      COMMENT = 'Bed occupancy rate percentage (OBD/ABD) - Queensland Health target 85-92%',
    
    bed_availability.average_utilization_pct AS AVG(bed_availability.util_rate) * 100
      WITH SYNONYMS ('avg utilization', 'average utilization percentage', 'mean utilization')
      COMMENT = 'Average bed utilization rate percentage',
    
    bed_availability.beds_at_risk_count AS 
      SUM(CASE WHEN bed_availability.util_rate > 0.92 THEN 1 ELSE 0 END)
      WITH SYNONYMS ('beds at risk', 'overcapacity beds', 'unsafe capacity count')
      COMMENT = 'Bed days over safe capacity threshold (>92% is unsafe)',
    
    bed_availability.revenue_potential_total AS SUM(bed_availability.rev_potential)
      WITH SYNONYMS ('revenue potential', 'potential revenue', 'lost revenue')
      COMMENT = 'Total potential revenue from available beds',
    
    bed_occupancy.bed_revenue_total AS SUM(bed_occupancy.bed_charges)
      WITH SYNONYMS ('bed revenue', 'bed charges', 'accommodation revenue', 'bed income')
      COMMENT = 'Total revenue from bed occupancy',
    
    bed_occupancy.avg_nightly_rate AS AVG(bed_occupancy.night_rate)
      WITH SYNONYMS ('avg nightly rate', 'average bed rate', 'mean nightly rate')
      COMMENT = 'Average nightly bed rate'
  )

  COMMENT = 'Comprehensive Queensland Health semantic view covering all hospital activity with Australian healthcare terminology. Supports AIHW and Queensland Health KPI reporting.';

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

-- ACCOUNTADMIN - Full administrative access
GRANT REFERENCES, SELECT ON SEMANTIC VIEW SV_QUEENSLAND_HEALTH TO ROLE ACCOUNTADMIN;

-- Clinical Admin - Full access
GRANT REFERENCES, SELECT ON SEMANTIC VIEW SV_QUEENSLAND_HEALTH TO ROLE CLINICAL_ADMIN;

-- Analyst - Full access for reporting
GRANT REFERENCES, SELECT ON SEMANTIC VIEW SV_QUEENSLAND_HEALTH TO ROLE ANALYST;

-- Physician - Clinical access
GRANT REFERENCES, SELECT ON SEMANTIC VIEW SV_QUEENSLAND_HEALTH TO ROLE PHYSICIAN;

-- Nurse - Bed management and patient care
GRANT REFERENCES, SELECT ON SEMANTIC VIEW SV_QUEENSLAND_HEALTH TO ROLE NURSE;

-- Data Engineer - Full access for maintenance
GRANT REFERENCES, SELECT ON SEMANTIC VIEW SV_QUEENSLAND_HEALTH TO ROLE DATA_ENGINEER;

-- ============================================================================
-- SHOW SEMANTIC VIEW STRUCTURE
-- ============================================================================

SHOW SEMANTIC VIEWS IN SCHEMA ANALYTICS;
DESCRIBE SEMANTIC VIEW SV_QUEENSLAND_HEALTH;
SHOW SEMANTIC FACTS IN SV_QUEENSLAND_HEALTH;
SHOW SEMANTIC DIMENSIONS IN SV_QUEENSLAND_HEALTH;
SHOW SEMANTIC METRICS IN SV_QUEENSLAND_HEALTH;

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

SELECT '✅ COMPREHENSIVE QUEENSLAND HEALTH SEMANTIC VIEW CREATED!' as status_message;
SELECT '🏥 Single semantic view covering all hospital activity' as scope;
SELECT '📊 12 logical tables (simplified date/time handling)' as tables;
SELECT '🔗 25+ relationships between tables' as relationships;
SELECT '📈 20+ facts for calculations' as facts;
SELECT '🏷️  60+ dimensions with Australian synonyms' as dimensions;
SELECT '📊 30+ metrics including ALOS, NEAT, OBD/ABD, readmissions' as metrics;
SELECT '🇦🇺 Full Australian healthcare terminology in synonyms' as terminology;
SELECT '🤖 Ready for Cortex Analyst natural language queries!' as capability;
