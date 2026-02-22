-- ============================================================================
-- Hospital Snowflake Demo - Dimensional Model Transformation
-- ============================================================================
-- This script transforms raw data into a dimensional model (star schema)

USE ROLE DATA_ENGINEER;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA TRANSFORMED;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;

-- 1. Create Dimension Tables

-- Date Dimension
CREATE OR REPLACE TABLE DIM_DATE (
    date_key INTEGER PRIMARY KEY,
    date_value DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name STRING,
    week INTEGER,
    day_of_year INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name STRING,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    season STRING
);

-- Patient Dimension (SCD Type 2 for historical tracking)
CREATE OR REPLACE TABLE DIM_PATIENT (
    patient_key INTEGER AUTOINCREMENT PRIMARY KEY,
    patient_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    date_of_birth DATE,
    age INTEGER,
    age_group STRING,
    gender STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    insurance_provider STRING,
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Department Dimension
CREATE OR REPLACE TABLE DIM_DEPARTMENT (
    department_key INTEGER AUTOINCREMENT PRIMARY KEY,
    department_id STRING NOT NULL,
    department_name STRING,
    department_head STRING,
    location_floor STRING,
    phone_extension STRING,
    budget_annual INTEGER,
    staff_count INTEGER,
    bed_capacity INTEGER,
    specialization_type STRING,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Physician Dimension
CREATE OR REPLACE TABLE DIM_PHYSICIAN (
    physician_key INTEGER AUTOINCREMENT PRIMARY KEY,
    physician_name STRING NOT NULL,
    specialty STRING,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Procedure Dimension
CREATE OR REPLACE TABLE DIM_PROCEDURE (
    procedure_key INTEGER AUTOINCREMENT PRIMARY KEY,
    procedure_code STRING NOT NULL,
    procedure_name STRING,
    procedure_category STRING,
    is_surgical BOOLEAN,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Time Dimension
CREATE OR REPLACE TABLE DIM_TIME (
    time_key INTEGER PRIMARY KEY,
    time_value TIME NOT NULL,
    hour INTEGER,
    minute INTEGER,
    hour_period STRING, -- AM/PM
    shift STRING, -- Day/Evening/Night
    is_business_hours BOOLEAN
);

-- Weather Dimension
CREATE OR REPLACE TABLE DIM_WEATHER (
    weather_key INTEGER AUTOINCREMENT PRIMARY KEY,
    weather_condition STRING,
    temperature_range STRING,
    season STRING,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Bed Dimension
CREATE OR REPLACE TABLE DIM_BED (
    bed_key INTEGER AUTOINCREMENT PRIMARY KEY,
    bed_id STRING NOT NULL,
    department_id STRING,
    room_number STRING,
    bed_number STRING,
    bed_type STRING,
    equipment STRING,
    is_active BOOLEAN,
    daily_rate DECIMAL(8,2),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Bed Status Dimension
CREATE OR REPLACE TABLE DIM_BED_STATUS (
    bed_status_key INTEGER AUTOINCREMENT PRIMARY KEY,
    status_name STRING NOT NULL,
    status_category STRING,
    is_billable BOOLEAN,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 2. Create Fact Tables

-- Admission Fact Table
CREATE OR REPLACE TABLE FACT_ADMISSIONS (
    admission_key INTEGER AUTOINCREMENT PRIMARY KEY,
    admission_id STRING NOT NULL,
    patient_key INTEGER,
    department_key INTEGER,
    physician_key INTEGER,
    weather_key INTEGER,
    admission_date_key INTEGER,
    admission_time_key INTEGER,
    discharge_date_key INTEGER,
    discharge_time_key INTEGER,
    admission_type STRING,
    chief_complaint STRING,
    diagnosis_primary STRING,
    diagnosis_secondary STRING,
    room_number STRING,
    bed_number STRING,
    insurance_authorization STRING,
    total_charges DECIMAL(12,2),
    length_of_stay_days INTEGER,
    length_of_stay_hours INTEGER,
    is_emergency BOOLEAN,
    is_readmission BOOLEAN,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (patient_key) REFERENCES DIM_PATIENT(patient_key),
    FOREIGN KEY (department_key) REFERENCES DIM_DEPARTMENT(department_key),
    FOREIGN KEY (physician_key) REFERENCES DIM_PHYSICIAN(physician_key),
    FOREIGN KEY (weather_key) REFERENCES DIM_WEATHER(weather_key),
    FOREIGN KEY (admission_date_key) REFERENCES DIM_DATE(date_key),
    FOREIGN KEY (admission_time_key) REFERENCES DIM_TIME(time_key),
    FOREIGN KEY (discharge_date_key) REFERENCES DIM_DATE(date_key),
    FOREIGN KEY (discharge_time_key) REFERENCES DIM_TIME(time_key)
);

-- Procedure Fact Table
CREATE OR REPLACE TABLE FACT_PROCEDURES (
    procedure_fact_key INTEGER AUTOINCREMENT PRIMARY KEY,
    procedure_id STRING NOT NULL,
    admission_key INTEGER,
    patient_key INTEGER,
    procedure_key INTEGER,
    physician_key INTEGER,
    procedure_date_key INTEGER,
    procedure_time_key INTEGER,
    procedure_duration_minutes INTEGER,
    procedure_cost DECIMAL(10,2),
    anesthesia_type STRING,
    complications STRING,
    is_successful BOOLEAN,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (admission_key) REFERENCES FACT_ADMISSIONS(admission_key),
    FOREIGN KEY (patient_key) REFERENCES DIM_PATIENT(patient_key),
    FOREIGN KEY (procedure_key) REFERENCES DIM_PROCEDURE(procedure_key),
    FOREIGN KEY (physician_key) REFERENCES DIM_PHYSICIAN(physician_key),
    FOREIGN KEY (procedure_date_key) REFERENCES DIM_DATE(date_key),
    FOREIGN KEY (procedure_time_key) REFERENCES DIM_TIME(time_key)
);

-- Bed Occupancy Fact Table
CREATE OR REPLACE TABLE FACT_BED_OCCUPANCY (
    bed_occupancy_key INTEGER AUTOINCREMENT PRIMARY KEY,
    bed_key INTEGER,
    patient_key INTEGER,
    date_key INTEGER,
    bed_status_key INTEGER,
    department_key INTEGER,
    booking_id STRING,
    occupancy_date DATE,
    check_in_time TIME,
    check_out_time TIME,
    planned_checkout_date DATE,
    actual_checkout_date DATE,
    total_nights INTEGER,
    nightly_rate DECIMAL(8,2),
    total_bed_charges DECIMAL(10,2),
    special_requirements STRING,
    is_occupied BOOLEAN,
    is_overdue BOOLEAN,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (bed_key) REFERENCES DIM_BED(bed_key),
    FOREIGN KEY (patient_key) REFERENCES DIM_PATIENT(patient_key),
    FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key),
    FOREIGN KEY (bed_status_key) REFERENCES DIM_BED_STATUS(bed_status_key),
    FOREIGN KEY (department_key) REFERENCES DIM_DEPARTMENT(department_key)
);

-- Bed Availability Fact Table (Daily Snapshots)
CREATE OR REPLACE TABLE FACT_BED_AVAILABILITY (
    bed_availability_key INTEGER AUTOINCREMENT PRIMARY KEY,
    bed_key INTEGER,
    date_key INTEGER,
    department_key INTEGER,
    availability_date DATE,
    status STRING,
    is_available BOOLEAN,
    is_maintenance BOOLEAN,
    is_occupied BOOLEAN,
    reserved_until TIME,
    utilization_rate DECIMAL(5,4),
    revenue_potential DECIMAL(8,2),
    last_updated TIMESTAMP,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (bed_key) REFERENCES DIM_BED(bed_key),
    FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key),
    FOREIGN KEY (department_key) REFERENCES DIM_DEPARTMENT(department_key)
);

-- 3. Populate Dimension Tables

-- Populate Date Dimension (3 years of dates)
INSERT INTO DIM_DATE
WITH date_range AS (
    SELECT DATEADD(day, seq4(), '2023-01-01') as date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 1095)) -- 3 years
)
SELECT 
    TO_NUMBER(TO_CHAR(date_value, 'YYYYMMDD')) as date_key,
    date_value,
    YEAR(date_value) as year,
    QUARTER(date_value) as quarter,
    MONTH(date_value) as month,
    MONTHNAME(date_value) as month_name,
    WEEK(date_value) as week,
    DAYOFYEAR(date_value) as day_of_year,
    DAY(date_value) as day_of_month,
    DAYOFWEEK(date_value) as day_of_week,
    DAYNAME(date_value) as day_name,
    CASE WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE ELSE FALSE END as is_weekend,
    FALSE as is_holiday, -- Simplified for demo
    CASE 
        WHEN MONTH(date_value) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(date_value) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(date_value) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END as season
FROM date_range;

-- Populate Time Dimension
INSERT INTO DIM_TIME
WITH time_range AS (
    SELECT DATEADD(minute, seq4(), '00:00:00'::TIME) as time_value
    FROM TABLE(GENERATOR(ROWCOUNT => 1440)) -- 24 hours * 60 minutes
)
SELECT 
    HOUR(time_value) * 100 + MINUTE(time_value) as time_key,
    time_value,
    HOUR(time_value) as hour,
    MINUTE(time_value) as minute,
    CASE WHEN HOUR(time_value) < 12 THEN 'AM' ELSE 'PM' END as hour_period,
    CASE 
        WHEN HOUR(time_value) BETWEEN 7 AND 15 THEN 'Day'
        WHEN HOUR(time_value) BETWEEN 16 AND 23 THEN 'Evening'
        ELSE 'Night'
    END as shift,
    CASE WHEN HOUR(time_value) BETWEEN 8 AND 17 THEN TRUE ELSE FALSE END as is_business_hours
FROM time_range
WHERE MINUTE(time_value) = 0; -- Only keep hourly records for simplicity

-- Populate Department Dimension
INSERT INTO DIM_DEPARTMENT (
    department_id, department_name, department_head, location_floor,
    phone_extension, budget_annual, staff_count, bed_capacity, specialization_type
)
SELECT 
    department_id, department_name, department_head, location_floor,
    phone_extension, budget_annual, staff_count, bed_capacity, specialization_type
FROM RAW_DATA.HOSPITAL_DEPARTMENTS_RAW;

-- Populate Physician Dimension
INSERT INTO DIM_PHYSICIAN (physician_name, specialty)
SELECT DISTINCT 
    attending_physician,
    CASE 
        WHEN department_id = 'CARD' THEN 'Cardiology'
        WHEN department_id = 'EMER' THEN 'Emergency Medicine'
        WHEN department_id = 'ORTH' THEN 'Orthopedics'
        WHEN department_id = 'OBGY' THEN 'Obstetrics & Gynecology'
        WHEN department_id = 'NEUR' THEN 'Neurology'
        WHEN department_id = 'GAST' THEN 'Gastroenterology'
        ELSE 'General Medicine'
    END as specialty
FROM RAW_DATA.PATIENT_ADMISSIONS_RAW
WHERE attending_physician IS NOT NULL;

-- Add procedures physicians
INSERT INTO DIM_PHYSICIAN (physician_name, specialty)
SELECT DISTINCT 
    performing_physician,
    'Specialist' as specialty
FROM RAW_DATA.MEDICAL_PROCEDURES_RAW
WHERE performing_physician IS NOT NULL
AND performing_physician NOT IN (SELECT physician_name FROM DIM_PHYSICIAN);

-- Populate Procedure Dimension
INSERT INTO DIM_PROCEDURE (procedure_code, procedure_name, procedure_category, is_surgical)
SELECT DISTINCT
    procedure_code,
    procedure_name,
    CASE 
        WHEN procedure_code LIKE '9%' THEN 'Diagnostic'
        WHEN procedure_code LIKE '6%' THEN 'Surgical'
        WHEN procedure_code LIKE '7%' THEN 'Imaging'
        WHEN procedure_code LIKE '8%' THEN 'Laboratory'
        ELSE 'Other'
    END as procedure_category,
    CASE 
        WHEN procedure_name ILIKE '%surgery%' 
          OR procedure_name ILIKE '%replacement%'
          OR procedure_name ILIKE '%repair%'
          OR procedure_name ILIKE '%resection%'
        THEN TRUE 
        ELSE FALSE 
    END as is_surgical
FROM RAW_DATA.MEDICAL_PROCEDURES_RAW;

-- Populate Patient Dimension
INSERT INTO DIM_PATIENT (
    patient_id, first_name, last_name, full_name, date_of_birth,
    age, age_group, gender, address, city, state, zip_code,
    insurance_provider, effective_date
)
SELECT 
    patient_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    date_of_birth,
    DATEDIFF(year, date_of_birth, CURRENT_DATE()) as age,
    CASE 
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 18 THEN 'Pediatric'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 18 AND 64 THEN 'Adult'
        ELSE 'Senior'
    END as age_group,
    gender,
    address,
    city,
    state,
    zip_code,
    insurance_provider,
    CURRENT_DATE() as effective_date
FROM RAW_DATA.PATIENT_DEMOGRAPHICS_RAW;

-- Populate Weather Dimension
INSERT INTO DIM_WEATHER (weather_condition, temperature_range, season)
SELECT DISTINCT
    weather_condition,
    CASE 
        WHEN temperature_f < 32 THEN 'Freezing'
        WHEN temperature_f BETWEEN 32 AND 50 THEN 'Cold'
        WHEN temperature_f BETWEEN 51 AND 70 THEN 'Moderate'
        WHEN temperature_f BETWEEN 71 AND 85 THEN 'Warm'
        ELSE 'Hot'
    END as temperature_range,
    CASE 
        WHEN MONTH(admission_date) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(admission_date) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(admission_date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END as season
FROM RAW_DATA.PATIENT_ADMISSIONS_RAW
WHERE weather_condition IS NOT NULL;

-- Populate Bed Dimension
INSERT INTO DIM_BED (
    bed_id, department_id, room_number, bed_number, bed_type,
    equipment, is_active, daily_rate
)
SELECT 
    bed_id, department_id, room_number, bed_number, bed_type,
    equipment, is_active, daily_rate
FROM RAW_DATA.BED_INVENTORY_RAW;

-- Populate Bed Status Dimension
INSERT INTO DIM_BED_STATUS (status_name, status_category, is_billable) VALUES
('Available', 'Available', FALSE),
('Occupied', 'Occupied', TRUE),
('Maintenance', 'Out of Service', FALSE),
('Cleaning', 'Out of Service', FALSE),
('Out of Service', 'Out of Service', FALSE),
('Reserved', 'Reserved', FALSE);

-- 4. Populate Fact Tables

-- Populate Admission Facts
INSERT INTO FACT_ADMISSIONS (
    admission_id, patient_key, department_key, physician_key, weather_key,
    admission_date_key, admission_time_key, discharge_date_key, discharge_time_key,
    admission_type, chief_complaint, diagnosis_primary, diagnosis_secondary,
    room_number, bed_number, insurance_authorization, total_charges,
    length_of_stay_days, length_of_stay_hours, is_emergency, is_readmission
)
SELECT 
    a.admission_id,
    p.patient_key,
    d.department_key,
    ph.physician_key,
    w.weather_key,
    TO_NUMBER(TO_CHAR(a.admission_date, 'YYYYMMDD')) as admission_date_key,
    HOUR(a.admission_time) * 100 as admission_time_key,
    TO_NUMBER(TO_CHAR(a.discharge_date, 'YYYYMMDD')) as discharge_date_key,
    HOUR(a.discharge_time) * 100 as discharge_time_key,
    a.admission_type,
    a.chief_complaint,
    a.diagnosis_primary,
    a.diagnosis_secondary,
    a.room_number,
    a.bed_number,
    a.insurance_authorization,
    a.total_charges,
    DATEDIFF(day, a.admission_date, a.discharge_date) as length_of_stay_days,
    DATEDIFF(hour, 
        TO_TIMESTAMP(a.admission_date::string || ' ' || a.admission_time::string),
        TO_TIMESTAMP(a.discharge_date::string || ' ' || a.discharge_time::string)
    ) as length_of_stay_hours,
    CASE WHEN a.admission_type = 'Emergency' THEN TRUE ELSE FALSE END as is_emergency,
    FALSE as is_readmission -- Simplified for demo
FROM RAW_DATA.PATIENT_ADMISSIONS_RAW a
JOIN DIM_PATIENT p ON a.patient_id = p.patient_id AND p.is_current = TRUE
JOIN DIM_DEPARTMENT d ON a.department_id = d.department_id
JOIN DIM_PHYSICIAN ph ON a.attending_physician = ph.physician_name
LEFT JOIN DIM_WEATHER w ON a.weather_condition = w.weather_condition;

-- Populate Procedure Facts
INSERT INTO FACT_PROCEDURES (
    procedure_id, admission_key, patient_key, procedure_key, physician_key,
    procedure_date_key, procedure_time_key, procedure_duration_minutes,
    procedure_cost, anesthesia_type, complications, is_successful
)
SELECT 
    pr.procedure_id,
    fa.admission_key,
    p.patient_key,
    proc.procedure_key,
    ph.physician_key,
    TO_NUMBER(TO_CHAR(pr.procedure_date, 'YYYYMMDD')) as procedure_date_key,
    HOUR(pr.procedure_time) * 100 as procedure_time_key,
    pr.procedure_duration_minutes,
    pr.procedure_cost,
    pr.anesthesia_type,
    pr.complications,
    CASE WHEN pr.complications = 'None' THEN TRUE ELSE FALSE END as is_successful
FROM RAW_DATA.MEDICAL_PROCEDURES_RAW pr
JOIN FACT_ADMISSIONS fa ON pr.admission_id = fa.admission_id
JOIN DIM_PATIENT p ON fa.patient_key = p.patient_key
JOIN DIM_PROCEDURE proc ON pr.procedure_code = proc.procedure_code
JOIN DIM_PHYSICIAN ph ON pr.performing_physician = ph.physician_name;

-- Populate Bed Occupancy Facts (Simplified approach)
-- First, let's check what data we have
SELECT 'Checking bed dimensions before populating facts...' as status;
SELECT 'Beds in DIM_BED:' as check_type, COUNT(*) as count FROM DIM_BED;
SELECT 'Patients in DIM_PATIENT:' as check_type, COUNT(*) as count FROM DIM_PATIENT WHERE is_current = TRUE;
SELECT 'Bed Status in DIM_BED_STATUS:' as check_type, COUNT(*) as count FROM DIM_BED_STATUS;
SELECT 'Bookings in RAW:' as check_type, COUNT(*) as count FROM RAW_DATA.BED_BOOKINGS_RAW;

-- Simplified bed occupancy facts - only populate if we have matching data
INSERT INTO FACT_BED_OCCUPANCY (
    bed_key, patient_key, date_key, bed_status_key, department_key,
    booking_id, occupancy_date, check_in_time, check_out_time,
    planned_checkout_date, actual_checkout_date, total_nights,
    nightly_rate, total_bed_charges, special_requirements,
    is_occupied, is_overdue
)
SELECT 
    db.bed_key,
    COALESCE(dp.patient_key, 1) as patient_key, -- Default patient if not found
    TO_NUMBER(TO_CHAR(bb.check_in_date, 'YYYYMMDD')) as date_key,
    COALESCE(dbs.bed_status_key, 2) as bed_status_key, -- Default to 'Occupied'
    dd.department_key,
    bb.booking_id,
    bb.check_in_date,
    bb.check_in_time,
    bb.actual_checkout_time,
    bb.expected_checkout_date,
    bb.actual_checkout_date,
    bb.total_nights,
    bb.nightly_rate,
    bb.total_charges,
    bb.special_requirements,
    CASE WHEN bb.booking_status = 'Active' THEN TRUE ELSE FALSE END as is_occupied,
    CASE WHEN bb.expected_checkout_date < CURRENT_DATE() AND bb.actual_checkout_date IS NULL THEN TRUE ELSE FALSE END as is_overdue
FROM RAW_DATA.BED_BOOKINGS_RAW bb
JOIN DIM_BED db ON bb.bed_id = db.bed_id
LEFT JOIN DIM_PATIENT dp ON bb.patient_id = dp.patient_id AND dp.is_current = TRUE
LEFT JOIN DIM_BED_STATUS dbs ON dbs.status_name = 'Occupied'
JOIN DIM_DEPARTMENT dd ON db.department_id = dd.department_id;

-- Check results
SELECT 'Bed Occupancy Facts populated:' as status, COUNT(*) as count FROM FACT_BED_OCCUPANCY;

-- Populate Bed Availability Facts
INSERT INTO FACT_BED_AVAILABILITY (
    bed_key, date_key, department_key, availability_date, status,
    is_available, is_maintenance, is_occupied, reserved_until,
    utilization_rate, revenue_potential, last_updated
)
SELECT 
    db.bed_key,
    TO_NUMBER(TO_CHAR(ba.date, 'YYYYMMDD')) as date_key,
    dd.department_key,
    ba.date,
    ba.status,
    CASE WHEN ba.status = 'Available' THEN TRUE ELSE FALSE END as is_available,
    CASE WHEN ba.status IN ('Maintenance', 'Out of Service', 'Cleaning') THEN TRUE ELSE FALSE END as is_maintenance,
    CASE WHEN ba.status = 'Occupied' THEN TRUE ELSE FALSE END as is_occupied,
    ba.reserved_until,
    -- Calculate utilization rate (simplified)
    CASE WHEN ba.status = 'Occupied' THEN 1.0 ELSE 0.0 END as utilization_rate,
    CASE WHEN ba.status = 'Available' THEN db.daily_rate ELSE 0 END as revenue_potential,
    ba.last_updated
FROM RAW_DATA.BED_AVAILABILITY_RAW ba
JOIN DIM_BED db ON ba.bed_id = db.bed_id
JOIN DIM_DEPARTMENT dd ON db.department_id = dd.department_id;

-- Check bed availability results
SELECT 'Bed Availability Facts populated:' as status, COUNT(*) as count FROM FACT_BED_AVAILABILITY;

-- 5. Create Summary Statistics View
CREATE OR REPLACE VIEW VW_DIMENSIONAL_SUMMARY AS
SELECT 
    'Patients' as entity,
    COUNT(*) as count
FROM DIM_PATIENT WHERE is_current = TRUE
UNION ALL
SELECT 'Departments', COUNT(*) FROM DIM_DEPARTMENT
UNION ALL
SELECT 'Physicians', COUNT(*) FROM DIM_PHYSICIAN
UNION ALL
SELECT 'Procedures', COUNT(*) FROM DIM_PROCEDURE
UNION ALL
SELECT 'Beds', COUNT(*) FROM DIM_BED
UNION ALL
SELECT 'Bed Status Types', COUNT(*) FROM DIM_BED_STATUS
UNION ALL
SELECT 'Admissions', COUNT(*) FROM FACT_ADMISSIONS
UNION ALL
SELECT 'Procedure Facts', COUNT(*) FROM FACT_PROCEDURES
UNION ALL
SELECT 'Bed Occupancy Facts', COUNT(*) FROM FACT_BED_OCCUPANCY
UNION ALL
SELECT 'Bed Availability Facts', COUNT(*) FROM FACT_BED_AVAILABILITY;

-- Show results
SELECT * FROM VW_DIMENSIONAL_SUMMARY;

SELECT 'Dimensional model transformation completed successfully!' as status_message;
SELECT 'Star schema ready for analytics and reporting.' as next_step;
