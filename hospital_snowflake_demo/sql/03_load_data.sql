-- ============================================================================
-- Hospital Snowflake Demo - Data Loading from S3/Stage
-- ============================================================================
-- This script demonstrates loading data from stages into raw tables

USE ROLE DATA_ENGINEER;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA RAW_DATA;
USE WAREHOUSE HOSPITAL_LOAD_WH;

-- 1. Create Raw Tables for Data Loading
CREATE OR REPLACE TABLE PATIENT_DEMOGRAPHICS_RAW (
    patient_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    address STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    phone STRING,
    email STRING,
    insurance_provider STRING,
    emergency_contact_name STRING,
    emergency_contact_phone STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

CREATE OR REPLACE TABLE PATIENT_ADMISSIONS_RAW (
    admission_id STRING,
    patient_id STRING,
    admission_date DATE,
    admission_time TIME,
    discharge_date DATE,
    discharge_time TIME,
    department_id STRING,
    admission_type STRING,
    chief_complaint STRING,
    diagnosis_primary STRING,
    diagnosis_secondary STRING,
    attending_physician STRING,
    room_number STRING,
    bed_number STRING,
    insurance_authorization STRING,
    total_charges DECIMAL(10,2),
    weather_condition STRING,
    temperature_f INTEGER,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

CREATE OR REPLACE TABLE HOSPITAL_DEPARTMENTS_RAW (
    department_id STRING,
    department_name STRING,
    department_head STRING,
    location_floor STRING,
    phone_extension STRING,
    budget_annual INTEGER,
    staff_count INTEGER,
    bed_capacity INTEGER,
    specialization_type STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

CREATE OR REPLACE TABLE MEDICAL_PROCEDURES_RAW (
    procedure_id STRING,
    admission_id STRING,
    procedure_code STRING,
    procedure_name STRING,
    procedure_date DATE,
    procedure_time TIME,
    performing_physician STRING,
    procedure_duration_minutes INTEGER,
    procedure_cost DECIMAL(10,2),
    anesthesia_type STRING,
    complications STRING,
    procedure_notes STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

CREATE OR REPLACE TABLE BED_INVENTORY_RAW (
    bed_id STRING,
    department_id STRING,
    room_number STRING,
    bed_number STRING,
    bed_type STRING,
    equipment STRING,
    is_active BOOLEAN,
    daily_rate DECIMAL(8,2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

CREATE OR REPLACE TABLE BED_BOOKINGS_RAW (
    booking_id STRING,
    bed_id STRING,
    patient_id STRING,
    check_in_date DATE,
    check_in_time TIME,
    expected_checkout_date DATE,
    expected_checkout_time TIME,
    actual_checkout_date DATE,
    actual_checkout_time TIME,
    booking_status STRING,
    total_nights INTEGER,
    nightly_rate DECIMAL(8,2),
    total_charges DECIMAL(10,2),
    special_requirements STRING,
    created_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

CREATE OR REPLACE TABLE BED_AVAILABILITY_RAW (
    availability_id STRING,
    bed_id STRING,
    date DATE,
    status STRING,
    reserved_until TIME,
    last_updated TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

CREATE OR REPLACE TABLE PHARMACY_INVENTORY_RAW (
    inventory_id STRING,
    medication_code STRING,
    medication_name STRING,
    medication_class STRING,
    therapeutic_category STRING,
    dosage_form STRING,
    strength STRING,
    unit_cost DECIMAL(8,2),
    lot_number STRING,
    expiration_date DATE,
    quantity_on_hand INTEGER,
    reorder_level INTEGER,
    supplier STRING,
    storage_location STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

CREATE OR REPLACE TABLE MEDICATION_ORDERS_RAW (
    order_id STRING,
    admission_id STRING,
    patient_id STRING,
    medication_code STRING,
    medication_name STRING,
    prescribing_physician STRING,
    order_date DATE,
    order_time TIME,
    quantity_ordered INTEGER,
    frequency STRING,
    duration_days INTEGER,
    route STRING,
    priority STRING,
    order_status STRING,
    allergies_checked BOOLEAN,
    interactions_checked BOOLEAN,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

CREATE OR REPLACE TABLE MEDICATION_DISPENSING_RAW (
    dispensing_id STRING,
    order_id STRING,
    patient_id STRING,
    medication_code STRING,
    inventory_id STRING,
    lot_number STRING,
    dispense_date DATE,
    dispense_time TIME,
    quantity_dispensed INTEGER,
    dispensing_pharmacist STRING,
    administration_time TIME,
    administered_by STRING,
    patient_response STRING,
    side_effects STRING,
    cost_per_unit DECIMAL(8,2),
    total_cost DECIMAL(10,2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

CREATE OR REPLACE TABLE ALLIED_HEALTH_SERVICES_RAW (
    service_id STRING,
    admission_id STRING,
    patient_id STRING,
    service_code STRING,
    service_name STRING,
    service_type STRING,
    service_date DATE,
    service_time TIME,
    duration_minutes INTEGER,
    provider_name STRING,
    provider_credentials STRING,
    service_location STRING,
    service_cost DECIMAL(8,2),
    patient_participation STRING,
    goals_met BOOLEAN,
    follow_up_needed BOOLEAN,
    notes STRING,
    insurance_covered BOOLEAN,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);

-- 2. Upload Data to Stage (In real scenario, this would be automated from S3)
-- For demo purposes, upload all files to the root of the stage:
-- Large dataset files generated by generate_large_datasets.py:
-- PUT file://path/to/patient_demographics_large.csv @HOSPITAL_DATA_STAGE;
-- PUT file://path/to/patient_admissions_large.csv @HOSPITAL_DATA_STAGE;
-- PUT file://path/to/hospital_departments_complete.csv @HOSPITAL_DATA_STAGE;
-- PUT file://path/to/medical_procedures_large.csv @HOSPITAL_DATA_STAGE;
-- PUT file://path/to/bed_inventory.csv @HOSPITAL_DATA_STAGE;
-- PUT file://path/to/bed_bookings.csv @HOSPITAL_DATA_STAGE;
-- PUT file://path/to/bed_availability.csv @HOSPITAL_DATA_STAGE;
-- PUT file://path/to/pharmacy_inventory.csv @HOSPITAL_DATA_STAGE;
-- PUT file://path/to/medication_orders.csv @HOSPITAL_DATA_STAGE;
-- PUT file://path/to/medication_dispensing.csv @HOSPITAL_DATA_STAGE;
-- PUT file://path/to/allied_health_services.csv @HOSPITAL_DATA_STAGE;

-- 3. Load Data Using COPY INTO Commands
-- Note: For the demo, you'll need to first upload the CSV files to the stage

-- Load Patient Demographics
COPY INTO PATIENT_DEMOGRAPHICS_RAW (
    patient_id, first_name, last_name, date_of_birth, gender,
    address, city, state, zip_code, phone, email,
    insurance_provider, emergency_contact_name, emergency_contact_phone,
    source_file
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
        METADATA$FILENAME
    FROM @HOSPITAL_DATA_STAGE
)
PATTERN = '.*patient_demographics.*\.csv'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load Patient Admissions
COPY INTO PATIENT_ADMISSIONS_RAW (
    admission_id, patient_id, admission_date, admission_time,
    discharge_date, discharge_time, department_id, admission_type,
    chief_complaint, diagnosis_primary, diagnosis_secondary,
    attending_physician, room_number, bed_number,
    insurance_authorization, total_charges, weather_condition,
    temperature_f, source_file
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
        METADATA$FILENAME
    FROM @HOSPITAL_DATA_STAGE
)
PATTERN = '.*patient_admissions.*\.csv'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load Hospital Departments
COPY INTO HOSPITAL_DEPARTMENTS_RAW (
    department_id, department_name, department_head, location_floor,
    phone_extension, budget_annual, staff_count, bed_capacity,
    specialization_type, source_file
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        METADATA$FILENAME
    FROM @HOSPITAL_DATA_STAGE
)
PATTERN = '.*hospital_departments.*\.csv'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load Medical Procedures
COPY INTO MEDICAL_PROCEDURES_RAW (
    procedure_id, admission_id, procedure_code, procedure_name,
    procedure_date, procedure_time, performing_physician,
    procedure_duration_minutes, procedure_cost, anesthesia_type,
    complications, procedure_notes, source_file
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
        METADATA$FILENAME
    FROM @HOSPITAL_DATA_STAGE
)
PATTERN = '.*medical_procedures.*\.csv'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load Bed Inventory
COPY INTO BED_INVENTORY_RAW (
    bed_id, department_id, room_number, bed_number,
    bed_type, equipment, is_active, daily_rate, source_file
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8,
        METADATA$FILENAME
    FROM @HOSPITAL_DATA_STAGE
)
PATTERN = '.*bed_inventory.*\.csv'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load Bed Bookings
COPY INTO BED_BOOKINGS_RAW (
    booking_id, bed_id, patient_id, check_in_date, check_in_time,
    expected_checkout_date, expected_checkout_time, actual_checkout_date,
    actual_checkout_time, booking_status, total_nights, nightly_rate,
    total_charges, special_requirements, created_timestamp, source_file
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
        METADATA$FILENAME
    FROM @HOSPITAL_DATA_STAGE
)
PATTERN = '.*bed_bookings.*\.csv'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load Bed Availability
COPY INTO BED_AVAILABILITY_RAW (
    availability_id, bed_id, date, status, reserved_until, last_updated, source_file
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6,
        METADATA$FILENAME
    FROM @HOSPITAL_DATA_STAGE
)
PATTERN = '.*bed_availability.*\.csv'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load Pharmacy Inventory
COPY INTO PHARMACY_INVENTORY_RAW (
    inventory_id, medication_code, medication_name, medication_class,
    therapeutic_category, dosage_form, strength, unit_cost,
    lot_number, expiration_date, quantity_on_hand, reorder_level,
    supplier, storage_location, source_file
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
        METADATA$FILENAME
    FROM @HOSPITAL_DATA_STAGE
)
PATTERN = '.*pharmacy_inventory.*\.csv'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load Medication Orders
COPY INTO MEDICATION_ORDERS_RAW (
    order_id, admission_id, patient_id, medication_code, medication_name,
    prescribing_physician, order_date, order_time, quantity_ordered,
    frequency, duration_days, route, priority, order_status,
    allergies_checked, interactions_checked, source_file
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
        METADATA$FILENAME
    FROM @HOSPITAL_DATA_STAGE
)
PATTERN = '.*medication_orders.*\.csv'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load Medication Dispensing
COPY INTO MEDICATION_DISPENSING_RAW (
    dispensing_id, order_id, patient_id, medication_code,
    inventory_id, lot_number, dispense_date, dispense_time,
    quantity_dispensed, dispensing_pharmacist, administration_time,
    administered_by, patient_response, side_effects, cost_per_unit, total_cost, source_file
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
        METADATA$FILENAME
    FROM @HOSPITAL_DATA_STAGE
)
PATTERN = '.*medication_dispensing.*\.csv'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load Allied Health Services
COPY INTO ALLIED_HEALTH_SERVICES_RAW (
    service_id, admission_id, patient_id, service_code, service_name,
    service_type, service_date, service_time, duration_minutes,
    provider_name, provider_credentials, service_location, service_cost,
    patient_participation, goals_met, follow_up_needed, notes, insurance_covered, source_file
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
        METADATA$FILENAME
    FROM @HOSPITAL_DATA_STAGE
)
PATTERN = '.*allied_health_services.*\.csv'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- 4. Validate Data Loading
SELECT 'Patient Demographics' as table_name, COUNT(*) as record_count FROM PATIENT_DEMOGRAPHICS_RAW
UNION ALL
SELECT 'Patient Admissions' as table_name, COUNT(*) as record_count FROM PATIENT_ADMISSIONS_RAW
UNION ALL
SELECT 'Hospital Departments' as table_name, COUNT(*) as record_count FROM HOSPITAL_DEPARTMENTS_RAW
UNION ALL
SELECT 'Medical Procedures' as table_name, COUNT(*) as record_count FROM MEDICAL_PROCEDURES_RAW
UNION ALL
SELECT 'Bed Inventory' as table_name, COUNT(*) as record_count FROM BED_INVENTORY_RAW
UNION ALL
SELECT 'Bed Bookings' as table_name, COUNT(*) as record_count FROM BED_BOOKINGS_RAW
UNION ALL
SELECT 'Bed Availability' as table_name, COUNT(*) as record_count FROM BED_AVAILABILITY_RAW
UNION ALL
SELECT 'Pharmacy Inventory' as table_name, COUNT(*) as record_count FROM PHARMACY_INVENTORY_RAW
UNION ALL
SELECT 'Medication Orders' as table_name, COUNT(*) as record_count FROM MEDICATION_ORDERS_RAW
UNION ALL
SELECT 'Medication Dispensing' as table_name, COUNT(*) as record_count FROM MEDICATION_DISPENSING_RAW
UNION ALL
SELECT 'Allied Health Services' as table_name, COUNT(*) as record_count FROM ALLIED_HEALTH_SERVICES_RAW;

-- 5. Data Quality Checks
-- Check for duplicate patient IDs
SELECT 
    'Duplicate Patient IDs' as check_name,
    COUNT(*) - COUNT(DISTINCT patient_id) as duplicate_count
FROM PATIENT_DEMOGRAPHICS_RAW;

-- Check for missing critical fields
SELECT 
    'Missing Patient Names' as check_name,
    COUNT(*) as missing_count
FROM PATIENT_DEMOGRAPHICS_RAW
WHERE first_name IS NULL OR last_name IS NULL;

-- Check admission date ranges
SELECT 
    'Admission Date Range' as check_name,
    MIN(admission_date) as earliest_admission,
    MAX(admission_date) as latest_admission,
    COUNT(*) as total_admissions
FROM PATIENT_ADMISSIONS_RAW;

-- 6. Show Load History and Statistics
SELECT 
    table_name,
    file_name,
    row_count,
    error_count,
    first_error_message,
    last_load_time
FROM INFORMATION_SCHEMA.LOAD_HISTORY
WHERE schema_name = 'RAW_DATA'
ORDER BY last_load_time DESC;

-- 7. Create Views for Data Exploration
CREATE OR REPLACE VIEW VW_RECENT_ADMISSIONS AS
SELECT 
    a.admission_id,
    p.first_name || ' ' || p.last_name as patient_name,
    a.admission_date,
    d.department_name,
    a.chief_complaint,
    a.diagnosis_primary,
    a.total_charges
FROM PATIENT_ADMISSIONS_RAW a
JOIN PATIENT_DEMOGRAPHICS_RAW p ON a.patient_id = p.patient_id
JOIN HOSPITAL_DEPARTMENTS_RAW d ON a.department_id = d.department_id
WHERE a.admission_date >= CURRENT_DATE - 30;

-- Grant permissions on views
GRANT SELECT ON VW_RECENT_ADMISSIONS TO ROLE PHYSICIAN;
GRANT SELECT ON VW_RECENT_ADMISSIONS TO ROLE NURSE;

-- 8. Directory Table Analysis (Enterprise Feature)
-- Show file metadata from the directory table
SELECT 'Directory Table Analysis - File Metadata:' as analysis_type;
SELECT 
    relative_path,
    size,
    ROUND(size / 1024 / 1024, 2) as size_mb,
    last_modified,
    file_url
FROM DIRECTORY(@HOSPITAL_DATA_STAGE)
ORDER BY size DESC;

-- File type analysis
SELECT 'File Type Distribution:' as analysis_type;
SELECT 
    SPLIT_PART(relative_path, '.', -1) as file_extension,
    COUNT(*) as file_count,
    SUM(size) as total_size_bytes,
    ROUND(SUM(size) / 1024 / 1024, 2) as total_size_mb
FROM DIRECTORY(@HOSPITAL_DATA_STAGE)
GROUP BY file_extension
ORDER BY total_size_bytes DESC;

-- 9. Demonstrate Time Travel (Snowflake Feature)
-- Show data as it was 1 hour ago (if any changes were made)
-- SELECT COUNT(*) FROM PATIENT_ADMISSIONS_RAW AT(OFFSET => -3600);

-- 10. Data Lineage and Audit Information
SELECT 'Data Lineage Information:' as info_type;
SELECT 
    table_name,
    source_file,
    MIN(load_timestamp) as first_load,
    MAX(load_timestamp) as last_load,
    COUNT(*) as records_loaded
FROM (
    SELECT 'PATIENT_DEMOGRAPHICS_RAW' as table_name, source_file, load_timestamp FROM PATIENT_DEMOGRAPHICS_RAW
    UNION ALL
    SELECT 'PATIENT_ADMISSIONS_RAW', source_file, load_timestamp FROM PATIENT_ADMISSIONS_RAW
    UNION ALL
    SELECT 'HOSPITAL_DEPARTMENTS_RAW', source_file, load_timestamp FROM HOSPITAL_DEPARTMENTS_RAW
    UNION ALL
    SELECT 'MEDICAL_PROCEDURES_RAW', source_file, load_timestamp FROM MEDICAL_PROCEDURES_RAW
    UNION ALL
    SELECT 'BED_INVENTORY_RAW', source_file, load_timestamp FROM BED_INVENTORY_RAW
    UNION ALL
    SELECT 'BED_BOOKINGS_RAW', source_file, load_timestamp FROM BED_BOOKINGS_RAW
    UNION ALL
    SELECT 'BED_AVAILABILITY_RAW', source_file, load_timestamp FROM BED_AVAILABILITY_RAW
)
GROUP BY table_name, source_file
ORDER BY table_name, first_load;

SELECT 'Data loading completed successfully!' as status_message;
SELECT 'Raw tables populated and ready for transformation.' as next_step;
SELECT 'Directory table enabled for file metadata tracking.' as enterprise_feature;
SELECT 'Server-side encryption protects all staged data.' as security_feature;
