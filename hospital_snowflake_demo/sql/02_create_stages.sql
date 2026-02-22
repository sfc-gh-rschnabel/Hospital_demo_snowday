-- ============================================================================
-- Hospital Snowflake Demo - Create Stages and File Formats
-- ============================================================================
-- This script creates stages for S3 data loading and file formats

USE ROLE DATA_ENGINEER;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA RAW_DATA;
USE WAREHOUSE HOSPITAL_LOAD_WH;

-- 1. Create File Format for CSV Files
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
TYPE = 'CSV'
COMPRESSION = 'AUTO'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
ESCAPE = 'NONE'
ESCAPE_UNENCLOSED_FIELD = '\134'
DATE_FORMAT = 'YYYY-MM-DD'
TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
NULL_IF = ('NULL', 'null', '', 'N/A', 'n/a');

-- 2. Create Internal Stage for Demo Data with Enterprise Features
-- Note: In a real scenario, you would create an external stage pointing to S3
CREATE OR REPLACE STAGE HOSPITAL_DATA_STAGE
FILE_FORMAT = CSV_FORMAT
DIRECTORY = (ENABLE = TRUE)
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
COMMENT = 'Internal stage for hospital demo data files with directory table and encryption';

-- 3. Alternative: External S3 Stage (Template with Enterprise Features)
-- Uncomment and modify for actual S3 bucket
/*
CREATE OR REPLACE STAGE HOSPITAL_S3_STAGE
URL = 's3://your-hospital-bucket/data/'
CREDENTIALS = (AWS_KEY_ID = 'your-access-key' AWS_SECRET_KEY = 'your-secret-key')
-- OR use AWS_ROLE for role-based access (recommended)
-- CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::123456789012:role/SnowflakeRole')
FILE_FORMAT = CSV_FORMAT
DIRECTORY = (ENABLE = TRUE)
ENCRYPTION = (TYPE = 'AWS_SSE_S3')
-- For customer-managed keys:
-- ENCRYPTION = (TYPE = 'AWS_SSE_KMS' KMS_KEY_ID = 'arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012')
COMMENT = 'External S3 stage for hospital data with directory table and encryption';
*/

-- 4. Create Additional File Formats for Different Data Types

-- JSON Format for semi-structured data
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
TYPE = 'JSON'
COMPRESSION = 'AUTO'
ENABLE_OCTAL = FALSE
ALLOW_DUPLICATE = FALSE
STRIP_OUTER_ARRAY = TRUE
STRIP_NULL_VALUES = FALSE
IGNORE_UTF8_ERRORS = FALSE;

-- Parquet Format for columnar data
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
TYPE = 'PARQUET'
COMPRESSION = 'AUTO';

-- 5. Create Pipe for Continuous Data Loading (Advanced Feature)
-- This would be used for real-time data ingestion from S3
-- Note: Commented out because table doesn't exist yet (created in script 3)
/*
CREATE OR REPLACE PIPE HOSPITAL_DATA_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO RAW_DATA.PATIENT_ADMISSIONS_RAW
FROM @HOSPITAL_DATA_STAGE/patient_admissions/
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';
*/

-- 6. Show Created Objects
SHOW FILE FORMATS IN SCHEMA RAW_DATA;
SHOW STAGES IN SCHEMA RAW_DATA;
-- SHOW PIPES IN SCHEMA RAW_DATA; -- No pipes created in this script

-- 7. Grant Permissions on Stages
-- For internal stages, must grant READ before WRITE
GRANT READ ON STAGE HOSPITAL_DATA_STAGE TO ROLE CLINICAL_ADMIN;
GRANT READ ON STAGE HOSPITAL_DATA_STAGE TO ROLE DATA_ENGINEER;
GRANT WRITE ON STAGE HOSPITAL_DATA_STAGE TO ROLE DATA_ENGINEER;

-- 8. Create Network Policy for Security (Optional)
-- This restricts access to specific IP ranges
/*
CREATE OR REPLACE NETWORK POLICY HOSPITAL_NETWORK_POLICY
ALLOWED_IP_LIST = ('192.168.1.0/24', '10.0.0.0/8')
BLOCKED_IP_LIST = ()
COMMENT = 'Network policy for hospital IP addresses only';

-- Apply to account
ALTER ACCOUNT SET NETWORK_POLICY = HOSPITAL_NETWORK_POLICY;
*/

-- 9. Demo: Show Stage Contents and Directory Table Features
LIST @HOSPITAL_DATA_STAGE;

-- Show directory table functionality (will be empty until files are uploaded)
SELECT 'Directory Table Example (will show file metadata after upload):' as info;
-- SELECT * FROM DIRECTORY(@HOSPITAL_DATA_STAGE);

-- Show encryption and directory settings
DESCRIBE STAGE HOSPITAL_DATA_STAGE;

-- Demo queries for after data upload:
-- File size analysis: SELECT relative_path, size, last_modified FROM DIRECTORY(@HOSPITAL_DATA_STAGE);
-- File type analysis: SELECT file_extension, COUNT(*) FROM (SELECT SPLIT_PART(relative_path, '.', -1) as file_extension FROM DIRECTORY(@HOSPITAL_DATA_STAGE)) GROUP BY file_extension;

SELECT 'Enterprise stage created with directory table and server-side encryption!' as status_message;
SELECT 'Directory table will track all file metadata automatically.' as feature_info;
SELECT 'Server-side encryption protects data at rest.' as security_info;
SELECT 'Ready for secure data loading in the next step.' as next_step;
