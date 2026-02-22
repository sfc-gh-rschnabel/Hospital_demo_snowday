# Hospital Analytics on Snowflake: A Healthcare Data Platform Quickstart

## Overview

**Duration**: 60-90 minutes  
**Level**: Intermediate

Welcome to this hands-on quickstart guide! You'll build a complete hospital analytics platform on Snowflake, demonstrating real-world healthcare data management capabilities including data loading, dimensional modeling, role-based access control (RBAC), compute scaling, and interactive dashboards.

### What You'll Learn

- How to create databases, schemas, warehouses, and roles in Snowflake
- How to create stages and load structured CSV data using the COPY command
- How to transform raw data into a dimensional (star schema) model
- How to implement RBAC and data governance for HIPAA compliance
- How to configure multi-cluster warehouses for different workloads
- How to build bed management analytics views
- How to deploy a Streamlit analytics dashboard in Snowflake

### Prerequisites

- A [Snowflake account](https://signup.snowflake.com/) (Enterprise edition recommended for multi-cluster warehouses)
- Basic knowledge of SQL and data warehousing concepts
- Familiarity with healthcare data terminology (helpful but not required)

### What You'll Build

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Hospital Analytics Platform                       │
├─────────────────────────────────────────────────────────────────────┤
│  Data Sources          │  Dimensional Model      │  Analytics        │
│  ─────────────         │  ────────────────       │  ─────────        │
│  • Patient Demographics │  • DIM_PATIENT (SCD2)   │  • Dashboards    │
│  • Admissions          │  • DIM_DEPARTMENT       │  • KPI Views     │
│  • Procedures          │  • DIM_PHYSICIAN        │  • Bed Analytics │
│  • Bed Management      │  • DIM_DATE/TIME        │  • RBAC Views    │
│  • Medications         │  • FACT_ADMISSIONS      │  • Alerts        │
│  • Allied Health       │  • FACT_PROCEDURES      │                  │
│                        │  • FACT_BED_OCCUPANCY   │                  │
└─────────────────────────────────────────────────────────────────────┘
```

### Data You'll Use

This quickstart uses synthetic hospital data with **325K+ records** including:
- 10,000 patient demographics records
- 6,985 patient admissions
- 8,299 medical procedures
- 60,242 bed bookings
- 87,840 bed availability records
- 18,870 medication orders
- 126,803 medication dispensing records
- 6,404 allied health service records

---

## Snowflake Features Showcased

This quickstart demonstrates a comprehensive set of Snowflake capabilities across compute, storage, security, and analytics:

### 🖥️ Compute & Performance

| Feature | Description | Script |
|---------|-------------|--------|
| **Multi-Cluster Warehouses** | Auto-scale compute clusters (1-8 clusters) for concurrent workloads | `01_setup_environment.sql`, `06_compute_scaling.sql` |
| **Warehouse Scaling Policies** | STANDARD (performance) vs ECONOMY (cost optimization) policies | `01_setup_environment.sql` |
| **Dynamic Warehouse Resizing** | Change warehouse size on-the-fly with `ALTER WAREHOUSE` | `06_compute_scaling.sql` |
| **Auto-Suspend/Auto-Resume** | Automatic pause and restart of warehouses to save costs | `01_setup_environment.sql` |
| **Resource Monitors** | Credit quotas with notification and suspension triggers | `01_setup_environment.sql` |
| **Query Performance Monitoring** | `INFORMATION_SCHEMA.QUERY_HISTORY()` for performance analysis | `06_compute_scaling.sql` |

### 📦 Data Loading & Storage

| Feature | Description | Script |
|---------|-------------|--------|
| **Internal Stages** | Secure file storage with encryption | `02_create_stages.sql` |
| **Directory Tables** | Automatic file metadata tracking (`DIRECTORY = TRUE`) | `02_create_stages.sql` |
| **Server-Side Encryption** | `SNOWFLAKE_SSE` for data at rest protection | `02_create_stages.sql` |
| **Multiple File Formats** | CSV, JSON, and Parquet format definitions | `02_create_stages.sql` |
| **COPY INTO with Patterns** | Bulk loading with regex file patterns | `03_load_data.sql` |
| **METADATA$FILENAME** | Data lineage tracking via source file capture | `03_load_data.sql` |
| **ON_ERROR Handling** | Graceful error handling during data loads | `03_load_data.sql` |
| **Snowpipe (Template)** | Continuous data ingestion with `AUTO_INGEST` | `02_create_stages.sql` |
| **Time Travel** | Query historical data states | `03_load_data.sql` |
| **Load History Tracking** | `INFORMATION_SCHEMA.LOAD_HISTORY` for audit | `03_load_data.sql` |

### 🔐 Security & Governance

| Feature | Description | Script |
|---------|-------------|--------|
| **Role-Based Access Control (RBAC)** | Hierarchical roles (CLINICAL_ADMIN → PHYSICIAN → NURSE) | `01_setup_environment.sql` |
| **Object Tagging** | Data classification tags (PII_LEVEL, PHI_INDICATOR) | `05_rbac_governance.sql` |
| **Column-Level Tags** | Fine-grained tagging on sensitive columns | `05_rbac_governance.sql` |
| **Dynamic Data Masking** | Role-based masking policies for PII protection | `05_rbac_governance.sql` |
| **Row Access Policies** | Department-based row-level security | `05_rbac_governance.sql` |
| **Secure Views** | Protected views that hide underlying logic | `05_rbac_governance.sql` |
| **Audit Logging** | Custom audit trail with stored procedures | `05_rbac_governance.sql` |

### 🏗️ Data Modeling & Transformation

| Feature | Description | Script |
|---------|-------------|--------|
| **Star Schema Design** | Dimensional modeling with facts and dimensions | `04_transform_dimensional.sql` |
| **SCD Type 2** | Slowly Changing Dimensions for patient history | `04_transform_dimensional.sql` |
| **AUTOINCREMENT** | Surrogate key generation | `04_transform_dimensional.sql` |
| **GENERATOR Function** | Programmatic date dimension population | `04_transform_dimensional.sql` |
| **Foreign Key Constraints** | Referential integrity documentation | `04_transform_dimensional.sql` |
| **Common Table Expressions (CTEs)** | Complex query organization | `04_transform_dimensional.sql`, `08_bed_analytics.sql` |

### ⚡ Dynamic Tables (Declarative Pipelines)

| Feature | Description | Script |
|---------|-------------|--------|
| **Dynamic Tables** | Declarative data pipelines with automatic refresh | `07_dynamic_tables.sql` |
| **Target Lag Configuration** | Time-based freshness (1 min, 5 min) | `07_dynamic_tables.sql` |
| **DOWNSTREAM Refresh** | Dependency-driven refresh chains | `07_dynamic_tables.sql` |
| **Bronze/Silver/Gold Layers** | Medallion architecture implementation | `07_dynamic_tables.sql` |
| **Pipeline Dependency Graph** | Automatic dependency management | `07_dynamic_tables.sql` |
| **Refresh History Monitoring** | `DYNAMIC_TABLE_REFRESH_HISTORY()` for observability | `07_dynamic_tables.sql` |
| **Incremental Refresh** | Automatic incremental processing when possible | `07_dynamic_tables.sql` |
| **Real-Time Alerting** | Near-real-time alert generation via dynamic tables | `07_dynamic_tables.sql` |

### 📊 Analytics & Reporting

| Feature | Description | Script |
|---------|-------------|--------|
| **Analytical Views** | Pre-built KPI dashboards and metrics | `08_bed_analytics.sql` |
| **Stored Procedures** | SQL-based business logic with cursors | `05_rbac_governance.sql`, `08_bed_analytics.sql` |
| **Alert Generation** | Automated monitoring with threshold-based alerts | `08_bed_analytics.sql` |
| **Streamlit in Snowflake** | Native interactive dashboard deployment | `09_deploy_streamlit_app.sql` |
| **Semantic Views** | Domain-specific terminology (Australian healthcare) | `10_australian_semantic_views.sql` |

### 📄 Semi-Structured Data (JSON Parsing)

| Feature | Description | Script |
|---------|-------------|--------|
| **VARIANT Data Type** | Store semi-structured JSON data natively | `13_json_data_parsing.sql` |
| **Dot Notation** | Navigate nested JSON with `raw_json:field.subfield` syntax | `13_json_data_parsing.sql` |
| **Type Casting (::)** | Cast JSON values to SQL types (::VARCHAR, ::INTEGER) | `13_json_data_parsing.sql` |
| **LATERAL FLATTEN** | Expand JSON arrays into rows | `13_json_data_parsing.sql` |
| **ARRAY_SIZE Function** | Count elements in JSON arrays | `13_json_data_parsing.sql` |
| **COALESCE for Optional Fields** | Handle missing nested JSON fields gracefully | `13_json_data_parsing.sql` |
| **JSON Stage Loading** | Load JSON files with STRIP_OUTER_ARRAY | `13_json_data_parsing.sql` |

### 📊 Snowflake Marketplace & Data Sharing

| Feature | Description | Script |
|---------|-------------|--------|
| **Marketplace Data Access** | Zero-copy access to third-party data | `15_marketplace_data_sharing.sql` |
| **Cross-Database Queries** | Join Marketplace data with internal tables | `15_marketplace_data_sharing.sql` |
| **Data Blending** | Combine external weather data with admissions | `15_marketplace_data_sharing.sql` |
| **Zero-ETL Integration** | Instant data access without pipelines | `15_marketplace_data_sharing.sql` |

### 🔧 Additional Capabilities

| Feature | Description | Script |
|---------|-------------|--------|
| **External Stage Template** | S3 integration with IAM credentials | `02_create_stages.sql` |
| **Network Policies (Template)** | IP-based access restrictions | `02_create_stages.sql` |
| **Data Quality Rules** | Validation rule definitions and tracking | `05_rbac_governance.sql` |
| **Weather Data Integration** | External data correlation with admissions | `04_transform_dimensional.sql` |

### Feature Requirements by Edition

| Feature | Standard | Enterprise | Business Critical |
|---------|:--------:|:----------:|:-----------------:|
| Dynamic Tables | ✅ | ✅ | ✅ |
| Multi-Cluster Warehouses | ❌ | ✅ | ✅ |
| Dynamic Data Masking | ❌ | ✅ | ✅ |
| Row Access Policies | ❌ | ✅ | ✅ |
| Object Tagging | ❌ | ✅ | ✅ |
| Directory Tables | ✅ | ✅ | ✅ |
| Time Travel (up to 90 days) | ❌ | ✅ | ✅ |
| Resource Monitors | ✅ | ✅ | ✅ |
| Secure Views | ✅ | ✅ | ✅ |

> **Note**: This quickstart works best with **Enterprise Edition** to demonstrate all governance features. Standard Edition users can skip the masking policies, row access policies, and tagging sections.

---

## Step 1: Prepare Your Environment

**Duration**: 5 minutes

### 1.1 Log into Snowflake

Open your browser and navigate to your Snowflake account URL. Log in with your credentials.

### 1.2 Create a New Worksheet

1. Navigate to **Projects > Worksheets**
2. Click the **+** button to create a new worksheet
3. Name it `Hospital_Analytics_Quickstart`

### 1.3 Set Your Context

In your worksheet, verify you're using the `ACCOUNTADMIN` role:

```sql
USE ROLE ACCOUNTADMIN;
```

> **Note**: We use `ACCOUNTADMIN` for initial setup. In production, you would use more restrictive roles.

---

## Step 2: Set Up the Database Environment

**Duration**: 10 minutes

### 2.1 Create Database and Schemas

Create the database with three schemas for organizing our data:

```sql
-- Create the main database
CREATE OR REPLACE DATABASE HOSPITAL_DEMO
COMMENT = 'Database for hospital clinical team Snowflake demonstration';

USE DATABASE HOSPITAL_DEMO;

-- Create schemas for different data layers
CREATE OR REPLACE SCHEMA RAW_DATA
COMMENT = 'Schema for raw data loaded from source files';

CREATE OR REPLACE SCHEMA TRANSFORMED
COMMENT = 'Schema for transformed dimensional model';

CREATE OR REPLACE SCHEMA ANALYTICS
COMMENT = 'Schema for analytics and reporting views';
```

### 2.2 Create Roles for RBAC

Create a role hierarchy that mimics a real hospital organization:

```sql
-- Clinical Roles
CREATE OR REPLACE ROLE CLINICAL_ADMIN
COMMENT = 'Full access for clinical administrators';

CREATE OR REPLACE ROLE PHYSICIAN
COMMENT = 'Access for attending physicians';

CREATE OR REPLACE ROLE NURSE
COMMENT = 'Limited access for nursing staff';

CREATE OR REPLACE ROLE ANALYST
COMMENT = 'Analytics access for hospital analysts';

CREATE OR REPLACE ROLE DATA_ENGINEER
COMMENT = 'Data engineering and ETL access';

-- Create Role Hierarchy
GRANT ROLE CLINICAL_ADMIN TO ROLE SYSADMIN;
GRANT ROLE DATA_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE ANALYST TO ROLE CLINICAL_ADMIN;
GRANT ROLE PHYSICIAN TO ROLE CLINICAL_ADMIN;
GRANT ROLE NURSE TO ROLE PHYSICIAN;
```

### 2.3 Grant Database Permissions

```sql
-- Database Permissions
GRANT USAGE ON DATABASE HOSPITAL_DEMO TO ROLE CLINICAL_ADMIN;
GRANT USAGE ON DATABASE HOSPITAL_DEMO TO ROLE DATA_ENGINEER;
GRANT USAGE ON DATABASE HOSPITAL_DEMO TO ROLE ANALYST;
GRANT USAGE ON DATABASE HOSPITAL_DEMO TO ROLE PHYSICIAN;
GRANT USAGE ON DATABASE HOSPITAL_DEMO TO ROLE NURSE;

-- Schema Permissions
GRANT ALL ON SCHEMA RAW_DATA TO ROLE DATA_ENGINEER;
GRANT ALL ON SCHEMA TRANSFORMED TO ROLE DATA_ENGINEER;
GRANT ALL ON SCHEMA ANALYTICS TO ROLE DATA_ENGINEER;

GRANT USAGE ON SCHEMA RAW_DATA TO ROLE CLINICAL_ADMIN;
GRANT USAGE ON SCHEMA TRANSFORMED TO ROLE CLINICAL_ADMIN;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE CLINICAL_ADMIN;

GRANT USAGE ON SCHEMA TRANSFORMED TO ROLE ANALYST;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE ANALYST;

GRANT USAGE ON SCHEMA ANALYTICS TO ROLE PHYSICIAN;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE NURSE;
```

### 2.4 Create Virtual Warehouses

Create warehouses for different workloads:

```sql
-- Warehouse for data loading operations
CREATE OR REPLACE WAREHOUSE HOSPITAL_LOAD_WH
WITH 
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'STANDARD'
COMMENT = 'Warehouse for data loading operations';

-- Warehouse for analytics and reporting
CREATE OR REPLACE WAREHOUSE HOSPITAL_ANALYTICS_WH
WITH 
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 5
    SCALING_POLICY = 'STANDARD'
COMMENT = 'Warehouse for analytics and reporting';

-- Warehouse for ad-hoc queries
CREATE OR REPLACE WAREHOUSE HOSPITAL_ADHOC_WH
WITH 
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'ECONOMY'
COMMENT = 'Warehouse for ad-hoc queries and exploration';

-- Grant warehouse permissions
GRANT USAGE ON WAREHOUSE HOSPITAL_LOAD_WH TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE HOSPITAL_ANALYTICS_WH TO ROLE CLINICAL_ADMIN;
GRANT USAGE ON WAREHOUSE HOSPITAL_ANALYTICS_WH TO ROLE ANALYST;
GRANT USAGE ON WAREHOUSE HOSPITAL_ADHOC_WH TO ROLE PHYSICIAN;
GRANT USAGE ON WAREHOUSE HOSPITAL_ADHOC_WH TO ROLE NURSE;
```

### 2.5 Create Resource Monitor for Cost Control

```sql
CREATE OR REPLACE RESOURCE MONITOR HOSPITAL_MONTHLY_LIMIT
WITH 
    CREDIT_QUOTA = 1000
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS 
        ON 75 PERCENT DO NOTIFY
        ON 90 PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- Apply monitor to warehouses
ALTER WAREHOUSE HOSPITAL_LOAD_WH SET RESOURCE_MONITOR = HOSPITAL_MONTHLY_LIMIT;
ALTER WAREHOUSE HOSPITAL_ANALYTICS_WH SET RESOURCE_MONITOR = HOSPITAL_MONTHLY_LIMIT;
ALTER WAREHOUSE HOSPITAL_ADHOC_WH SET RESOURCE_MONITOR = HOSPITAL_MONTHLY_LIMIT;
```

### 2.6 Verify Setup

```sql
SHOW DATABASES LIKE 'HOSPITAL_DEMO';
SHOW SCHEMAS IN DATABASE HOSPITAL_DEMO;
SHOW ROLES LIKE '%CLINICAL%' OR LIKE '%PHYSICIAN%' OR LIKE '%NURSE%' OR LIKE '%ANALYST%';
SHOW WAREHOUSES LIKE 'HOSPITAL_%';
```

---

## Step 3: Create Stages and File Formats

**Duration**: 5 minutes

### 3.1 Switch to Data Engineer Role

```sql
USE ROLE DATA_ENGINEER;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA RAW_DATA;
USE WAREHOUSE HOSPITAL_LOAD_WH;
```

### 3.2 Create File Format for CSV Files

```sql
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
```

### 3.3 Create Internal Stage

```sql
CREATE OR REPLACE STAGE HOSPITAL_DATA_STAGE
FILE_FORMAT = CSV_FORMAT
DIRECTORY = (ENABLE = TRUE)
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
COMMENT = 'Internal stage for hospital demo data files with directory table and encryption';
```

> **Enterprise Feature**: The `DIRECTORY` option creates a directory table that automatically tracks file metadata.

### 3.4 Grant Stage Permissions

```sql
USE ROLE ACCOUNTADMIN;
GRANT READ ON STAGE RAW_DATA.HOSPITAL_DATA_STAGE TO ROLE CLINICAL_ADMIN;
GRANT READ ON STAGE RAW_DATA.HOSPITAL_DATA_STAGE TO ROLE DATA_ENGINEER;
GRANT WRITE ON STAGE RAW_DATA.HOSPITAL_DATA_STAGE TO ROLE DATA_ENGINEER;
```

---

## Step 4: Create Raw Tables and Load Data

**Duration**: 15 minutes

### 4.1 Create Raw Tables

Switch context and create the raw tables:

```sql
USE ROLE DATA_ENGINEER;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA RAW_DATA;
USE WAREHOUSE HOSPITAL_LOAD_WH;

-- Patient Demographics
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

-- Patient Admissions
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

-- Hospital Departments
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

-- Medical Procedures
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

-- Bed Inventory
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

-- Bed Bookings
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

-- Bed Availability
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

-- Medication Orders
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

-- Allied Health Services
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
```

### 4.2 Upload Data to Stage

**Option A: Using Snowsight UI**

1. Navigate to **Data > Databases > HOSPITAL_DEMO > RAW_DATA > Stages > HOSPITAL_DATA_STAGE**
2. Click on the stage and select **+ Files**
3. Upload the CSV files from the `data/` directory

**Option B: Using SnowSQL CLI**

```bash
# From the project directory
snowsql -a <your_account> -u <your_user>

USE ROLE DATA_ENGINEER;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA RAW_DATA;

PUT file://./hospital_snowflake_demo/data/patient_demographics_large.csv @HOSPITAL_DATA_STAGE AUTO_COMPRESS=TRUE;
PUT file://./hospital_snowflake_demo/data/patient_admissions_large.csv @HOSPITAL_DATA_STAGE AUTO_COMPRESS=TRUE;
PUT file://./hospital_snowflake_demo/data/hospital_departments_complete.csv @HOSPITAL_DATA_STAGE AUTO_COMPRESS=TRUE;
PUT file://./hospital_snowflake_demo/data/medical_procedures_large.csv @HOSPITAL_DATA_STAGE AUTO_COMPRESS=TRUE;
PUT file://./hospital_snowflake_demo/data/bed_inventory.csv @HOSPITAL_DATA_STAGE AUTO_COMPRESS=TRUE;
PUT file://./hospital_snowflake_demo/data/bed_bookings.csv @HOSPITAL_DATA_STAGE AUTO_COMPRESS=TRUE;
PUT file://./hospital_snowflake_demo/data/bed_availability.csv @HOSPITAL_DATA_STAGE AUTO_COMPRESS=TRUE;
PUT file://./hospital_snowflake_demo/data/medication_orders.csv @HOSPITAL_DATA_STAGE AUTO_COMPRESS=TRUE;
PUT file://./hospital_snowflake_demo/data/allied_health_services.csv @HOSPITAL_DATA_STAGE AUTO_COMPRESS=TRUE;
```

### 4.3 Load Data Using COPY INTO

```sql
-- Verify files are in stage
LIST @HOSPITAL_DATA_STAGE;

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
PATTERN = '.*patient_demographics.*\.csv.*'
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
PATTERN = '.*patient_admissions.*\.csv.*'
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
PATTERN = '.*hospital_departments.*\.csv.*'
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
PATTERN = '.*medical_procedures.*\.csv.*'
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
PATTERN = '.*bed_inventory.*\.csv.*'
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- Load remaining tables (Bed Bookings, Bed Availability, Medication Orders, Allied Health)
-- Similar COPY INTO statements for other tables...
```

### 4.4 Validate Data Loading

```sql
SELECT 'Patient Demographics' as table_name, COUNT(*) as record_count FROM PATIENT_DEMOGRAPHICS_RAW
UNION ALL SELECT 'Patient Admissions', COUNT(*) FROM PATIENT_ADMISSIONS_RAW
UNION ALL SELECT 'Hospital Departments', COUNT(*) FROM HOSPITAL_DEPARTMENTS_RAW
UNION ALL SELECT 'Medical Procedures', COUNT(*) FROM MEDICAL_PROCEDURES_RAW
UNION ALL SELECT 'Bed Inventory', COUNT(*) FROM BED_INVENTORY_RAW;
```

---

## Step 5: Transform Data into Dimensional Model

**Duration**: 15 minutes

### 5.1 Switch to Transformed Schema

```sql
USE ROLE DATA_ENGINEER;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA TRANSFORMED;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;
```

### 5.2 Create Dimension Tables

```sql
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

-- Patient Dimension (SCD Type 2)
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
```

### 5.3 Create Fact Tables

```sql
-- Admission Fact Table
CREATE OR REPLACE TABLE FACT_ADMISSIONS (
    admission_key INTEGER AUTOINCREMENT PRIMARY KEY,
    admission_id STRING NOT NULL,
    patient_key INTEGER,
    department_key INTEGER,
    physician_key INTEGER,
    weather_key INTEGER,
    admission_date_key INTEGER,
    discharge_date_key INTEGER,
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
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### 5.4 Populate Dimension Tables

```sql
-- Populate Date Dimension (3 years)
INSERT INTO DIM_DATE
WITH date_range AS (
    SELECT DATEADD(day, seq4(), '2023-01-01') as date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 1095))
)
SELECT 
    TO_NUMBER(TO_CHAR(date_value, 'YYYYMMDD')) as date_key,
    date_value,
    YEAR(date_value), QUARTER(date_value), MONTH(date_value),
    MONTHNAME(date_value), WEEK(date_value), DAYOFYEAR(date_value),
    DAY(date_value), DAYOFWEEK(date_value), DAYNAME(date_value),
    DAYOFWEEK(date_value) IN (1, 7) as is_weekend,
    FALSE as is_holiday,
    CASE 
        WHEN MONTH(date_value) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(date_value) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(date_value) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END as season
FROM date_range;

-- Populate Department Dimension
INSERT INTO DIM_DEPARTMENT (
    department_id, department_name, department_head, location_floor,
    phone_extension, budget_annual, staff_count, bed_capacity, specialization_type
)
SELECT 
    department_id, department_name, department_head, location_floor,
    phone_extension, budget_annual, staff_count, bed_capacity, specialization_type
FROM RAW_DATA.HOSPITAL_DEPARTMENTS_RAW;

-- Populate Patient Dimension
INSERT INTO DIM_PATIENT (
    patient_id, first_name, last_name, full_name, date_of_birth,
    age, age_group, gender, address, city, state, zip_code,
    insurance_provider, effective_date
)
SELECT 
    patient_id, first_name, last_name,
    first_name || ' ' || last_name as full_name,
    date_of_birth,
    DATEDIFF(year, date_of_birth, CURRENT_DATE()) as age,
    CASE 
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 18 THEN 'Pediatric'
        WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 18 AND 64 THEN 'Adult'
        ELSE 'Senior'
    END as age_group,
    gender, address, city, state, zip_code,
    insurance_provider,
    CURRENT_DATE() as effective_date
FROM RAW_DATA.PATIENT_DEMOGRAPHICS_RAW;

-- Populate Physician Dimension
INSERT INTO DIM_PHYSICIAN (physician_name, specialty)
SELECT DISTINCT 
    attending_physician,
    CASE 
        WHEN department_id = 'CARD' THEN 'Cardiology'
        WHEN department_id = 'EMER' THEN 'Emergency Medicine'
        WHEN department_id = 'ORTH' THEN 'Orthopedics'
        ELSE 'General Medicine'
    END as specialty
FROM RAW_DATA.PATIENT_ADMISSIONS_RAW
WHERE attending_physician IS NOT NULL;

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
```

### 5.5 Populate Fact Tables

```sql
-- Populate Admission Facts
INSERT INTO FACT_ADMISSIONS (
    admission_id, patient_key, department_key, physician_key, weather_key,
    admission_date_key, discharge_date_key,
    admission_type, chief_complaint, diagnosis_primary, diagnosis_secondary,
    room_number, bed_number, insurance_authorization, total_charges,
    length_of_stay_days, is_emergency, is_readmission
)
SELECT 
    a.admission_id,
    p.patient_key,
    d.department_key,
    ph.physician_key,
    w.weather_key,
    TO_NUMBER(TO_CHAR(a.admission_date, 'YYYYMMDD')) as admission_date_key,
    TO_NUMBER(TO_CHAR(a.discharge_date, 'YYYYMMDD')) as discharge_date_key,
    a.admission_type, a.chief_complaint, a.diagnosis_primary, a.diagnosis_secondary,
    a.room_number, a.bed_number, a.insurance_authorization, a.total_charges,
    DATEDIFF(day, a.admission_date, a.discharge_date) as length_of_stay_days,
    a.admission_type = 'Emergency' as is_emergency,
    FALSE as is_readmission
FROM RAW_DATA.PATIENT_ADMISSIONS_RAW a
JOIN DIM_PATIENT p ON a.patient_id = p.patient_id AND p.is_current = TRUE
JOIN DIM_DEPARTMENT d ON a.department_id = d.department_id
JOIN DIM_PHYSICIAN ph ON a.attending_physician = ph.physician_name
LEFT JOIN DIM_WEATHER w ON a.weather_condition = w.weather_condition;
```

### 5.6 Verify Transformation

```sql
SELECT 'Patients' as entity, COUNT(*) as count FROM DIM_PATIENT WHERE is_current = TRUE
UNION ALL SELECT 'Departments', COUNT(*) FROM DIM_DEPARTMENT
UNION ALL SELECT 'Physicians', COUNT(*) FROM DIM_PHYSICIAN
UNION ALL SELECT 'Admissions', COUNT(*) FROM FACT_ADMISSIONS;
```

---

## Step 6: Implement RBAC and Data Governance

**Duration**: 10 minutes

### 6.1 Create Data Classification Tags

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA ANALYTICS;

-- Create tags for HIPAA compliance
CREATE OR REPLACE TAG PII_LEVEL
ALLOWED_VALUES 'PUBLIC', 'INTERNAL', 'CONFIDENTIAL', 'RESTRICTED';

CREATE OR REPLACE TAG PHI_INDICATOR
ALLOWED_VALUES 'YES', 'NO';

CREATE OR REPLACE TAG DATA_CLASSIFICATION
ALLOWED_VALUES 'DEMOGRAPHIC', 'CLINICAL', 'FINANCIAL', 'OPERATIONAL';

-- Apply tags to Patient table
ALTER TABLE TRANSFORMED.DIM_PATIENT SET TAG 
    PII_LEVEL = 'RESTRICTED',
    PHI_INDICATOR = 'YES',
    DATA_CLASSIFICATION = 'DEMOGRAPHIC';
```

### 6.2 Create Masking Policies

```sql
-- Policy to mask patient names based on role
CREATE OR REPLACE MASKING POLICY MASK_PATIENT_NAME
AS (val STRING) RETURNS STRING ->
    CASE 
        WHEN CURRENT_ROLE() IN ('CLINICAL_ADMIN', 'PHYSICIAN', 'SYSADMIN', 'ACCOUNTADMIN') THEN val
        WHEN CURRENT_ROLE() = 'NURSE' THEN LEFT(val, 1) || REPEAT('*', LENGTH(val) - 1)
        ELSE '***MASKED***'
    END;

-- Policy to mask financial data
CREATE OR REPLACE MASKING POLICY MASK_FINANCIAL
AS (val NUMBER) RETURNS NUMBER ->
    CASE 
        WHEN CURRENT_ROLE() IN ('CLINICAL_ADMIN', 'ANALYST', 'SYSADMIN', 'ACCOUNTADMIN') THEN val
        ELSE NULL
    END;

-- Apply masking policies
ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN first_name 
SET MASKING POLICY MASK_PATIENT_NAME;

ALTER TABLE TRANSFORMED.DIM_PATIENT MODIFY COLUMN last_name 
SET MASKING POLICY MASK_PATIENT_NAME;

ALTER TABLE TRANSFORMED.FACT_ADMISSIONS MODIFY COLUMN total_charges 
SET MASKING POLICY MASK_FINANCIAL;
```

### 6.3 Create Secure Views for Different Roles

```sql
-- View for Physicians - Full clinical data
CREATE OR REPLACE SECURE VIEW VW_PHYSICIAN_DASHBOARD AS
SELECT 
    fa.admission_id,
    dp.full_name as patient_name,
    dp.age, dp.gender,
    dd.department_name,
    fa.diagnosis_primary,
    fa.diagnosis_secondary,
    fa.length_of_stay_days,
    fa.is_emergency
FROM TRANSFORMED.FACT_ADMISSIONS fa
JOIN TRANSFORMED.DIM_PATIENT dp ON fa.patient_key = dp.patient_key
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
WHERE dp.is_current = TRUE;

-- View for Analysts - Aggregated data without PII
CREATE OR REPLACE SECURE VIEW VW_ANALYST_DASHBOARD AS
SELECT 
    dt.date_value as admission_date,
    dt.month_name, dt.day_name,
    dd.department_name, dd.specialization_type,
    COUNT(*) as admission_count,
    AVG(fa.length_of_stay_days) as avg_length_of_stay,
    SUM(fa.total_charges) as total_revenue,
    COUNT(CASE WHEN fa.is_emergency THEN 1 END) as emergency_admissions,
    dw.weather_condition, dw.temperature_range
FROM TRANSFORMED.FACT_ADMISSIONS fa
JOIN TRANSFORMED.DIM_DATE dt ON fa.admission_date_key = dt.date_key
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
LEFT JOIN TRANSFORMED.DIM_WEATHER dw ON fa.weather_key = dw.weather_key
GROUP BY dt.date_value, dt.month_name, dt.day_name, 
         dd.department_name, dd.specialization_type, 
         dw.weather_condition, dw.temperature_range;

-- Grant permissions
GRANT SELECT ON VW_PHYSICIAN_DASHBOARD TO ROLE PHYSICIAN;
GRANT SELECT ON VW_ANALYST_DASHBOARD TO ROLE ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS TO ROLE CLINICAL_ADMIN;
```

### 6.4 Test Role-Based Access

```sql
-- Test as Physician
USE ROLE PHYSICIAN;
SELECT 'Testing PHYSICIAN access:' as test;
SELECT COUNT(*) as accessible_records FROM ANALYTICS.VW_PHYSICIAN_DASHBOARD;

-- Test as Analyst
USE ROLE ANALYST;
SELECT 'Testing ANALYST access:' as test;
SELECT COUNT(*) as accessible_records FROM ANALYTICS.VW_ANALYST_DASHBOARD;

-- Return to admin role
USE ROLE ACCOUNTADMIN;
```

---

## Step 7: Create Dynamic Tables for Real-Time Pipelines

**Duration**: 10 minutes

Dynamic Tables provide declarative data pipelines that automatically refresh based on target lag settings. No orchestration required!

### 7.1 Understanding Dynamic Table Architecture

```
  RAW DATA (Base Tables)
       │
       ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  BRONZE LAYER (1-minute lag) - Data Cleansing               │
  │  ├── DT_CLEAN_ADMISSIONS                                    │
  │  ├── DT_CLEAN_PROCEDURES                                    │
  │  └── DT_CLEAN_MEDICATIONS                                   │
  └─────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  SILVER LAYER (5-minute lag) - Aggregations                 │
  │  ├── DT_DEPARTMENT_ADMISSION_SUMMARY                        │
  │  ├── DT_PHYSICIAN_PERFORMANCE                               │
  │  └── DT_WEATHER_ADMISSION_CORRELATION                       │
  └─────────────────────────────────────────────────────────────┘
       │
       ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  GOLD LAYER (DOWNSTREAM) - Executive Dashboards             │
  │  ├── DT_HOSPITAL_KPIS                                       │
  │  ├── DT_DEPARTMENT_RANKINGS                                 │
  │  └── DT_PHYSICIAN_LEADERBOARD                               │
  └─────────────────────────────────────────────────────────────┘
```

### 7.2 Create Bronze Layer Dynamic Tables (Near Real-Time Cleansing)

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;

-- Clean Admissions - Refreshes every 1 minute
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
    department_id,
    UPPER(TRIM(admission_type)) as admission_type,
    TRIM(chief_complaint) as chief_complaint,
    TRIM(diagnosis_primary) as diagnosis_primary,
    TRIM(attending_physician) as attending_physician,
    COALESCE(total_charges, 0) as total_charges,
    UPPER(TRIM(weather_condition)) as weather_condition,
    temperature_f,
    DATEDIFF(day, admission_date, COALESCE(discharge_date, CURRENT_DATE())) as length_of_stay_days,
    CASE WHEN admission_type ILIKE '%emergency%' THEN TRUE ELSE FALSE END as is_emergency,
    load_timestamp
FROM RAW_DATA.PATIENT_ADMISSIONS_RAW
WHERE admission_id IS NOT NULL AND patient_id IS NOT NULL;

-- Clean Procedures - Refreshes every 1 minute
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
    INITCAP(TRIM(performing_physician)) as performing_physician,
    COALESCE(procedure_duration_minutes, 0) as procedure_duration_minutes,
    COALESCE(procedure_cost, 0) as procedure_cost,
    COALESCE(TRIM(complications), 'None') as complications,
    CASE WHEN TRIM(complications) IS NULL OR TRIM(complications) = 'None' THEN TRUE ELSE FALSE END as is_successful,
    load_timestamp
FROM RAW_DATA.MEDICAL_PROCEDURES_RAW
WHERE procedure_id IS NOT NULL AND admission_id IS NOT NULL;
```

### 7.3 Create Silver Layer Dynamic Tables (Aggregations)

```sql
-- Department Admission Summary - Refreshes every 5 minutes
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
GROUP BY d.department_id, d.department_name, d.specialization_type, DATE_TRUNC('hour', a.admission_date);

-- Weather Impact Analysis
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
        ELSE 'Hot (>85°F)'
    END as temperature_range,
    DATE_TRUNC('day', a.admission_date) as admission_date,
    COUNT(*) as total_admissions,
    COUNT(CASE WHEN a.is_emergency THEN 1 END) as emergency_admissions,
    ROUND(COUNT(CASE WHEN a.is_emergency THEN 1 END) * 100.0 / COUNT(*), 2) as emergency_pct
FROM TRANSFORMED.DT_CLEAN_ADMISSIONS a
WHERE a.weather_condition IS NOT NULL
GROUP BY a.weather_condition, 
    CASE 
        WHEN a.temperature_f < 32 THEN 'Freezing (<32°F)'
        WHEN a.temperature_f BETWEEN 32 AND 50 THEN 'Cold (32-50°F)'
        WHEN a.temperature_f BETWEEN 51 AND 70 THEN 'Moderate (51-70°F)'
        WHEN a.temperature_f BETWEEN 71 AND 85 THEN 'Warm (71-85°F)'
        ELSE 'Hot (>85°F)'
    END,
    DATE_TRUNC('day', a.admission_date);
```

### 7.4 Create Gold Layer Dynamic Tables (DOWNSTREAM Refresh)

```sql
-- Hospital-Wide KPIs - Refreshes when upstream tables refresh
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
    MAX(last_updated) as data_freshness
FROM ANALYTICS.DT_DEPARTMENT_ADMISSION_SUMMARY
WHERE DATE_TRUNC('day', admission_hour) = CURRENT_DATE();

-- Department Rankings
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DT_DEPARTMENT_RANKINGS
TARGET_LAG = DOWNSTREAM
WAREHOUSE = HOSPITAL_ANALYTICS_WH
AS
SELECT 
    department_name,
    specialization_type,
    SUM(admission_count) as total_admissions,
    SUM(total_charges) as total_revenue,
    RANK() OVER (ORDER BY SUM(total_charges) DESC) as revenue_rank,
    RANK() OVER (ORDER BY SUM(admission_count) DESC) as volume_rank,
    MAX(last_updated) as data_freshness
FROM ANALYTICS.DT_DEPARTMENT_ADMISSION_SUMMARY
GROUP BY department_name, specialization_type;
```

### 7.5 Monitor Dynamic Table Refresh Status

```sql
-- View all dynamic tables
SHOW DYNAMIC TABLES IN DATABASE HOSPITAL_DEMO;

-- Check refresh history
SELECT 
    name,
    state,
    refresh_trigger,
    data_timestamp
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE name LIKE 'DT_%'
ORDER BY data_timestamp DESC
LIMIT 10;

-- Query the auto-refreshing KPIs
SELECT * FROM ANALYTICS.DT_HOSPITAL_KPIS;
SELECT * FROM ANALYTICS.DT_DEPARTMENT_RANKINGS ORDER BY revenue_rank;
```

> **Key Concept**: Dynamic tables with `TARGET_LAG = DOWNSTREAM` only refresh when their downstream dependents need fresh data, creating efficient refresh chains.

---

## Step 8: Build Bed Management Analytics

**Duration**: 10 minutes

### 8.1 Create Bed Utilization Views

```sql
USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA ANALYTICS;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;

-- Bed Utilization Dashboard
CREATE OR REPLACE VIEW VW_BED_UTILIZATION_DASHBOARD AS
SELECT 
    dd.department_name,
    dd.specialization_type,
    COUNT(DISTINCT db.bed_key) as total_beds,
    db.bed_type,
    AVG(db.daily_rate) as avg_daily_rate,
    CURRENT_DATE() as report_date
FROM TRANSFORMED.DIM_BED db
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON db.department_id = dd.department_id
WHERE db.is_active = TRUE
GROUP BY dd.department_name, dd.specialization_type, db.bed_type
ORDER BY dd.department_name, db.bed_type;

-- Bed Revenue Analysis
CREATE OR REPLACE VIEW VW_BED_REVENUE_ANALYSIS AS
SELECT 
    dd.department_name,
    db.bed_type,
    COUNT(DISTINCT db.bed_key) as bed_count,
    AVG(db.daily_rate) as avg_daily_rate,
    SUM(db.daily_rate) * 30 as estimated_monthly_revenue
FROM TRANSFORMED.DIM_BED db
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON db.department_id = dd.department_id
WHERE db.is_active = TRUE
GROUP BY dd.department_name, db.bed_type
ORDER BY estimated_monthly_revenue DESC;

-- Bed Management KPIs
CREATE OR REPLACE VIEW VW_BED_MANAGEMENT_KPIS AS
SELECT 
    'Total Active Beds' as kpi_name,
    COUNT(*) as kpi_value,
    'Count' as unit
FROM TRANSFORMED.DIM_BED WHERE is_active = TRUE
UNION ALL
SELECT 'Total Departments', COUNT(DISTINCT department_id), 'Count' FROM TRANSFORMED.DIM_BED
UNION ALL
SELECT 'Avg Daily Rate', ROUND(AVG(daily_rate), 2), 'USD' FROM TRANSFORMED.DIM_BED WHERE is_active = TRUE;
```

### 8.2 Grant Permissions

```sql
GRANT SELECT ON VW_BED_UTILIZATION_DASHBOARD TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON VW_BED_REVENUE_ANALYSIS TO ROLE ANALYST;
GRANT SELECT ON VW_BED_MANAGEMENT_KPIS TO ROLE CLINICAL_ADMIN;
```

### 8.3 View Results

```sql
-- View bed utilization
SELECT * FROM VW_BED_UTILIZATION_DASHBOARD;

-- View revenue analysis
SELECT * FROM VW_BED_REVENUE_ANALYSIS;

-- View KPIs
SELECT * FROM VW_BED_MANAGEMENT_KPIS;
```

---

## Step 9: Deploy Streamlit Analytics Dashboard

**Duration**: 10 minutes

### 9.1 Navigate to Streamlit in Snowflake

1. In Snowsight, go to **Projects > Streamlit**
2. Click **+ Streamlit App**
3. Configure the app:
   - **Name**: `Hospital_Analytics_Dashboard`
   - **Database**: `HOSPITAL_DEMO`
   - **Schema**: `ANALYTICS`
   - **Warehouse**: `HOSPITAL_ANALYTICS_WH`

### 9.2 Add the Streamlit Code

Copy and paste the following code into the Streamlit editor:

```python
import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# Initialize Snowflake session
session = get_active_session()

# Page configuration
st.set_page_config(
    page_title="Hospital Analytics Dashboard",
    page_icon="🏥",
    layout="wide"
)

# Header
st.title("🏥 Hospital Analytics Dashboard")
st.markdown("Comprehensive Healthcare Analytics for Clinical Teams")

# Helper function to run queries
@st.cache_data
def run_query(query):
    return session.sql(query).to_pandas()

# Sidebar filters
st.sidebar.header("Filters")

# Main metrics
col1, col2, col3, col4 = st.columns(4)

# Get summary stats
stats_query = """
SELECT 
    (SELECT COUNT(DISTINCT patient_id) FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_DEMOGRAPHICS_RAW) as patients,
    (SELECT COUNT(*) FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW) as admissions,
    (SELECT COUNT(*) FROM HOSPITAL_DEMO.RAW_DATA.MEDICAL_PROCEDURES_RAW) as procedures,
    (SELECT COUNT(*) FROM HOSPITAL_DEMO.TRANSFORMED.DIM_BED WHERE is_active = TRUE) as beds
"""
try:
    stats = run_query(stats_query)
    col1.metric("Total Patients", f"{stats['PATIENTS'].iloc[0]:,}")
    col2.metric("Total Admissions", f"{stats['ADMISSIONS'].iloc[0]:,}")
    col3.metric("Total Procedures", f"{stats['PROCEDURES'].iloc[0]:,}")
    col4.metric("Active Beds", f"{stats['BEDS'].iloc[0]:,}")
except Exception as e:
    st.error(f"Error loading stats: {e}")

# Department Analysis
st.subheader("📊 Department Analysis")

dept_query = """
SELECT 
    d.department_name,
    d.specialization_type,
    COUNT(a.admission_id) as admissions,
    AVG(a.total_charges) as avg_charges
FROM HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d
LEFT JOIN HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW a 
    ON d.department_id = a.department_id
GROUP BY d.department_name, d.specialization_type
ORDER BY admissions DESC
"""

try:
    dept_data = run_query(dept_query)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            dept_data.head(10), 
            x='DEPARTMENT_NAME', 
            y='ADMISSIONS',
            title='Top 10 Departments by Admissions',
            color='SPECIALIZATION_TYPE'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.pie(
            dept_data, 
            values='ADMISSIONS', 
            names='SPECIALIZATION_TYPE',
            title='Admissions by Specialization Type'
        )
        st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.error(f"Error loading department data: {e}")

# Weather Impact Analysis
st.subheader("🌤️ Weather Impact on Admissions")

weather_query = """
SELECT 
    weather_condition,
    COUNT(*) as admissions,
    AVG(total_charges) as avg_charges
FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW
WHERE weather_condition IS NOT NULL
GROUP BY weather_condition
ORDER BY admissions DESC
"""

try:
    weather_data = run_query(weather_query)
    
    fig = px.bar(
        weather_data, 
        x='WEATHER_CONDITION', 
        y='ADMISSIONS',
        title='Admissions by Weather Condition'
    )
    st.plotly_chart(fig, use_container_width=True)
except Exception as e:
    st.error(f"Error loading weather data: {e}")

# Bed Inventory Summary
st.subheader("🛏️ Bed Inventory Summary")

bed_query = """
SELECT 
    d.department_name,
    b.bed_type,
    COUNT(*) as bed_count,
    AVG(b.daily_rate) as avg_rate
FROM HOSPITAL_DEMO.TRANSFORMED.DIM_BED b
JOIN HOSPITAL_DEMO.TRANSFORMED.DIM_DEPARTMENT d 
    ON b.department_id = d.department_id
WHERE b.is_active = TRUE
GROUP BY d.department_name, b.bed_type
ORDER BY bed_count DESC
"""

try:
    bed_data = run_query(bed_query)
    st.dataframe(bed_data, use_container_width=True)
except Exception as e:
    st.error(f"Error loading bed data: {e}")

st.markdown("---")
st.caption("Hospital Analytics Dashboard | Powered by Snowflake")
```

### 9.3 Add Required Packages

In the Streamlit editor, click on **Packages** and add:
- `pandas`
- `plotly`

### 9.4 Run the App

Click **Run** to launch your analytics dashboard!

### 9.5 Alternative: Deploy the Full Hospital Analytics Dashboard (Optional)

For a more comprehensive dashboard with role-based views (CEO, Clinical Administrator, Physician, Nurse, Analyst), use the pre-built `hospital_analytics_app_sis.py` file included in this repository.

**To deploy:**

1. In Snowsight, go to **Projects > Streamlit**
2. Click **+ Streamlit App**
3. Configure:
   - **Name**: `Hospital_Analytics_Full_Dashboard`
   - **Database**: `HOSPITAL_DEMO`
   - **Schema**: `ANALYTICS`
   - **Warehouse**: `HOSPITAL_ANALYTICS_WH`
4. Copy the entire contents of `hospital_snowflake_demo/hospital_analytics_app_sis.py`
5. Paste into the Streamlit editor, replacing all existing code
6. Add required packages: `pandas`, `plotly`
7. Click **Run**

**Features of the full dashboard:**
- **CEO Dashboard**: Executive KPIs, revenue analysis, strategic metrics
- **Clinical Administrator**: Department performance, bed utilization, financial analysis
- **Physician Dashboard**: Clinical trends, medication management, patient safety alerts
- **Nurse Dashboard**: Bed management, shift-based views, allied health coordination
- **Analyst Dashboard**: Advanced analytics, demographic analysis, financial efficiency

The app automatically uses `get_active_session()` to connect to Snowflake, eliminating the need for connection configuration.

---

## Step 10: Compute Scaling Demonstration (Optional)

**Duration**: 5 minutes

Demonstrate Snowflake's compute scaling capabilities:

```sql
USE ROLE ACCOUNTADMIN;

-- Create different warehouse sizes for comparison
CREATE OR REPLACE WAREHOUSE HOSPITAL_SMALL_WH
WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

CREATE OR REPLACE WAREHOUSE HOSPITAL_LARGE_WH
WITH WAREHOUSE_SIZE = 'LARGE' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- Run a query on small warehouse
USE WAREHOUSE HOSPITAL_SMALL_WH;
SELECT 
    dd.department_name,
    COUNT(*) as admission_count,
    AVG(fa.length_of_stay_days) as avg_los
FROM TRANSFORMED.FACT_ADMISSIONS fa
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
GROUP BY dd.department_name;

-- Run the same query on large warehouse
USE WAREHOUSE HOSPITAL_LARGE_WH;
SELECT 
    dd.department_name,
    COUNT(*) as admission_count,
    AVG(fa.length_of_stay_days) as avg_los
FROM TRANSFORMED.FACT_ADMISSIONS fa
JOIN TRANSFORMED.DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
GROUP BY dd.department_name;

-- Compare query times in Query History
-- Navigate to Activity > Query History to see performance differences
```

---

## Step 11: Deploy Snowflake Intelligence Agent

**Duration**: 15 minutes

Create a powerful AI-powered hospital analytics assistant using Snowflake Intelligence that combines structured data queries (Cortex Analyst) with policy/procedure document search (Cortex Search).

### 11.1 Understanding the Components

The Hospital Intelligence Agent includes:
- **Cortex Analyst**: Query structured operational data via the semantic view
- **Cortex Search**: Search hospital policies, procedures, and guidelines
- **Combined Intelligence**: Answer complex questions using both structured and unstructured data

### 11.2 Run the Setup Script

Execute the complete Snowflake Intelligence Agent setup:

```sql
-- Run the full setup script
-- This script is located at: sql/12_snowflake_intelligence_agent.sql

USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;

-- Create documents schema
CREATE SCHEMA IF NOT EXISTS HOSPITAL_DEMO.DOCUMENTS;
USE SCHEMA HOSPITAL_DEMO.DOCUMENTS;

-- Create document table (the script inserts 30 document chunks covering 10 hospital policies)
CREATE OR REPLACE TABLE HOSPITAL_DOCUMENTS (
    document_id VARCHAR(50) PRIMARY KEY,
    document_title VARCHAR(500),
    policy_number VARCHAR(50),
    department VARCHAR(200),
    document_type VARCHAR(100),
    effective_date DATE,
    content TEXT,
    chunk_id INT,
    chunk_content TEXT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert policy documents (see full script for complete inserts)
-- Policies include: Admission, Emergency, Discharge, Bed Management, 
-- Medication, Infection Control, HIPAA, Quality Metrics, Allied Health, Department Guide

-- Create Cortex Search Service
CREATE OR REPLACE CORTEX SEARCH SERVICE HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCS_SEARCH
    ON chunk_content
    WAREHOUSE = HOSPITAL_ANALYTICS_WH
    TARGET_LAG = '1 hour'
    AS (
        SELECT 
            document_id, document_title, policy_number, department,
            document_type, effective_date, chunk_id, chunk_content
        FROM HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCUMENTS
    );

-- Create Snowflake Intelligence database (required location for SI agents)
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE PUBLIC;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;
GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE PUBLIC;

-- Create the Hospital Intelligence Agent
CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT
    COMMENT = 'Hospital Analytics Intelligence Agent'
    PROFILE = '{"display_name": "Hospital Analytics Assistant"}'
    FROM SPECIFICATION $$
    {
        "models": {"orchestration": "claude-4-sonnet"},
        "instructions": {
            "orchestration": "Use hospital_data for operational metrics and data analysis. Use policy_search for policies, procedures, guidelines, and targets.",
            "response": "Provide clear, actionable insights. Reference policies when applicable."
        },
        "tools": [
            {"tool_spec": {"type": "cortex_analyst_text_to_sql", "name": "hospital_data", 
             "description": "Query hospital operational data including admissions, procedures, bed utilization, and financial metrics."}},
            {"tool_spec": {"type": "cortex_search", "name": "policy_search",
             "description": "Search hospital policies, procedures, guidelines, and protocols."}}
        ],
        "tool_resources": {
            "hospital_data": {
                "semantic_view": "HOSPITAL_DEMO.ANALYTICS.SV_QUEENSLAND_HEALTH",
                "execution_environment": {"type": "warehouse", "warehouse": "HOSPITAL_ANALYTICS_WH"}
            },
            "policy_search": {
                "search_service": "HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCS_SEARCH",
                "max_results": 10
            }
        }
    }
    $$;

-- Grant access
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT TO ROLE PUBLIC;
```

### 11.3 Access the Agent

1. In Snowsight, navigate to **AI & ML > Snowflake Intelligence**
2. Find the **Hospital Analytics Assistant** agent
3. Start asking questions!

### 11.4 Example Questions to Try

**Structured Data Queries (Cortex Analyst)**:
- "What is the average length of stay by department?"
- "How many emergency admissions did we have?"
- "What is our current bed occupancy rate?"
- "Show me the top 5 departments by revenue"
- "What is our NEAT compliance rate?"

**Policy/Document Searches (Cortex Search)**:
- "What is the NEAT target and how is it measured?"
- "What are the Five Rights of medication administration?"
- "What infection control precautions are needed for MRSA?"
- "What is our readmission rate target?"

**Combined Queries (Both Tools)**:
- "What is our current bed occupancy and what does policy say is the target?"
- "How does our CLABSI rate compare to our policy targets?"
- "What is the average discharge time vs our policy target?"

### 11.5 Hospital Policy Documents Included

| Policy | Description |
|--------|-------------|
| ADM-001 | Hospital Admission Policy and Procedures |
| ED-001 | Emergency Department Protocols |
| DIS-001 | Patient Discharge Guidelines |
| BED-001 | Bed Management Procedures |
| MED-001 | Medication Administration Policy |
| IC-001 | Infection Control Guidelines |
| HIPAA-001 | Patient Privacy and HIPAA Compliance |
| QM-001 | Quality Metrics and KPI Definitions |
| AH-001 | Allied Health Services Guidelines |
| DEPT-001 | Department Specializations Guide |

---

## Step 12: Parse Semi-Structured JSON Data

**Duration**: 10 minutes

In this step, you'll learn how to load and parse JSON data in Snowflake. We'll use patient vitals monitoring data from IoT medical devices to demonstrate semi-structured data handling.

### 12.1 Understanding the JSON Data

The patient vitals JSON data represents real-time monitoring from medical devices:

```json
{
  "patient_id": "PAT001",
  "monitoring_session": "MON001",
  "timestamp": "2024-01-15T08:30:00Z",
  "device_id": "DEV-CARD-101",
  "vitals": {
    "heart_rate": {"value": 92, "unit": "bpm", "status": "elevated"},
    "blood_pressure": {"systolic": 158, "diastolic": 95, "unit": "mmHg", "status": "high"},
    "oxygen_saturation": {"value": 94, "unit": "%", "status": "normal"}
  },
  "alerts": [
    {"type": "BP_HIGH", "severity": "warning", "message": "Blood pressure above normal"}
  ],
  "location": {"department": "CARD", "room": "201", "bed": "A"}
}
```

### 12.2 Create Table with VARIANT Column

```sql
USE ROLE DATA_ENGINEER;
USE WAREHOUSE HOSPITAL_LOAD_WH;
USE DATABASE HOSPITAL_DEMO;
USE SCHEMA RAW_DATA;

-- Create table to store raw JSON data
CREATE OR REPLACE TABLE RAW_DATA.PATIENT_VITALS_JSON_RAW (
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file VARCHAR(255),
    raw_json VARIANT  -- VARIANT stores any semi-structured data
);

-- Create stage for JSON files
CREATE OR REPLACE STAGE RAW_DATA.VITALS_JSON_STAGE
    FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);
```

### 12.3 Load JSON Data

Upload the JSON file and load into the table:

```sql
-- Upload via Snowsight: Stages > VITALS_JSON_STAGE > Upload Files
-- Or use SnowSQL: PUT file:///path/to/patient_vitals_monitoring.json @RAW_DATA.VITALS_JSON_STAGE;

-- Load JSON into table
COPY INTO RAW_DATA.PATIENT_VITALS_JSON_RAW (source_file, raw_json)
FROM (
    SELECT 
        METADATA$FILENAME,
        $1
    FROM @RAW_DATA.VITALS_JSON_STAGE
)
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
ON_ERROR = 'CONTINUE';
```

### 12.4 Parse JSON with Dot Notation

Extract structured data from JSON using Snowflake's powerful JSON functions:

```sql
USE SCHEMA TRANSFORMED;

-- Create parsed view using dot notation and type casting
CREATE OR REPLACE VIEW TRANSFORMED.V_PATIENT_VITALS_PARSED AS
SELECT
    -- Extract simple fields with :: for type casting
    raw_json:patient_id::VARCHAR AS patient_id,
    raw_json:monitoring_session::VARCHAR AS monitoring_session,
    raw_json:timestamp::TIMESTAMP_NTZ AS measurement_timestamp,
    raw_json:device_id::VARCHAR AS device_id,
    
    -- Navigate nested objects with dot notation
    raw_json:location.department::VARCHAR AS department_code,
    raw_json:location.room::VARCHAR AS room_number,
    raw_json:location.bed::VARCHAR AS bed_id,
    
    -- Extract deeply nested vitals data
    raw_json:vitals.heart_rate.value::INTEGER AS heart_rate_value,
    raw_json:vitals.heart_rate.status::VARCHAR AS heart_rate_status,
    raw_json:vitals.blood_pressure.systolic::INTEGER AS bp_systolic,
    raw_json:vitals.blood_pressure.diastolic::INTEGER AS bp_diastolic,
    raw_json:vitals.blood_pressure.status::VARCHAR AS bp_status,
    raw_json:vitals.oxygen_saturation.value::INTEGER AS oxygen_saturation,
    raw_json:vitals.temperature.value::FLOAT AS temperature_value,
    raw_json:vitals.respiratory_rate.value::INTEGER AS respiratory_rate,
    
    -- Handle optional nested fields with COALESCE
    COALESCE(raw_json:vitals.fetal_heart_rate.value::INTEGER, NULL) AS fetal_heart_rate,
    COALESCE(raw_json:vitals.intracranial_pressure.value::FLOAT, NULL) AS intracranial_pressure,
    
    -- Count array elements
    ARRAY_SIZE(raw_json:alerts) AS alert_count
FROM RAW_DATA.PATIENT_VITALS_JSON_RAW;
```

### 12.5 Flatten JSON Arrays with LATERAL FLATTEN

Expand the alerts array into individual rows:

```sql
-- Flatten alerts array - one row per alert
CREATE OR REPLACE VIEW TRANSFORMED.V_PATIENT_VITALS_ALERTS AS
SELECT
    raw_json:patient_id::VARCHAR AS patient_id,
    raw_json:monitoring_session::VARCHAR AS monitoring_session,
    raw_json:timestamp::TIMESTAMP_NTZ AS measurement_timestamp,
    raw_json:location.department::VARCHAR AS department_code,
    
    -- LATERAL FLATTEN expands the array
    alert.value:type::VARCHAR AS alert_type,
    alert.value:severity::VARCHAR AS alert_severity,
    alert.value:message::VARCHAR AS alert_message,
    alert.index AS alert_sequence
FROM RAW_DATA.PATIENT_VITALS_JSON_RAW,
    LATERAL FLATTEN(input => raw_json:alerts) AS alert
WHERE ARRAY_SIZE(raw_json:alerts) > 0;
```

### 12.6 Create Analytics Views

```sql
USE SCHEMA ANALYTICS;

-- Patient vitals summary with risk scoring
CREATE OR REPLACE VIEW ANALYTICS.V_PATIENT_VITALS_SUMMARY AS
SELECT
    patient_id,
    department_code,
    heart_rate_value,
    bp_systolic,
    bp_diastolic,
    oxygen_saturation,
    temperature_value,
    respiratory_rate,
    -- Calculate risk score based on vital signs
    CASE WHEN oxygen_saturation < 90 THEN 3 WHEN oxygen_saturation < 94 THEN 2 ELSE 0 END +
    CASE WHEN bp_systolic > 160 OR bp_systolic < 90 THEN 3 WHEN bp_systolic > 150 THEN 2 ELSE 0 END +
    CASE WHEN heart_rate_value > 120 OR heart_rate_value < 50 THEN 3 ELSE 0 END AS vital_risk_score,
    alert_count
FROM TRANSFORMED.V_PATIENT_VITALS_PARSED;

-- Critical alerts dashboard
CREATE OR REPLACE VIEW ANALYTICS.V_CRITICAL_ALERTS AS
SELECT
    patient_id,
    measurement_timestamp,
    department_code,
    alert_type,
    alert_severity,
    alert_message
FROM TRANSFORMED.V_PATIENT_VITALS_ALERTS
WHERE alert_severity = 'critical'
ORDER BY measurement_timestamp DESC;

-- Department vitals overview
CREATE OR REPLACE VIEW ANALYTICS.V_DEPARTMENT_VITALS_OVERVIEW AS
SELECT
    department_code,
    COUNT(DISTINCT patient_id) AS patient_count,
    ROUND(AVG(heart_rate_value), 1) AS avg_heart_rate,
    ROUND(AVG(bp_systolic), 1) AS avg_systolic_bp,
    ROUND(AVG(oxygen_saturation), 1) AS avg_oxygen_saturation,
    SUM(alert_count) AS total_alerts,
    SUM(CASE WHEN vital_risk_score >= 5 THEN 1 ELSE 0 END) AS high_risk_patients
FROM ANALYTICS.V_PATIENT_VITALS_SUMMARY
GROUP BY department_code
ORDER BY high_risk_patients DESC;
```

### 12.7 Query the Parsed Data

```sql
-- Basic JSON extraction
SELECT 
    raw_json:patient_id::VARCHAR AS patient,
    raw_json:vitals.heart_rate.value::INT AS heart_rate
FROM RAW_DATA.PATIENT_VITALS_JSON_RAW
LIMIT 5;

-- Access array elements by index
SELECT 
    raw_json:patient_id::VARCHAR AS patient,
    ARRAY_SIZE(raw_json:alerts) AS num_alerts,
    raw_json:alerts[0]:type::VARCHAR AS first_alert_type,
    raw_json:alerts[0]:severity::VARCHAR AS first_alert_severity
FROM RAW_DATA.PATIENT_VITALS_JSON_RAW
WHERE ARRAY_SIZE(raw_json:alerts) > 0;

-- View critical alerts
SELECT * FROM ANALYTICS.V_CRITICAL_ALERTS;

-- Department vitals overview
SELECT * FROM ANALYTICS.V_DEPARTMENT_VITALS_OVERVIEW;
```

### 12.8 Key JSON Parsing Techniques Summary

| Technique | Syntax | Example |
|-----------|--------|---------||
| Dot Notation | `json_col:field` | `raw_json:patient_id` |
| Nested Access | `json_col:parent.child` | `raw_json:vitals.heart_rate.value` |
| Type Casting | `::TYPE` | `raw_json:field::VARCHAR` |
| Array Access | `[index]` | `raw_json:alerts[0]` |
| Array Size | `ARRAY_SIZE()` | `ARRAY_SIZE(raw_json:alerts)` |
| Flatten Arrays | `LATERAL FLATTEN` | `LATERAL FLATTEN(input => raw_json:alerts)` |
| Optional Fields | `COALESCE` | `COALESCE(raw_json:optional::INT, 0)` |

> **Tip**: JSON parsing in Snowflake is optimized for performance. The VARIANT type stores data efficiently and queries are executed without schema-on-read overhead.

---

## Step 13: Snowflake Marketplace Data Sharing

**Duration**: 10 minutes

In this step, you'll experience the power of Snowflake's Data Marketplace - instantly accessing third-party data and blending it with your hospital data without any ETL pipelines.

### 13.1 Access the Snowflake Marketplace

1. In Snowsight, click **Data Products > Marketplace** in the left navigation
2. Search for **"Weather Source"** or **"weather"**
3. Look for free datasets like:
   - "Weather Source LLC: frostbyte" (free sample)
   - "Knoema Economy Data Atlas"
   - "US Weather Events"
4. Click **Get** on your chosen dataset
5. Accept the terms and the data appears instantly in your account!

> **Key Insight**: Zero-copy data sharing means no data movement - the provider's data is instantly queryable from your account.

### 13.2 Create Sample Weather Data (for Demo)

If Marketplace access isn't available, create a simulated weather dataset:

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;
USE DATABASE HOSPITAL_DEMO;

-- Create schema for marketplace-style data
CREATE SCHEMA IF NOT EXISTS MARKETPLACE_DATA;
USE SCHEMA MARKETPLACE_DATA;

-- Simulated weather data (mimics Marketplace weather structure)
CREATE OR REPLACE TABLE SAMPLE_WEATHER_DATA AS
WITH date_range AS (
    SELECT DATEADD(day, seq4(), '2023-01-01')::DATE AS weather_date
    FROM TABLE(GENERATOR(rowcount => 730))  -- 2 years
)
SELECT 
    weather_date,
    'BRISBANE' AS city,
    'QLD' AS state,
    -- Temperature with seasonal variation
    ROUND(22 + 8 * SIN((DAYOFYEAR(weather_date) - 172) * 3.14159 / 182.5) + 
          UNIFORM(-3::FLOAT, 3::FLOAT, RANDOM()), 1) AS avg_temperature_c,
    ROUND(UNIFORM(40::FLOAT, 90::FLOAT, RANDOM()), 0) AS avg_humidity_pct,
    CASE WHEN UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) > 0.7 
         THEN ROUND(UNIFORM(1::FLOAT, 50::FLOAT, RANDOM()), 1) 
         ELSE 0 END AS precipitation_mm,
    ROUND(30 + UNIFORM(0::FLOAT, 50::FLOAT, RANDOM()), 0) AS air_quality_index,
    CASE 
        WHEN precipitation_mm > 20 THEN 'Heavy Rain'
        WHEN precipitation_mm > 5 THEN 'Light Rain'
        WHEN avg_temperature_c > 35 THEN 'Extreme Heat'
        ELSE 'Clear'
    END AS weather_condition
FROM date_range;
```

### 13.3 Blend Weather Data with Hospital Admissions

Join the Marketplace weather data with your internal admission records:

```sql
USE SCHEMA ANALYTICS;

-- Create analytical view joining admissions with weather
CREATE OR REPLACE VIEW ANALYTICS.V_ADMISSIONS_WEATHER_ANALYSIS AS
SELECT 
    a.admission_date,
    a.department_code,
    d.department_name,
    a.admission_type,
    a.primary_diagnosis,
    a.length_of_stay_days,
    a.total_charges,
    
    -- Weather data from Marketplace
    w.avg_temperature_c,
    w.avg_humidity_pct,
    w.precipitation_mm,
    w.air_quality_index,
    w.weather_condition,
    
    -- Derived categories for analysis
    CASE 
        WHEN w.avg_temperature_c >= 35 THEN 'Extreme Heat'
        WHEN w.avg_temperature_c >= 25 THEN 'Hot'
        WHEN w.avg_temperature_c >= 15 THEN 'Mild'
        ELSE 'Cold'
    END AS temperature_category

FROM RAW_DATA.PATIENT_ADMISSIONS_RAW a
JOIN RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d ON a.department_code = d.department_code
LEFT JOIN MARKETPLACE_DATA.SAMPLE_WEATHER_DATA w ON a.admission_date = w.weather_date;
```

### 13.4 Analyze Weather Impact on Admissions

Run the analytical query to discover correlations:

```sql
-- Weather impact analysis
CREATE OR REPLACE VIEW ANALYTICS.V_WEATHER_ADMISSION_CORRELATION AS
SELECT 
    weather_condition,
    temperature_category,
    
    -- Admission metrics
    COUNT(*) AS total_admissions,
    ROUND(COUNT(*) / COUNT(DISTINCT admission_date), 1) AS avg_daily_admissions,
    
    -- Emergency ratio
    ROUND(100.0 * SUM(CASE WHEN admission_type = 'Emergency' THEN 1 ELSE 0 END) 
          / COUNT(*), 1) AS emergency_pct,
    
    -- Financial impact
    ROUND(AVG(total_charges), 2) AS avg_charges,
    ROUND(AVG(length_of_stay_days), 1) AS avg_los

FROM ANALYTICS.V_ADMISSIONS_WEATHER_ANALYSIS
WHERE admission_date >= '2023-01-01'
GROUP BY weather_condition, temperature_category
ORDER BY total_admissions DESC;

-- View the correlation results
SELECT * FROM ANALYTICS.V_WEATHER_ADMISSION_CORRELATION;
```

### 13.5 Example Insights

| Weather Condition | Insight | Action |
|-------------------|---------|--------|
| **Extreme Heat** | Emergency admissions spike 25% | Pre-position staff during heat waves |
| **Heavy Rain** | Slip-and-fall injuries increase | Alert orthopedic department |
| **Poor Air Quality** | Respiratory admissions up 15% | Trigger respiratory readiness protocols |

> **Business Value**: By blending Marketplace weather data with hospital admissions, you can predict staffing needs, optimize inventory, and improve patient outcomes - all without building any data pipelines!

---

## Step 14: Clean Up (Optional)

If you want to remove all objects created in this quickstart:

```sql
USE ROLE ACCOUNTADMIN;

-- Drop Snowflake Intelligence database (includes agents)
DROP DATABASE IF EXISTS SNOWFLAKE_INTELLIGENCE;

-- Drop database (this will remove all schemas, tables, and views)
DROP DATABASE IF EXISTS HOSPITAL_DEMO;

-- Drop warehouses
DROP WAREHOUSE IF EXISTS HOSPITAL_LOAD_WH;
DROP WAREHOUSE IF EXISTS HOSPITAL_ANALYTICS_WH;
DROP WAREHOUSE IF EXISTS HOSPITAL_ADHOC_WH;
DROP WAREHOUSE IF EXISTS HOSPITAL_SMALL_WH;
DROP WAREHOUSE IF EXISTS HOSPITAL_LARGE_WH;

-- Drop roles
DROP ROLE IF EXISTS CLINICAL_ADMIN;
DROP ROLE IF EXISTS PHYSICIAN;
DROP ROLE IF EXISTS NURSE;
DROP ROLE IF EXISTS ANALYST;
DROP ROLE IF EXISTS DATA_ENGINEER;

-- Drop resource monitor
DROP RESOURCE MONITOR IF EXISTS HOSPITAL_MONTHLY_LIMIT;
```

---

## Conclusion and Next Steps

Congratulations! You've successfully built a complete hospital analytics platform on Snowflake. You learned how to:

✅ Create databases, schemas, and role hierarchies  
✅ Set up stages and load structured CSV data  
✅ Transform raw data into a dimensional star schema model  
✅ Implement RBAC with masking policies for HIPAA compliance  
✅ Build Dynamic Tables for declarative, auto-refreshing data pipelines  
✅ Create bed management analytics views  
✅ Deploy an interactive Streamlit dashboard  
✅ Configure compute scaling for different workloads  
✅ Build a Snowflake Intelligence Agent with Cortex Analyst and Cortex Search  
✅ Parse semi-structured JSON data from IoT medical devices  
✅ Blend Snowflake Marketplace data with internal hospital data

### What You Can Do Next

1. **Expand the Agent**: Add more tools like web search or custom procedures
2. **Add More Data Sources**: Integrate with real-time data feeds using Snowpipe
3. **Expand Marketplace Data**: Add demographic or economic data from Marketplace
4. **Machine Learning**: Build predictive models for patient readmission using Snowpark ML
5. **Advanced Analytics**: Create more sophisticated KPIs and alerts
6. **Dynamic Table Monitoring**: Use Snowsight to monitor dynamic table refresh performance

### Related Resources

- [Snowflake Documentation](https://docs.snowflake.com/)
- [Snowflake Developer Guides](https://www.snowflake.com/en/developers/guides/)
- [Snowflake Intelligence](https://docs.snowflake.com/user-guide/snowflake-cortex/snowflake-intelligence)
- [Cortex Agents Documentation](https://docs.snowflake.com/user-guide/snowflake-cortex/cortex-agents)
- [Cortex Search Documentation](https://docs.snowflake.com/user-guide/snowflake-cortex/cortex-search)
- [Semantic Views (Cortex Analyst)](https://docs.snowflake.com/en/user-guide/views-semantic)
- [Dynamic Tables Documentation](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [Snowpark Python Developer Guide](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
- [Snowflake Marketplace](https://docs.snowflake.com/en/user-guide/data-marketplace)

---

*This quickstart was created based on the Hospital Snowflake Demo project, designed for clinical teams and healthcare IT professionals.*
