# Hospital Snowflake Demo - Presentation Script

## Demo Overview (5 minutes)
**Audience**: Clinical team at hospital  
**Duration**: 45-60 minutes  
**Goal**: Demonstrate Snowflake's value for healthcare data management

---

## 1. Introduction & Setup (5 minutes)

### Opening Statement
> "Today I'll show you how Snowflake can transform your hospital's data management, focusing on four key areas that matter most to healthcare organizations: secure data loading, robust governance, scalable performance, and predictive analytics using external data."

### Key Demo Points to Cover:
- **Data Loading**: From S3 buckets to analytics-ready tables
- **Data Governance**: HIPAA-compliant security and access controls
- **Compute Scaling**: Right-sizing resources for different workloads
- **Marketplace Integration**: Weather data for admission predictions

### Run Setup Script
```sql
-- Execute: 01_setup_environment.sql
-- Note: Run as ACCOUNTADMIN for warehouse creation privileges
-- Show the created databases, schemas, roles, and warehouses
SHOW DATABASES LIKE 'HOSPITAL_DEMO';
SHOW ROLES LIKE 'CLINICAL_%';
SHOW WAREHOUSES LIKE 'HOSPITAL_%';
```

**Talking Points:**
- "We've created a complete healthcare data environment"
- "Notice the role-based structure matching your clinical hierarchy"
- "Different warehouse sizes for different types of work"
- "ACCOUNTADMIN privileges needed for warehouse management"

---

## 2. Data Loading from S3 (10 minutes)

### Demonstrate Stage Creation
```sql
-- Execute: 02_create_stages.sql
-- Show file formats and stages
SHOW FILE FORMATS IN SCHEMA RAW_DATA;
SHOW STAGES IN SCHEMA RAW_DATA;
```

**Talking Points:**
- "Snowflake connects directly to your S3 buckets"
- "File formats handle different data types automatically"
- "Stages act as secure gateways to your cloud storage"
- "Directory tables automatically track file metadata"
- "Server-side encryption protects data at rest"

### Show Sample Data Files
Display the CSV files in the `/data` folder:
- `patient_demographics.csv` - Patient information
- `patient_admissions.csv` - Admission records with weather data
- `hospital_departments.csv` - Department information
- `medical_procedures.csv` - Procedure details

**Key Points:**
- "Real healthcare data with realistic patterns"
- "Notice we already have weather information embedded"
- "Data includes financial, clinical, and operational metrics"

### Execute Data Loading
```sql
-- Execute: 03_load_data.sql
-- Show data validation results
SELECT 'Patient Demographics' as table_name, COUNT(*) as record_count FROM PATIENT_DEMOGRAPHICS_RAW
UNION ALL
SELECT 'Patient Admissions', COUNT(*) FROM PATIENT_ADMISSIONS_RAW
UNION ALL
SELECT 'Hospital Departments', COUNT(*) FROM HOSPITAL_DEPARTMENTS_RAW
UNION ALL
SELECT 'Medical Procedures', COUNT(*) FROM MEDICAL_PROCEDURES_RAW;
```

**Talking Points:**
- "Data loaded with full audit trail and error handling"
- "Automatic schema detection and data type conversion"
- "Built-in data quality checks and validation"

### Show Enterprise Stage Features
```sql
-- Show directory table with file metadata
SELECT * FROM DIRECTORY(@HOSPITAL_DATA_STAGE);

-- File size analysis
SELECT 
    relative_path,
    ROUND(size_bytes / 1024 / 1024, 2) as size_mb,
    last_modified
FROM DIRECTORY(@HOSPITAL_DATA_STAGE)
ORDER BY size_bytes DESC;
```

**Key Points:**
- "Directory table automatically tracks all file metadata"
- "No manual file cataloging needed"
- "Built-in data lineage and audit trail"
- "Server-side encryption protects sensitive healthcare data"

---

## 3. Data Transformation to Dimensional Model (8 minutes)

### Show Dimensional Design
```sql
-- Execute: 04_transform_dimensional.sql
-- Display the dimensional model summary
SELECT * FROM VW_DIMENSIONAL_SUMMARY;
```

**Talking Points:**
- "Transformed raw data into a star schema for analytics"
- "Dimension tables for patients, departments, physicians, procedures"
- "Fact tables for admissions and procedures with full relationships"

### Demonstrate Key Features
```sql
-- Show patient dimension with SCD Type 2
SELECT patient_id, full_name, age_group, effective_date, is_current 
FROM DIM_PATIENT 
WHERE patient_id = 'PAT001';

-- Show fact table with all relationships
SELECT 
    fa.admission_id,
    dp.full_name,
    dd.department_name,
    fa.diagnosis_primary,
    fa.total_charges
FROM FACT_ADMISSIONS fa
JOIN DIM_PATIENT dp ON fa.patient_key = dp.patient_key
JOIN DIM_DEPARTMENT dd ON fa.department_key = dd.department_key
LIMIT 5;
```

**Key Points:**
- "Slowly changing dimensions track patient history"
- "Star schema optimized for analytical queries"
- "All relationships maintained with foreign keys"

---

## 4. Data Governance & RBAC (12 minutes)

### Show Role-Based Access Control
```sql
-- Execute: 05_rbac_governance.sql
-- Demonstrate different role access levels

-- As PHYSICIAN role
USE ROLE PHYSICIAN;
SELECT COUNT(*) as accessible_records FROM ANALYTICS.VW_PHYSICIAN_DASHBOARD;
SELECT * FROM ANALYTICS.VW_PHYSICIAN_DASHBOARD LIMIT 3;

-- As NURSE role  
USE ROLE NURSE;
SELECT COUNT(*) as accessible_records FROM ANALYTICS.VW_NURSE_DASHBOARD;
SELECT * FROM ANALYTICS.VW_NURSE_DASHBOARD LIMIT 3;

-- As ANALYST role
USE ROLE ANALYST;
SELECT * FROM ANALYTICS.VW_ANALYST_DASHBOARD LIMIT 3;
```

**Talking Points:**
- "Each role sees only appropriate data for their function"
- "Physicians see clinical details, nurses see operational info"
- "Analysts see aggregated data without patient identifiers"

### Demonstrate Data Masking
```sql
-- Show masking in action
USE ROLE ACCOUNTADMIN;
SELECT patient_id, full_name, address FROM TRANSFORMED.DIM_PATIENT LIMIT 3;

USE ROLE NURSE;
SELECT patient_id, full_name, address FROM TRANSFORMED.DIM_PATIENT LIMIT 3;
```

**Key Points:**
- "Data masking automatically applied based on role"
- "HIPAA compliance built into the platform"
- "No application changes needed for security"

### Show Data Classification Tags
```sql
-- Display tag usage
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES 
WHERE TAG_DATABASE = 'HOSPITAL_DEMO'
ORDER BY OBJECT_NAME, COLUMN_NAME;
```

**Talking Points:**
- "Data automatically classified for compliance"
- "Tags identify PII and PHI data"
- "Audit trail for all data access"

---

## 5. Compute Scaling Demonstration (8 minutes)

### Show Different Warehouse Sizes
```sql
-- Execute: 06_compute_scaling.sql
-- Show warehouse configurations
SHOW WAREHOUSES LIKE 'HOSPITAL_%';
```

**Talking Points:**
- "Different warehouse sizes for different workloads"
- "Auto-suspend prevents unnecessary costs"
- "Multi-cluster scaling for high concurrency"

### Demonstrate Performance Scaling
```sql
-- Simple query on small warehouse
USE WAREHOUSE HOSPITAL_SMALL_WH;
SELECT department_name, COUNT(*) as patient_count
FROM VW_ANALYST_DASHBOARD
GROUP BY department_name;

-- Complex query on large warehouse  
USE WAREHOUSE HOSPITAL_LARGE_WH;
-- [Execute the complex analytical query from the script]
```

### Show Performance Monitoring
```sql
-- Display warehouse performance metrics
SELECT * FROM VW_WAREHOUSE_PERFORMANCE;
SELECT * FROM VW_WAREHOUSE_COSTS;
```

**Key Points:**
- "Right-size compute for each workload type"
- "Pay only for what you use with per-second billing"
- "Automatic scaling based on demand"

---

## 6. Marketplace Integration - Weather Analysis (10 minutes)

### Introduce Weather Data Integration
```sql
-- Execute: 07_marketplace_integration.sql
-- Show marketplace data integration
SELECT COUNT(*) as weather_records FROM MARKETPLACE_DATA.WEATHER_HISTORY;
SELECT * FROM MARKETPLACE_DATA.WEATHER_HISTORY LIMIT 5;
```

**Talking Points:**
- "External data from Snowflake Marketplace"
- "Weather data enriches our admission analysis"
- "No complex ETL needed for data integration"

### Show Weather-Admission Correlation
```sql
-- Weather impact analysis
SELECT 
    weather_condition,
    COUNT(*) as days_observed,
    AVG(admission_count) as avg_daily_admissions,
    AVG(emergency_admissions) as avg_emergency_admissions
FROM VW_WEATHER_ADMISSION_ANALYSIS
GROUP BY weather_condition
ORDER BY avg_daily_admissions DESC;
```

**Key Insights:**
- "Snowy weather correlates with higher admissions"
- "Emergency admissions spike during extreme weather"
- "Data-driven staffing decisions possible"

### Demonstrate Predictive Analytics
```sql
-- Show admission forecasting
SELECT 
    weather_condition,
    temperature_category,
    avg_admissions,
    forecasted_admissions
FROM VW_ADMISSION_FORECAST_FACTORS
ORDER BY forecasted_admissions DESC
LIMIT 10;
```

**Talking Points:**
- "Predict admission patterns based on weather forecasts"
- "Optimize staffing and resource allocation"
- "Proactive rather than reactive management"

### Show Department-Specific Impact
```sql
-- Department weather correlation
SELECT 
    department_name,
    correlation_strength,
    AVG(admissions) as avg_admissions
FROM VW_DEPARTMENT_WEATHER_CORRELATION
GROUP BY department_name, correlation_strength
ORDER BY department_name;
```

**Clinical Value:**
- "Cardiology shows high weather correlation"
- "Emergency department most weather-sensitive"
- "Tailor preparations by department"

---

## 7. Business Value Summary (5 minutes)

### Key Metrics to Highlight
```sql
-- Summary statistics
SELECT 'Total Patients' as metric, COUNT(DISTINCT patient_id) as value FROM TRANSFORMED.DIM_PATIENT
UNION ALL
SELECT 'Total Admissions', COUNT(*) FROM TRANSFORMED.FACT_ADMISSIONS
UNION ALL
SELECT 'Total Revenue', SUM(total_charges) FROM TRANSFORMED.FACT_ADMISSIONS
UNION ALL
SELECT 'Avg Length of Stay', AVG(length_of_stay_days) FROM TRANSFORMED.FACT_ADMISSIONS;
```

### Cost and Performance Benefits
- **Data Loading**: "Automated, scalable, and secure"
- **Governance**: "HIPAA-compliant out of the box"
- **Performance**: "Scale from small queries to complex analytics"
- **Innovation**: "Easy integration with external data sources"

---

## 8. Q&A and Next Steps (7 minutes)

### Common Questions and Answers

**Q: How does this integrate with our existing EHR system?**
A: "Snowflake connects to any system via standard APIs, JDBC/ODBC, or file-based integration. We can maintain real-time or batch synchronization."

**Q: What about HIPAA compliance?**
A: "Snowflake is HIPAA-eligible with built-in encryption, access controls, and audit logging. The governance features we showed ensure compliance."

**Q: How quickly can we implement this?**
A: "Basic setup in days, full implementation in weeks. We can start with a pilot department and expand gradually."

**Q: What about costs?**
A: "Pay-per-use model means you only pay for compute when running queries. Storage costs are very competitive with traditional solutions."

### Next Steps
1. **Pilot Project**: Start with one department's data
2. **Training**: Clinical team training on Snowflake basics
3. **Integration Planning**: Connect to existing systems
4. **Advanced Analytics**: Implement predictive models
5. **Marketplace Exploration**: Identify valuable external datasets

---

## Demo Tips

### Before the Demo
- [ ] Ensure all SQL scripts run successfully
- [ ] Load sample data files to stages
- [ ] Test role switching functionality
- [ ] Prepare backup queries if needed

### During the Demo
- Focus on business value, not technical details
- Use clinical terminology the audience understands
- Show real results, not just capabilities
- Encourage questions throughout
- Relate features to their daily challenges

### Key Messages
1. **Security First**: "Your patient data is protected at every level"
2. **Scale on Demand**: "Resources adapt to your workload automatically"
3. **Insights Ready**: "From data loading to actionable insights in minutes"
4. **Future Proof**: "Platform grows with your organization's needs"
