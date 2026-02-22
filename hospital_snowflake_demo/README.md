# Hospital Snowflake Demo Project

This project demonstrates Snowflake's core capabilities for healthcare data management, designed specifically for clinical teams.

## Demo Overview

This demo showcases:
1. **Data Loading** from S3 buckets with healthcare data
2. **Data Transformation** to dimensional modeling
3. **Data Governance & RBAC** for healthcare compliance
4. **Compute Scaling** for varying workloads
5. **Marketplace Integration** with weather data to analyze admission patterns

## Project Structure

```
hospital_snowflake_demo/
├── README.md                           # This file
├── data/                              # Healthcare datasets (325K+ records)
│   ├── patient_demographics_large.csv # 10,000 patients (70% active, 30% historic)
│   ├── patient_admissions_large.csv   # 6,985 admissions
│   ├── medical_procedures_large.csv   # 8,299 procedures
│   ├── hospital_departments.csv       # 21 departments
│   ├── bed_inventory.csv              # 240 beds
│   ├── bed_bookings.csv               # 60,242 bookings
│   ├── bed_availability.csv           # 87,840 availability records
│   ├── pharmacy_inventory.csv         # 92 medication items
│   ├── medication_orders.csv          # 18,870 orders
│   ├── medication_dispensing.csv      # 126,803 dispensing records
│   └── allied_health_services.csv     # 6,404 service records
├── sql/                               # SQL scripts
│   ├── 01_setup_environment.sql       # Database, schema, roles setup
│   ├── 02_create_stages.sql           # S3 stages and file formats
│   ├── 03_load_data.sql              # Data loading from S3
│   ├── 04_transform_dimensional.sql   # Dimensional model creation
│   ├── 05_rbac_governance.sql        # Security and governance
│   ├── 06_compute_scaling.sql        # Warehouse scaling demo
│   └── 08_bed_analytics.sql          # Bed management analytics
├── hospital_analytics_app.py          # Streamlit analytics dashboard
├── generate_large_datasets.py         # Data generation script
├── requirements.txt                   # Python dependencies
├── streamlit_deployment_guide.md      # App deployment instructions
├── demo_script.md                     # Step-by-step demo guide
├── presentation_notes.md              # Key talking points
└── DATASET_SUMMARY.md                # Complete dataset overview
```

## Quick Start

1. **Generate Data**: Run `python3 generate_large_datasets.py`
2. **Setup Database**: Run SQL scripts in order (01-06, 08)
3. **Launch Analytics**: Run `streamlit run hospital_analytics_app.py`
4. **Follow Demo**: Use demo script for presentation
5. **Key Points**: Reference presentation notes for talking points

## Key Demo Points

- **Healthcare Data Security**: RBAC with PHI protection
- **Scalable Analytics**: Auto-scaling for peak admission periods
- **Data Integration**: Combining internal data with external weather patterns
- **Real-time Insights**: Predicting admission patterns based on weather
- **Allied Health Analytics**: Comprehensive provider performance and outcomes tracking

## Target Audience

Clinical teams, healthcare IT professionals, hospital administrators interested in:
- Modern data warehousing for healthcare
- Compliance and security in cloud analytics
- Predictive analytics for hospital operations
- Cost-effective scaling of data infrastructure
