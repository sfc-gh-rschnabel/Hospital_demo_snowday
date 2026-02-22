# Hospital Analytics Streamlit App - Deployment Guide

## Overview

This Streamlit application provides role-based analytics dashboards for the Hospital Snowflake Demo, showcasing different user personas and their data access patterns.

## Features

### 🔐 Role-Based Dashboards

#### CEO (Default)
- **Strategic oversight** and executive performance metrics
- **Hospital-wide KPIs** and financial performance
- **Department performance** analysis and benchmarking
- **Quality metrics** and strategic alerts
- **Growth opportunities** and revenue optimization

#### Clinical Administrator
- **Complete hospital oversight**
- Department performance metrics
- Bed utilization management
- Financial performance analysis
- Quality and safety indicators

#### Physician
- **Clinical decision support**
- Patient care analytics
- Medication management insights
- Clinical trends and patterns
- Safety alerts and warnings

#### Nurse
- **Operational support**
- Current shift overview
- Bed management status
- Patient care coordination
- Allied health scheduling

#### Analyst
- **Business intelligence**
- Advanced analytics and reporting
- Financial performance analysis
- Operational efficiency metrics
- Data quality dashboards

#### Capacity Planner
- **Bed capacity optimization**
- Utilization analysis and forecasting
- Turnover efficiency metrics
- Capacity recommendations and scenario planning
- Resource allocation optimization

#### Allied Health Coordinator
- **Comprehensive allied health services management**
- Provider performance analytics and optimization
- Service utilization trends and forecasting
- Department integration analysis
- Clinical outcomes and patient engagement metrics
- Revenue optimization and productivity analysis

### 📊 Key Analytics Features

- **Real-time bed utilization** across departments
- **Medication management** with cost analysis
- **Allied health services** tracking and outcomes
- **Financial performance** by service line
- **Patient demographics** and population health
- **Data quality monitoring** and alerts

## Prerequisites

### 1. Snowflake Environment
- Hospital demo database fully deployed (scripts 1-6, 8)
- All data loaded (100,000+ patients, 1M+ records)
- Appropriate role permissions configured

### 2. Python Environment
- Python 3.8 or higher
- Required packages (see requirements.txt)

### 3. Snowflake Connection
- Active Snowflake session or connection parameters
- Access to HOSPITAL_DEMO database

## Installation

### Step 1: Deploy to Snowflake Streamlit (Recommended)

#### Option A: Snowflake Native Streamlit (Primary Method)
1. **Upload the app** to your Snowflake account
2. **Create Streamlit app** in Snowflake:
   ```sql
   USE ROLE ACCOUNTADMIN;
   USE DATABASE HOSPITAL_DEMO;
   USE SCHEMA ANALYTICS;
   
   CREATE STREAMLIT HOSPITAL_ANALYTICS_APP
   ROOT_LOCATION = '@HOSPITAL_DATA_STAGE'
   MAIN_FILE = 'hospital_analytics_app.py'
   QUERY_WAREHOUSE = HOSPITAL_ANALYTICS_WH;
   ```
3. **Grant permissions**:
   ```sql
   GRANT USAGE ON STREAMLIT HOSPITAL_ANALYTICS_APP TO ROLE CLINICAL_ADMIN;
   GRANT USAGE ON STREAMLIT HOSPITAL_ANALYTICS_APP TO ROLE PHYSICIAN;
   GRANT USAGE ON STREAMLIT HOSPITAL_ANALYTICS_APP TO ROLE NURSE;
   GRANT USAGE ON STREAMLIT HOSPITAL_ANALYTICS_APP TO ROLE ANALYST;
   ```

#### Option B: Local Development (For Testing Only)
1. **Install dependencies**:
   ```bash
   pip install streamlit pandas plotly snowflake-snowpark-python
   ```
2. **Configure connection** in `.streamlit/secrets.toml`:
   ```toml
   [connections.snowflake]
   account = "your-account-identifier"
   user = "your-username"
   password = "your-password"
   warehouse = "HOSPITAL_ANALYTICS_WH"
   database = "HOSPITAL_DEMO"
   schema = "RAW_DATA"
   role = "CLINICAL_ADMIN"
   ```
3. **Run locally**:
   ```bash
   streamlit run hospital_analytics_app.py
   ```

### Step 2: Upload App Files to Snowflake Stage
```sql
-- Upload the Streamlit app to the stage
PUT file://path/to/hospital_analytics_app.py @HOSPITAL_DATA_STAGE;
```

## Usage

### 1. Role Selection
- Choose your role from the sidebar dropdown
- Each role shows different data based on RBAC policies
- Data masking and filtering applied automatically

### 2. Time Period Selection
- Select analysis period (7 days to Year to Date)
- Custom date ranges supported
- All analytics update based on selected period

### 3. Dashboard Navigation
- Role-specific dashboards load automatically
- Interactive charts and visualizations
- Export capabilities for reports

### 4. Advanced Features
- Data quality metrics toggle
- Export functionality for reports
- Real-time data refresh

## Demo Scenarios

### Scenario 1: CEO Strategic Review
1. **Select**: CEO role (default)
2. **Review**: Hospital-wide performance KPIs
3. **Analyze**: Revenue distribution across service lines
4. **Monitor**: Department performance and efficiency
5. **Identify**: Strategic alerts and growth opportunities

### Scenario 2: Clinical Administrator Review
1. **Select**: Clinical Administrator role
2. **View**: Complete hospital overview
3. **Analyze**: Department performance and bed utilization
4. **Review**: Financial performance across service lines
5. **Export**: Executive summary reports

### Scenario 2: Physician Clinical Analysis
1. **Select**: Physician role
2. **Focus**: Clinical trends and patient safety
3. **Review**: Medication management insights
4. **Monitor**: Emergency admission patterns
5. **Analyze**: Case complexity by department

### Scenario 4: Nurse Operational Support
1. **Select**: Nurse role
2. **Check**: Current shift bed status
3. **Coordinate**: Allied health services
4. **Monitor**: Medication administration
5. **Plan**: Patient care activities

### Scenario 5: Capacity Planning Analysis
1. **Select**: Capacity Planner role
2. **Analyze**: Bed utilization and turnover rates
3. **Review**: Department capacity recommendations
4. **Plan**: Scenario-based capacity changes
5. **Export**: Capacity planning reports

### Scenario 6: Business Intelligence Analysis
1. **Select**: Analyst role
2. **Analyze**: Population health demographics
3. **Review**: Financial performance trends
4. **Monitor**: Operational efficiency metrics
5. **Generate**: Business intelligence reports

### Scenario 7: Allied Health Services Management
1. **Select**: Allied Health Coordinator role
2. **Review**: Service utilization and provider performance
3. **Analyze**: Department integration and outcomes
4. **Monitor**: Patient engagement and success rates
5. **Optimize**: Provider productivity and service delivery
6. **Plan**: Resource allocation and service expansion

## Security Features

### Data Masking
- **Patient names**: Masked for Nurse role
- **Financial data**: Hidden from clinical roles
- **Addresses**: Partially masked based on role

### Row-Level Security
- **Department access**: Limited by role permissions
- **Data filtering**: Automatic based on user role
- **Audit logging**: All access tracked

### Role Permissions
- **Clinical Admin**: Full access to all data
- **Physician**: Clinical data with limited PII
- **Nurse**: Operational data with masked PII
- **Analyst**: Aggregated data without PII

## Troubleshooting

### Common Issues

1. **Connection Error**
   ```
   Error: Cannot connect to Snowflake
   ```
   **Solution**: Check connection parameters and network access

2. **Permission Denied**
   ```
   Error: Insufficient privileges
   ```
   **Solution**: Verify role permissions and database access

3. **No Data Displayed**
   ```
   Error: No data found
   ```
   **Solution**: Ensure all demo scripts have been executed

4. **Role Not Working**
   ```
   Error: Role-specific data not showing
   ```
   **Solution**: Check RBAC policies are applied correctly

### Debug Steps

1. **Test Snowflake Connection**:
   ```python
   from snowflake.snowpark.context import get_active_session
   session = get_active_session()
   print(session.sql("SELECT CURRENT_USER(), CURRENT_ROLE()").collect())
   ```

2. **Verify Database Access**:
   ```sql
   USE DATABASE HOSPITAL_DEMO;
   SHOW TABLES IN SCHEMA RAW_DATA;
   ```

3. **Check Role Permissions**:
   ```sql
   SHOW GRANTS TO ROLE CURRENT_ROLE();
   ```

## Performance Optimization

### For Large Datasets
- Use appropriate warehouse size (MEDIUM or LARGE)
- Enable result caching with `@st.cache_data`
- Limit query result sizes for interactive charts
- Use data sampling for exploratory analysis

### Query Optimization
- Add date filters to reduce data scanned
- Use indexed columns for filtering
- Aggregate data at appropriate levels
- Cache frequently accessed results

## Customization

### Adding New Metrics
1. Create new cached data function
2. Add to appropriate role dashboard
3. Include in sidebar controls if needed
4. Update permissions as required

### Role Customization
1. Modify role selection in sidebar
2. Add new dashboard section
3. Update security notices
4. Test data access patterns

### Visual Customization
1. Update CSS styling in the header
2. Modify color schemes for charts
3. Add new chart types as needed
4. Update layout and spacing

## Production Considerations

### Security
- Deploy in Snowflake native environment for best security
- Use service accounts for automated deployments
- Implement additional authentication if needed
- Regular security reviews and updates

### Performance
- Monitor query performance and optimize as needed
- Use appropriate warehouse sizes
- Implement query result caching
- Consider data partitioning for very large datasets

### Maintenance
- Regular updates to match schema changes
- Monitor for deprecated Snowflake features
- Update dependencies as needed
- Backup and version control

---

**🚀 Ready for Clinical Team Demo!**

This Streamlit app provides a comprehensive, role-based analytics platform that showcases Snowflake's capabilities for healthcare data management and analysis.
