-- =============================================================================
-- FILE: 15_marketplace_data_sharing.sql
-- PURPOSE: Demonstrate Snowflake Marketplace data acquisition and blending
-- FEATURES SHOWCASED:
--   - Snowflake Marketplace data discovery and access
--   - Zero-copy data sharing (no ETL required)
--   - Blending third-party data with internal data
--   - Cross-database queries
-- =============================================================================

-- =============================================================================
-- ABOUT THIS DEMO
-- =============================================================================
-- We will use Weather Source data from the Snowflake Marketplace to analyze
-- how weather conditions correlate with hospital admissions. This demonstrates:
--   1. Getting free data from Marketplace (no data movement)
--   2. Instantly querying shared data alongside your own data
--   3. Building analytics that combine internal + external data sources

-- =============================================================================
-- STEP 1: GET DATA FROM SNOWFLAKE MARKETPLACE
-- =============================================================================
-- 
-- Follow these steps in Snowsight:
-- 
-- 1. Click on "Data Products" > "Marketplace" in the left navigation
-- 2. Search for "Weather Source" or "Knoema" (both offer free weather data)
-- 3. Find: "Weather Source LLC: frostbyte" (Free sample dataset)
--    OR: "Knoema Economy Data Atlas" (includes weather)
--    OR: "US Weather Events" 
-- 4. Click "Get" and follow the prompts to create a database
-- 5. The shared database will appear in your account instantly (zero-copy!)
--
-- For this demo, we'll use the common naming pattern from Weather Source:
-- Database: WEATHER_SOURCE_LLC__FROSTBYTE
-- 
-- Alternative: If Weather Source isn't available, search for any weather
-- dataset - the SQL below can be adapted to match the schema.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;

-- =============================================================================
-- STEP 2: EXPLORE THE MARKETPLACE DATA
-- =============================================================================

-- After getting the Marketplace data, explore what's available
-- (Uncomment and modify based on your actual Marketplace database name)

/*
-- List available schemas
SHOW SCHEMAS IN DATABASE WEATHER_SOURCE_LLC__FROSTBYTE;

-- List tables in the database
SHOW TABLES IN DATABASE WEATHER_SOURCE_LLC__FROSTBYTE;

-- Preview the weather data structure
SELECT * FROM WEATHER_SOURCE_LLC__FROSTBYTE.ONPOINT_ID.HISTORY_DAY LIMIT 10;
*/

-- =============================================================================
-- STEP 3: CREATE A SIMULATED WEATHER DATASET
-- =============================================================================
-- For demo purposes (if Marketplace access isn't available), we'll create
-- a sample weather table that mirrors typical Marketplace weather data.
-- In production, you would skip this and query the Marketplace data directly.

USE DATABASE HOSPITAL_DEMO;
CREATE SCHEMA IF NOT EXISTS MARKETPLACE_DATA;
USE SCHEMA MARKETPLACE_DATA;

-- Simulated weather data (mimics Marketplace weather structure)
CREATE OR REPLACE TABLE SAMPLE_WEATHER_DATA AS
WITH date_range AS (
    SELECT DATEADD(day, seq4(), '2023-01-01')::DATE AS weather_date
    FROM TABLE(GENERATOR(rowcount => 730))  -- 2 years of data
),
weather_generated AS (
    SELECT 
        weather_date,
        'BRISBANE' AS city,
        'QLD' AS state,
        'AU' AS country,
        -- Temperature (Celsius) - seasonal variation for Southern Hemisphere
        ROUND(22 + 8 * SIN((DAYOFYEAR(weather_date) - 172) * 3.14159 / 182.5) + 
              UNIFORM(-3::FLOAT, 3::FLOAT, RANDOM()), 1) AS avg_temperature_c,
        ROUND(avg_temperature_c - UNIFORM(4::FLOAT, 8::FLOAT, RANDOM()), 1) AS min_temperature_c,
        ROUND(avg_temperature_c + UNIFORM(4::FLOAT, 8::FLOAT, RANDOM()), 1) AS max_temperature_c,
        -- Humidity
        ROUND(UNIFORM(40::FLOAT, 90::FLOAT, RANDOM()), 0) AS avg_humidity_pct,
        -- Precipitation
        CASE WHEN UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) > 0.7 
             THEN ROUND(UNIFORM(1::FLOAT, 50::FLOAT, RANDOM()), 1) 
             ELSE 0 END AS precipitation_mm,
        -- Air Quality Index (higher in winter/dry periods)
        ROUND(30 + UNIFORM(0::FLOAT, 40::FLOAT, RANDOM()) + 
              CASE WHEN precipitation_mm = 0 THEN 15 ELSE 0 END, 0) AS air_quality_index,
        -- Weather condition
        CASE 
            WHEN precipitation_mm > 20 THEN 'Heavy Rain'
            WHEN precipitation_mm > 5 THEN 'Light Rain'
            WHEN avg_temperature_c > 35 THEN 'Extreme Heat'
            WHEN avg_temperature_c < 10 THEN 'Cold'
            WHEN avg_humidity_pct > 80 THEN 'Humid'
            ELSE 'Clear'
        END AS weather_condition
    FROM date_range
)
SELECT * FROM weather_generated;

-- View sample of weather data
SELECT * FROM SAMPLE_WEATHER_DATA ORDER BY weather_date DESC LIMIT 10;

-- =============================================================================
-- STEP 4: BLEND MARKETPLACE DATA WITH HOSPITAL ADMISSIONS
-- =============================================================================

USE SCHEMA ANALYTICS;

-- Create analytical view joining hospital admissions with weather data
CREATE OR REPLACE VIEW ANALYTICS.V_ADMISSIONS_WEATHER_ANALYSIS AS
SELECT 
    -- Admission data
    a.admission_date,
    a.department_code,
    d.department_name,
    a.admission_type,
    a.primary_diagnosis,
    a.length_of_stay_days,
    a.total_charges,
    
    -- Weather data (from Marketplace or simulated)
    w.avg_temperature_c,
    w.min_temperature_c,
    w.max_temperature_c,
    w.avg_humidity_pct,
    w.precipitation_mm,
    w.air_quality_index,
    w.weather_condition,
    
    -- Derived weather categories for analysis
    CASE 
        WHEN w.max_temperature_c >= 35 THEN 'Extreme Heat'
        WHEN w.max_temperature_c >= 30 THEN 'Hot'
        WHEN w.avg_temperature_c >= 20 THEN 'Warm'
        WHEN w.avg_temperature_c >= 10 THEN 'Mild'
        ELSE 'Cold'
    END AS temperature_category,
    
    CASE 
        WHEN w.precipitation_mm > 20 THEN 'Heavy Rain'
        WHEN w.precipitation_mm > 0 THEN 'Rainy'
        ELSE 'Dry'
    END AS precipitation_category,
    
    CASE 
        WHEN w.air_quality_index > 100 THEN 'Poor AQI'
        WHEN w.air_quality_index > 50 THEN 'Moderate AQI'
        ELSE 'Good AQI'
    END AS air_quality_category

FROM RAW_DATA.PATIENT_ADMISSIONS_RAW a
JOIN RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d ON a.department_code = d.department_code
LEFT JOIN MARKETPLACE_DATA.SAMPLE_WEATHER_DATA w ON a.admission_date = w.weather_date;

-- =============================================================================
-- STEP 5: ANALYTICAL QUERY - WEATHER IMPACT ON ADMISSIONS
-- =============================================================================

-- This query demonstrates the value of blending Marketplace weather data
-- with internal hospital data to uncover actionable insights

CREATE OR REPLACE VIEW ANALYTICS.V_WEATHER_ADMISSION_CORRELATION AS
SELECT 
    weather_condition,
    temperature_category,
    air_quality_category,
    
    -- Admission metrics
    COUNT(*) AS total_admissions,
    COUNT(DISTINCT admission_date) AS days_with_admissions,
    ROUND(COUNT(*) / COUNT(DISTINCT admission_date), 1) AS avg_daily_admissions,
    
    -- Department breakdown for respiratory-related admissions
    SUM(CASE WHEN department_code IN ('EMER', 'CARD', 'NEUR') THEN 1 ELSE 0 END) AS critical_dept_admissions,
    
    -- Financial impact
    ROUND(AVG(total_charges), 2) AS avg_charges_per_admission,
    ROUND(SUM(total_charges), 2) AS total_revenue,
    
    -- Length of stay
    ROUND(AVG(length_of_stay_days), 1) AS avg_length_of_stay,
    
    -- Emergency vs Elective ratio
    ROUND(100.0 * SUM(CASE WHEN admission_type = 'Emergency' THEN 1 ELSE 0 END) / COUNT(*), 1) AS emergency_pct

FROM ANALYTICS.V_ADMISSIONS_WEATHER_ANALYSIS
WHERE admission_date >= '2023-01-01'
GROUP BY weather_condition, temperature_category, air_quality_category
ORDER BY total_admissions DESC;

-- =============================================================================
-- STEP 6: VIEW THE RESULTS
-- =============================================================================

-- See how weather impacts hospital admissions
SELECT * FROM ANALYTICS.V_WEATHER_ADMISSION_CORRELATION;

-- Detailed daily analysis
SELECT 
    admission_date,
    weather_condition,
    avg_temperature_c,
    air_quality_index,
    COUNT(*) AS admissions,
    SUM(CASE WHEN admission_type = 'Emergency' THEN 1 ELSE 0 END) AS emergency_admissions,
    ROUND(AVG(total_charges), 2) AS avg_charges
FROM ANALYTICS.V_ADMISSIONS_WEATHER_ANALYSIS
GROUP BY admission_date, weather_condition, avg_temperature_c, air_quality_index
ORDER BY admission_date DESC
LIMIT 30;

-- =============================================================================
-- STEP 7: GRANT PERMISSIONS
-- =============================================================================

GRANT USAGE ON SCHEMA MARKETPLACE_DATA TO ROLE CLINICAL_ADMIN;
GRANT USAGE ON SCHEMA MARKETPLACE_DATA TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA MARKETPLACE_DATA TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA MARKETPLACE_DATA TO ROLE ANALYST;
GRANT SELECT ON ANALYTICS.V_ADMISSIONS_WEATHER_ANALYSIS TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON ANALYTICS.V_ADMISSIONS_WEATHER_ANALYSIS TO ROLE ANALYST;
GRANT SELECT ON ANALYTICS.V_WEATHER_ADMISSION_CORRELATION TO ROLE CLINICAL_ADMIN;
GRANT SELECT ON ANALYTICS.V_WEATHER_ADMISSION_CORRELATION TO ROLE ANALYST;

-- =============================================================================
-- KEY INSIGHTS FROM WEATHER + ADMISSIONS ANALYSIS
-- =============================================================================
/*
EXAMPLE INSIGHTS YOU CAN DISCOVER:

1. EXTREME HEAT CORRELATION
   - Emergency department admissions spike during extreme heat events
   - Higher rates of cardiac and dehydration-related conditions
   - Action: Pre-position staff and resources during heat waves

2. AIR QUALITY IMPACT  
   - Poor AQI days correlate with respiratory admissions
   - Longer average length of stay during poor air quality periods
   - Action: Trigger respiratory readiness protocols when AQI forecasts are bad

3. RAINY DAY PATTERNS
   - Slip-and-fall injuries increase during wet weather
   - Orthopedic department sees higher admission rates
   - Action: Adjust ED staffing based on weather forecasts

4. SEASONAL PLANNING
   - Use historical weather + admission data for capacity planning
   - Predict staffing needs based on weather forecasts
   - Optimize supply chain for weather-dependent supplies

BUSINESS VALUE:
- Zero-copy data sharing means instant access to weather data
- No ETL pipelines to build or maintain
- Always current data (provider keeps it updated)
- Pay only for queries, not storage
*/

-- =============================================================================
-- VERIFICATION
-- =============================================================================

SELECT '✅ MARKETPLACE DATA INTEGRATION COMPLETE!' AS status;
SELECT 'Weather data blended with ' || COUNT(*) || ' admission records' AS integration_summary
FROM ANALYTICS.V_ADMISSIONS_WEATHER_ANALYSIS;
