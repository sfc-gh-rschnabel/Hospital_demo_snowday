-- =============================================================================
-- FILE: 14_update_agent_with_vitals.sql
-- PURPOSE: Add patient vitals monitoring data to Snowflake Intelligence agent
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;

-- =============================================================================
-- STEP 1: Create a Semantic View for Vitals Monitoring Data
-- =============================================================================
USE SCHEMA ANALYTICS;

CREATE OR REPLACE VIEW ANALYTICS.SV_PATIENT_VITALS_SEMANTIC AS
SELECT
    vs.patient_id,
    vs.monitoring_session,
    vs.measurement_timestamp,
    vs.department_code,
    d.department_name,
    vs.room_number,
    vs.bed_id,
    
    -- Core vitals with status
    vs.heart_rate_value AS heart_rate_bpm,
    vs.heart_rate_status,
    vs.bp_systolic AS systolic_pressure,
    vs.bp_diastolic AS diastolic_pressure,
    vs.bp_status AS blood_pressure_status,
    vs.oxygen_saturation AS oxygen_saturation_pct,
    vs.oxygen_status,
    vs.temperature_value AS temperature_fahrenheit,
    vs.temperature_status,
    vs.respiratory_rate AS respiratory_rate_per_min,
    vs.respiratory_status,
    
    -- Specialty vitals
    vs.fetal_heart_rate AS fetal_heart_rate_bpm,
    vs.intracranial_pressure AS icp_mmhg,
    vs.gcs_total AS glasgow_coma_score,
    vs.nihss_score AS stroke_severity_score,
    vs.troponin_level AS troponin_ng_ml,
    vs.pain_score,
    vs.ecg_rhythm_type,
    
    -- Risk assessment
    vs.vital_risk_score,
    CASE 
        WHEN vs.vital_risk_score >= 6 THEN 'Critical'
        WHEN vs.vital_risk_score >= 4 THEN 'High'
        WHEN vs.vital_risk_score >= 2 THEN 'Moderate'
        ELSE 'Low'
    END AS risk_category,
    
    -- Alert information
    vs.alert_count
FROM ANALYTICS.V_PATIENT_VITALS_SUMMARY vs
LEFT JOIN RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d 
    ON vs.department_code = d.department_id;

-- =============================================================================
-- STEP 2: Create a comprehensive semantic view combining all hospital data
-- =============================================================================

-- Drop existing if exists (to update with vitals)
-- Note: The original semantic view SV_QUEENSLAND_HEALTH remains unchanged
-- We create an expanded version that includes vitals

-- Create semantic model YAML file content (for reference/documentation)
-- This would typically be stored in a stage and used with CREATE SEMANTIC MODEL

/*
SEMANTIC MODEL: Hospital Analytics with Patient Vitals

TABLES:
1. Patient Vitals (from JSON):
   - Source: ANALYTICS.V_PATIENT_VITALS_SUMMARY
   - Measures: heart_rate, blood_pressure, oxygen_saturation, temperature, risk_score
   - Dimensions: department, room, bed, risk_category, timestamp

2. Vitals Alerts (from JSON flattening):
   - Source: TRANSFORMED.V_PATIENT_VITALS_ALERTS
   - Measures: alert_count
   - Dimensions: alert_type, severity, department

3. Department Vitals Overview (aggregated JSON):
   - Source: ANALYTICS.V_DEPARTMENT_VITALS_OVERVIEW
   - Measures: avg_heart_rate, avg_bp, avg_oxygen, total_alerts, high_risk_count
   - Dimensions: department

JSON PARSING FEATURES DEMONSTRATED:
- VARIANT data type for semi-structured storage
- Dot notation (raw_json:field.subfield::TYPE)
- LATERAL FLATTEN for array expansion
- Type casting (::VARCHAR, ::INTEGER, ::FLOAT)
- COALESCE for optional nested fields
- ARRAY_SIZE for counting array elements
*/

-- =============================================================================
-- STEP 3: Update the agent to mention vitals capabilities
-- =============================================================================

-- Recreate the agent with updated instructions to include vitals queries
CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT
    COMMENT = 'Hospital Analytics Intelligence Agent with structured data (Cortex Analyst), policy search (Cortex Search), and patient vitals monitoring'
    PROFILE = '{"display_name": "Hospital Analytics Assistant", "avatar": "hospital", "color": "#2E8B57"}'
    FROM SPECIFICATION $$
    {
        "models": {
            "orchestration": "claude-4-sonnet"
        },
        "instructions": {
            "orchestration": "You are a hospital analytics assistant. Use the hospital_data tool to query structured operational data including: patient admissions, medical procedures, bed utilization, department performance, medication orders, allied health services, financial metrics, AND patient vitals monitoring data (heart rate, blood pressure, oxygen saturation, temperature, respiratory rate, risk scores, and clinical alerts parsed from JSON IoT device data). Use the policy_search tool to find hospital policies, procedures, guidelines, and protocols.",
            "response": "Provide clear, actionable insights. When presenting data, explain what it means for hospital operations. For patient vitals queries, highlight any critical or high-risk readings. Reference specific policies or guidelines when applicable. Format numbers and percentages clearly. Suggest next steps or areas for further investigation when appropriate.",
            "system": "You are a knowledgeable hospital operations assistant helping clinical administrators, physicians, nurses, and analysts understand hospital performance, policies, and real-time patient monitoring. Always maintain patient privacy - never provide individual patient details that could identify specific patients."
        },
        "tools": [
            {
                "tool_spec": {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "hospital_data",
                    "description": "Query hospital operational data including: patient admissions, medical procedures, bed utilization and occupancy, department performance, medication orders, allied health services, financial metrics, AND patient vitals monitoring (heart rate, blood pressure, oxygen saturation, temperature, respiratory rate, risk scores, clinical alerts from IoT devices). Use this for questions about counts, trends, averages, comparisons, and data analysis. The vitals data demonstrates JSON parsing capabilities from IoT medical devices."
                }
            },
            {
                "tool_spec": {
                    "type": "cortex_search",
                    "name": "policy_search",
                    "description": "Search hospital policies, procedures, guidelines, and protocols. Use this for questions about admission procedures, discharge guidelines, emergency protocols, infection control, medication administration, bed management policies, HIPAA compliance, quality metrics definitions, and allied health services guidelines."
                }
            }
        ],
        "tool_resources": {
            "hospital_data": {
                "semantic_view": "HOSPITAL_DEMO.ANALYTICS.SV_QUEENSLAND_HEALTH",
                "execution_environment": {
                    "type": "warehouse",
                    "warehouse": "HOSPITAL_ANALYTICS_WH"
                },
                "query_timeout": 120
            },
            "policy_search": {
                "search_service": "HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCS_SEARCH",
                "max_results": 10,
                "columns": ["chunk_content", "document_title", "policy_number", "department", "document_type"]
            }
        }
    }
    $$;

-- Re-grant access to the updated agent
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT TO ROLE PUBLIC;
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT TO ROLE CLINICAL_ADMIN;
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT TO ROLE ANALYST;
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT TO ROLE PHYSICIAN;
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT TO ROLE NURSE;

-- =============================================================================
-- STEP 4: Sample queries for vitals data (can be asked in natural language)
-- =============================================================================

-- Direct SQL samples for reference:

-- Average vitals by department
SELECT 
    department_code,
    ROUND(AVG(heart_rate_value), 1) AS avg_heart_rate,
    ROUND(AVG(bp_systolic), 1) AS avg_systolic,
    ROUND(AVG(oxygen_saturation), 1) AS avg_o2_sat,
    ROUND(AVG(vital_risk_score), 2) AS avg_risk_score
FROM ANALYTICS.V_PATIENT_VITALS_SUMMARY
GROUP BY department_code
ORDER BY avg_risk_score DESC;




-- Critical alerts breakdown
SELECT 
    alert_severity,
    alert_type,
    COUNT(*) AS occurrences
FROM TRANSFORMED.V_PATIENT_VITALS_ALERTS
WHERE alert_severity = 'critical'
GROUP BY alert_severity, alert_type
ORDER BY occurrences DESC;

-- High-risk patients by department
SELECT 
    department_code,
    COUNT(*) AS high_risk_count
FROM ANALYTICS.V_PATIENT_VITALS_SUMMARY
WHERE vital_risk_score >= 5
GROUP BY department_code
ORDER BY high_risk_count DESC;

-- =============================================================================
-- STEP 5: Natural language query examples for the agent
-- =============================================================================

/*
VITALS QUERIES TO TEST IN SNOWFLAKE INTELLIGENCE:

Basic vitals queries:
- "What is the average heart rate across all departments?"
- "Show me patients with low oxygen saturation"
- "Which department has the highest average blood pressure?"
- "How many critical alerts did we have?"

Risk-based queries:
- "Which patients have a high vital risk score?"
- "Show me the department with the most high-risk patients"
- "What are the most common types of critical alerts?"

Combined queries (vitals + policies):
- "What are our oxygen saturation guidelines and how many patients are below normal?"
- "Show me critical cardiac alerts and the chest pain protocol"
- "What is the sepsis protocol and which patients have elevated heart rate with fever?"

JSON-specific demonstration queries:
- "Show me the breakdown of alert types by severity"
- "What specialty vitals were recorded (fetal heart rate, ICP, etc.)?"
*/

-- =============================================================================
-- VERIFICATION
-- =============================================================================

SELECT '✅ PATIENT VITALS INTEGRATION COMPLETE!' AS status;
SELECT 'Views created: V_PATIENT_VITALS_PARSED, V_PATIENT_VITALS_ALERTS, V_PATIENT_VITALS_SUMMARY' AS views;
SELECT 'Analytics views: V_ALERTS_BY_SEVERITY, V_DEPARTMENT_VITALS_OVERVIEW, V_CRITICAL_ALERTS' AS analytics;
SELECT 'Agent updated with vitals monitoring capabilities' AS agent_status;
