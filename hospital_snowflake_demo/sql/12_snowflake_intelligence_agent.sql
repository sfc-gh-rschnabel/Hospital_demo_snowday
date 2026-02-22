-- ============================================================================
-- Hospital Snowflake Demo - Snowflake Intelligence Agent Setup
-- ============================================================================
-- This script creates:
-- 1. Document table for hospital policies (for Cortex Search)
-- 2. Cortex Search Service on the document table
-- 3. Snowflake Intelligence Agent combining Cortex Analyst + Cortex Search
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE HOSPITAL_DEMO;
USE WAREHOUSE HOSPITAL_ANALYTICS_WH;

-- ============================================================================
-- STEP 1: CREATE DOCUMENTS SCHEMA AND TABLE
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS HOSPITAL_DEMO.DOCUMENTS;
USE SCHEMA HOSPITAL_DEMO.DOCUMENTS;

-- Create table to store hospital policy documents
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

-- ============================================================================
-- STEP 2: INSERT HOSPITAL POLICY DOCUMENTS
-- ============================================================================
-- Note: In production, you would load these from files using COPY INTO
-- For this demo, we insert the content directly

-- Hospital Admission Policy
INSERT INTO HOSPITAL_DOCUMENTS (document_id, document_title, policy_number, department, document_type, effective_date, content, chunk_id, chunk_content)
VALUES 
('ADM-001-1', 'Hospital Admission Policy and Procedures', 'ADM-001', 'Patient Access Services', 'Policy', '2024-01-01',
'Hospital Admission Policy and Procedures. This policy establishes standardized procedures for patient admissions to ensure efficient, safe, and patient-centered care from the moment of arrival through bed assignment.',
1, 'Emergency admissions are processed through the Emergency Department (ED) and have the highest priority for bed assignment. The target time from ED decision-to-admit to bed assignment is 4 hours (NEAT compliance target 90% or higher). The process involves ED physician determining need for admission, admitting orders entered into EHR, Bed Management notification, patient registration, insurance verification, bed assignment based on clinical needs, and patient transport to assigned unit.'),

('ADM-001-2', 'Hospital Admission Policy and Procedures', 'ADM-001', 'Patient Access Services', 'Policy', '2024-01-01',
'Hospital Admission Policy and Procedures. This policy establishes standardized procedures for patient admissions.',
2, 'Elective admissions are pre-scheduled and require pre-admission testing completion, insurance pre-authorization minimum 48 hours prior, pre-operative clearance for surgical cases, and assigned admission date and time. Bed assignments are made based on clinical acuity, specialty requirements, infection control status, gender-appropriate placement, and special equipment needs.'),

('ADM-001-3', 'Hospital Admission Policy and Procedures', 'ADM-001', 'Patient Access Services', 'Policy', '2024-01-01',
'Hospital Admission Policy and Procedures. Documentation requirements.',
3, 'Required documentation includes Admission History and Physical within 24 hours by the Admitting Physician, Nursing Assessment within 4 hours, Medication Reconciliation within 4 hours by Pharmacist or RN, Fall Risk Assessment upon admission, Skin Assessment upon admission, and Nutritional Screening within 24 hours by Dietary Services.');

-- Emergency Department Protocols
INSERT INTO HOSPITAL_DOCUMENTS (document_id, document_title, policy_number, department, document_type, effective_date, content, chunk_id, chunk_content)
VALUES 
('ED-001-1', 'Emergency Department Protocols', 'ED-001', 'Emergency Medicine', 'Protocol', '2024-01-01',
'Emergency Department Protocols. To establish standardized protocols for the efficient and safe management of patients presenting to the Emergency Department.',
1, 'ESI Triage Levels: ESI Level 1 (Resuscitation) requires immediate attention for cardiac arrest, severe trauma, respiratory failure. ESI Level 2 (Emergent) has less than 10 minute target for chest pain, stroke symptoms, severe allergic reactions. ESI Level 3 (Urgent) has less than 30 minute target for abdominal pain, fractures. ESI Level 4 (Less Urgent) and Level 5 (Non-Urgent) have longer wait times for minor issues.'),

('ED-001-2', 'Emergency Department Protocols', 'ED-001', 'Emergency Medicine', 'Protocol', '2024-01-01',
'Emergency Department Protocols. NEAT Compliance and clinical pathways.',
2, 'NEAT (National Emergency Access Target) requires 90% of ED patients seen, treated, and discharged or admitted within 4 hours. Clock starts at time of ED registration and stops at time of physical departure from ED. Strategies include rapid triage, early senior physician involvement, parallel processing of diagnostics, and fast-track pathway for ESI 4-5 patients.'),

('ED-001-3', 'Emergency Department Protocols', 'ED-001', 'Emergency Medicine', 'Protocol', '2024-01-01',
'Emergency Department Protocols. Clinical pathways for specific conditions.',
3, 'Chest Pain Pathway: Immediate ECG within 10 minutes of arrival, troponin and basic metabolic panel, aspirin 324mg if not contraindicated, cardiology consult for STEMI activation, risk stratification using HEART score. Stroke Pathway: CODE STROKE activation, non-contrast CT head within 25 minutes, neurology notification, tPA consideration if within window, NIH Stroke Scale assessment. Sepsis Pathway: Blood cultures before antibiotics, lactate level, broad-spectrum antibiotics within 1 hour, fluid resuscitation 30 mL/kg crystalloid.');

-- Patient Discharge Guidelines
INSERT INTO HOSPITAL_DOCUMENTS (document_id, document_title, policy_number, department, document_type, effective_date, content, chunk_id, chunk_content)
VALUES 
('DIS-001-1', 'Patient Discharge Guidelines', 'DIS-001', 'Care Coordination', 'Guideline', '2024-01-01',
'Patient Discharge Guidelines. To ensure safe, efficient, and patient-centered discharge processes that minimize readmission risk.',
1, 'Discharge planning begins at admission with expected length of stay documented within 24 hours, discharge needs assessment completed, Case Management notified for complex patients. Discharge readiness criteria include clinical stability achieved, oral medications tolerated, ambulating at baseline, pain controlled with oral medications, follow-up appointments scheduled, and post-discharge care arranged.'),

('DIS-001-2', 'Patient Discharge Guidelines', 'DIS-001', 'Care Coordination', 'Guideline', '2024-01-01',
'Patient Discharge Guidelines. Target discharge times and high-risk patient identification.',
2, 'Target Discharge Times: Morning Discharge Target is 11:00 AM, Afternoon Discharge Target is 2:00 PM, Goal is 70% of discharges before 12:00 PM. High-risk patients requiring enhanced discharge planning include those with prior 30-day readmission, heart failure, COPD exacerbation, diabetes with complications, multiple chronic conditions (3 or more), age 65 or older with complex needs, poor social support, or history of non-compliance.'),

('DIS-001-3', 'Patient Discharge Guidelines', 'DIS-001', 'Care Coordination', 'Guideline', '2024-01-01',
'Patient Discharge Guidelines. 30-day readmission prevention.',
3, 'The 30-day all-cause readmission rate target is less than 10%. Prevention strategies include Care Transitions Program enrollment, community health worker visits, medication therapy management referral, and connection to community resources. Post-discharge telephone follow-up is required for all inpatients within 72 hours and for high-risk patients within 24-48 hours.');

-- Bed Management Procedures
INSERT INTO HOSPITAL_DOCUMENTS (document_id, document_title, policy_number, department, document_type, effective_date, content, chunk_id, chunk_content)
VALUES 
('BED-001-1', 'Bed Management Procedures', 'BED-001', 'Bed Management', 'Procedure', '2024-01-01',
'Bed Management Procedures. To optimize patient flow, maximize bed utilization, and ensure appropriate bed placement.',
1, 'Bed Utilization Targets: Green status is 70-80% occupancy with normal operations. Yellow status is 80-85% with enhanced monitoring. Orange status is 85-92% triggering capacity alert. Red status above 92% activates surge protocols. Target occupancy range is 85-92% per Queensland Health standard.'),

('BED-001-2', 'Bed Management Procedures', 'BED-001', 'Bed Management', 'Procedure', '2024-01-01',
'Bed Management Procedures. Bed types and daily rates.',
2, 'Bed Types and Daily Rates: ICU beds cost $2,500-3,500 per day with full monitoring and ventilator capability. Step-Down beds cost $1,500-2,000 with telemetry monitoring. Private rooms cost $800-1,200 for single occupancy. Semi-Private rooms cost $500-800 for shared room. Standard beds cost $400-600 for basic accommodation. Isolation rooms cost $1,000-1,500 with negative pressure and anteroom.'),

('BED-001-3', 'Bed Management Procedures', 'BED-001', 'Bed Management', 'Procedure', '2024-01-01',
'Bed Management Procedures. Daily bed huddles and surge protocols.',
3, 'Daily Bed Huddles occur at 7:00 AM, 11:00 AM, 3:00 PM, and 7:00 PM with Bed Management, Charge Nurses, House Supervisor, and Case Management. Agenda includes current census and occupancy, pending discharges by unit, pending admissions from ED and OR, potential barriers to discharge, and overflow or diversion status. Level 1 Surge at 85-90% cancels elective surgeries requiring overnight stay and accelerates discharge rounds. Level 2 Surge at 90-95% activates overflow units and considers ED ambulance diversion.');

-- Medication Administration Policy
INSERT INTO HOSPITAL_DOCUMENTS (document_id, document_title, policy_number, department, document_type, effective_date, content, chunk_id, chunk_content)
VALUES 
('MED-001-1', 'Medication Administration Policy', 'MED-001', 'Pharmacy and Nursing', 'Policy', '2024-01-01',
'Medication Administration Policy. To ensure safe, accurate, and timely medication administration to all patients.',
1, 'The Five Rights of Medication Administration: Right Patient verified with two patient identifiers (name and DOB or MRN), Right Drug verified medication name matches order, Right Dose confirmed dose calculation is correct, Right Route (oral, IV, IM, subcutaneous, topical), and Right Time within acceptable administration window.'),

('MED-001-2', 'Medication Administration Policy', 'MED-001', 'Pharmacy and Nursing', 'Policy', '2024-01-01',
'Medication Administration Policy. High-alert medications.',
2, 'High-Alert Medications requiring extra verification include: Anticoagulants (heparin, warfarin, enoxaparin, DOACs), Insulin (all formulations), Opioids (morphine, fentanyl, hydromorphone, oxycodone), Neuromuscular Blocking Agents (rocuronium, succinylcholine), Chemotherapy (all cytotoxic agents), Concentrated Electrolytes (potassium chloride, hypertonic saline), and IV Sedatives (propofol, midazolam, ketamine). Protocol requires independent double-check by two licensed personnel, barcode verification at bedside, patient weight verification for weight-based dosing.'),

('MED-001-3', 'Medication Administration Policy', 'MED-001', 'Pharmacy and Nursing', 'Policy', '2024-01-01',
'Medication Administration Policy. Controlled substances and medication reconciliation.',
3, 'Controlled Substance Management: Schedule II-V medications stored in locked automated dispensing cabinet (ADC), waste witnessed and documented, count verified at shift change, discrepancies reported immediately, random audits conducted monthly. Medication Reconciliation at Admission: Obtain complete medication history, verify with pharmacy records, pill bottles, and caregivers, document home medications in EHR, reconcile with admission orders, address discrepancies with provider.');

-- Infection Control Guidelines
INSERT INTO HOSPITAL_DOCUMENTS (document_id, document_title, policy_number, department, document_type, effective_date, content, chunk_id, chunk_content)
VALUES 
('IC-001-1', 'Infection Control Guidelines', 'IC-001', 'Infection Prevention and Control', 'Guideline', '2024-01-01',
'Infection Control Guidelines. To prevent the transmission of healthcare-associated infections (HAIs).',
1, 'Standard Precautions apply to ALL patient care. The Five Moments for Hand Hygiene (WHO): Before touching a patient, Before clean or aseptic procedure, After body fluid exposure risk, After touching a patient, After touching patient surroundings. Hand Hygiene Methods: Alcohol-based hand rub for 20 seconds minimum, Soap and water for 40-60 seconds when visibly soiled, Before and after glove use.'),

('IC-001-2', 'Infection Control Guidelines', 'IC-001', 'Infection Prevention and Control', 'Guideline', '2024-01-01',
'Infection Control Guidelines. Transmission-based precautions.',
2, 'Contact Precautions for MRSA, VRE, C. difficile, scabies, draining wounds require private room preferred, gown and gloves for all room entry, dedicated patient care equipment, enhanced environmental cleaning. Droplet Precautions for Influenza, RSV, pertussis, meningococcal disease require surgical mask within 6 feet of patient. Airborne Precautions for Tuberculosis, measles, varicella, COVID-19 require negative pressure airborne infection isolation room, N95 respirator for all room entry, door must remain closed.'),

('IC-001-3', 'Infection Control Guidelines', 'IC-001', 'Infection Prevention and Control', 'Guideline', '2024-01-01',
'Infection Control Guidelines. Surveillance metrics and targets.',
3, 'Surveillance Metrics and Targets: Central Line-Associated BSI (CLABSI) target is 0 with national benchmark 0.8 per 1,000 central line days. Catheter-Associated UTI (CAUTI) target is 0 with national benchmark 1.1 per 1,000 catheter days. C. difficile Rate target is below national benchmark of 6.8 per 10,000 patient days. Hand Hygiene Compliance target is above 90%. All metrics reported monthly to Quality Committee.');

-- Patient Privacy and HIPAA Policy
INSERT INTO HOSPITAL_DOCUMENTS (document_id, document_title, policy_number, department, document_type, effective_date, content, chunk_id, chunk_content)
VALUES 
('HIPAA-001-1', 'Patient Privacy and HIPAA Compliance Policy', 'HIPAA-001', 'Privacy and Compliance', 'Policy', '2024-01-01',
'Patient Privacy and HIPAA Compliance Policy. To ensure compliance with HIPAA and protect the privacy and security of all protected health information.',
1, 'Protected Health Information (PHI) includes any individually identifiable health information relating to past, present, or future physical or mental health condition, healthcare provided to an individual, or payment for healthcare. The 18 HIPAA Identifiers include names, geographic data smaller than state, dates except year, phone numbers, fax numbers, email addresses, Social Security numbers, medical record numbers, health plan beneficiary numbers, account numbers, and more.'),

('HIPAA-001-2', 'Patient Privacy and HIPAA Compliance Policy', 'HIPAA-001', 'Privacy and Compliance', 'Policy', '2024-01-01',
'Patient Privacy and HIPAA Compliance Policy. Patient rights.',
2, 'Patient Rights under HIPAA: Right to Access allows patients to request copies of medical records with response within 30 days. Right to Amendment allows patients to request amendment of records with response within 60 days. Right to Accounting of Disclosures covers 6 years prior to request. Right to Request Restrictions allows patients to restrict uses and disclosures. Right to Confidential Communications allows alternative communication methods.'),

('HIPAA-001-3', 'Patient Privacy and HIPAA Compliance Policy', 'HIPAA-001', 'Privacy and Compliance', 'Policy', '2024-01-01',
'Patient Privacy and HIPAA Compliance Policy. Breach notification.',
3, 'Breach Notification Requirements: For less than 500 affected individuals, individual notice within 60 days of discovery. For 500 or more affected individuals, individual notice plus media notification plus HHS notification without unreasonable delay. All breaches require annual log to HHS by February 28. Breach Response Process includes discovery and containment, risk assessment, notification determination, individual notification, media notification if applicable, HHS notification, and documentation and remediation.');

-- Quality Metrics Definitions
INSERT INTO HOSPITAL_DOCUMENTS (document_id, document_title, policy_number, department, document_type, effective_date, content, chunk_id, chunk_content)
VALUES 
('QM-001-1', 'Hospital Quality Metrics and KPI Definitions', 'QM-001', 'Quality Improvement', 'Reference', '2024-01-01',
'Hospital Quality Metrics and KPI Definitions. To define and standardize quality metrics and Key Performance Indicators.',
1, 'Patient Safety Metrics: CLABSI (Central Line-Associated Bloodstream Infection) calculated as Number of CLABSIs divided by Central Line Days times 1,000 with target of 0 and national benchmark of 0.8. CAUTI (Catheter-Associated UTI) calculated similarly with target of 0 and benchmark of 1.1. SSI (Surgical Site Infection) is infection at or near surgical incision within 30 days or 90 days for implants. Patient Falls target is less than 3.0 per 1,000 patient days. Medication Error Rate target is less than 0.5%.'),

('QM-001-2', 'Hospital Quality Metrics and KPI Definitions', 'QM-001', 'Quality Improvement', 'Reference', '2024-01-01',
'Hospital Quality Metrics and KPI Definitions. Clinical outcome metrics.',
2, 'Clinical Outcome Metrics: Risk-Adjusted Mortality Rate calculated as Observed mortality divided by Expected mortality with target less than 1.0. AMI Mortality target less than 12% (national average 11.8%). Heart Failure Mortality target less than 10% (national average 9.5%). 30-Day All-Cause Readmission target less than 10% (national benchmark 15.5%). Heart Failure Readmission target less than 20% (national average 21.9%).'),

('QM-001-3', 'Hospital Quality Metrics and KPI Definitions', 'QM-001', 'Quality Improvement', 'Reference', '2024-01-01',
'Hospital Quality Metrics and KPI Definitions. Operational efficiency metrics.',
3, 'Operational Efficiency Metrics: ALOS (Average Length of Stay) calculated as Total patient days divided by Total discharges with target within Case Mix Index adjusted benchmark. NEAT Compliance (4-Hour Target) is percentage of ED patients with total ED time 4 hours or less with target 90% or higher. Left Without Being Seen (LWBS) target less than 2%. Bed Occupancy Rate target 85-92%. OR Utilization target 80-85%. First Case On-Time Start target 85% or higher. Discharge Before Noon target 70% or higher.');

-- Allied Health Services Guidelines
INSERT INTO HOSPITAL_DOCUMENTS (document_id, document_title, policy_number, department, document_type, effective_date, content, chunk_id, chunk_content)
VALUES 
('AH-001-1', 'Allied Health Services Guidelines', 'AH-001', 'Allied Health Services', 'Guideline', '2024-01-01',
'Allied Health Services Guidelines. Guidelines for the delivery of allied health services including physical therapy, occupational therapy, speech therapy, and respiratory therapy.',
1, 'Allied Health Disciplines: Physical Therapy (PT) scope includes mobility assessment and training, gait training and assistive device fitting, therapeutic exercise prescription, pain management through physical modalities, post-surgical rehabilitation, and fall prevention programs. Credentials required are Licensed Physical Therapist (PT) or Physical Therapist Assistant (PTA). Occupational Therapy (OT) scope includes activities of daily living training, upper extremity rehabilitation, cognitive rehabilitation, adaptive equipment assessment, and home safety evaluation.'),

('AH-001-2', 'Allied Health Services Guidelines', 'AH-001', 'Allied Health Services', 'Guideline', '2024-01-01',
'Allied Health Services Guidelines. Speech-Language Pathology and Respiratory Therapy.',
2, 'Speech-Language Pathology (SLP) scope includes swallowing assessment and dysphagia management, speech and language rehabilitation, cognitive-communication therapy, voice therapy, and tracheostomy and ventilator communication. Credentials required are Licensed Speech-Language Pathologist with Certificate of Clinical Competence (CCC-SLP). Respiratory Therapy (RT) scope includes oxygen therapy management, mechanical ventilation, airway management, pulmonary function testing, and bronchial hygiene therapy.'),

('AH-001-3', 'Allied Health Services Guidelines', 'AH-001', 'Allied Health Services', 'Guideline', '2024-01-01',
'Allied Health Services Guidelines. Referral process and treatment standards.',
3, 'Referral Priority Levels: Emergent response within 1 hour for airway issues or acute aspiration risk. Urgent response within 4 hours for ICU patients or post-surgical day 0. Routine response within 24 hours for new admissions and scheduled evaluations. Treatment Frequency in Acute Care is 1 time daily, 5-7 days per week. Treatment Duration: PT evaluation 45-60 minutes, PT treatment 30-45 minutes, OT evaluation 45-60 minutes, OT treatment 30-45 minutes, SLP evaluation 30-45 minutes, SLP treatment 30 minutes.');

-- Department Specializations Guide
INSERT INTO HOSPITAL_DOCUMENTS (document_id, document_title, policy_number, department, document_type, effective_date, content, chunk_id, chunk_content)
VALUES 
('DEPT-001-1', 'Department Specializations and Services Guide', 'DEPT-001', 'Hospital Administration', 'Reference', '2024-01-01',
'Department Specializations and Services Guide. Hospital Department Directory.',
1, 'Cardiology Department (CARD) on Floor 2, Extension 2201, headed by Dr. Sarah Chen. Services include cardiac catheterization and angioplasty, electrophysiology studies and ablation, pacemaker and ICD implantation, heart failure management, cardiac rehabilitation, and non-invasive cardiac testing. Bed capacity is 30 beds, specialization type is Cardiac Care, annual budget is $2,500,000 with 25 staff. Common diagnoses include myocardial infarction, congestive heart failure, atrial fibrillation, angina pectoris, and cardiomyopathy.'),

('DEPT-001-2', 'Department Specializations and Services Guide', 'DEPT-001', 'Hospital Administration', 'Reference', '2024-01-01',
'Department Specializations and Services Guide. Emergency and Surgical departments.',
2, 'Emergency Department (EMER) on Floor 1, Extension 1101, headed by Dr. Michael Torres. Services include 24/7 emergency care, Level II trauma services, chest pain evaluation, stroke assessment, and pediatric emergencies. Bed capacity is 15 treatment bays, annual budget is $3,200,000 with 45 staff. Orthopedics Department (ORTH) on Floor 3, Extension 3301, headed by Dr. Jennifer Park. Services include joint replacement, fracture repair, sports medicine, spine surgery, and arthroscopy. Bed capacity is 25 beds, annual budget is $1,800,000 with 20 staff.'),

('DEPT-001-3', 'Department Specializations and Services Guide', 'DEPT-001', 'Hospital Administration', 'Reference', '2024-01-01',
'Department Specializations and Services Guide. Medical specialties overview.',
3, 'Neurology Department (NEUR) on Floor 5, Extension 5501, headed by Dr. Robert Kim, specializes in stroke care, epilepsy management, and movement disorders with 20 beds and $2,800,000 budget. Oncology Department (ONCO) on Floor 4, Extension 4402, headed by Dr. Patricia Davis, provides chemotherapy, radiation therapy, surgical oncology, and bone marrow transplant with 20 beds and $3,500,000 budget. Pediatrics Department (PEDI) on Floor 2, Extension 2202, headed by Dr. Mark Thompson, offers general pediatric care and neonatal care with 25 beds and $2,200,000 budget. Total hospital capacity is 180+ beds with 260+ healthcare professionals and combined annual budget of $23,100,000.');

-- ============================================================================
-- STEP 3: CREATE CORTEX SEARCH SERVICE
-- ============================================================================

-- Create search service on the document chunks
CREATE OR REPLACE CORTEX SEARCH SERVICE HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCS_SEARCH
    ON chunk_content
    WAREHOUSE = HOSPITAL_ANALYTICS_WH
    TARGET_LAG = '1 hour'
    AS (
        SELECT 
            document_id,
            document_title,
            policy_number,
            department,
            document_type,
            effective_date,
            chunk_id,
            chunk_content
        FROM HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCUMENTS
    );

-- Grant permissions on the search service
GRANT USAGE ON CORTEX SEARCH SERVICE HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCS_SEARCH TO ROLE CLINICAL_ADMIN;
GRANT USAGE ON CORTEX SEARCH SERVICE HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCS_SEARCH TO ROLE ANALYST;
GRANT USAGE ON CORTEX SEARCH SERVICE HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCS_SEARCH TO ROLE PHYSICIAN;
GRANT USAGE ON CORTEX SEARCH SERVICE HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCS_SEARCH TO ROLE NURSE;
GRANT USAGE ON CORTEX SEARCH SERVICE HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCS_SEARCH TO ROLE DATA_ENGINEER;

-- ============================================================================
-- STEP 4: CREATE SNOWFLAKE INTELLIGENCE AGENT
-- ============================================================================

-- Create the Snowflake Intelligence database and schema (required location)
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE PUBLIC;

CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;
GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE PUBLIC;

-- Grant CREATE AGENT permission
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE ACCOUNTADMIN;

-- Create the Hospital Intelligence Agent
CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT
    COMMENT = 'Hospital Analytics Intelligence Agent combining structured data queries (Cortex Analyst) with policy/procedure search (Cortex Search)'
    PROFILE = '{"display_name": "Hospital Analytics Assistant", "avatar": "hospital", "color": "#2E8B57"}'
    FROM SPECIFICATION $$
    {
        "models": {
            "orchestration": "claude-4-sonnet"
        },
        "instructions": {
            "orchestration": "You are a hospital analytics assistant. Use the hospital_data tool to query structured operational data like admissions, procedures, bed utilization, patient demographics, and clinical metrics. Use the policy_search tool to find hospital policies, procedures, guidelines, and protocols. When asked about targets, KPIs, or best practices, search the policy documents. When asked for specific numbers or data analysis, use the structured data tool.",
            "response": "Provide clear, actionable insights. When presenting data, explain what it means for hospital operations. Reference specific policies or guidelines when applicable. Format numbers and percentages clearly. Suggest next steps or areas for further investigation when appropriate.",
            "system": "You are a knowledgeable hospital operations assistant helping clinical administrators, physicians, nurses, and analysts understand hospital performance and policies. Always maintain patient privacy - never provide individual patient details."
        },
        "tools": [
            {
                "tool_spec": {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "hospital_data",
                    "description": "Query hospital operational data including patient admissions, medical procedures, bed utilization and occupancy, department performance, medication orders, allied health services, and financial metrics. Use this for questions about counts, trends, averages, comparisons, and data analysis."
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

-- Grant access to the agent
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT TO ROLE PUBLIC;
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT TO ROLE CLINICAL_ADMIN;
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT TO ROLE ANALYST;
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT TO ROLE PHYSICIAN;
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT TO ROLE NURSE;

-- ============================================================================
-- STEP 5: VERIFY SETUP
-- ============================================================================

-- Verify documents loaded
SELECT COUNT(*) as document_count, COUNT(DISTINCT document_title) as unique_documents
FROM HOSPITAL_DEMO.DOCUMENTS.HOSPITAL_DOCUMENTS;

-- Verify search service
SHOW CORTEX SEARCH SERVICES IN SCHEMA HOSPITAL_DEMO.DOCUMENTS;

-- Verify agent created
SHOW AGENTS IN SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS;

-- Describe the agent
DESCRIBE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.HOSPITAL_INTELLIGENCE_AGENT;

-- ============================================================================
-- STEP 6: TEST QUERIES (Manual testing in Snowflake Intelligence UI)
-- ============================================================================

-- Test Cortex Analyst queries (these would be asked in natural language in the agent):
-- "What is the average length of stay by department?"
-- "How many emergency admissions did we have last month?"
-- "What is our bed occupancy rate?"
-- "Show me the top 5 departments by total revenue"
-- "What is our NEAT compliance rate?"

-- Test Cortex Search queries (these would be asked in natural language in the agent):
-- "What is the NEAT target and how is it measured?"
-- "What are the Five Rights of medication administration?"
-- "What infection control precautions are needed for MRSA patients?"
-- "What is our readmission rate target?"
-- "What is the discharge before noon target?"

-- Combined queries that use both tools:
-- "What is our current bed occupancy rate and what does the policy say is the target?"
-- "How many CLABSI infections did we have and what are the prevention guidelines?"
-- "What is the average discharge time and how does it compare to our policy targets?"

-- ============================================================================
-- COMPLETION MESSAGE
-- ============================================================================

SELECT '✅ SNOWFLAKE INTELLIGENCE AGENT SETUP COMPLETE!' as status_message;
SELECT '🏥 Hospital Intelligence Agent created in SNOWFLAKE_INTELLIGENCE.AGENTS schema' as agent_location;
SELECT '📊 Cortex Analyst: Connected to SV_QUEENSLAND_HEALTH semantic view' as analyst_tool;
SELECT '🔍 Cortex Search: Connected to HOSPITAL_DOCS_SEARCH service with 30 document chunks' as search_tool;
SELECT '🚀 Access the agent at: AI & ML > Snowflake Intelligence in Snowsight' as access_instructions;
