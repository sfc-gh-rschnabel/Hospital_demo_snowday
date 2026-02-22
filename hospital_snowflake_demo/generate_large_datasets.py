#!/usr/bin/env python3
"""
Hospital Snowflake Demo - Dataset Generator
Generates 10,000 patients (70% active, 30% historic) with associated transactions and bed management data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
import csv

# Set random seeds for reproducibility
np.random.seed(42)
random.seed(42)
fake = Faker()
Faker.seed(42)

# Configuration
NUM_PATIENTS = 10000  # Realistic patient population for demo
ADMISSION_RATE = 0.25  # 25% of patients have admissions in the period (higher rate for smaller population)
AVG_ADMISSIONS_PER_PATIENT = 1.4
PROCEDURE_RATE = 0.6  # 60% of admissions have procedures
AVG_PROCEDURES_PER_ADMISSION = 1.8
MEDICATION_RATE = 0.85  # 85% of admissions have medications
AVG_MEDICATIONS_PER_ADMISSION = 3.2
ALLIED_HEALTH_RATE = 0.4  # 40% of admissions have allied health services
AVG_ALLIED_SERVICES_PER_ADMISSION = 2.1

# Current vs Historical patient configuration
CURRENT_ADMISSION_RATE = 0.05  # 5% of patients currently admitted
HISTORICAL_PERIOD_YEARS = 2  # 2 years of historical data

print(f"Generating hospital data for {NUM_PATIENTS:,} patients...")

# Department data
departments = [
    ('CARD', 'Cardiology', 'Dr. Sarah Chen', '2', '2201', 2500000, 25, 30, 'Cardiac Care'),
    ('EMER', 'Emergency Department', 'Dr. Michael Torres', '1', '1101', 3200000, 45, 15, 'Emergency Medicine'),
    ('ORTH', 'Orthopedics', 'Dr. Jennifer Park', '3', '3301', 1800000, 20, 25, 'Bone and Joint'),
    ('OBGY', 'Obstetrics & Gynecology', 'Dr. Amanda Rodriguez', '4', '4401', 2100000, 30, 20, 'Women\'s Health'),
    ('NEUR', 'Neurology', 'Dr. Robert Kim', '5', '5501', 2800000, 22, 20, 'Neurological Care'),
    ('GAST', 'Gastroenterology', 'Dr. Lisa Wang', '3', '3302', 1600000, 18, 15, 'Digestive Health'),
    ('PEDI', 'Pediatrics', 'Dr. Mark Thompson', '2', '2202', 2200000, 28, 25, 'Child Care'),
    ('ONCO', 'Oncology', 'Dr. Patricia Davis', '4', '4402', 3500000, 35, 20, 'Cancer Treatment'),
    ('PSYC', 'Psychiatry', 'Dr. James Wilson', '1', '1102', 1400000, 15, 10, 'Mental Health'),
    ('RADI', 'Radiology', 'Dr. Maria Gonzalez', 'B1', '0101', 2000000, 20, 0, 'Medical Imaging'),
    ('PULM', 'Pulmonology', 'Dr. David Lee', '3', '3303', 1900000, 20, 18, 'Respiratory Care'),
    ('ENDO', 'Endocrinology', 'Dr. Susan Miller', '2', '2203', 1700000, 15, 12, 'Hormonal Disorders'),
    ('DERM', 'Dermatology', 'Dr. Kevin Brown', '1', '1103', 1300000, 12, 8, 'Skin Care'),
    ('UROL', 'Urology', 'Dr. Rachel Green', '3', '3304', 1650000, 18, 16, 'Urinary System'),
    ('RHEU', 'Rheumatology', 'Dr. Thomas White', '2', '2204', 1450000, 14, 10, 'Joint Disorders'),
    ('PHAR', 'Pharmacy', 'Dr. Michelle Adams', 'B1', '0102', 1200000, 25, 0, 'Medication Management'),
    ('PHYS', 'Physical Therapy', 'Sarah Johnson PT', '1', '1104', 900000, 20, 0, 'Rehabilitation'),
    ('OCCU', 'Occupational Therapy', 'Mark Thompson OT', '1', '1105', 800000, 15, 0, 'Occupational Health'),
    ('RESP', 'Respiratory Therapy', 'Lisa Chen RRT', '2', '2205', 750000, 18, 0, 'Breathing Support'),
    ('NUTR', 'Nutrition Services', 'Jennifer Martinez RD', '1', '1106', 600000, 12, 0, 'Dietary Services'),
    ('SOCI', 'Social Services', 'David Kim MSW', '1', '1107', 500000, 8, 0, 'Patient Support')
]

# Insurance providers
insurance_providers = ['Blue Cross', 'Aetna', 'Cigna', 'United Healthcare', 'Medicare', 'Medicaid', 'Humana', 'Kaiser Permanente']

# Cities in Massachusetts
ma_cities = [
    'Boston', 'Cambridge', 'Somerville', 'Newton', 'Brookline', 'Quincy', 'Lynn', 'Lowell',
    'Worcester', 'Springfield', 'New Bedford', 'Brockton', 'Fall River', 'Lawrence', 'Malden',
    'Medford', 'Waltham', 'Framingham', 'Arlington', 'Lexington', 'Watertown', 'Belmont',
    'Everett', 'Revere', 'Chelsea', 'Winthrop', 'Nahant', 'Peabody', 'Salem', 'Beverly'
]

# Weather conditions with probabilities
weather_conditions = [
    ('Sunny', 0.30), ('Cloudy', 0.25), ('Rainy', 0.20), ('Snowy', 0.15), 
    ('Partly Cloudy', 0.10)
]

# Medical conditions by department
medical_conditions = {
    'CARD': ['Myocardial Infarction', 'Angina', 'Atrial Fibrillation', 'Congestive Heart Failure', 'Hypertension', 'Coronary Artery Disease'],
    'EMER': ['Trauma', 'Chest Pain', 'Abdominal Pain', 'Motor Vehicle Accident', 'Drug Overdose', 'Allergic Reaction'],
    'ORTH': ['Fracture', 'Osteoarthritis', 'Hip Replacement', 'Knee Replacement', 'Spinal Surgery', 'Joint Dislocation'],
    'OBGY': ['Normal Delivery', 'Cesarean Section', 'Preeclampsia', 'Pregnancy Complications', 'Gynecological Surgery'],
    'NEUR': ['Stroke', 'Epilepsy', 'Migraine', 'Dementia', 'Parkinson\'s Disease', 'Multiple Sclerosis'],
    'GAST': ['Appendicitis', 'Gallbladder Surgery', 'Hernia Repair', 'Gastritis', 'Inflammatory Bowel Disease'],
    'PEDI': ['Bronchiolitis', 'Pneumonia', 'Gastroenteritis', 'Asthma', 'Ear Infection', 'Vaccination'],
    'ONCO': ['Breast Cancer', 'Lung Cancer', 'Colon Cancer', 'Chemotherapy', 'Radiation Therapy'],
    'PSYC': ['Depression', 'Anxiety Disorder', 'Bipolar Disorder', 'Substance Abuse', 'PTSD'],
    'RADI': ['CT Scan', 'MRI', 'X-Ray', 'Ultrasound', 'Mammography'],
    'PULM': ['Pneumonia', 'COPD', 'Asthma', 'Pulmonary Embolism', 'Sleep Apnea'],
    'ENDO': ['Diabetes', 'Thyroid Disorder', 'Adrenal Disorder', 'Metabolic Syndrome'],
    'DERM': ['Skin Cancer', 'Eczema', 'Psoriasis', 'Dermatitis', 'Melanoma'],
    'UROL': ['Kidney Stones', 'Prostate Surgery', 'Bladder Cancer', 'Urinary Tract Infection'],
    'RHEU': ['Rheumatoid Arthritis', 'Lupus', 'Fibromyalgia', 'Gout', 'Osteoporosis'],
    'PHAR': ['Medication Counseling', 'Drug Interaction Review', 'Dosage Adjustment', 'Pharmacy Consultation'],
    'PHYS': ['Physical Assessment', 'Mobility Training', 'Strength Training', 'Balance Training'],
    'OCCU': ['Activities of Daily Living', 'Cognitive Assessment', 'Work Hardening', 'Adaptive Equipment'],
    'RESP': ['Breathing Exercises', 'Ventilator Weaning', 'Oxygen Therapy', 'Pulmonary Rehabilitation'],
    'NUTR': ['Nutritional Assessment', 'Diet Planning', 'Diabetes Education', 'Weight Management'],
    'SOCI': ['Discharge Planning', 'Family Counseling', 'Resource Coordination', 'Crisis Intervention']
}

# Medications database
medications = [
    ('MED001', 'Lisinopril', 'ACE Inhibitor', 'Cardiovascular', 'Tablet', '10mg', 15.50),
    ('MED002', 'Metformin', 'Antidiabetic', 'Endocrine', 'Tablet', '500mg', 12.25),
    ('MED003', 'Atorvastatin', 'Statin', 'Cardiovascular', 'Tablet', '20mg', 28.75),
    ('MED004', 'Omeprazole', 'Proton Pump Inhibitor', 'Gastrointestinal', 'Capsule', '20mg', 22.00),
    ('MED005', 'Amlodipine', 'Calcium Channel Blocker', 'Cardiovascular', 'Tablet', '5mg', 18.50),
    ('MED006', 'Simvastatin', 'Statin', 'Cardiovascular', 'Tablet', '40mg', 25.75),
    ('MED007', 'Levothyroxine', 'Thyroid Hormone', 'Endocrine', 'Tablet', '50mcg', 35.25),
    ('MED008', 'Azithromycin', 'Antibiotic', 'Anti-Infective', 'Tablet', '250mg', 45.00),
    ('MED009', 'Prednisone', 'Corticosteroid', 'Anti-Inflammatory', 'Tablet', '10mg', 20.50),
    ('MED010', 'Warfarin', 'Anticoagulant', 'Cardiovascular', 'Tablet', '5mg', 32.75),
    ('MED011', 'Insulin Glargine', 'Long-Acting Insulin', 'Endocrine', 'Injection', '100units/mL', 125.00),
    ('MED012', 'Morphine', 'Opioid Analgesic', 'Pain Management', 'Injection', '10mg/mL', 85.50),
    ('MED013', 'Furosemide', 'Loop Diuretic', 'Cardiovascular', 'Tablet', '40mg', 16.25),
    ('MED014', 'Hydrocodone', 'Opioid Analgesic', 'Pain Management', 'Tablet', '5mg', 55.75),
    ('MED015', 'Sertraline', 'SSRI Antidepressant', 'Psychiatric', 'Tablet', '50mg', 42.00),
    ('MED016', 'Gabapentin', 'Anticonvulsant', 'Neurological', 'Capsule', '300mg', 38.25),
    ('MED017', 'Clopidogrel', 'Antiplatelet', 'Cardiovascular', 'Tablet', '75mg', 65.50),
    ('MED018', 'Metoprolol', 'Beta Blocker', 'Cardiovascular', 'Tablet', '50mg', 19.75),
    ('MED019', 'Pantoprazole', 'Proton Pump Inhibitor', 'Gastrointestinal', 'Tablet', '40mg', 28.00),
    ('MED020', 'Tramadol', 'Analgesic', 'Pain Management', 'Tablet', '50mg', 33.50)
]

# Allied health services
allied_health_services = [
    ('PHYS001', 'Initial Physical Therapy Assessment', 'Assessment', 60, 150.00),
    ('PHYS002', 'Therapeutic Exercise', 'Treatment', 45, 120.00),
    ('PHYS003', 'Manual Therapy', 'Treatment', 30, 135.00),
    ('PHYS004', 'Gait Training', 'Treatment', 45, 125.00),
    ('OCCU001', 'Occupational Therapy Evaluation', 'Assessment', 60, 160.00),
    ('OCCU002', 'Activities of Daily Living Training', 'Treatment', 45, 130.00),
    ('OCCU003', 'Cognitive Rehabilitation', 'Treatment', 60, 145.00),
    ('OCCU004', 'Adaptive Equipment Training', 'Treatment', 30, 110.00),
    ('RESP001', 'Respiratory Assessment', 'Assessment', 30, 125.00),
    ('RESP002', 'Breathing Exercise Training', 'Treatment', 30, 100.00),
    ('RESP003', 'Ventilator Weaning', 'Treatment', 60, 200.00),
    ('RESP004', 'Oxygen Therapy Management', 'Treatment', 15, 85.00),
    ('NUTR001', 'Nutritional Assessment', 'Assessment', 45, 140.00),
    ('NUTR002', 'Diet Education', 'Education', 30, 95.00),
    ('NUTR003', 'Diabetes Nutrition Counseling', 'Education', 45, 120.00),
    ('NUTR004', 'Weight Management Program', 'Treatment', 30, 105.00),
    ('SOCI001', 'Social Work Assessment', 'Assessment', 45, 135.00),
    ('SOCI002', 'Discharge Planning', 'Coordination', 30, 110.00),
    ('SOCI003', 'Family Counseling', 'Counseling', 60, 150.00),
    ('SOCI004', 'Resource Coordination', 'Coordination', 30, 100.00)
]

def generate_patient_demographics():
    """Generate patient demographics data"""
    print("Generating patient demographics...")
    
    patients = []
    for i in range(NUM_PATIENTS):
        if i % 1000 == 0:
            print(f"  Generated {i:,} patients...")
            
        patient_id = f"PAT{i+1:06d}"
        gender = random.choice(['M', 'F'])
        
        if gender == 'M':
            first_name = fake.first_name_male()
        else:
            first_name = fake.first_name_female()
            
        last_name = fake.last_name()
        
        # Age distribution: more realistic hospital demographics
        age_group = random.choices(['pediatric', 'young_adult', 'adult', 'senior'], 
                                 weights=[0.15, 0.25, 0.35, 0.25])[0]
        
        if age_group == 'pediatric':
            birth_year = random.randint(2010, 2024)
        elif age_group == 'young_adult':
            birth_year = random.randint(1995, 2005)
        elif age_group == 'adult':
            birth_year = random.randint(1970, 1994)
        else:  # senior
            birth_year = random.randint(1940, 1969)
            
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)  # Safe day range
        date_of_birth = f"{birth_year}-{birth_month:02d}-{birth_day:02d}"
        
        city = random.choice(ma_cities)
        zip_codes = {
            'Boston': ['02101', '02102', '02103', '02104', '02105'],
            'Cambridge': ['02138', '02139', '02140', '02141', '02142'],
            'Somerville': ['02143', '02144', '02145'],
            'Newton': ['02458', '02459', '02460', '02461', '02462'],
            'Brookline': ['02445', '02446', '02447']
        }
        
        if city in zip_codes:
            zip_code = random.choice(zip_codes[city])
        else:
            zip_code = f"0{random.randint(2100, 2799)}"
        
        # Determine patient status (Active vs Historic)
        # Active: 70% of patients (have recent activity or current admissions)
        # Historic: 30% of patients (no recent activity, historical records only)
        patient_status = random.choices(['Active', 'Historic'], weights=[0.7, 0.3])[0]
        
        # Registration date - Active patients registered more recently
        if patient_status == 'Active':
            # Active patients registered within last 3 years
            registration_days_ago = random.randint(1, 1095)  # 1 day to 3 years
        else:
            # Historic patients registered 3-10 years ago
            registration_days_ago = random.randint(1095, 3650)  # 3 to 10 years ago
            
        registration_date = datetime.now() - timedelta(days=registration_days_ago)
        
        # Last visit date
        if patient_status == 'Active':
            # Active patients have visited within last 2 years
            last_visit_days_ago = random.randint(1, 730)  # 1 day to 2 years
        else:
            # Historic patients haven't visited in 2+ years
            last_visit_days_ago = random.randint(730, 2190)  # 2 to 6 years ago
            
        last_visit_date = datetime.now() - timedelta(days=last_visit_days_ago)
            
        patients.append({
            'patient_id': patient_id,
            'first_name': first_name,
            'last_name': last_name,
            'date_of_birth': date_of_birth,
            'gender': gender,
            'address': fake.street_address(),
            'city': city,
            'state': 'MA',
            'zip_code': zip_code,
            'phone': fake.phone_number()[:12],  # Limit length
            'email': f"{first_name.lower()}.{last_name.lower()}@email.com",
            'insurance_provider': random.choice(insurance_providers),
            'emergency_contact_name': fake.name(),
            'emergency_contact_phone': fake.phone_number()[:12],
            'patient_status': patient_status,
            'registration_date': registration_date.strftime('%Y-%m-%d'),
            'last_visit_date': last_visit_date.strftime('%Y-%m-%d'),
            'is_active_patient': patient_status == 'Active'
        })
    
    print(f"Generated {len(patients):,} patient records")
    print(f"  Active patients: {len([p for p in patients if p['patient_status'] == 'Active']):,}")
    print(f"  Historic patients: {len([p for p in patients if p['patient_status'] == 'Historic']):,}")
    return patients

def generate_admissions_and_procedures(patients):
    """Generate admissions and procedures data"""
    print("Generating admissions and procedures...")
    
    admissions = []
    procedures = []
    
    # Date range for admissions (2 years of historical data)
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    current_date = datetime(2024, 12, 15)  # Simulate current date
    date_range = (end_date - start_date).days
    
    # Split patients into current and historical based on their patient_status
    active_patients = [p for p in patients if p['patient_status'] == 'Active']
    historic_patients = [p for p in patients if p['patient_status'] == 'Historic']
    
    # Current admissions come primarily from active patients
    current_patients = random.sample(active_patients, min(int(len(active_patients) * CURRENT_ADMISSION_RATE), len(active_patients)))
    
    admission_counter = 1
    procedure_counter = 1
    
    # Active patients have higher admission rates than historic patients
    active_admission_rate = 0.35  # 35% of active patients have admissions
    historic_admission_rate = 0.15  # 15% of historic patients have historical admissions
    
    active_patients_with_admissions = random.sample(active_patients, int(len(active_patients) * active_admission_rate))
    historic_patients_with_admissions = random.sample(historic_patients, int(len(historic_patients) * historic_admission_rate))
    
    patients_with_admissions = active_patients_with_admissions + historic_patients_with_admissions
    
    for patient in patients_with_admissions:
        # Number of admissions for this patient
        num_admissions = np.random.poisson(AVG_ADMISSIONS_PER_PATIENT) + 1
        
        for _ in range(num_admissions):
            admission_id = f"ADM{admission_counter:06d}"
            admission_counter += 1
            
            # Determine if this is a current or historical admission based on patient status
            is_current_patient = patient in current_patients
            is_active_patient = patient['patient_status'] == 'Active'
            
            if is_active_patient:
            if is_current_patient and random.random() < 0.8:  # 80% chance current patients have recent admission
                # Current admission (last 30 days, not yet discharged)
                admission_date = current_date - timedelta(days=random.randint(1, 30))
                is_current_admission = True
            else:
                    # Recent historical admission for active patients (last 2 years)
                    admission_date = current_date - timedelta(days=random.randint(30, 730))
                    is_current_admission = False
            else:
                # Historic patients only have old admissions (2+ years ago)
                admission_date = start_date + timedelta(days=random.randint(0, date_range - 730))
                is_current_admission = False
            
            # Department selection with realistic probabilities (including new departments)
            dept_weights = [0.20, 0.12, 0.10, 0.06, 0.06, 0.05, 0.08, 0.04, 0.02, 0.02, 0.05, 0.02, 0.02, 0.02, 0.02, 0.02, 0.05, 0.05, 0.04, 0.04, 0.02]
            department = random.choices([d[0] for d in departments], weights=dept_weights)[0]
            
            # Admission type
            admission_type = random.choices(['Emergency', 'Elective', 'Urgent'], 
                                          weights=[0.6, 0.3, 0.1])[0]
            
            # Length of stay (realistic distribution)
            if admission_type == 'Emergency':
                los_days = max(1, int(np.random.exponential(3)))
            elif admission_type == 'Elective':
                los_days = max(1, int(np.random.normal(4, 2)))
            else:  # Urgent
                los_days = max(1, int(np.random.exponential(2)))
                
            los_days = min(los_days, 30)  # Cap at 30 days
            
            # Set discharge date based on admission type
            if is_current_admission:
                # Current patients: no discharge date yet or very recent
                if random.random() < 0.3:  # 30% discharged in last few days
                    discharge_date = admission_date + timedelta(days=los_days)
                else:
                    discharge_date = None  # Still admitted
            else:
                # Historical patients: all have discharge dates
                discharge_date = admission_date + timedelta(days=los_days)
            
            # Times
            admission_time = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:00"
            discharge_time = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:00"
            
            # Medical conditions
            primary_diagnosis = random.choice(medical_conditions.get(department, ['General Care']))
            secondary_diagnosis = random.choice([None, None, None] + medical_conditions.get(department, []))
            
            # Physician assignment
            dept_info = next(d for d in departments if d[0] == department)
            attending_physician = dept_info[2]
            
            # Room and bed assignment
            if department == 'EMER':
                room_number = None
                bed_number = None
            else:
                floor = dept_info[3]
                room_number = f"{floor}{random.randint(10, 99):02d}"
                bed_number = random.choice(['A', 'B', 'C', 'D'])
            
            # Charges (realistic distribution)
            base_charge = {
                'Emergency': random.randint(500, 5000),
                'Elective': random.randint(8000, 50000),
                'Urgent': random.randint(2000, 15000)
            }[admission_type]
            
            total_charges = base_charge + (los_days * random.randint(1000, 3000))
            
            # Weather data
            weather_condition = random.choices([w[0] for w in weather_conditions], 
                                             weights=[w[1] for w in weather_conditions])[0]
            
            temp_ranges = {
                'Sunny': (45, 75), 'Cloudy': (35, 65), 'Rainy': (40, 60), 
                'Snowy': (20, 35), 'Partly Cloudy': (40, 70)
            }
            temp_range = temp_ranges[weather_condition]
            temperature_f = random.randint(temp_range[0], temp_range[1])
            
            admissions.append({
                'admission_id': admission_id,
                'patient_id': patient['patient_id'],
                'admission_date': admission_date.strftime('%Y-%m-%d'),
                'admission_time': admission_time,
                'discharge_date': discharge_date.strftime('%Y-%m-%d') if discharge_date else None,
                'discharge_time': discharge_time if discharge_date else None,
                'department_id': department,
                'admission_type': admission_type,
                'chief_complaint': f"Complaint related to {primary_diagnosis}",
                'diagnosis_primary': primary_diagnosis,
                'diagnosis_secondary': secondary_diagnosis,
                'attending_physician': attending_physician,
                'room_number': room_number,
                'bed_number': bed_number,
                'insurance_authorization': f"AUTH{random.randint(100000, 999999)}",
                'total_charges': total_charges,
                'weather_condition': weather_condition,
                'temperature_f': temperature_f
            })
            
            # Generate procedures for this admission
            if random.random() < PROCEDURE_RATE:
                num_procedures = max(1, int(np.random.poisson(AVG_PROCEDURES_PER_ADMISSION)))
                
                for _ in range(num_procedures):
                    procedure_id = f"PROC{procedure_counter:06d}"
                    procedure_counter += 1
                    
                    # Procedure date within admission period
                    procedure_date = admission_date + timedelta(
                        days=random.randint(0, max(1, los_days-1))
                    )
                    procedure_time = f"{random.randint(7, 18):02d}:{random.randint(0, 59):02d}:00"
                    
                    # Procedure codes and names (simplified)
                    procedure_codes = {
                        'CARD': [('92928', 'Cardiac Catheterization'), ('93000', 'Electrocardiogram'), ('93306', 'Echocardiogram')],
                        'EMER': [('36415', 'Blood Draw'), ('71020', 'Chest X-Ray'), ('80053', 'Comprehensive Metabolic Panel')],
                        'ORTH': [('27447', 'Total Knee Replacement'), ('27130', 'Total Hip Replacement'), ('25500', 'Fracture Repair')],
                        'OBGY': [('59400', 'Vaginal Delivery'), ('59510', 'Cesarean Section'), ('58150', 'Hysterectomy')],
                        'NEUR': [('61533', 'Craniotomy'), ('95819', 'EEG'), ('70450', 'CT Head')],
                        'GAST': [('47562', 'Laparoscopic Cholecystectomy'), ('44970', 'Appendectomy'), ('43239', 'Upper Endoscopy')]
                    }
                    
                    default_procedures = [('99213', 'Office Visit'), ('36415', 'Blood Draw'), ('85025', 'Complete Blood Count')]
                    proc_list = procedure_codes.get(department, default_procedures)
                    proc_code, proc_name = random.choice(proc_list)
                    
                    # Procedure duration and cost
                    duration = random.randint(15, 300)  # 15 minutes to 5 hours
                    procedure_cost = random.randint(200, 15000)
                    
                    # Anesthesia type
                    anesthesia_types = ['None', 'Local', 'General', 'Spinal', 'Epidural']
                    anesthesia_weights = [0.4, 0.3, 0.2, 0.05, 0.05]
                    anesthesia_type = random.choices(anesthesia_types, weights=anesthesia_weights)[0]
                    
                    # Complications
                    complications = random.choices(['None', 'Minor bleeding', 'Infection', 'Other'], 
                                                 weights=[0.85, 0.08, 0.05, 0.02])[0]
                    
                    procedures.append({
                        'procedure_id': procedure_id,
                        'admission_id': admission_id,
                        'procedure_code': proc_code,
                        'procedure_name': proc_name,
                        'procedure_date': procedure_date.strftime('%Y-%m-%d'),
                        'procedure_time': procedure_time,
                        'performing_physician': attending_physician,
                        'procedure_duration_minutes': duration,
                        'procedure_cost': procedure_cost,
                        'anesthesia_type': anesthesia_type,
                        'complications': complications,
                        'procedure_notes': f"Procedure completed successfully for {primary_diagnosis}"
                    })
        
        if len(admissions) % 1000 == 0:
            print(f"  Generated {len(admissions):,} admissions and {len(procedures):,} procedures...")
    
    print(f"Generated {len(admissions):,} admissions and {len(procedures):,} procedures")
    return admissions, procedures

def generate_bed_management_data():
    """Generate bed booking and availability data"""
    print("Generating bed management data...")
    
    # Generate bed inventory
    bed_inventory = []
    bed_bookings = []
    bed_availability = []
    
    bed_counter = 1
    booking_counter = 1
    
    # Create beds for each department
    for dept_id, dept_name, _, floor, _, _, _, bed_capacity, _ in departments:
        if bed_capacity > 0:  # Skip departments with no beds (like Radiology)
            for room_num in range(1, (bed_capacity // 2) + 1):
                room_number = f"{floor}{room_num:02d}"
                for bed_letter in ['A', 'B']:
                    bed_id = f"BED{bed_counter:05d}"
                    bed_counter += 1
                    
                    # Bed types
                    bed_types = ['Standard', 'ICU', 'Private', 'Semi-Private', 'Isolation']
                    bed_type_weights = [0.5, 0.15, 0.15, 0.15, 0.05]
                    bed_type = random.choices(bed_types, weights=bed_type_weights)[0]
                    
                    # Equipment
                    equipment_options = ['Basic', 'Cardiac Monitor', 'Ventilator', 'Dialysis', 'Isolation Equipment']
                    equipment = random.choice(equipment_options)
                    
                    bed_inventory.append({
                        'bed_id': bed_id,
                        'department_id': dept_id,
                        'room_number': room_number,
                        'bed_number': bed_letter,
                        'bed_type': bed_type,
                        'equipment': equipment,
                        'is_active': True,
                        'daily_rate': random.randint(800, 3000)
                    })
    
    print(f"Generated {len(bed_inventory):,} beds")
    
    # Generate bed bookings for the past year
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    # Create daily availability records
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        for bed in bed_inventory:
            # 70% occupancy rate on average
            is_occupied = random.random() < 0.7
            
            # Higher occupancy on weekdays, lower on weekends
            if current_date.weekday() < 5:  # Weekday
                is_occupied = random.random() < 0.75
            else:  # Weekend
                is_occupied = random.random() < 0.65
            
            status = 'Occupied' if is_occupied else 'Available'
            
            # Sometimes beds are out of service
            if random.random() < 0.05:  # 5% chance
                status = random.choice(['Maintenance', 'Cleaning', 'Out of Service'])
            
            bed_availability.append({
                'availability_id': f"AVAIL{len(bed_availability)+1:08d}",
                'bed_id': bed['bed_id'],
                'date': date_str,
                'status': status,
                'reserved_until': None if status != 'Occupied' else f"{random.randint(8, 20):02d}:00:00",
                'last_updated': f"{current_date.strftime('%Y-%m-%d')} {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:00"
            })
            
            # Generate booking record if occupied
            if status == 'Occupied':
                booking_id = f"BOOK{booking_counter:08d}"
                booking_counter += 1
                
                # Random patient (simplified - using patient IDs)
                patient_id = f"PAT{random.randint(1, min(NUM_PATIENTS, 10000)):06d}"
                
                # Booking duration
                nights = random.randint(1, 14)
                checkout_date = current_date + timedelta(days=nights)
                
                bed_bookings.append({
                    'booking_id': booking_id,
                    'bed_id': bed['bed_id'],
                    'patient_id': patient_id,
                    'check_in_date': date_str,
                    'check_in_time': f"{random.randint(8, 20):02d}:{random.randint(0, 59):02d}:00",
                    'expected_checkout_date': checkout_date.strftime('%Y-%m-%d'),
                    'expected_checkout_time': f"{random.randint(8, 16):02d}:{random.randint(0, 59):02d}:00",
                    'actual_checkout_date': None,
                    'actual_checkout_time': None,
                    'booking_status': 'Active',
                    'total_nights': nights,
                    'nightly_rate': bed['daily_rate'],
                    'total_charges': bed['daily_rate'] * nights,
                    'special_requirements': random.choice([None, 'Isolation', 'Cardiac Monitor', 'Quiet Room', 'Window View']),
                    'created_timestamp': f"{current_date.strftime('%Y-%m-%d')} {random.randint(0, 23):02d}:{random.randint(0, 59):02d}:00"
                })
        
        current_date += timedelta(days=1)
        
        if current_date.day == 1:  # Progress update monthly
            print(f"  Generated bed data through {current_date.strftime('%B %Y')}")
    
    print(f"Generated {len(bed_bookings):,} bed bookings and {len(bed_availability):,} availability records")
    return bed_inventory, bed_bookings, bed_availability

def generate_medication_data(admissions):
    """Generate medication dispensing data"""
    print("Generating medication dispensing data...")
    
    medication_orders = []
    medication_dispensing = []
    pharmacy_inventory = []
    
    order_counter = 1
    dispensing_counter = 1
    
    # Create pharmacy inventory
    for med_code, med_name, med_class, category, form, strength, unit_cost in medications:
        # Generate multiple lot numbers for each medication
        for lot_num in range(1, random.randint(3, 8)):
            lot_id = f"LOT{len(pharmacy_inventory)+1:06d}"
            
            pharmacy_inventory.append({
                'inventory_id': f"INV{len(pharmacy_inventory)+1:06d}",
                'medication_code': med_code,
                'medication_name': med_name,
                'medication_class': med_class,
                'therapeutic_category': category,
                'dosage_form': form,
                'strength': strength,
                'unit_cost': unit_cost,
                'lot_number': lot_id,
                'expiration_date': (datetime.now() + timedelta(days=random.randint(180, 1095))).strftime('%Y-%m-%d'),
                'quantity_on_hand': random.randint(50, 500),
                'reorder_level': random.randint(10, 50),
                'supplier': random.choice(['Cardinal Health', 'McKesson', 'AmerisourceBergen', 'Morris & Dickson']),
                'storage_location': f"Shelf-{random.randint(1, 20)}-{random.choice(['A', 'B', 'C', 'D'])}"
            })
    
    # Generate medication orders and dispensing for admissions
    for admission in admissions:
        if random.random() < MEDICATION_RATE:
            num_medications = max(1, int(np.random.poisson(AVG_MEDICATIONS_PER_ADMISSION)))
            
            for _ in range(num_medications):
                order_id = f"ORD{order_counter:08d}"
                order_counter += 1
                
                # Select medication based on department
                dept_id = admission['department_id']
                if dept_id == 'CARD':
                    med_options = [m for m in medications if m[3] == 'Cardiovascular']
                elif dept_id == 'NEUR':
                    med_options = [m for m in medications if m[3] in ['Neurological', 'Pain Management']]
                elif dept_id == 'ENDO':
                    med_options = [m for m in medications if m[3] == 'Endocrine']
                elif dept_id == 'GAST':
                    med_options = [m for m in medications if m[3] == 'Gastrointestinal']
                else:
                    med_options = medications
                
                selected_med = random.choice(med_options)
                med_code, med_name, med_class, category, form, strength, unit_cost = selected_med
                
                # Order details
                order_date = datetime.strptime(admission['admission_date'], '%Y-%m-%d')
                order_time = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:00"
                
                quantity_ordered = random.randint(1, 30)
                frequency = random.choice(['Once daily', 'Twice daily', 'Three times daily', 'Four times daily', 'As needed', 'Every 6 hours'])
                duration_days = random.randint(1, 14)
                
                medication_orders.append({
                    'order_id': order_id,
                    'admission_id': admission['admission_id'],
                    'patient_id': admission['patient_id'],
                    'medication_code': med_code,
                    'medication_name': med_name,
                    'prescribing_physician': admission['attending_physician'],
                    'order_date': order_date.strftime('%Y-%m-%d'),
                    'order_time': order_time,
                    'quantity_ordered': quantity_ordered,
                    'frequency': frequency,
                    'duration_days': duration_days,
                    'route': random.choice(['Oral', 'IV', 'IM', 'Topical', 'Inhalation']),
                    'priority': random.choice(['Routine', 'Urgent', 'STAT']),
                    'order_status': random.choice(['Active', 'Completed', 'Discontinued']),
                    'allergies_checked': True,
                    'interactions_checked': True
                })
                
                # Generate dispensing records (multiple per order)
                if random.random() < 0.9:  # 90% of orders are dispensed
                    doses_to_dispense = min(quantity_ordered, duration_days * 4)  # Realistic dosing
                    
                    for dose_num in range(1, min(doses_to_dispense + 1, 10)):  # Limit for demo
                        dispensing_id = f"DISP{dispensing_counter:08d}"
                        dispensing_counter += 1
                        
                        dispense_date = order_date + timedelta(days=random.randint(0, duration_days))
                        dispense_time = f"{random.randint(6, 22):02d}:{random.randint(0, 59):02d}:00"
                        
                        # Select inventory item
                        matching_inventory = [inv for inv in pharmacy_inventory if inv['medication_code'] == med_code]
                        if matching_inventory:
                            inventory_item = random.choice(matching_inventory)
                            
                            medication_dispensing.append({
                                'dispensing_id': dispensing_id,
                                'order_id': order_id,
                                'patient_id': admission['patient_id'],
                                'medication_code': med_code,
                                'inventory_id': inventory_item['inventory_id'],
                                'lot_number': inventory_item['lot_number'],
                                'dispense_date': dispense_date.strftime('%Y-%m-%d'),
                                'dispense_time': dispense_time,
                                'quantity_dispensed': 1,
                                'dispensing_pharmacist': random.choice(['PharmD John Smith', 'PharmD Sarah Lee', 'PharmD Michael Chen', 'PharmD Lisa Wang']),
                                'administration_time': f"{random.randint(6, 22):02d}:{random.randint(0, 59):02d}:00",
                                'administered_by': random.choice(['RN Jennifer Brown', 'RN David Kim', 'RN Maria Rodriguez', 'RN Kevin Johnson']),
                                'patient_response': random.choice(['Good', 'Mild side effects', 'No response', 'Excellent']),
                                'side_effects': random.choice([None, 'Nausea', 'Drowsiness', 'Headache', 'Dizziness']),
                                'cost_per_unit': unit_cost,
                                'total_cost': unit_cost * 1
                            })
    
    print(f"Generated {len(medication_orders):,} medication orders and {len(medication_dispensing):,} dispensing records")
    return medication_orders, medication_dispensing, pharmacy_inventory

def generate_allied_health_data(admissions):
    """Generate allied health services data"""
    print("Generating allied health services data...")
    
    allied_health_services_data = []
    service_counter = 1
    
    for admission in admissions:
        if random.random() < ALLIED_HEALTH_RATE:
            num_services = max(1, int(np.random.poisson(AVG_ALLIED_SERVICES_PER_ADMISSION)))
            
            for _ in range(num_services):
                service_id = f"AHS{service_counter:08d}"
                service_counter += 1
                
                # Select service type
                service_code, service_name, service_type, duration_minutes, cost = random.choice(allied_health_services)
                
                # Service date within admission period
                admission_date = datetime.strptime(admission['admission_date'], '%Y-%m-%d')
                
                if admission['discharge_date']:
                    discharge_date = datetime.strptime(admission['discharge_date'], '%Y-%m-%d')
                    los_days = (discharge_date - admission_date).days
                else:
                    # Current patient - use current date as end date
                    discharge_date = datetime(2024, 12, 15)  # Current date
                    los_days = (discharge_date - admission_date).days
                
                service_date = admission_date + timedelta(days=random.randint(0, max(1, los_days)))
                service_time = f"{random.randint(8, 17):02d}:{random.randint(0, 59):02d}:00"
                
                # Provider based on service type
                if service_code.startswith('PHYS'):
                    provider = random.choice(['Sarah Johnson PT', 'Mike Chen PT', 'Lisa Rodriguez PT'])
                elif service_code.startswith('OCCU'):
                    provider = random.choice(['Mark Thompson OT', 'Jennifer Kim OT', 'David Lee OT'])
                elif service_code.startswith('RESP'):
                    provider = random.choice(['Lisa Chen RRT', 'Kevin Brown RRT', 'Maria Garcia RRT'])
                elif service_code.startswith('NUTR'):
                    provider = random.choice(['Jennifer Martinez RD', 'Susan White RD', 'Carlos Lopez RD'])
                else:  # Social services
                    provider = random.choice(['David Kim MSW', 'Rachel Green MSW', 'Thomas Wilson MSW'])
                
                allied_health_services_data.append({
                    'service_id': service_id,
                    'admission_id': admission['admission_id'],
                    'patient_id': admission['patient_id'],
                    'service_code': service_code,
                    'service_name': service_name,
                    'service_type': service_type,
                    'service_date': service_date.strftime('%Y-%m-%d'),
                    'service_time': service_time,
                    'duration_minutes': duration_minutes,
                    'provider_name': provider,
                    'provider_credentials': provider.split()[-1],  # PT, OT, RRT, RD, MSW
                    'service_location': random.choice(['Bedside', 'Therapy Gym', 'Conference Room', 'Patient Room']),
                    'service_cost': cost,
                    'patient_participation': random.choice(['Excellent', 'Good', 'Fair', 'Poor']),
                    'goals_met': random.choice([True, False]),
                    'follow_up_needed': random.choice([True, False]),
                    'notes': f"Patient responded well to {service_name.lower()}",
                    'insurance_covered': random.choice([True, False])
                })
    
    print(f"Generated {len(allied_health_services_data):,} allied health service records")
    return allied_health_services_data

def save_to_csv(data, filename, fieldnames):
    """Save data to CSV file"""
    filepath = f"/Users/rbotha/Documents/Cursor_code/hospital_snowflake_demo/data/{filename}"
    print(f"Saving {len(data):,} records to {filename}...")
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Saved {filename}")

def main():
    """Main execution function"""
    print("=" * 60)
    print("Hospital Snowflake Demo - Large Dataset Generator")
    print("=" * 60)
    
    # Generate patient demographics
    patients = generate_patient_demographics()
    
    # Generate admissions and procedures
    admissions, procedures = generate_admissions_and_procedures(patients)
    
    # Generate bed management data
    bed_inventory, bed_bookings, bed_availability = generate_bed_management_data()
    
    # Generate medication data
    medication_orders, medication_dispensing, pharmacy_inventory = generate_medication_data(admissions)
    
    # Generate allied health data
    allied_health_services = generate_allied_health_data(admissions)
    
    # Save all datasets
    print("\nSaving datasets to CSV files...")
    
    # Patient demographics
    patient_fields = ['patient_id', 'first_name', 'last_name', 'date_of_birth', 'gender',
                     'address', 'city', 'state', 'zip_code', 'phone', 'email',
                     'insurance_provider', 'emergency_contact_name', 'emergency_contact_phone',
                     'patient_status', 'registration_date', 'last_visit_date', 'is_active_patient']
    save_to_csv(patients, 'patient_demographics_large.csv', patient_fields)
    
    # Admissions
    admission_fields = ['admission_id', 'patient_id', 'admission_date', 'admission_time',
                       'discharge_date', 'discharge_time', 'department_id', 'admission_type',
                       'chief_complaint', 'diagnosis_primary', 'diagnosis_secondary',
                       'attending_physician', 'room_number', 'bed_number', 'insurance_authorization',
                       'total_charges', 'weather_condition', 'temperature_f']
    save_to_csv(admissions, 'patient_admissions_large.csv', admission_fields)
    
    # Procedures
    procedure_fields = ['procedure_id', 'admission_id', 'procedure_code', 'procedure_name',
                       'procedure_date', 'procedure_time', 'performing_physician',
                       'procedure_duration_minutes', 'procedure_cost', 'anesthesia_type',
                       'complications', 'procedure_notes']
    save_to_csv(procedures, 'medical_procedures_large.csv', procedure_fields)
    
    # Bed inventory
    bed_inventory_fields = ['bed_id', 'department_id', 'room_number', 'bed_number',
                           'bed_type', 'equipment', 'is_active', 'daily_rate']
    save_to_csv(bed_inventory, 'bed_inventory.csv', bed_inventory_fields)
    
    # Bed bookings
    booking_fields = ['booking_id', 'bed_id', 'patient_id', 'check_in_date', 'check_in_time',
                     'expected_checkout_date', 'expected_checkout_time', 'actual_checkout_date',
                     'actual_checkout_time', 'booking_status', 'total_nights', 'nightly_rate',
                     'total_charges', 'special_requirements', 'created_timestamp']
    save_to_csv(bed_bookings, 'bed_bookings.csv', booking_fields)
    
    # Bed availability
    availability_fields = ['availability_id', 'bed_id', 'date', 'status', 'reserved_until', 'last_updated']
    save_to_csv(bed_availability, 'bed_availability.csv', availability_fields)
    
    # Pharmacy inventory
    pharmacy_fields = ['inventory_id', 'medication_code', 'medication_name', 'medication_class',
                      'therapeutic_category', 'dosage_form', 'strength', 'unit_cost',
                      'lot_number', 'expiration_date', 'quantity_on_hand', 'reorder_level',
                      'supplier', 'storage_location']
    save_to_csv(pharmacy_inventory, 'pharmacy_inventory.csv', pharmacy_fields)
    
    # Medication orders
    order_fields = ['order_id', 'admission_id', 'patient_id', 'medication_code', 'medication_name',
                   'prescribing_physician', 'order_date', 'order_time', 'quantity_ordered',
                   'frequency', 'duration_days', 'route', 'priority', 'order_status',
                   'allergies_checked', 'interactions_checked']
    save_to_csv(medication_orders, 'medication_orders.csv', order_fields)
    
    # Medication dispensing
    dispensing_fields = ['dispensing_id', 'order_id', 'patient_id', 'medication_code',
                        'inventory_id', 'lot_number', 'dispense_date', 'dispense_time',
                        'quantity_dispensed', 'dispensing_pharmacist', 'administration_time',
                        'administered_by', 'patient_response', 'side_effects', 'cost_per_unit', 'total_cost']
    save_to_csv(medication_dispensing, 'medication_dispensing.csv', dispensing_fields)
    
    # Allied health services
    allied_fields = ['service_id', 'admission_id', 'patient_id', 'service_code', 'service_name',
                    'service_type', 'service_date', 'service_time', 'duration_minutes',
                    'provider_name', 'provider_credentials', 'service_location', 'service_cost',
                    'patient_participation', 'goals_met', 'follow_up_needed', 'notes', 'insurance_covered']
    save_to_csv(allied_health_services, 'allied_health_services.csv', allied_fields)
    
    # Generate summary statistics
    print("\n" + "=" * 60)
    print("DATASET SUMMARY")
    print("=" * 60)
    print(f"Patients:              {len(patients):,}")
    print(f"Admissions:            {len(admissions):,}")
    print(f"Procedures:            {len(procedures):,}")
    print(f"Bed Inventory:         {len(bed_inventory):,}")
    print(f"Bed Bookings:          {len(bed_bookings):,}")
    print(f"Bed Availability:      {len(bed_availability):,}")
    print(f"Pharmacy Inventory:    {len(pharmacy_inventory):,}")
    print(f"Medication Orders:     {len(medication_orders):,}")
    print(f"Medication Dispensing: {len(medication_dispensing):,}")
    print(f"Allied Health Services:{len(allied_health_services):,}")
    total_records = (len(patients) + len(admissions) + len(procedures) + len(bed_inventory) + 
                    len(bed_bookings) + len(bed_availability) + len(pharmacy_inventory) + 
                    len(medication_orders) + len(medication_dispensing) + len(allied_health_services))
    print(f"Total Records:         {total_records:,}")
    print("=" * 60)
    print("Large dataset generation completed successfully!")

if __name__ == "__main__":
    main()
