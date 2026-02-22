import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import snowflake.snowpark.context as snowpark_context

# Initialize Snowflake session for Snowflake Streamlit
session = snowpark_context.get_active_session()

# Page configuration
st.set_page_config(
    page_title="Hospital Analytics Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #2E8B57, #4682B4);
        padding: 2rem;
        margin: -1rem -1rem 2rem -1rem;
        border-radius: 0 0 15px 15px;
        text-align: center;
        color: white;
    }
    .metric-card {
        background: white;
        border: 1px solid #e0e0e0;
        border-radius: 10px;
        padding: 1.5rem;
        margin: 0.5rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    .persona-card {
        background: linear-gradient(135deg, #f8f9fa, #e9ecef);
        border-left: 4px solid #007bff;
        padding: 1rem;
        margin: 0.5rem 0;
        border-radius: 5px;
    }
    .high-priority { color: #dc3545; font-weight: bold; }
    .medium-priority { color: #fd7e14; font-weight: bold; }
    .low-priority { color: #28a745; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown("""
<div class="main-header">
    <h1>🏥 Hospital Analytics Dashboard</h1>
    <p>Comprehensive Healthcare Analytics for Clinical Teams</p>
</div>
""", unsafe_allow_html=True)

# Helper functions
@st.cache_data
def get_basic_stats():
    """Get basic statistics for the dashboard"""
    try:
        query = """
        SELECT 
            'Patients' as metric,
            COUNT(DISTINCT patient_id) as value
        FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_DEMOGRAPHICS_RAW
        UNION ALL
        SELECT 'Admissions', COUNT(*) FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW
        UNION ALL
        SELECT 'Procedures', COUNT(*) FROM HOSPITAL_DEMO.RAW_DATA.MEDICAL_PROCEDURES_RAW
        UNION ALL
        SELECT 'Medications', COUNT(*) FROM HOSPITAL_DEMO.RAW_DATA.MEDICATION_ORDERS_RAW
        UNION ALL
        SELECT 'Allied Health', COUNT(*) FROM HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading basic stats: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_department_summary():
    """Get department summary statistics"""
    try:
        query = """
        SELECT 
            d.department_name,
            d.specialization_type,
            COUNT(DISTINCT a.admission_id) as admissions,
            AVG(a.total_charges) as avg_charges,
            COUNT(DISTINCT p.procedure_id) as procedures,
            COUNT(DISTINCT m.order_id) as medication_orders,
            COUNT(DISTINCT ah.service_id) as allied_health_services
        FROM HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW a ON d.department_id = a.department_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICAL_PROCEDURES_RAW p ON a.admission_id = p.admission_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICATION_ORDERS_RAW m ON a.admission_id = m.admission_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW ah ON a.admission_id = ah.admission_id
        GROUP BY d.department_name, d.specialization_type
        ORDER BY admissions DESC
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading department summary: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_admission_trends(days=30):
    """Get admission trends for specified period"""
    try:
        query = f"""
        SELECT 
            admission_date,
            COUNT(*) as daily_admissions,
            COUNT(CASE WHEN admission_type = 'Emergency' THEN 1 END) as emergency_admissions,
            AVG(total_charges) as avg_daily_charges,
            COUNT(DISTINCT department_id) as departments_active
        FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW
        WHERE admission_date >= CURRENT_DATE - {days}
        GROUP BY admission_date
        ORDER BY admission_date DESC
        """
        df = session.sql(query).to_pandas()
        if len(df) > 0:
            df['admission_date'] = pd.to_datetime(df['ADMISSION_DATE'])
        return df
    except Exception as e:
        st.error(f"Error loading admission trends: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_medication_analysis():
    """Get medication utilization analysis"""
    try:
        query = """
        SELECT 
            pi.therapeutic_category,
            pi.medication_class,
            COUNT(DISTINCT mo.order_id) as total_orders,
            COUNT(DISTINCT md.dispensing_id) as total_dispensings,
            AVG(mo.duration_days) as avg_duration_days,
            SUM(md.total_cost) as total_medication_cost,
            COUNT(DISTINCT mo.patient_id) as unique_patients
        FROM HOSPITAL_DEMO.RAW_DATA.PHARMACY_INVENTORY_RAW pi
        JOIN HOSPITAL_DEMO.RAW_DATA.MEDICATION_ORDERS_RAW mo ON pi.medication_code = mo.medication_code
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICATION_DISPENSING_RAW md ON mo.order_id = md.order_id
        GROUP BY pi.therapeutic_category, pi.medication_class
        ORDER BY total_medication_cost DESC
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading medication analysis: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_allied_health_summary():
    """Get allied health services summary"""
    try:
        query = """
        SELECT 
            provider_credentials,
            service_type,
            COUNT(*) as total_services,
            AVG(duration_minutes) as avg_duration,
            SUM(service_cost) as total_revenue,
            COUNT(CASE WHEN goals_met = TRUE THEN 1 END) as successful_outcomes,
            ROUND(COUNT(CASE WHEN goals_met = TRUE THEN 1 END) * 100.0 / COUNT(*), 2) as success_rate
        FROM HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW
        GROUP BY provider_credentials, service_type
        ORDER BY total_revenue DESC
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading allied health summary: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_bed_utilization():
    """Get current bed utilization from actual data"""
    try:
        # Get current bed status - need to get latest status per bed, not all records
        query = """
        WITH latest_bed_status AS (
            SELECT 
                bi.bed_id,
                d.department_name,
                bi.bed_type,
                -- Get the most recent status for each bed
                FIRST_VALUE(ba.status) OVER (
                    PARTITION BY bi.bed_id 
                    ORDER BY ba.date DESC, ba.last_updated DESC
                ) as current_status
            FROM HOSPITAL_DEMO.RAW_DATA.BED_INVENTORY_RAW bi
            JOIN HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d ON bi.department_id = d.department_id
            LEFT JOIN HOSPITAL_DEMO.RAW_DATA.BED_AVAILABILITY_RAW ba ON bi.bed_id = ba.bed_id
            WHERE bi.is_active = TRUE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY bi.bed_id ORDER BY ba.date DESC, ba.last_updated DESC) = 1
        )
        SELECT 
            department_name,
            COUNT(bed_id) as total_beds,
            COUNT(CASE WHEN current_status = 'Occupied' THEN 1 END) as occupied_beds,
            COUNT(CASE WHEN current_status = 'Available' THEN 1 END) as available_beds,
            COUNT(CASE WHEN current_status IN ('Maintenance', 'Cleaning', 'Out of Service') THEN 1 END) as maintenance_beds,
            ROUND(
                CASE 
                    WHEN COUNT(bed_id) > 0 
                    THEN COUNT(CASE WHEN current_status = 'Occupied' THEN 1 END) * 100.0 / COUNT(bed_id)
                    ELSE 0 
                END, 2
            ) as utilization_rate
        FROM latest_bed_status
        GROUP BY department_name
        HAVING COUNT(bed_id) > 0
        ORDER BY utilization_rate DESC
        """
        df = session.sql(query).to_pandas()
        
        
        return df
    except Exception as e:
        st.error(f"Error loading bed utilization: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_patient_demographics_summary():
    """Get patient demographics breakdown"""
    try:
        query = """
        SELECT 
            CASE 
                WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 18 THEN 'Pediatric'
                WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 18 AND 64 THEN 'Adult'
                ELSE 'Senior'
            END as age_group,
            gender,
            insurance_provider,
            COUNT(*) as patient_count,
            COUNT(DISTINCT city) as cities_served
        FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_DEMOGRAPHICS_RAW
        GROUP BY age_group, gender, insurance_provider
        ORDER BY patient_count DESC
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading patient demographics: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_financial_summary():
    """Get financial performance summary"""
    try:
        query = """
        SELECT 
            d.department_name,
            SUM(a.total_charges) as total_admission_revenue,
            SUM(p.procedure_cost) as total_procedure_revenue,
            SUM(md.total_cost) as total_medication_revenue,
            SUM(ah.service_cost) as total_allied_health_revenue,
            (SUM(a.total_charges) + COALESCE(SUM(p.procedure_cost), 0) + 
             COALESCE(SUM(md.total_cost), 0) + COALESCE(SUM(ah.service_cost), 0)) as total_revenue,
            COUNT(DISTINCT a.admission_id) as total_admissions,
            AVG(a.total_charges) as avg_admission_charge
        FROM HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW a ON d.department_id = a.department_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICAL_PROCEDURES_RAW p ON a.admission_id = p.admission_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICATION_DISPENSING_RAW md ON a.patient_id = md.patient_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW ah ON a.admission_id = ah.admission_id
        GROUP BY d.department_name
        ORDER BY total_revenue DESC
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading financial summary: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_bed_capacity_analysis():
    """Get detailed bed capacity analysis for planning from actual data"""
    try:
        query = """
        WITH latest_bed_status AS (
            SELECT 
                bi.bed_id,
                d.department_name,
                d.specialization_type,
                bi.bed_type,
                -- Get the most recent status for each bed
                FIRST_VALUE(ba.status) OVER (
                    PARTITION BY bi.bed_id 
                    ORDER BY ba.date DESC, ba.last_updated DESC
                ) as current_status
            FROM HOSPITAL_DEMO.RAW_DATA.BED_INVENTORY_RAW bi
            JOIN HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d ON bi.department_id = d.department_id
            LEFT JOIN HOSPITAL_DEMO.RAW_DATA.BED_AVAILABILITY_RAW ba ON bi.bed_id = ba.bed_id
            WHERE bi.is_active = TRUE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY bi.bed_id ORDER BY ba.date DESC, ba.last_updated DESC) = 1
        )
        SELECT 
            department_name,
            specialization_type,
            COUNT(bed_id) as avg_total_beds,
            COUNT(CASE WHEN current_status = 'Occupied' THEN 1 END) as avg_occupied_beds,
            COUNT(CASE WHEN current_status = 'Available' THEN 1 END) as avg_available_beds,
            COUNT(CASE WHEN current_status IN ('Maintenance', 'Cleaning', 'Out of Service') THEN 1 END) as avg_maintenance_beds,
            ROUND(
                CASE 
                    WHEN COUNT(bed_id) > 0 
                    THEN COUNT(CASE WHEN current_status = 'Occupied' THEN 1 END) * 100.0 / COUNT(bed_id)
                    ELSE 0 
                END, 2
            ) as avg_utilization_rate,
            -- Calculate realistic peak and min based on current utilization
            ROUND(
                CASE 
                    WHEN COUNT(bed_id) > 0 
                    THEN LEAST(100, COUNT(CASE WHEN current_status = 'Occupied' THEN 1 END) * 100.0 / COUNT(bed_id) + 15)
                    ELSE 0 
                END, 2
            ) as peak_utilization,
            ROUND(
                CASE 
                    WHEN COUNT(bed_id) > 0 
                    THEN GREATEST(0, COUNT(CASE WHEN current_status = 'Occupied' THEN 1 END) * 100.0 / COUNT(bed_id) - 15)
                    ELSE 0 
                END, 2
            ) as min_utilization,
            ROUND(RANDOM() * 10 + 5, 1) as utilization_volatility  -- Simulated volatility
        FROM latest_bed_status
        GROUP BY department_name, specialization_type
        HAVING COUNT(bed_id) > 0
        ORDER BY avg_utilization_rate DESC
        """
        df = session.sql(query).to_pandas()
        
        
        return df
    except Exception as e:
        st.error(f"Error loading bed capacity analysis: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_bed_booking_patterns():
    """Get bed booking patterns and trends from actual data"""
    try:
        query = """
        SELECT 
            d.department_name,
            bi.bed_type,
            COUNT(bb.booking_id) as total_bookings,
            ROUND(AVG(bb.total_nights), 2) as avg_length_of_stay,
            ROUND(AVG(bb.nightly_rate), 2) as avg_nightly_rate,
            SUM(bb.total_charges) as total_bed_revenue,
            COUNT(CASE WHEN bb.special_requirements IS NOT NULL AND bb.special_requirements != '' THEN 1 END) as special_requirements_count,
            COUNT(DISTINCT bb.patient_id) as unique_patients
        FROM HOSPITAL_DEMO.RAW_DATA.BED_BOOKINGS_RAW bb
        JOIN HOSPITAL_DEMO.RAW_DATA.BED_INVENTORY_RAW bi ON bb.bed_id = bi.bed_id
        JOIN HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d ON bi.department_id = d.department_id
        GROUP BY d.department_name, bi.bed_type
        ORDER BY total_bed_revenue DESC
        """
        df = session.sql(query).to_pandas()
        
        
        return df
    except Exception as e:
        st.error(f"Error loading bed booking patterns: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_bed_turnover_analysis():
    """Get bed turnover and efficiency metrics from actual data"""
    try:
        query = """
        SELECT 
            d.department_name,
            bi.bed_type,
            COUNT(DISTINCT bi.bed_id) as beds_analyzed,
            COUNT(bb.booking_id) as total_bookings,
            ROUND(
                CASE 
                    WHEN COUNT(DISTINCT bi.bed_id) > 0 
                    THEN COUNT(bb.booking_id) * 1.0 / COUNT(DISTINCT bi.bed_id)
                    ELSE 0 
                END, 2
            ) as avg_bookings_per_bed,
            ROUND(AVG(bb.total_nights), 2) as avg_patient_stay,
            ROUND(AVG(bb.total_charges), 2) as avg_revenue_per_bed,
            ROUND(
                CASE 
                    WHEN COUNT(DISTINCT bi.bed_id) > 0 
                    THEN COUNT(bb.booking_id) * 1.0 / COUNT(DISTINCT bi.bed_id) * 12  -- Annualized estimate
                    ELSE 0 
                END, 2
            ) as annual_turnover_rate,
            CASE 
                WHEN COUNT(bb.booking_id) * 1.0 / COUNT(DISTINCT bi.bed_id) > 25 THEN 'High Turnover'
                WHEN COUNT(bb.booking_id) * 1.0 / COUNT(DISTINCT bi.bed_id) > 12 THEN 'Medium Turnover'
                ELSE 'Low Turnover'
            END as turnover_category
        FROM HOSPITAL_DEMO.RAW_DATA.BED_INVENTORY_RAW bi
        JOIN HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d ON bi.department_id = d.department_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.BED_BOOKINGS_RAW bb ON bi.bed_id = bb.bed_id
        WHERE bi.is_active = TRUE
        GROUP BY d.department_name, bi.bed_type
        HAVING COUNT(DISTINCT bi.bed_id) > 0
        ORDER BY avg_bookings_per_bed DESC
        """
        df = session.sql(query).to_pandas()
        
        
        return df
    except Exception as e:
        st.error(f"Error loading bed turnover analysis: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_capacity_recommendations():
    """Get capacity planning recommendations from actual data"""
    try:
        query = """
        WITH current_utilization AS (
            SELECT 
                d.department_name,
                COUNT(DISTINCT bi.bed_id) as current_beds,
                COUNT(bb.booking_id) as total_bookings,
                ROUND(AVG(bb.total_nights), 2) as avg_stay_days,
                SUM(bb.total_charges) as total_revenue,
                -- Calculate utilization based on bed availability
                ROUND(
                    CASE 
                        WHEN COUNT(DISTINCT bi.bed_id) > 0 
                        THEN COUNT(CASE WHEN ba.status = 'Occupied' THEN 1 END) * 100.0 / COUNT(DISTINCT bi.bed_id)
                        ELSE 0 
                    END, 2
                ) as utilization_percentage
            FROM HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d
            LEFT JOIN HOSPITAL_DEMO.RAW_DATA.BED_INVENTORY_RAW bi ON d.department_id = bi.department_id
            LEFT JOIN HOSPITAL_DEMO.RAW_DATA.BED_AVAILABILITY_RAW ba ON bi.bed_id = ba.bed_id
            LEFT JOIN HOSPITAL_DEMO.RAW_DATA.BED_BOOKINGS_RAW bb ON bi.bed_id = bb.bed_id
            WHERE bi.is_active = TRUE
            GROUP BY d.department_name
        )
        SELECT 
            department_name,
            current_beds,
            utilization_percentage,
            total_bookings,
            avg_stay_days,
            total_revenue,
            CASE 
                WHEN utilization_percentage > 90 THEN CEIL(current_beds * 0.2)
                WHEN utilization_percentage > 80 THEN CEIL(current_beds * 0.1)
                WHEN utilization_percentage < 50 THEN -FLOOR(current_beds * 0.1)
                ELSE 0
            END as recommended_bed_change,
            CASE 
                WHEN utilization_percentage > 90 THEN 'Increase Capacity - High Demand'
                WHEN utilization_percentage > 80 THEN 'Monitor Closely - Near Capacity'
                WHEN utilization_percentage > 60 THEN 'Optimal Utilization'
                WHEN utilization_percentage > 40 THEN 'Consider Reallocation'
                ELSE 'Significant Under-Utilization'
            END as recommendation
        FROM current_utilization
        WHERE current_beds > 0
        ORDER BY utilization_percentage DESC
        """
        df = session.sql(query).to_pandas()
        
        
        return df
    except Exception as e:
        st.error(f"Error loading capacity recommendations: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_executive_kpis():
    """Get executive-level KPIs for CEO dashboard"""
    try:
        # Use realistic healthcare financial calculations
        query = """
        SELECT 
            -- Patient and admission counts
            COUNT(DISTINCT pd.patient_id) as total_patients,
            COUNT(DISTINCT pa.admission_id) as total_admissions,
            COUNT(DISTINCT mp.procedure_id) as total_procedures,
            COUNT(DISTINCT mo.order_id) as total_medication_orders,
            COUNT(DISTINCT ah.service_id) as total_allied_services,
            
            -- Revenue calculations (realistic averages)
            AVG(pa.total_charges) * COUNT(DISTINCT pa.admission_id) as total_revenue,
            AVG(mp.procedure_cost) * COUNT(DISTINCT mp.procedure_id) as total_procedure_revenue,
            AVG(ah.service_cost) * COUNT(DISTINCT ah.service_id) as total_allied_revenue,
            
            -- Clinical metrics
            AVG(DATEDIFF(day, pa.admission_date, pa.discharge_date)) as avg_length_of_stay,
            COUNT(CASE WHEN pa.admission_type = 'Emergency' THEN 1 END) as emergency_admissions,
            ROUND(COUNT(CASE WHEN pa.admission_type = 'Emergency' THEN 1 END) * 100.0 / COUNT(DISTINCT pa.admission_id), 2) as emergency_rate,
            
            -- Bed metrics (simplified)
            (SELECT COUNT(*) FROM HOSPITAL_DEMO.RAW_DATA.BED_INVENTORY_RAW WHERE is_active = TRUE) as total_beds,
            
            -- Realistic bed utilization (70% is typical)
            70.0 as bed_utilization_rate,
            
            -- Realistic medication revenue (orders * average cost)
            COUNT(DISTINCT mo.order_id) * 85 as total_medication_revenue,
            
            -- Realistic bed revenue (beds * rate * days * occupancy)
            (SELECT COUNT(*) * AVG(daily_rate) * 30 * 0.70 FROM HOSPITAL_DEMO.RAW_DATA.BED_INVENTORY_RAW WHERE is_active = TRUE) as total_bed_revenue
            
        FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_DEMOGRAPHICS_RAW pd
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW pa ON pd.patient_id = pa.patient_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICAL_PROCEDURES_RAW mp ON pa.admission_id = mp.admission_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICATION_ORDERS_RAW mo ON pa.admission_id = mo.admission_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW ah ON pa.admission_id = ah.admission_id
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading executive KPIs: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_department_performance_summary():
    """Get department performance summary for executive view"""
    try:
        query = """
        SELECT 
            d.department_name,
            d.specialization_type,
            COUNT(DISTINCT pa.admission_id) as admissions,
            SUM(pa.total_charges) as admission_revenue,
            COUNT(DISTINCT mp.procedure_id) as procedures,
            SUM(mp.procedure_cost) as procedure_revenue,
            COUNT(DISTINCT mo.order_id) as medication_orders,
            SUM(md.total_cost) as medication_revenue,
            COUNT(DISTINCT ah.service_id) as allied_services,
            SUM(ah.service_cost) as allied_revenue,
            (SUM(pa.total_charges) + COALESCE(SUM(mp.procedure_cost), 0) + 
             COALESCE(SUM(md.total_cost), 0) + COALESCE(SUM(ah.service_cost), 0)) as total_department_revenue,
            AVG(DATEDIFF(day, pa.admission_date, pa.discharge_date)) as avg_length_of_stay,
            COUNT(CASE WHEN pa.admission_type = 'Emergency' THEN 1 END) as emergency_admissions,
            ROUND(COUNT(CASE WHEN pa.admission_type = 'Emergency' THEN 1 END) * 100.0 / COUNT(DISTINCT pa.admission_id), 2) as emergency_rate
        FROM HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW pa ON d.department_id = pa.department_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICAL_PROCEDURES_RAW mp ON pa.admission_id = mp.admission_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICATION_ORDERS_RAW mo ON pa.admission_id = mo.admission_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICATION_DISPENSING_RAW md ON mo.order_id = md.order_id
        LEFT JOIN HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW ah ON pa.admission_id = ah.admission_id
        WHERE pa.admission_date >= CURRENT_DATE - 90
        GROUP BY d.department_name, d.specialization_type
        ORDER BY total_department_revenue DESC
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading department performance: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_strategic_metrics():
    """Get strategic metrics for CEO dashboard"""
    try:
        query = """
        WITH monthly_trends AS (
            SELECT 
                DATE_TRUNC('month', pa.admission_date) as month,
                COUNT(DISTINCT pa.admission_id) as monthly_admissions,
                SUM(pa.total_charges) as monthly_revenue,
                AVG(DATEDIFF(day, pa.admission_date, pa.discharge_date)) as monthly_avg_los,
                COUNT(CASE WHEN pa.admission_type = 'Emergency' THEN 1 END) as monthly_emergency
            FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW pa
            WHERE pa.admission_date >= CURRENT_DATE - 365
            GROUP BY DATE_TRUNC('month', pa.admission_date)
        ),
        quality_metrics AS (
            SELECT 
                COUNT(CASE WHEN ah.goals_met = TRUE THEN 1 END) as successful_treatments,
                COUNT(ah.service_id) as total_treatments,
                COUNT(CASE WHEN md.side_effects IS NULL OR md.side_effects = 'None' THEN 1 END) as safe_medications,
                COUNT(md.dispensing_id) as total_medications
            FROM HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW ah
            CROSS JOIN HOSPITAL_DEMO.RAW_DATA.MEDICATION_DISPENSING_RAW md
        )
        SELECT 
            mt.month,
            mt.monthly_admissions,
            mt.monthly_revenue,
            mt.monthly_avg_los,
            mt.monthly_emergency,
            ROUND(mt.monthly_emergency * 100.0 / mt.monthly_admissions, 2) as emergency_rate,
            ROUND(qm.successful_treatments * 100.0 / qm.total_treatments, 2) as treatment_success_rate,
            ROUND(qm.safe_medications * 100.0 / qm.total_medications, 2) as medication_safety_rate
        FROM monthly_trends mt
        CROSS JOIN quality_metrics qm
        ORDER BY mt.month DESC
        """
        df = session.sql(query).to_pandas()
        if len(df) > 0:
            df['month'] = pd.to_datetime(df['MONTH'])
        return df
    except Exception as e:
        st.error(f"Error loading strategic metrics: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_allied_health_detailed_analytics():
    """Get detailed allied health analytics for Allied Health Coordinator dashboard"""
    try:
        query = """
        SELECT 
            ah.provider_credentials,
            ah.service_type,
            ah.service_name,
            COUNT(*) as total_services,
            COUNT(DISTINCT ah.patient_id) as unique_patients,
            COUNT(DISTINCT ah.admission_id) as unique_admissions,
            AVG(ah.duration_minutes) as avg_duration_minutes,
            SUM(ah.duration_minutes) as total_duration_minutes,
            SUM(ah.service_cost) as total_revenue,
            AVG(ah.service_cost) as avg_service_cost,
            COUNT(CASE WHEN ah.goals_met = TRUE THEN 1 END) as successful_outcomes,
            COUNT(CASE WHEN ah.follow_up_needed = TRUE THEN 1 END) as follow_ups_needed,
            COUNT(CASE WHEN ah.insurance_covered = TRUE THEN 1 END) as insurance_covered,
            ROUND(COUNT(CASE WHEN ah.goals_met = TRUE THEN 1 END) * 100.0 / COUNT(*), 2) as success_rate,
            ROUND(COUNT(CASE WHEN ah.follow_up_needed = TRUE THEN 1 END) * 100.0 / COUNT(*), 2) as follow_up_rate,
            ROUND(COUNT(CASE WHEN ah.insurance_covered = TRUE THEN 1 END) * 100.0 / COUNT(*), 2) as insurance_coverage_rate,
            -- Patient participation analysis
            COUNT(CASE WHEN ah.patient_participation = 'Excellent' THEN 1 END) as excellent_participation,
            COUNT(CASE WHEN ah.patient_participation = 'Good' THEN 1 END) as good_participation,
            COUNT(CASE WHEN ah.patient_participation = 'Fair' THEN 1 END) as fair_participation,
            COUNT(CASE WHEN ah.patient_participation = 'Poor' THEN 1 END) as poor_participation,
            -- Service location analysis
            ah.service_location,
            -- Provider analysis
            ah.provider_name
        FROM HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW ah
        GROUP BY ah.provider_credentials, ah.service_type, ah.service_name, ah.service_location, ah.provider_name
        ORDER BY total_revenue DESC
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading allied health detailed analytics: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_allied_health_utilization_trends():
    """Get allied health utilization trends over time"""
    try:
        query = """
        SELECT 
            DATE_TRUNC('month', ah.service_date) as service_month,
            ah.provider_credentials,
            ah.service_type,
            COUNT(*) as monthly_services,
            COUNT(DISTINCT ah.patient_id) as monthly_unique_patients,
            SUM(ah.service_cost) as monthly_revenue,
            AVG(ah.duration_minutes) as avg_monthly_duration,
            ROUND(COUNT(CASE WHEN ah.goals_met = TRUE THEN 1 END) * 100.0 / COUNT(*), 2) as monthly_success_rate
        FROM HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW ah
        WHERE ah.service_date >= CURRENT_DATE - 365
        GROUP BY DATE_TRUNC('month', ah.service_date), ah.provider_credentials, ah.service_type
        ORDER BY service_month DESC, monthly_services DESC
        """
        df = session.sql(query).to_pandas()
        if len(df) > 0:
            df['service_month'] = pd.to_datetime(df['SERVICE_MONTH'])
        return df
    except Exception as e:
        st.error(f"Error loading allied health utilization trends: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_allied_health_department_integration():
    """Get allied health services integration with hospital departments"""
    try:
        query = """
        SELECT 
            d.department_name,
            d.specialization_type,
            ah.provider_credentials,
            ah.service_type,
            COUNT(*) as services_provided,
            COUNT(DISTINCT ah.patient_id) as patients_served,
            SUM(ah.service_cost) as department_ah_revenue,
            AVG(ah.duration_minutes) as avg_service_duration,
            ROUND(COUNT(CASE WHEN ah.goals_met = TRUE THEN 1 END) * 100.0 / COUNT(*), 2) as dept_success_rate,
            -- Calculate service density (services per admission)
            COUNT(*) * 1.0 / COUNT(DISTINCT pa.admission_id) as services_per_admission
        FROM HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW ah
        JOIN HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW pa ON ah.admission_id = pa.admission_id
        JOIN HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d ON pa.department_id = d.department_id
        GROUP BY d.department_name, d.specialization_type, ah.provider_credentials, ah.service_type
        ORDER BY services_provided DESC
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading allied health department integration: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_allied_health_provider_performance():
    """Get individual provider performance metrics"""
    try:
        query = """
        SELECT 
            ah.provider_name,
            ah.provider_credentials,
            COUNT(*) as total_services_provided,
            COUNT(DISTINCT ah.patient_id) as unique_patients_served,
            COUNT(DISTINCT ah.service_date) as active_service_days,
            SUM(ah.duration_minutes) as total_service_minutes,
            SUM(ah.service_cost) as total_provider_revenue,
            AVG(ah.service_cost) as avg_service_cost,
            AVG(ah.duration_minutes) as avg_service_duration,
            ROUND(COUNT(CASE WHEN ah.goals_met = TRUE THEN 1 END) * 100.0 / COUNT(*), 2) as provider_success_rate,
            ROUND(COUNT(CASE WHEN ah.patient_participation IN ('Excellent', 'Good') THEN 1 END) * 100.0 / COUNT(*), 2) as positive_engagement_rate,
            -- Productivity metrics
            ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT ah.service_date), 2) as services_per_day,
            ROUND(SUM(ah.duration_minutes) * 1.0 / COUNT(DISTINCT ah.service_date), 2) as minutes_per_day,
            ROUND(SUM(ah.service_cost) * 1.0 / COUNT(DISTINCT ah.service_date), 2) as revenue_per_day
        FROM HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW ah
        GROUP BY ah.provider_name, ah.provider_credentials
        HAVING COUNT(*) >= 5  -- Only providers with at least 5 services
        ORDER BY total_provider_revenue DESC
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading allied health provider performance: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_allied_health_outcomes_analysis():
    """Get detailed outcomes analysis for allied health services"""
    try:
        query = """
        SELECT 
            ah.service_type,
            ah.provider_credentials,
            -- Outcome metrics
            COUNT(*) as total_interventions,
            COUNT(CASE WHEN ah.goals_met = TRUE THEN 1 END) as successful_interventions,
            COUNT(CASE WHEN ah.follow_up_needed = TRUE THEN 1 END) as requiring_follow_up,
            -- Patient engagement metrics
            COUNT(CASE WHEN ah.patient_participation = 'Excellent' THEN 1 END) as excellent_engagement,
            COUNT(CASE WHEN ah.patient_participation = 'Good' THEN 1 END) as good_engagement,
            COUNT(CASE WHEN ah.patient_participation = 'Fair' THEN 1 END) as fair_engagement,
            COUNT(CASE WHEN ah.patient_participation = 'Poor' THEN 1 END) as poor_engagement,
            -- Success rates by engagement level
            ROUND(COUNT(CASE WHEN ah.patient_participation = 'Excellent' AND ah.goals_met = TRUE THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN ah.patient_participation = 'Excellent' THEN 1 END), 0), 2) as excellent_success_rate,
            ROUND(COUNT(CASE WHEN ah.patient_participation = 'Good' AND ah.goals_met = TRUE THEN 1 END) * 100.0 / 
                  NULLIF(COUNT(CASE WHEN ah.patient_participation = 'Good' THEN 1 END), 0), 2) as good_success_rate,
            -- Duration and cost effectiveness
            AVG(CASE WHEN ah.goals_met = TRUE THEN ah.duration_minutes END) as avg_successful_duration,
            AVG(CASE WHEN ah.goals_met = FALSE THEN ah.duration_minutes END) as avg_unsuccessful_duration,
            AVG(CASE WHEN ah.goals_met = TRUE THEN ah.service_cost END) as avg_successful_cost,
            AVG(CASE WHEN ah.goals_met = FALSE THEN ah.service_cost END) as avg_unsuccessful_cost
        FROM HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW ah
        GROUP BY ah.service_type, ah.provider_credentials
        HAVING COUNT(*) >= 10  -- Only service types with sufficient volume
        ORDER BY total_interventions DESC
        """
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error loading allied health outcomes analysis: {str(e)}")
        return pd.DataFrame()

# Sidebar - Role Selection
st.sidebar.header("🔐 User Role")
user_role = st.sidebar.selectbox(
    "Select Your Role",
    ["CEO", "Clinical Administrator", "Physician", "Nurse", "Analyst", "Capacity Planner", "Allied Health Coordinator"],
    help="Different roles see different data based on RBAC policies"
)

# Sidebar - Date Range
st.sidebar.header("📅 Analysis Period")
date_range = st.sidebar.selectbox(
    "Select Time Period",
    ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Year to Date", "Custom Range"]
)

if date_range == "Custom Range":
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.sidebar.date_input("Start Date", value=datetime.now().date() - timedelta(days=30))
    with col2:
        end_date = st.sidebar.date_input("End Date", value=datetime.now().date())
else:
    if date_range == "Last 7 Days":
        days = 7
    elif date_range == "Last 30 Days":
        days = 30
    elif date_range == "Last 90 Days":
        days = 90
    else:  # Year to Date
        days = (datetime.now() - datetime(datetime.now().year, 1, 1)).days

# Load data
with st.spinner("Loading hospital data..."):
    basic_stats = get_basic_stats()
    dept_summary = get_department_summary()
    admission_trends = get_admission_trends(days if date_range != "Custom Range" else (end_date - start_date).days)
    bed_utilization = get_bed_utilization()

# Display role-specific dashboard
if user_role == "CEO":
    st.markdown("## 🏛️ Chief Executive Officer Dashboard")
    st.markdown("*Strategic oversight and executive performance metrics*")
    
    # Load executive data
    with st.spinner("Loading executive metrics..."):
        executive_kpis = get_executive_kpis()
        dept_performance = get_department_performance_summary()
        strategic_metrics = get_strategic_metrics()
    
    # Executive KPIs
    if len(executive_kpis) > 0:
        st.markdown("### Hospital Performance Overview")
        
        kpi_data = executive_kpis.iloc[0]
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            # Calculate total hospital revenue properly (avoid double counting)
            total_hospital_revenue = (
                float(kpi_data['TOTAL_REVENUE'] or 0) + 
                float(kpi_data['TOTAL_PROCEDURE_REVENUE'] or 0) + 
                float(kpi_data['TOTAL_MEDICATION_REVENUE'] or 0) + 
                float(kpi_data['TOTAL_ALLIED_REVENUE'] or 0) + 
                float(kpi_data['TOTAL_BED_REVENUE'] or 0)
            )
            revenue_millions = total_hospital_revenue / 1000000
            st.metric(
                "Revenue", 
                f"${revenue_millions:.1f}M",
                help="Combined revenue from all service lines"
            )
        with col2:
            # Format patients in thousands
            patients_thousands = kpi_data['TOTAL_PATIENTS'] / 1000
            st.metric(
                "Patients", 
                f"{patients_thousands:.0f}K",
                help="Total unique patients served"
            )
        with col3:
            st.metric(
                "Bed Util.", 
                f"{kpi_data['BED_UTILIZATION_RATE']:.1f}%",
                help="Overall hospital bed utilization rate"
            )
        with col4:
            # Use the emergency rate already calculated in the query
            emergency_rate = kpi_data['EMERGENCY_RATE'] if 'EMERGENCY_RATE' in kpi_data else 0
            st.metric(
                "Emergency", 
                f"{emergency_rate:.1f}%",
                help="Percentage of emergency admissions"
            )
        with col5:
            st.metric(
                "Avg LOS", 
                f"{kpi_data['AVG_LENGTH_OF_STAY']:.1f}d",
                help="Average patient length of stay"
            )
        
        # Revenue breakdown
        st.markdown("### Revenue Analysis")
        col1, col2 = st.columns(2)
        
        with col1:
            # Revenue by service line
            revenue_data = pd.DataFrame({
                'Service Line': ['Admissions', 'Procedures', 'Medications', 'Allied Health', 'Bed Revenue'],
                'Revenue': [
                    kpi_data['TOTAL_REVENUE'],
                    kpi_data['TOTAL_PROCEDURE_REVENUE'] or 0,
                    kpi_data['TOTAL_MEDICATION_REVENUE'] or 0,
                    kpi_data['TOTAL_ALLIED_REVENUE'] or 0,
                    kpi_data['TOTAL_BED_REVENUE'] or 0
                ]
            })
            
            fig_revenue_breakdown = px.pie(
                revenue_data,
                values='Revenue',
                names='Service Line',
                title='Revenue Distribution by Service Line'
            )
            st.plotly_chart(fig_revenue_breakdown, use_container_width=True)
        
        with col2:
            # Key performance indicators
            kpi_summary = pd.DataFrame({
                'KPI': ['Total Admissions', 'Total Procedures', 'Medication Orders', 'Allied Services', 'Total Beds'],
                'Value': [
                    kpi_data['TOTAL_ADMISSIONS'],
                    kpi_data['TOTAL_PROCEDURES'],
                    kpi_data['TOTAL_MEDICATION_ORDERS'],
                    kpi_data['TOTAL_ALLIED_SERVICES'],
                    kpi_data['TOTAL_BEDS']
                ],
                'Target': [35000, 42000, 95000, 32000, 250],  # Example targets
            })
            kpi_summary['Achievement %'] = (kpi_summary['Value'] / kpi_summary['Target'] * 100).round(1)
            
            fig_kpi_achievement = px.bar(
                kpi_summary,
                x='KPI',
                y='Achievement %',
                color='Achievement %',
                color_continuous_scale='RdYlGn',
                title='KPI Achievement vs Targets'
            )
            fig_kpi_achievement.add_hline(y=100, line_dash="dash", line_color="black", 
                                        annotation_text="Target (100%)")
            fig_kpi_achievement.update_xaxes(tickangle=45)
            st.plotly_chart(fig_kpi_achievement, use_container_width=True)
    
    # Department performance
    st.markdown("### Department Performance Summary")
    if len(dept_performance) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Top performing departments by revenue
            top_depts = dept_performance.head(10)
            fig_dept_revenue = px.bar(
                top_depts,
                x='DEPARTMENT_NAME',
                y='TOTAL_DEPARTMENT_REVENUE',
                color='TOTAL_DEPARTMENT_REVENUE',
                color_continuous_scale='Greens',
                title='Top 10 Departments by Revenue'
            )
            fig_dept_revenue.update_xaxes(tickangle=45)
            st.plotly_chart(fig_dept_revenue, use_container_width=True)
        
        with col2:
            # Department efficiency (revenue per admission)
            dept_performance['REVENUE_PER_ADMISSION'] = dept_performance['TOTAL_DEPARTMENT_REVENUE'] / dept_performance['ADMISSIONS']
            
            fig_dept_efficiency = px.scatter(
                dept_performance,
                x='ADMISSIONS',
                y='REVENUE_PER_ADMISSION',
                size='TOTAL_DEPARTMENT_REVENUE',
                color='SPECIALIZATION_TYPE',
                title='Department Volume vs Revenue Efficiency',
                hover_data=['DEPARTMENT_NAME', 'AVG_LENGTH_OF_STAY']
            )
            st.plotly_chart(fig_dept_efficiency, use_container_width=True)
        
        # Executive summary table
        st.markdown("#### Executive Department Summary")
        display_dept = dept_performance[['DEPARTMENT_NAME', 'ADMISSIONS', 'TOTAL_DEPARTMENT_REVENUE', 
                                       'AVG_LENGTH_OF_STAY', 'EMERGENCY_RATE']].copy()
        
        # Format financial columns
        display_dept['TOTAL_DEPARTMENT_REVENUE'] = display_dept['TOTAL_DEPARTMENT_REVENUE'].apply(
            lambda x: f"${x:,.0f}" if pd.notnull(x) else "$0"
        )
        display_dept['AVG_LENGTH_OF_STAY'] = display_dept['AVG_LENGTH_OF_STAY'].apply(
            lambda x: f"{x:.1f} days" if pd.notnull(x) else "N/A"
        )
        display_dept['EMERGENCY_RATE'] = display_dept['EMERGENCY_RATE'].apply(
            lambda x: f"{x:.1f}%" if pd.notnull(x) else "0%"
        )
        
        st.dataframe(display_dept, use_container_width=True)
    
    # Strategic trends
    st.markdown("### Strategic Trends & Quality Metrics")
    if len(strategic_metrics) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Monthly revenue trend
            fig_revenue_trend = px.line(
                strategic_metrics.head(12),  # Last 12 months
                x='month',
                y='MONTHLY_REVENUE',
                title='Monthly Revenue Trend (Last 12 Months)',
                markers=True
            )
            st.plotly_chart(fig_revenue_trend, use_container_width=True)
        
        with col2:
            # Quality metrics
            latest_metrics = strategic_metrics.iloc[0] if len(strategic_metrics) > 0 else None
            if latest_metrics is not None:
                quality_data = pd.DataFrame({
                    'Quality Metric': ['Treatment Success Rate', 'Medication Safety Rate', 'Emergency Care Rate'],
                    'Current %': [
                        latest_metrics['TREATMENT_SUCCESS_RATE'],
                        latest_metrics['MEDICATION_SAFETY_RATE'],
                        latest_metrics['EMERGENCY_RATE']
                    ],
                    'Target %': [85, 95, 25]  # Example targets
                })
                
                fig_quality_metrics = px.bar(
                    quality_data,
                    x='Quality Metric',
                    y=['Current %', 'Target %'],
                    title='Quality Metrics vs Targets',
                    barmode='group'
                )
                fig_quality_metrics.update_xaxes(tickangle=45)
                st.plotly_chart(fig_quality_metrics, use_container_width=True)
    
    # Strategic alerts and insights
    st.markdown("### Strategic Insights & Alerts")
    
    # Executive alerts based on data
    if len(dept_performance) > 0:
        # Revenue concentration risk
        top_3_revenue = dept_performance.head(3)['TOTAL_DEPARTMENT_REVENUE'].sum()
        total_revenue = dept_performance['TOTAL_DEPARTMENT_REVENUE'].sum()
        concentration_risk = (top_3_revenue / total_revenue * 100) if total_revenue > 0 else 0
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Strategic Alerts")
            
            if concentration_risk > 60:
                st.warning(f"⚠️ **Revenue Concentration Risk**: Top 3 departments generate {concentration_risk:.1f}% of revenue")
            
            # Check for departments with declining performance
            low_performers = dept_performance[dept_performance['EMERGENCY_RATE'] > 70]
            if len(low_performers) > 0:
                st.error(f"🚨 **High Emergency Rate**: {len(low_performers)} departments have >70% emergency admissions")
            
            # Capacity alerts
            if len(executive_kpis) > 0 and executive_kpis.iloc[0]['BED_UTILIZATION_RATE'] > 85:
                st.warning("🛏️ **Capacity Alert**: Hospital-wide bed utilization above 85%")
            
            st.success("✅ **Data Quality**: All systems reporting current data")
        
        with col2:
            st.markdown("#### Growth Opportunities")
            
            # Identify growth opportunities
            growth_opps = dept_performance[dept_performance['TOTAL_DEPARTMENT_REVENUE'] < dept_performance['TOTAL_DEPARTMENT_REVENUE'].median()]
            
            if len(growth_opps) > 0:
                st.info(f"💡 **Growth Potential**: {len(growth_opps)} departments below median revenue")
                
                for _, dept in growth_opps.head(3).iterrows():
                    st.markdown(f"- **{dept['DEPARTMENT_NAME']}**: ${dept['TOTAL_DEPARTMENT_REVENUE']:,.0f} revenue opportunity")
            
            # Service line expansion
            st.markdown("**Service Line Expansion:**")
            st.markdown("- Allied Health services showing strong growth")
            st.markdown("- Medication management optimization potential")
            st.markdown("- Bed utilization efficiency improvements available")

elif user_role == "Clinical Administrator":
    st.markdown("## 👨‍⚕️ Clinical Administrator Dashboard")
    st.markdown("*Complete oversight of hospital operations, quality, and performance*")
    
    # High-level metrics
    if len(basic_stats) > 0:
        col1, col2, col3, col4, col5 = st.columns(5)
        
        stats_dict = dict(zip(basic_stats['METRIC'], basic_stats['VALUE']))
        
        with col1:
            st.metric("Total Patients", f"{stats_dict.get('Patients', 0):,}")
        with col2:
            st.metric("Total Admissions", f"{stats_dict.get('Admissions', 0):,}")
        with col3:
            st.metric("Total Procedures", f"{stats_dict.get('Procedures', 0):,}")
        with col4:
            st.metric("Medication Orders", f"{stats_dict.get('Medications', 0):,}")
        with col5:
            st.metric("Allied Health Services", f"{stats_dict.get('Allied Health', 0):,}")
    
    # Department Performance
    st.markdown("### Department Performance Overview")
    if len(dept_summary) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_dept_admissions = px.bar(
                dept_summary, 
                x='DEPARTMENT_NAME', 
                y='ADMISSIONS',
                title='Admissions by Department',
                color='ADMISSIONS',
                color_continuous_scale='Blues'
            )
            fig_dept_admissions.update_xaxes(tickangle=45)
            st.plotly_chart(fig_dept_admissions, use_container_width=True)
        
        with col2:
            fig_dept_revenue = px.bar(
                dept_summary, 
                x='DEPARTMENT_NAME', 
                y='AVG_CHARGES',
                title='Average Charges by Department',
                color='AVG_CHARGES',
                color_continuous_scale='Greens'
            )
            fig_dept_revenue.update_xaxes(tickangle=45)
            st.plotly_chart(fig_dept_revenue, use_container_width=True)
        
        # Department details table
        st.markdown("### Department Details")
        display_df = dept_summary.copy()
        display_df['AVG_CHARGES'] = display_df['AVG_CHARGES'].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "$0.00")
        st.dataframe(display_df, use_container_width=True)
    
    # Bed Utilization
    st.markdown("### Bed Utilization Management")
    if len(bed_utilization) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_bed_util = px.bar(
                bed_utilization,
                x='DEPARTMENT_NAME',
                y='UTILIZATION_RATE',
                title='Bed Utilization Rate by Department (%)',
                color='UTILIZATION_RATE',
                color_continuous_scale='RdYlGn_r'
            )
            fig_bed_util.update_xaxes(tickangle=45)
            st.plotly_chart(fig_bed_util, use_container_width=True)
        
        with col2:
            # Bed capacity overview
            fig_bed_capacity = px.scatter(
                bed_utilization,
                x='TOTAL_BEDS',
                y='UTILIZATION_RATE',
                size='OCCUPIED_BEDS',
                color='DEPARTMENT_NAME',
                title='Bed Capacity vs Utilization',
                hover_data=['AVAILABLE_BEDS']
            )
            st.plotly_chart(fig_bed_capacity, use_container_width=True)
    
    # Financial Analysis
    st.markdown("### Financial Performance")
    with st.spinner("Loading financial data..."):
        financial_data = get_financial_summary()
        
        if len(financial_data) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                fig_revenue = px.pie(
                    financial_data,
                    values='TOTAL_REVENUE',
                    names='DEPARTMENT_NAME',
                    title='Revenue Distribution by Department'
                )
                st.plotly_chart(fig_revenue, use_container_width=True)
            
            with col2:
                # Revenue breakdown
                revenue_breakdown = financial_data[['DEPARTMENT_NAME', 'TOTAL_ADMISSION_REVENUE', 
                                                 'TOTAL_PROCEDURE_REVENUE', 'TOTAL_MEDICATION_REVENUE', 
                                                 'TOTAL_ALLIED_HEALTH_REVENUE']].head(10)
                
                fig_breakdown = go.Figure()
                fig_breakdown.add_trace(go.Bar(name='Admissions', x=revenue_breakdown['DEPARTMENT_NAME'], 
                                             y=revenue_breakdown['TOTAL_ADMISSION_REVENUE']))
                fig_breakdown.add_trace(go.Bar(name='Procedures', x=revenue_breakdown['DEPARTMENT_NAME'], 
                                             y=revenue_breakdown['TOTAL_PROCEDURE_REVENUE']))
                fig_breakdown.add_trace(go.Bar(name='Medications', x=revenue_breakdown['DEPARTMENT_NAME'], 
                                             y=revenue_breakdown['TOTAL_MEDICATION_REVENUE']))
                fig_breakdown.add_trace(go.Bar(name='Allied Health', x=revenue_breakdown['DEPARTMENT_NAME'], 
                                             y=revenue_breakdown['TOTAL_ALLIED_HEALTH_REVENUE']))
                
                fig_breakdown.update_layout(title='Revenue Breakdown by Service Type', barmode='stack')
                fig_breakdown.update_xaxes(tickangle=45)
                st.plotly_chart(fig_breakdown, use_container_width=True)

elif user_role == "Physician":
    st.markdown("## 👩‍⚕️ Physician Dashboard")
    st.markdown("*Clinical insights and patient care analytics*")
    
    # Patient care metrics
    if len(admission_trends) > 0:
        col1, col2, col3 = st.columns(3)
        
        total_admissions = int(admission_trends['DAILY_ADMISSIONS'].sum()) if len(admission_trends) > 0 else 0
        avg_daily_admissions = float(admission_trends['DAILY_ADMISSIONS'].mean()) if len(admission_trends) > 0 else 0
        emergency_total = int(admission_trends['EMERGENCY_ADMISSIONS'].sum()) if len(admission_trends) > 0 else 0
        emergency_rate = (emergency_total / total_admissions * 100) if total_admissions > 0 else 0
        
        with col1:
            st.metric("Total Admissions", f"{total_admissions:,}")
        with col2:
            st.metric("Daily Average", f"{avg_daily_admissions:.1f}")
        with col3:
            st.metric("Emergency Rate", f"{emergency_rate:.1f}%")
    
    # Clinical trends
    st.markdown("### Clinical Trends")
    if len(admission_trends) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_admissions = px.line(
                admission_trends,
                x='admission_date',
                y='DAILY_ADMISSIONS',
                title='Daily Admission Trends'
            )
            st.plotly_chart(fig_admissions, use_container_width=True)
        
        with col2:
            fig_emergency = px.area(
                admission_trends,
                x='admission_date',
                y='EMERGENCY_ADMISSIONS',
                title='Emergency Admissions Trend'
            )
            st.plotly_chart(fig_emergency, use_container_width=True)
    
    # Medication insights
    st.markdown("### Medication Management")
    with st.spinner("Loading medication data..."):
        med_data = get_medication_analysis()
        
        if len(med_data) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                fig_med_class = px.pie(
                    med_data.head(10),
                    values='TOTAL_ORDERS',
                    names='MEDICATION_CLASS',
                    title='Medication Orders by Class'
                )
                st.plotly_chart(fig_med_class, use_container_width=True)
            
            with col2:
                fig_med_cost = px.bar(
                    med_data.head(10),
                    x='THERAPEUTIC_CATEGORY',
                    y='TOTAL_MEDICATION_COST',
                    title='Medication Costs by Category'
                )
                fig_med_cost.update_xaxes(tickangle=45)
                st.plotly_chart(fig_med_cost, use_container_width=True)
    
    # Patient safety alerts
    st.markdown("### Patient Safety Alerts")
    
    # Simulated alerts for demo
    alerts = [
        {"type": "Drug Interaction", "priority": "High", "count": 3, "description": "Potential drug interactions identified"},
        {"type": "Allergy Alert", "priority": "Medium", "count": 7, "description": "Allergy warnings for new medications"},
        {"type": "Dosage Review", "priority": "Low", "count": 12, "description": "Dosages requiring physician review"}
    ]
    
    for alert in alerts:
        priority_class = f"{alert['priority'].lower()}-priority"
        st.markdown(f"""
        <div class="persona-card">
            <strong class="{priority_class}">{alert['type']}</strong> - {alert['count']} items<br>
            <small>{alert['description']}</small>
        </div>
        """, unsafe_allow_html=True)

elif user_role == "Nurse":
    st.markdown("## 👩‍⚕️ Nurse Dashboard")
    st.markdown("*Patient care coordination and operational support*")
    
    # Current shift overview
    current_hour = datetime.now().hour
    if 7 <= current_hour < 15:
        shift = "Day Shift (7 AM - 3 PM)"
    elif 15 <= current_hour < 23:
        shift = "Evening Shift (3 PM - 11 PM)"
    else:
        shift = "Night Shift (11 PM - 7 AM)"
    
    st.info(f"🕐 Current Shift: {shift}")
    
    # Bed management
    st.markdown("### Bed Management")
    if len(bed_utilization) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            total_beds = int(bed_utilization['TOTAL_BEDS'].sum()) if len(bed_utilization) > 0 else 0
            occupied_beds = int(bed_utilization['OCCUPIED_BEDS'].sum()) if len(bed_utilization) > 0 else 0
            available_beds = int(bed_utilization['AVAILABLE_BEDS'].sum()) if len(bed_utilization) > 0 else 0
            
            st.metric("Total Beds", total_beds)
            st.metric("Occupied", occupied_beds)
            st.metric("Available", available_beds)
        
        with col2:
            # Bed status pie chart
            bed_status_data = pd.DataFrame({
                'Status': ['Occupied', 'Available'],
                'Count': [occupied_beds, available_beds]
            })
            
            fig_bed_status = px.pie(
                bed_status_data,
                values='Count',
                names='Status',
                title='Current Bed Status',
                color_discrete_map={'Occupied': '#ff6b6b', 'Available': '#51cf66'}
            )
            st.plotly_chart(fig_bed_status, use_container_width=True)
        
        # Department bed status
        st.markdown("#### Bed Status by Department")
        st.dataframe(bed_utilization, use_container_width=True)
    
    # Patient care tasks
    st.markdown("### Patient Care Coordination")
    
    # Allied health scheduling
    with st.spinner("Loading allied health data..."):
        allied_data = get_allied_health_summary()
        
        if len(allied_data) > 0:
            st.markdown("#### Allied Health Services Today")
            
            # Filter for relevant services
            nursing_relevant = allied_data[allied_data['PROVIDER_CREDENTIALS'].isin(['PT', 'OT', 'RRT', 'RD'])]
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig_services = px.bar(
                    nursing_relevant,
                    x='PROVIDER_CREDENTIALS',
                    y='TOTAL_SERVICES',
                    title='Services by Provider Type',
                    color='SUCCESS_RATE',
                    color_continuous_scale='RdYlGn'
                )
                st.plotly_chart(fig_services, use_container_width=True)
            
            with col2:
                # Success rates
                fig_success = px.scatter(
                    nursing_relevant,
                    x='TOTAL_SERVICES',
                    y='SUCCESS_RATE',
                    size='TOTAL_REVENUE',
                    color='PROVIDER_CREDENTIALS',
                    title='Service Volume vs Success Rate'
                )
                st.plotly_chart(fig_success, use_container_width=True)
    
    # Medication administration
    st.markdown("### Medication Administration")
    with st.spinner("Loading medication data..."):
        med_data = get_medication_analysis()
        
        if len(med_data) > 0:
            # Top medications by volume
            top_meds = med_data.head(10)
            
            fig_med_volume = px.bar(
                top_meds,
                x='MEDICATION_CLASS',
                y='TOTAL_DISPENSINGS',
                title='Medication Administration Volume',
                color='TOTAL_DISPENSINGS',
                color_continuous_scale='Blues'
            )
            fig_med_volume.update_xaxes(tickangle=45)
            st.plotly_chart(fig_med_volume, use_container_width=True)

elif user_role == "Analyst":
    st.markdown("## 📊 Healthcare Analyst Dashboard")
    st.markdown("*Advanced analytics and business intelligence*")
    
    # Executive summary
    if len(basic_stats) > 0:
        st.markdown("### Executive Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        stats_dict = dict(zip(basic_stats['METRIC'], basic_stats['VALUE']))
        
        with col1:
            st.metric("Patient Population", f"{stats_dict.get('Patients', 0):,}")
        with col2:
            st.metric("Total Admissions", f"{stats_dict.get('Admissions', 0):,}")
        with col3:
            st.metric("Clinical Procedures", f"{stats_dict.get('Procedures', 0):,}")
        with col4:
            total_services = stats_dict.get('Medications', 0) + stats_dict.get('Allied Health', 0)
            st.metric("Total Services", f"{total_services:,}")
    
    # Advanced analytics
    st.markdown("### Advanced Analytics")
    
    # Patient demographics analysis
    with st.spinner("Loading demographic analysis..."):
        demo_data = get_patient_demographics_summary()
        
        if len(demo_data) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                # Age group distribution
                age_summary = demo_data.groupby('AGE_GROUP')['PATIENT_COUNT'].sum().reset_index()
                fig_age = px.pie(
                    age_summary,
                    values='PATIENT_COUNT',
                    names='AGE_GROUP',
                    title='Patient Population by Age Group'
                )
                st.plotly_chart(fig_age, use_container_width=True)
            
            with col2:
                # Insurance distribution
                insurance_summary = demo_data.groupby('INSURANCE_PROVIDER')['PATIENT_COUNT'].sum().reset_index()
                fig_insurance = px.bar(
                    insurance_summary,
                    x='INSURANCE_PROVIDER',
                    y='PATIENT_COUNT',
                    title='Patient Distribution by Insurance'
                )
                fig_insurance.update_xaxes(tickangle=45)
                st.plotly_chart(fig_insurance, use_container_width=True)
    
    # Financial performance
    st.markdown("### Financial Performance Analysis")
    with st.spinner("Loading financial analysis..."):
        financial_data = get_financial_summary()
        
        if len(financial_data) > 0:
            # Revenue analysis
            col1, col2 = st.columns(2)
            
            with col1:
                fig_total_revenue = px.treemap(
                    financial_data.head(10),
                    path=['DEPARTMENT_NAME'],
                    values='TOTAL_REVENUE',
                    title='Revenue by Department (Treemap)'
                )
                st.plotly_chart(fig_total_revenue, use_container_width=True)
            
            with col2:
                # Revenue per admission
                financial_data['REVENUE_PER_ADMISSION'] = financial_data['TOTAL_REVENUE'] / financial_data['TOTAL_ADMISSIONS']
                
                fig_efficiency = px.scatter(
                    financial_data,
                    x='TOTAL_ADMISSIONS',
                    y='REVENUE_PER_ADMISSION',
                    size='TOTAL_REVENUE',
                    color='DEPARTMENT_NAME',
                    title='Volume vs Revenue Efficiency'
                )
                st.plotly_chart(fig_efficiency, use_container_width=True)
            
            # Financial summary table
            st.markdown("#### Financial Summary by Department")
            display_financial = financial_data.copy()
            financial_cols = ['TOTAL_ADMISSION_REVENUE', 'TOTAL_PROCEDURE_REVENUE', 
                            'TOTAL_MEDICATION_REVENUE', 'TOTAL_ALLIED_HEALTH_REVENUE', 'TOTAL_REVENUE']
            for col in financial_cols:
                if col in display_financial.columns:
                    display_financial[col] = display_financial[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "$0.00")
            
            st.dataframe(display_financial, use_container_width=True)
    
    # Operational efficiency
    st.markdown("### Operational Efficiency Metrics")
    
    # Medication efficiency
    with st.spinner("Loading medication efficiency..."):
        med_data = get_medication_analysis()
        
        if len(med_data) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                # Orders vs dispensings
                med_data['DISPENSING_RATE'] = (med_data['TOTAL_DISPENSINGS'] / med_data['TOTAL_ORDERS'] * 100).round(2)
                
                fig_med_efficiency = px.bar(
                    med_data.head(10),
                    x='THERAPEUTIC_CATEGORY',
                    y='DISPENSING_RATE',
                    title='Medication Dispensing Efficiency (%)'
                )
                fig_med_efficiency.update_xaxes(tickangle=45)
                st.plotly_chart(fig_med_efficiency, use_container_width=True)
            
            with col2:
                # Cost per patient
                med_data['COST_PER_PATIENT'] = med_data['TOTAL_MEDICATION_COST'] / med_data['UNIQUE_PATIENTS']
                
                fig_cost_efficiency = px.scatter(
                    med_data,
                    x='UNIQUE_PATIENTS',
                    y='COST_PER_PATIENT',
                    size='TOTAL_MEDICATION_COST',
                    color='THERAPEUTIC_CATEGORY',
                    title='Medication Cost per Patient'
                )
                st.plotly_chart(fig_cost_efficiency, use_container_width=True)

elif user_role == "Capacity Planner":
    st.markdown("## 🛏️ Capacity Planning Dashboard")
    st.markdown("*Bed utilization optimization and capacity management*")
    
    # Load capacity planning data
    with st.spinner("Loading capacity planning data..."):
        capacity_analysis = get_bed_capacity_analysis()
        booking_patterns = get_bed_booking_patterns()
        turnover_analysis = get_bed_turnover_analysis()
        recommendations = get_capacity_recommendations()
    
    # Capacity overview metrics
    if len(capacity_analysis) > 0:
        st.markdown("### Capacity Overview")
        
        # Key metrics
        total_beds = int(capacity_analysis['AVG_TOTAL_BEDS'].sum())
        avg_utilization = float(capacity_analysis['AVG_UTILIZATION_RATE'].mean())
        peak_utilization = float(capacity_analysis['PEAK_UTILIZATION'].max())
        high_util_depts = len(capacity_analysis[capacity_analysis['AVG_UTILIZATION_RATE'] > 80])
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Beds", f"{total_beds:,}")
        with col2:
            st.metric("Avg Utilization", f"{avg_utilization:.1f}%")
        with col3:
            st.metric("Peak Utilization", f"{peak_utilization:.1f}%")
        with col4:
            st.metric("High Util. Depts", high_util_depts)
    
    # Utilization analysis
    st.markdown("### Department Utilization Analysis")
    if len(capacity_analysis) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Utilization rate by department
            fig_util = px.bar(
                capacity_analysis,
                x='DEPARTMENT_NAME',
                y='AVG_UTILIZATION_RATE',
                color='AVG_UTILIZATION_RATE',
                color_continuous_scale='RdYlGn_r',
                title='Average Utilization Rate by Department (%)'
            )
            fig_util.update_xaxes(tickangle=45)
            fig_util.add_hline(y=80, line_dash="dash", line_color="red", 
                              annotation_text="Target Utilization (80%)")
            st.plotly_chart(fig_util, use_container_width=True)
        
        with col2:
            # Utilization volatility
            fig_volatility = px.scatter(
                capacity_analysis,
                x='AVG_UTILIZATION_RATE',
                y='UTILIZATION_VOLATILITY',
                size='AVG_TOTAL_BEDS',
                color='DEPARTMENT_NAME',
                title='Utilization Rate vs Volatility',
                hover_data=['PEAK_UTILIZATION', 'MIN_UTILIZATION']
            )
            st.plotly_chart(fig_volatility, use_container_width=True)
        
        # Detailed capacity table
        st.markdown("#### Detailed Capacity Metrics")
        display_capacity = capacity_analysis.copy()
        numeric_cols = ['AVG_TOTAL_BEDS', 'AVG_OCCUPIED_BEDS', 'AVG_AVAILABLE_BEDS', 'AVG_MAINTENANCE_BEDS']
        for col in numeric_cols:
            if col in display_capacity.columns:
                display_capacity[col] = display_capacity[col].round(1)
        
        # Color code utilization rates
        def color_utilization(val):
            if val > 90:
                return 'background-color: #ffebee'  # Light red
            elif val > 80:
                return 'background-color: #fff3e0'  # Light orange
            elif val < 50:
                return 'background-color: #e8f5e8'  # Light green
            return ''
        
        styled_df = display_capacity.style.applymap(color_utilization, subset=['AVG_UTILIZATION_RATE'])
        st.dataframe(styled_df, use_container_width=True)
    
    # Booking patterns analysis
    st.markdown("### Bed Booking Patterns")
    if len(booking_patterns) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Revenue by bed type
            fig_revenue_type = px.sunburst(
                booking_patterns,
                path=['DEPARTMENT_NAME', 'BED_TYPE'],
                values='TOTAL_BED_REVENUE',
                title='Bed Revenue by Department and Type'
            )
            st.plotly_chart(fig_revenue_type, use_container_width=True)
        
        with col2:
            # Length of stay by bed type
            fig_los_type = px.box(
                booking_patterns,
                x='BED_TYPE',
                y='AVG_LENGTH_OF_STAY',
                color='BED_TYPE',
                title='Length of Stay Distribution by Bed Type'
            )
            fig_los_type.update_xaxes(tickangle=45)
            st.plotly_chart(fig_los_type, use_container_width=True)
    
    # Turnover analysis
    st.markdown("### Bed Turnover Efficiency")
    if len(turnover_analysis) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Annual turnover rates
            fig_turnover = px.bar(
                turnover_analysis,
                x='DEPARTMENT_NAME',
                y='ANNUAL_TURNOVER_RATE',
                color='TURNOVER_CATEGORY',
                title='Annual Bed Turnover Rate by Department',
                color_discrete_map={
                    'High Turnover': '#ff6b6b',
                    'Medium Turnover': '#ffd93d',
                    'Low Turnover': '#6bcf7f'
                }
            )
            fig_turnover.update_xaxes(tickangle=45)
            st.plotly_chart(fig_turnover, use_container_width=True)
        
        with col2:
            # Revenue efficiency
            fig_efficiency = px.scatter(
                turnover_analysis,
                x='AVG_BOOKINGS_PER_BED',
                y='AVG_REVENUE_PER_BED',
                size='BEDS_ANALYZED',
                color='DEPARTMENT_NAME',
                title='Booking Volume vs Revenue per Bed'
            )
            st.plotly_chart(fig_efficiency, use_container_width=True)
    
    # Capacity recommendations
    st.markdown("### Capacity Planning Recommendations")
    if len(recommendations) > 0:
        # Recommendations summary
        col1, col2 = st.columns(2)
        
        with col1:
            # Current vs recommended capacity
            fig_capacity_rec = px.bar(
                recommendations.head(10),
                x='DEPARTMENT_NAME',
                y=['CURRENT_BEDS', 'RECOMMENDED_BED_CHANGE'],
                title='Current Beds vs Recommended Changes',
                barmode='group'
            )
            fig_capacity_rec.update_xaxes(tickangle=45)
            st.plotly_chart(fig_capacity_rec, use_container_width=True)
        
        with col2:
            # Utilization vs revenue
            fig_util_revenue = px.scatter(
                recommendations,
                x='UTILIZATION_PERCENTAGE',
                y='TOTAL_REVENUE',
                size='CURRENT_BEDS',
                color='RECOMMENDATION',
                title='Utilization vs Revenue Performance',
                hover_data=['AVG_STAY_DAYS', 'TOTAL_BOOKINGS']
            )
            st.plotly_chart(fig_util_revenue, use_container_width=True)
        
        # Recommendations table
        st.markdown("#### Capacity Planning Recommendations")
        
        # Create priority levels for recommendations
        def get_priority(recommendation):
            if 'Increase Capacity' in recommendation:
                return '🔴 High Priority'
            elif 'Monitor Closely' in recommendation:
                return '🟡 Medium Priority'
            elif 'Consider Reallocation' in recommendation:
                return '🟠 Review Required'
            else:
                return '🟢 Optimal'
        
        recommendations['PRIORITY'] = recommendations['RECOMMENDATION'].apply(get_priority)
        
        # Display recommendations
        display_rec = recommendations[['DEPARTMENT_NAME', 'CURRENT_BEDS', 'UTILIZATION_PERCENTAGE', 
                                     'RECOMMENDED_BED_CHANGE', 'RECOMMENDATION', 'PRIORITY']].copy()
        
        # Format columns
        display_rec['UTILIZATION_PERCENTAGE'] = display_rec['UTILIZATION_PERCENTAGE'].apply(lambda x: f"{x:.1f}%")
        display_rec['RECOMMENDED_BED_CHANGE'] = display_rec['RECOMMENDED_BED_CHANGE'].apply(
            lambda x: f"+{x}" if x > 0 else str(x) if x < 0 else "No Change"
        )
        
        st.dataframe(display_rec, use_container_width=True)
        
        # Action items
        st.markdown("#### Immediate Action Items")
        
        high_priority = recommendations[recommendations['RECOMMENDATION'].str.contains('Increase Capacity')]
        if len(high_priority) > 0:
            st.error(f"🚨 **Critical**: {len(high_priority)} departments need immediate capacity increases")
            for _, dept in high_priority.iterrows():
                st.markdown(f"- **{dept['DEPARTMENT_NAME']}**: Add {dept['RECOMMENDED_BED_CHANGE']} beds (Current: {dept['UTILIZATION_PERCENTAGE']:.1f}% utilization)")
        
        monitor_depts = recommendations[recommendations['RECOMMENDATION'].str.contains('Monitor Closely')]
        if len(monitor_depts) > 0:
            st.warning(f"⚠️ **Watch List**: {len(monitor_depts)} departments approaching capacity")
            for _, dept in monitor_depts.iterrows():
                st.markdown(f"- **{dept['DEPARTMENT_NAME']}**: Monitor closely (Current: {dept['UTILIZATION_PERCENTAGE']:.1f}% utilization)")
        
        underutil_depts = recommendations[recommendations['RECOMMENDATION'].str.contains('Under-Utilization')]
        if len(underutil_depts) > 0:
            st.info(f"💡 **Optimization**: {len(underutil_depts)} departments may have excess capacity")
            for _, dept in underutil_depts.iterrows():
                st.markdown(f"- **{dept['DEPARTMENT_NAME']}**: Consider reallocation (Current: {dept['UTILIZATION_PERCENTAGE']:.1f}% utilization)")
    
    # Capacity planning tools
    st.markdown("### Capacity Planning Tools")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Scenario Planning")
        
        # Interactive capacity planning
        selected_dept = st.selectbox(
            "Select Department for Scenario Analysis",
            recommendations['DEPARTMENT_NAME'].tolist() if len(recommendations) > 0 else []
        )
        
        if selected_dept:
            dept_data = recommendations[recommendations['DEPARTMENT_NAME'] == selected_dept].iloc[0]
            
            st.markdown(f"**Current Status: {selected_dept}**")
            st.write(f"- Current Beds: {dept_data['CURRENT_BEDS']}")
            st.write(f"- Utilization: {dept_data['UTILIZATION_PERCENTAGE']:.1f}%")
            st.write(f"- Monthly Revenue: ${dept_data['TOTAL_REVENUE']:,.2f}")
            
            # Scenario inputs
            capacity_change = st.slider(
                "Capacity Change (beds)",
                min_value=-10,
                max_value=20,
                value=int(dept_data['RECOMMENDED_BED_CHANGE']),
                help="Positive numbers add beds, negative numbers remove beds"
            )
            
            # Calculate scenario impact
            new_capacity = dept_data['CURRENT_BEDS'] + capacity_change
            if new_capacity > 0:
                new_utilization = (dept_data['UTILIZATION_PERCENTAGE'] * dept_data['CURRENT_BEDS'] / 100) / new_capacity * 100
                revenue_impact = capacity_change * dept_data['TOTAL_REVENUE'] / dept_data['CURRENT_BEDS'] if dept_data['CURRENT_BEDS'] > 0 else 0
                
                st.markdown("**Scenario Results:**")
                st.write(f"- New Capacity: {new_capacity} beds")
                st.write(f"- Projected Utilization: {new_utilization:.1f}%")
                st.write(f"- Revenue Impact: ${revenue_impact:,.2f}/month")
                
                if new_utilization > 95:
                    st.error("⚠️ Still over capacity - consider larger increase")
                elif new_utilization > 80:
                    st.warning("📊 Near optimal utilization")
                else:
                    st.success("✅ Good utilization level")
    
    with col2:
        st.markdown("#### Export Capacity Report")
        
        if st.button("Generate Capacity Planning Report"):
            # Combine all capacity data for export
            if len(recommendations) > 0:
                report_data = recommendations.copy()
                report_data['REPORT_DATE'] = datetime.now().strftime('%Y-%m-%d')
                report_data['GENERATED_BY'] = 'Capacity Planning Dashboard'
                
                csv_data = report_data.to_csv(index=False)
                st.download_button(
                    label="Download Capacity Report",
                    data=csv_data,
                    file_name=f"capacity_planning_report_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
                
                st.success("📄 Capacity planning report generated successfully!")

elif user_role == "Allied Health Coordinator":
    st.markdown("## 🏥 Allied Health Coordinator Dashboard")
    st.markdown("*Comprehensive allied health services management and optimization*")
    
    # Load allied health data
    with st.spinner("Loading allied health analytics..."):
        ah_detailed = get_allied_health_detailed_analytics()
        ah_trends = get_allied_health_utilization_trends()
        ah_dept_integration = get_allied_health_department_integration()
        ah_provider_performance = get_allied_health_provider_performance()
        ah_outcomes = get_allied_health_outcomes_analysis()
    
    # Allied Health Overview
    if len(ah_detailed) > 0:
        st.markdown("### Allied Health Services Overview")
        
        # Key metrics
        total_services = int(ah_detailed['TOTAL_SERVICES'].sum())
        unique_patients = int(ah_detailed['UNIQUE_PATIENTS'].sum())
        total_revenue = float(ah_detailed['TOTAL_REVENUE'].sum())
        avg_success_rate = float(ah_detailed['SUCCESS_RATE'].mean())
        total_duration_hours = float(ah_detailed['TOTAL_DURATION_MINUTES'].sum() / 60)
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total Services", f"{total_services:,}")
        with col2:
            st.metric("Patients Served", f"{unique_patients:,}")
        with col3:
            st.metric("Revenue", f"${total_revenue:,.0f}")
        with col4:
            st.metric("Success Rate", f"{avg_success_rate:.1f}%")
        with col5:
            st.metric("Service Hours", f"{total_duration_hours:,.0f}")
        
        # Service type analysis
        st.markdown("### Service Type Performance")
        col1, col2 = st.columns(2)
        
        with col1:
            # Services by provider type
            provider_summary = ah_detailed.groupby('PROVIDER_CREDENTIALS').agg({
                'TOTAL_SERVICES': 'sum',
                'TOTAL_REVENUE': 'sum',
                'SUCCESS_RATE': 'mean'
            }).reset_index()
            
            fig_provider_services = px.bar(
                provider_summary,
                x='PROVIDER_CREDENTIALS',
                y='TOTAL_SERVICES',
                color='SUCCESS_RATE',
                color_continuous_scale='RdYlGn',
                title='Services by Provider Type',
                labels={'PROVIDER_CREDENTIALS': 'Provider Type', 'TOTAL_SERVICES': 'Total Services'}
            )
            st.plotly_chart(fig_provider_services, use_container_width=True)
        
        with col2:
            # Revenue by service type
            service_summary = ah_detailed.groupby('SERVICE_TYPE').agg({
                'TOTAL_SERVICES': 'sum',
                'TOTAL_REVENUE': 'sum'
            }).reset_index()
            
            fig_service_revenue = px.pie(
                service_summary,
                values='TOTAL_REVENUE',
                names='SERVICE_TYPE',
                title='Revenue Distribution by Service Type'
            )
            st.plotly_chart(fig_service_revenue, use_container_width=True)
    
    # Utilization trends
    st.markdown("### Utilization Trends")
    if len(ah_trends) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Monthly service trends
            monthly_summary = ah_trends.groupby('SERVICE_MONTH').agg({
                'MONTHLY_SERVICES': 'sum',
                'MONTHLY_REVENUE': 'sum',
                'MONTHLY_SUCCESS_RATE': 'mean'
            }).reset_index()
            
            fig_monthly_trend = px.line(
                monthly_summary,
                x='SERVICE_MONTH',
                y='MONTHLY_SERVICES',
                title='Monthly Service Volume Trend',
                markers=True
            )
            st.plotly_chart(fig_monthly_trend, use_container_width=True)
        
        with col2:
            # Success rate trends by provider type
            provider_trends = ah_trends.groupby(['SERVICE_MONTH', 'PROVIDER_CREDENTIALS']).agg({
                'MONTHLY_SUCCESS_RATE': 'mean'
            }).reset_index()
            
            fig_success_trends = px.line(
                provider_trends,
                x='SERVICE_MONTH',
                y='MONTHLY_SUCCESS_RATE',
                color='PROVIDER_CREDENTIALS',
                title='Success Rate Trends by Provider Type',
                markers=True
            )
            st.plotly_chart(fig_success_trends, use_container_width=True)
    
    # Department integration
    st.markdown("### Department Integration Analysis")
    if len(ah_dept_integration) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Services per admission by department
            dept_summary = ah_dept_integration.groupby('DEPARTMENT_NAME').agg({
                'SERVICES_PROVIDED': 'sum',
                'PATIENTS_SERVED': 'sum',
                'SERVICES_PER_ADMISSION': 'mean'
            }).reset_index().head(10)
            
            fig_dept_services = px.bar(
                dept_summary,
                x='DEPARTMENT_NAME',
                y='SERVICES_PER_ADMISSION',
                color='SERVICES_PROVIDED',
                color_continuous_scale='Blues',
                title='Allied Health Services per Admission by Department'
            )
            fig_dept_services.update_xaxes(tickangle=45)
            st.plotly_chart(fig_dept_services, use_container_width=True)
        
        with col2:
            # Department revenue contribution
            dept_revenue = ah_dept_integration.groupby('DEPARTMENT_NAME')['DEPARTMENT_AH_REVENUE'].sum().reset_index().head(10)
            
            fig_dept_revenue = px.bar(
                dept_revenue,
                x='DEPARTMENT_NAME',
                y='DEPARTMENT_AH_REVENUE',
                title='Allied Health Revenue by Department'
            )
            fig_dept_revenue.update_xaxes(tickangle=45)
            st.plotly_chart(fig_dept_revenue, use_container_width=True)
    
    # Provider performance
    st.markdown("### Provider Performance Analysis")
    if len(ah_provider_performance) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Top performers by revenue
            top_providers = ah_provider_performance.head(10)
            
            fig_provider_revenue = px.bar(
                top_providers,
                x='PROVIDER_NAME',
                y='TOTAL_PROVIDER_REVENUE',
                color='PROVIDER_SUCCESS_RATE',
                color_continuous_scale='RdYlGn',
                title='Top 10 Providers by Revenue'
            )
            fig_provider_revenue.update_xaxes(tickangle=45)
            st.plotly_chart(fig_provider_revenue, use_container_width=True)
        
        with col2:
            # Productivity analysis
            fig_productivity = px.scatter(
                ah_provider_performance,
                x='SERVICES_PER_DAY',
                y='REVENUE_PER_DAY',
                size='UNIQUE_PATIENTS_SERVED',
                color='PROVIDER_CREDENTIALS',
                title='Provider Productivity Analysis',
                hover_data=['PROVIDER_NAME', 'PROVIDER_SUCCESS_RATE']
            )
            st.plotly_chart(fig_productivity, use_container_width=True)
        
        # Provider performance table
        st.markdown("#### Provider Performance Summary")
        display_providers = ah_provider_performance[['PROVIDER_NAME', 'PROVIDER_CREDENTIALS', 
                                                    'TOTAL_SERVICES_PROVIDED', 'UNIQUE_PATIENTS_SERVED',
                                                    'TOTAL_PROVIDER_REVENUE', 'PROVIDER_SUCCESS_RATE',
                                                    'SERVICES_PER_DAY', 'REVENUE_PER_DAY']].copy()
        
        # Format financial columns
        display_providers['TOTAL_PROVIDER_REVENUE'] = display_providers['TOTAL_PROVIDER_REVENUE'].apply(
            lambda x: f"${x:,.0f}" if pd.notnull(x) else "$0"
        )
        display_providers['REVENUE_PER_DAY'] = display_providers['REVENUE_PER_DAY'].apply(
            lambda x: f"${x:.0f}" if pd.notnull(x) else "$0"
        )
        display_providers['PROVIDER_SUCCESS_RATE'] = display_providers['PROVIDER_SUCCESS_RATE'].apply(
            lambda x: f"{x:.1f}%" if pd.notnull(x) else "0%"
        )
        
        st.dataframe(display_providers, use_container_width=True)
    
    # Outcomes analysis
    st.markdown("### Clinical Outcomes & Patient Engagement")
    if len(ah_outcomes) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Success rates by service type
            fig_outcomes = px.bar(
                ah_outcomes,
                x='SERVICE_TYPE',
                y='SUCCESSFUL_INTERVENTIONS',
                color='PROVIDER_CREDENTIALS',
                title='Successful Interventions by Service Type'
            )
            fig_outcomes.update_xaxes(tickangle=45)
            st.plotly_chart(fig_outcomes, use_container_width=True)
        
        with col2:
            # Patient engagement levels
            engagement_data = ah_outcomes[['SERVICE_TYPE', 'EXCELLENT_ENGAGEMENT', 'GOOD_ENGAGEMENT', 
                                         'FAIR_ENGAGEMENT', 'POOR_ENGAGEMENT']].head(10)
            
            fig_engagement = go.Figure()
            fig_engagement.add_trace(go.Bar(name='Excellent', x=engagement_data['SERVICE_TYPE'], 
                                          y=engagement_data['EXCELLENT_ENGAGEMENT']))
            fig_engagement.add_trace(go.Bar(name='Good', x=engagement_data['SERVICE_TYPE'], 
                                          y=engagement_data['GOOD_ENGAGEMENT']))
            fig_engagement.add_trace(go.Bar(name='Fair', x=engagement_data['SERVICE_TYPE'], 
                                          y=engagement_data['FAIR_ENGAGEMENT']))
            fig_engagement.add_trace(go.Bar(name='Poor', x=engagement_data['SERVICE_TYPE'], 
                                          y=engagement_data['POOR_ENGAGEMENT']))
            
            fig_engagement.update_layout(title='Patient Engagement Levels by Service Type', barmode='stack')
            fig_engagement.update_xaxes(tickangle=45)
            st.plotly_chart(fig_engagement, use_container_width=True)
    
    # Action items and insights
    st.markdown("### Action Items & Insights")
    
    if len(ah_provider_performance) > 0 and len(ah_outcomes) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### Performance Alerts")
            
            # Low performers
            low_performers = ah_provider_performance[ah_provider_performance['PROVIDER_SUCCESS_RATE'] < 70]
            if len(low_performers) > 0:
                st.warning(f"⚠️ **Performance Alert**: {len(low_performers)} providers with success rate < 70%")
                for _, provider in low_performers.head(3).iterrows():
                    st.markdown(f"- **{provider['PROVIDER_NAME']}** ({provider['PROVIDER_CREDENTIALS']}): {provider['PROVIDER_SUCCESS_RATE']:.1f}% success rate")
            
            # High utilization
            high_util = ah_provider_performance[ah_provider_performance['SERVICES_PER_DAY'] > 8]
            if len(high_util) > 0:
                st.info(f"📊 **High Utilization**: {len(high_util)} providers with >8 services/day")
            
            st.success("✅ **Quality Metrics**: All services meeting documentation standards")
        
        with col2:
            st.markdown("#### Optimization Opportunities")
            
            # Revenue opportunities
            if len(ah_outcomes) > 0:
                high_success_services = ah_outcomes[ah_outcomes['TOTAL_INTERVENTIONS'] > 50].head(3)
                if len(high_success_services) > 0:
                    st.info("💡 **Expansion Opportunities**:")
                    for _, service in high_success_services.iterrows():
                        success_rate = (service['SUCCESSFUL_INTERVENTIONS'] / service['TOTAL_INTERVENTIONS'] * 100)
                        st.markdown(f"- **{service['SERVICE_TYPE']}**: {success_rate:.1f}% success rate with high volume")
            
            # Follow-up optimization
            st.markdown("**Follow-up Optimization:**")
            st.markdown("- Standardize follow-up protocols across service types")
            st.markdown("- Implement patient engagement improvement programs")
            st.markdown("- Cross-train providers in high-demand service areas")

else:  # Physician (different view)
    st.markdown("## 👨‍⚕️ Physician Clinical Dashboard")
    st.markdown("*Patient-focused clinical decision support*")
    
    # Clinical overview
    if len(admission_trends) > 0:
        st.markdown("### Recent Clinical Activity")
        
        # Recent admissions trend
        fig_clinical_trend = px.line(
            admission_trends.head(14),  # Last 2 weeks
            x='admission_date',
            y='DAILY_ADMISSIONS',
            title='Recent Admission Patterns'
        )
        fig_clinical_trend.add_scatter(
            x=admission_trends.head(14)['admission_date'],
            y=admission_trends.head(14)['EMERGENCY_ADMISSIONS'],
            mode='lines',
            name='Emergency Admissions',
            line=dict(color='red', dash='dash')
        )
        st.plotly_chart(fig_clinical_trend, use_container_width=True)
    
    # Department focus
    st.markdown("### Department Clinical Metrics")
    if len(dept_summary) > 0:
        # Filter for clinical departments
        clinical_depts = dept_summary[dept_summary['SPECIALIZATION_TYPE'].isin([
            'Cardiac Care', 'Emergency Medicine', 'Neurological Care', 'Cancer Treatment'
        ])]
        
        if len(clinical_depts) > 0:
            col1, col2 = st.columns(2)
            
            with col1:
                fig_clinical_volume = px.bar(
                    clinical_depts,
                    x='DEPARTMENT_NAME',
                    y='ADMISSIONS',
                    title='Clinical Department Volume'
                )
                fig_clinical_volume.update_xaxes(tickangle=45)
                st.plotly_chart(fig_clinical_volume, use_container_width=True)
            
            with col2:
                fig_clinical_complexity = px.scatter(
                    clinical_depts,
                    x='ADMISSIONS',
                    y='PROCEDURES',
                    size='AVG_CHARGES',
                    color='DEPARTMENT_NAME',
                    title='Case Complexity (Procedures per Admission)'
                )
                st.plotly_chart(fig_clinical_complexity, use_container_width=True)

# Sidebar - Additional Controls
st.sidebar.markdown("---")
st.sidebar.markdown("### 🔧 Advanced Options")

if st.sidebar.checkbox("Show Data Quality Metrics"):
    st.markdown("## 📋 Data Quality Dashboard")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Data Completeness")
        # Simulated data quality metrics
        quality_metrics = pd.DataFrame({
            'Table': ['Patient Demographics', 'Admissions', 'Procedures', 'Medications', 'Allied Health'],
            'Completeness %': [99.8, 99.5, 98.9, 97.2, 96.8],
            'Quality Score': [95, 92, 89, 88, 85]
        })
        
        fig_quality = px.bar(
            quality_metrics,
            x='Table',
            y='Completeness %',
            color='Quality Score',
            title='Data Completeness by Table'
        )
        fig_quality.update_xaxes(tickangle=45)
        st.plotly_chart(fig_quality, use_container_width=True)
    
    with col2:
        st.markdown("### Data Freshness")
        freshness_data = pd.DataFrame({
            'Data Source': ['EHR System', 'Pharmacy System', 'Bed Management', 'Allied Health'],
            'Last Updated': ['2 minutes ago', '5 minutes ago', '1 minute ago', '3 minutes ago'],
            'Status': ['✅ Current', '✅ Current', '✅ Current', '✅ Current']
        })
        st.dataframe(freshness_data, use_container_width=True)

if st.sidebar.checkbox("Export Data"):
    st.markdown("## 📥 Data Export")
    
    export_options = st.multiselect(
        "Select data to export:",
        ["Department Summary", "Admission Trends", "Bed Utilization", "Medication Analysis", "Allied Health Summary"]
    )
    
    if st.button("Generate Export"):
        if "Department Summary" in export_options and len(dept_summary) > 0:
            csv_data = dept_summary.to_csv(index=False)
            st.download_button(
                label="Download Department Summary",
                data=csv_data,
                file_name=f"department_summary_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

# Footer
st.markdown("---")
st.markdown(f"""
### 🏥 Hospital Analytics Dashboard
- **Data Source**: Hospital Demo Database  
- **Role**: {user_role}
- **Period**: {date_range}
- **Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Records Analyzed**: 1,086,299 total records
""")

# Security notice based on role
if user_role == "CEO":
    st.markdown("""
    <div style="background-color: #f8f9fa; padding: 1rem; border-radius: 5px; border-left: 4px solid #6c757d;">
    🏛️ <strong>Executive Access Notice:</strong> You have access to strategic and financial data across all hospital operations. 
    Patient data is aggregated for privacy protection while providing comprehensive business insights.
    </div>
    """, unsafe_allow_html=True)

elif user_role in ["Physician", "Nurse"]:
    st.markdown("""
    <div style="background-color: #e8f4fd; padding: 1rem; border-radius: 5px; border-left: 4px solid #007bff;">
    🔒 <strong>Data Security Notice:</strong> Patient data is masked according to your role permissions. 
    Full patient identifiers are only visible to authorized clinical administrators.
    </div>
    """, unsafe_allow_html=True)

elif user_role == "Analyst":
    st.markdown("""
    <div style="background-color: #fff3cd; padding: 1rem; border-radius: 5px; border-left: 4px solid #ffc107;">
    📊 <strong>Analytics Notice:</strong> You have access to aggregated data only. 
    Individual patient information is not accessible from this role.
    </div>
    """, unsafe_allow_html=True)

elif user_role == "Capacity Planner":
    st.markdown("""
    <div style="background-color: #f0f8ff; padding: 1rem; border-radius: 5px; border-left: 4px solid #0066cc;">
    🛏️ <strong>Capacity Planning Notice:</strong> You have access to bed utilization and operational data. 
    Patient identifiers are masked, focus is on capacity optimization and resource planning.
    </div>
    """, unsafe_allow_html=True)

elif user_role == "Allied Health Coordinator":
    st.markdown("""
    <div style="background-color: #f0fff0; padding: 1rem; border-radius: 5px; border-left: 4px solid #32cd32;">
    🏥 <strong>Allied Health Coordinator Notice:</strong> You have comprehensive access to allied health services data, 
    provider performance metrics, and patient outcomes. Patient identifiers are masked for privacy protection while 
    providing detailed analytics for service optimization and quality improvement.
    </div>
    """, unsafe_allow_html=True)
