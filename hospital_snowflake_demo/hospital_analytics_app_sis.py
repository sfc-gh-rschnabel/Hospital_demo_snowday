"""
Hospital Analytics Dashboard - Streamlit in Snowflake (SIS)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from snowflake.snowpark.context import get_active_session

# Get Snowflake session
session = get_active_session()

# Page configuration
st.set_page_config(
    page_title="Hospital Analytics Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional healthcare styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #2E8B57;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }
    .kpi-card {
        background: white;
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        border-left: 5px solid #2E8B57;
        margin: 0.5rem 0;
    }
    .section-header {
        color: #2c3e50;
        border-bottom: 2px solid #2E8B57;
        padding-bottom: 0.5rem;
        margin: 2rem 0 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def query_snowflake(sql: str) -> pd.DataFrame:
    """Execute SQL query using Snowpark session"""
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.error(f"Query failed: {str(e)}")
        return pd.DataFrame()

def get_basic_stats() -> dict:
    """Get basic hospital statistics"""
    sql = """
    SELECT 
        (SELECT COUNT(DISTINCT patient_id) FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_DEMOGRAPHICS_RAW) as total_patients,
        (SELECT COUNT(*) FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW) as total_admissions,
        (SELECT COUNT(*) FROM HOSPITAL_DEMO.RAW_DATA.MEDICAL_PROCEDURES_RAW) as total_procedures,
        (SELECT COUNT(*) FROM HOSPITAL_DEMO.RAW_DATA.MEDICATION_ORDERS_RAW) as total_medications,
        (SELECT COUNT(*) FROM HOSPITAL_DEMO.RAW_DATA.ALLIED_HEALTH_SERVICES_RAW) as total_allied_health
    """
    df = query_snowflake(sql)
    if not df.empty:
        return {
            'total_patients': int(df.iloc[0]['TOTAL_PATIENTS'] or 0),
            'total_admissions': int(df.iloc[0]['TOTAL_ADMISSIONS'] or 0),
            'total_procedures': int(df.iloc[0]['TOTAL_PROCEDURES'] or 0),
            'total_medications': int(df.iloc[0]['TOTAL_MEDICATIONS'] or 0),
            'total_allied_health': int(df.iloc[0]['TOTAL_ALLIED_HEALTH'] or 0)
        }
    return {'total_patients': 0, 'total_admissions': 0, 'total_procedures': 0, 
            'total_medications': 0, 'total_allied_health': 0}

def get_financial_kpis(start_date: str, end_date: str) -> dict:
    """Get financial KPIs"""
    sql = f"""
    SELECT 
        COALESCE(SUM(total_charges), 0) as total_revenue,
        COALESCE(AVG(total_charges), 0) as avg_charges,
        COALESCE(AVG(DATEDIFF(day, admission_date, discharge_date)), 0) as avg_los,
        COUNT(*) as admission_count,
        COUNT(CASE WHEN admission_type = 'Emergency' THEN 1 END) as emergency_count,
        CASE WHEN COUNT(*) > 0 
             THEN COUNT(CASE WHEN admission_type = 'Emergency' THEN 1 END) * 100.0 / COUNT(*)
             ELSE 0 END as emergency_rate
    FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW
    WHERE admission_date BETWEEN '{start_date}' AND '{end_date}'
    """
    df = query_snowflake(sql)
    if not df.empty:
        return {
            'total_revenue': float(df.iloc[0]['TOTAL_REVENUE'] or 0),
            'avg_charges': float(df.iloc[0]['AVG_CHARGES'] or 0),
            'avg_los': float(df.iloc[0]['AVG_LOS'] or 0),
            'admission_count': int(df.iloc[0]['ADMISSION_COUNT'] or 0),
            'emergency_count': int(df.iloc[0]['EMERGENCY_COUNT'] or 0),
            'emergency_rate': float(df.iloc[0]['EMERGENCY_RATE'] or 0)
        }
    return {'total_revenue': 0, 'avg_charges': 0, 'avg_los': 0, 
            'admission_count': 0, 'emergency_count': 0, 'emergency_rate': 0}

def get_department_summary(start_date: str, end_date: str) -> pd.DataFrame:
    """Get department performance summary"""
    sql = f"""
    SELECT 
        d.department_name,
        d.specialization_type,
        COUNT(DISTINCT a.admission_id) as admissions,
        COALESCE(SUM(a.total_charges), 0) as total_revenue,
        COALESCE(AVG(a.total_charges), 0) as avg_charges,
        COUNT(DISTINCT p.procedure_id) as procedures
    FROM HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d
    LEFT JOIN HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW a 
        ON d.department_id = a.department_id
        AND a.admission_date BETWEEN '{start_date}' AND '{end_date}'
    LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICAL_PROCEDURES_RAW p 
        ON a.admission_id = p.admission_id
    GROUP BY d.department_name, d.specialization_type
    ORDER BY admissions DESC
    """
    return query_snowflake(sql)

def get_admission_trends(start_date: str, end_date: str) -> pd.DataFrame:
    """Get admission trends over time"""
    sql = f"""
    SELECT 
        admission_date,
        COUNT(*) as daily_admissions,
        COUNT(CASE WHEN admission_type = 'Emergency' THEN 1 END) as emergency_admissions,
        COUNT(CASE WHEN admission_type = 'Elective' THEN 1 END) as elective_admissions,
        SUM(total_charges) as daily_revenue
    FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_ADMISSIONS_RAW
    WHERE admission_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY admission_date
    ORDER BY admission_date
    """
    return query_snowflake(sql)

def get_bed_utilization() -> pd.DataFrame:
    """Get bed utilization by department"""
    sql = """
    SELECT 
        d.department_name,
        COUNT(bi.bed_id) as total_beds,
        SUM(CASE WHEN ba.status = 'Occupied' THEN 1 ELSE 0 END) as occupied_beds,
        SUM(CASE WHEN ba.status = 'Available' THEN 1 ELSE 0 END) as available_beds,
        ROUND(SUM(CASE WHEN ba.status = 'Occupied' THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(COUNT(bi.bed_id), 0), 1) as utilization_rate
    FROM HOSPITAL_DEMO.RAW_DATA.BED_INVENTORY_RAW bi
    JOIN HOSPITAL_DEMO.RAW_DATA.HOSPITAL_DEPARTMENTS_RAW d ON bi.department_id = d.department_id
    LEFT JOIN HOSPITAL_DEMO.RAW_DATA.BED_AVAILABILITY_RAW ba ON bi.bed_id = ba.bed_id
        AND ba.date = (SELECT MAX(date) FROM HOSPITAL_DEMO.RAW_DATA.BED_AVAILABILITY_RAW)
    WHERE bi.is_active = TRUE
    GROUP BY d.department_name
    HAVING COUNT(bi.bed_id) > 0
    ORDER BY utilization_rate DESC
    """
    return query_snowflake(sql)

def get_medication_summary() -> pd.DataFrame:
    """Get medication analysis"""
    sql = """
    SELECT 
        pi.therapeutic_category,
        COUNT(DISTINCT mo.order_id) as total_orders,
        COALESCE(SUM(md.total_cost), 0) as total_cost,
        COUNT(DISTINCT mo.patient_id) as unique_patients
    FROM HOSPITAL_DEMO.RAW_DATA.PHARMACY_INVENTORY_RAW pi
    JOIN HOSPITAL_DEMO.RAW_DATA.MEDICATION_ORDERS_RAW mo ON pi.medication_code = mo.medication_code
    LEFT JOIN HOSPITAL_DEMO.RAW_DATA.MEDICATION_DISPENSING_RAW md ON mo.order_id = md.order_id
    GROUP BY pi.therapeutic_category
    ORDER BY total_orders DESC
    LIMIT 10
    """
    return query_snowflake(sql)

def get_patient_demographics() -> pd.DataFrame:
    """Get patient demographics summary"""
    sql = """
    SELECT 
        CASE 
            WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) < 18 THEN 'Pediatric (0-17)'
            WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 18 AND 44 THEN 'Adult (18-44)'
            WHEN DATEDIFF(year, date_of_birth, CURRENT_DATE()) BETWEEN 45 AND 64 THEN 'Middle Age (45-64)'
            ELSE 'Senior (65+)'
        END as age_group,
        gender,
        COUNT(*) as patient_count
    FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_DEMOGRAPHICS_RAW
    GROUP BY age_group, gender
    ORDER BY patient_count DESC
    """
    return query_snowflake(sql)

def get_insurance_mix() -> pd.DataFrame:
    """Get insurance provider mix"""
    sql = """
    SELECT 
        insurance_provider,
        COUNT(*) as patient_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
    FROM HOSPITAL_DEMO.RAW_DATA.PATIENT_DEMOGRAPHICS_RAW
    GROUP BY insurance_provider
    ORDER BY patient_count DESC
    """
    return query_snowflake(sql)

def create_metric_card(label: str, value: str, delta: str = None, icon: str = "📊"):
    """Create a styled metric card"""
    delta_html = f'<p style="color: #666; font-size: 0.9rem; margin: 0;">{delta}</p>' if delta else ""
    st.markdown(f"""
    <div class="kpi-card">
        <h3 style="color: #2E8B57; margin: 0; font-size: 1.1rem;">{icon} {label}</h3>
        <h2 style="color: #2c3e50; margin: 0.5rem 0 0 0; font-size: 2rem;">{value}</h2>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

def main():
    """Main dashboard application"""
    
    # Header
    st.markdown('<h1 class="main-header">🏥 Hospital Analytics Dashboard</h1>', unsafe_allow_html=True)
    st.markdown('<p style="text-align: center; color: #666; font-size: 1.1rem; margin-bottom: 2rem;">Comprehensive Healthcare Analytics for Clinical Teams</p>', unsafe_allow_html=True)
    
    # Sidebar configuration
    st.sidebar.markdown("## 🎛️ Dashboard Controls")
    st.sidebar.success("✅ Connected to Snowflake")
    
    # Role selection
    st.sidebar.markdown("### 🔐 User Role")
    user_role = st.sidebar.selectbox(
        "Select Your Role",
        ["CEO", "Clinical Administrator", "Physician", "Nurse", "Analyst"],
        help="Different roles see different data based on RBAC policies"
    )
    
    # Date range selection
    st.sidebar.markdown("### 📅 Analysis Period")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now()
        )
    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime.now(),
            max_value=datetime.now()
        )
    
    if start_date > end_date:
        st.sidebar.error("⚠️ Start date must be before end date")
        return
    
    # Quick date range buttons
    st.sidebar.markdown("#### Quick Select")
    col1, col2, col3 = st.sidebar.columns(3)
    with col1:
        if st.button("7D"):
            start_date = datetime.now() - timedelta(days=7)
    with col2:
        if st.button("30D"):
            start_date = datetime.now() - timedelta(days=30)
    with col3:
        if st.button("90D"):
            start_date = datetime.now() - timedelta(days=90)
    
    # Refresh button
    st.sidebar.markdown("### 🔄 Data Controls")
    if st.sidebar.button("🔄 Refresh Data", type="primary"):
        st.cache_data.clear()
        st.experimental_rerun()
    
    # Get data
    basic_stats = get_basic_stats()
    financial_data = get_financial_kpis(str(start_date), str(end_date))
    
    # CEO Dashboard
    if user_role == "CEO":
        st.markdown('<h2 class="section-header">🏛️ Executive Summary</h2>', unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            create_metric_card("Total Revenue", f"${financial_data['total_revenue']:,.0f}", 
                             f"{financial_data['admission_count']:,} Admissions", "💰")
        with col2:
            create_metric_card("Total Patients", f"{basic_stats['total_patients']:,}", 
                             f"{basic_stats['total_procedures']:,} Procedures", "👥")
        with col3:
            create_metric_card("Avg Length of Stay", f"{financial_data['avg_los']:.1f} days", 
                             f"${financial_data['avg_charges']:,.0f} Avg Charge", "🛏️")
        with col4:
            create_metric_card("Emergency Rate", f"{financial_data['emergency_rate']:.1f}%", 
                             f"{financial_data['emergency_count']:,} Emergency Cases", "🚨")
        
        # Department Performance
        st.markdown('<h2 class="section-header">📊 Department Performance</h2>', unsafe_allow_html=True)
        dept_data = get_department_summary(str(start_date), str(end_date))
        
        if not dept_data.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(dept_data, x='DEPARTMENT_NAME', y='TOTAL_REVENUE',
                           title="💰 Revenue by Department", color='TOTAL_REVENUE',
                           color_continuous_scale='Greens')
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.pie(dept_data, values='ADMISSIONS', names='DEPARTMENT_NAME',
                           title="🏥 Admissions Distribution", hole=0.4)
                st.plotly_chart(fig, use_container_width=True)
        
        # Insurance Mix
        st.markdown('<h2 class="section-header">💳 Insurance Mix</h2>', unsafe_allow_html=True)
        insurance_data = get_insurance_mix()
        if not insurance_data.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(insurance_data, values='PATIENT_COUNT', names='INSURANCE_PROVIDER',
                           title="Insurance Provider Distribution", color_discrete_sequence=px.colors.qualitative.Set3)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                st.dataframe(insurance_data, use_container_width=True)
    
    # Clinical Administrator Dashboard
    elif user_role == "Clinical Administrator":
        st.markdown('<h2 class="section-header">👨‍⚕️ Clinical Operations Overview</h2>', unsafe_allow_html=True)
        
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            create_metric_card("Patients", f"{basic_stats['total_patients']:,}", None, "👥")
        with col2:
            create_metric_card("Admissions", f"{basic_stats['total_admissions']:,}", None, "🏥")
        with col3:
            create_metric_card("Procedures", f"{basic_stats['total_procedures']:,}", None, "⚕️")
        with col4:
            create_metric_card("Medications", f"{basic_stats['total_medications']:,}", None, "💊")
        with col5:
            create_metric_card("Allied Health", f"{basic_stats['total_allied_health']:,}", None, "🏃")
        
        # Bed Utilization
        st.markdown('<h2 class="section-header">🛏️ Bed Utilization</h2>', unsafe_allow_html=True)
        bed_data = get_bed_utilization()
        if not bed_data.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(bed_data, x='DEPARTMENT_NAME', y='UTILIZATION_RATE',
                           title="Bed Utilization Rate by Department (%)",
                           color='UTILIZATION_RATE', color_continuous_scale='RdYlGn_r')
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                st.dataframe(bed_data, use_container_width=True)
        
        # Department Details
        st.markdown('<h2 class="section-header">📋 Department Details</h2>', unsafe_allow_html=True)
        dept_data = get_department_summary(str(start_date), str(end_date))
        if not dept_data.empty:
            st.dataframe(dept_data, use_container_width=True)
    
    # Physician Dashboard
    elif user_role == "Physician":
        st.markdown('<h2 class="section-header">👩‍⚕️ Clinical Insights</h2>', unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            create_metric_card("Total Admissions", f"{financial_data['admission_count']:,}", 
                             f"Period: {start_date} to {end_date}", "🏥")
        with col2:
            create_metric_card("Emergency Cases", f"{financial_data['emergency_count']:,}", 
                             f"{financial_data['emergency_rate']:.1f}% of total", "🚨")
        with col3:
            create_metric_card("Avg Length of Stay", f"{financial_data['avg_los']:.1f} days", None, "📅")
        
        # Admission Trends
        st.markdown('<h2 class="section-header">📈 Admission Trends</h2>', unsafe_allow_html=True)
        trend_data = get_admission_trends(str(start_date), str(end_date))
        if not trend_data.empty:
            fig = px.line(trend_data, x='ADMISSION_DATE', 
                         y=['DAILY_ADMISSIONS', 'EMERGENCY_ADMISSIONS', 'ELECTIVE_ADMISSIONS'],
                         title="Daily Admission Trends", markers=True)
            fig.update_layout(xaxis_title="Date", yaxis_title="Admissions", hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
        
        # Medication Analysis
        st.markdown('<h2 class="section-header">💊 Medication Analysis</h2>', unsafe_allow_html=True)
        med_data = get_medication_summary()
        if not med_data.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(med_data, x='THERAPEUTIC_CATEGORY', y='TOTAL_ORDERS',
                           title="Medication Orders by Category", color='TOTAL_ORDERS',
                           color_continuous_scale='Blues')
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                st.dataframe(med_data, use_container_width=True)
    
    # Nurse Dashboard
    elif user_role == "Nurse":
        st.markdown('<h2 class="section-header">👩‍⚕️ Nursing Operations</h2>', unsafe_allow_html=True)
        
        current_hour = datetime.now().hour
        if 7 <= current_hour < 15:
            shift = "Day Shift (7 AM - 3 PM)"
        elif 15 <= current_hour < 23:
            shift = "Evening Shift (3 PM - 11 PM)"
        else:
            shift = "Night Shift (11 PM - 7 AM)"
        st.info(f"🕐 Current Shift: {shift}")
        
        # Bed Management
        st.markdown('<h2 class="section-header">🛏️ Bed Management</h2>', unsafe_allow_html=True)
        bed_data = get_bed_utilization()
        if not bed_data.empty:
            total_beds = int(bed_data['TOTAL_BEDS'].sum())
            occupied = int(bed_data['OCCUPIED_BEDS'].sum())
            available = int(bed_data['AVAILABLE_BEDS'].sum())
            
            col1, col2, col3 = st.columns(3)
            with col1:
                create_metric_card("Total Beds", f"{total_beds}", None, "🛏️")
            with col2:
                create_metric_card("Occupied", f"{occupied}", f"{occupied/total_beds*100:.1f}%", "🔴")
            with col3:
                create_metric_card("Available", f"{available}", f"{available/total_beds*100:.1f}%", "🟢")
            
            st.dataframe(bed_data, use_container_width=True)
    
    # Analyst Dashboard
    elif user_role == "Analyst":
        st.markdown('<h2 class="section-header">📊 Analytics Overview</h2>', unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            create_metric_card("Patient Population", f"{basic_stats['total_patients']:,}", None, "👥")
        with col2:
            create_metric_card("Total Admissions", f"{basic_stats['total_admissions']:,}", None, "🏥")
        with col3:
            create_metric_card("Clinical Procedures", f"{basic_stats['total_procedures']:,}", None, "⚕️")
        with col4:
            total_services = basic_stats['total_medications'] + basic_stats['total_allied_health']
            create_metric_card("Total Services", f"{total_services:,}", None, "📋")
        
        # Demographics Analysis
        st.markdown('<h2 class="section-header">👥 Patient Demographics</h2>', unsafe_allow_html=True)
        demo_data = get_patient_demographics()
        if not demo_data.empty:
            col1, col2 = st.columns(2)
            with col1:
                age_summary = demo_data.groupby('AGE_GROUP')['PATIENT_COUNT'].sum().reset_index()
                fig = px.pie(age_summary, values='PATIENT_COUNT', names='AGE_GROUP',
                           title="Patient Population by Age Group", hole=0.4)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.bar(demo_data, x='AGE_GROUP', y='PATIENT_COUNT', color='GENDER',
                           title="Demographics by Age and Gender", barmode='group')
                st.plotly_chart(fig, use_container_width=True)
        
        # Trend Analysis
        st.markdown('<h2 class="section-header">📈 Trend Analysis</h2>', unsafe_allow_html=True)
        trend_data = get_admission_trends(str(start_date), str(end_date))
        if not trend_data.empty:
            fig = px.area(trend_data, x='ADMISSION_DATE', y='DAILY_REVENUE',
                        title="Daily Revenue Trend", color_discrete_sequence=['#2E8B57'])
            fig.update_layout(xaxis_title="Date", yaxis_title="Revenue ($)")
            st.plotly_chart(fig, use_container_width=True)
    
    # Footer
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📅 Analysis Period", f"{(end_date - start_date).days + 1} days")
    with col2:
        st.metric("💼 Total Revenue", f"${financial_data['total_revenue']:,.0f}")
    with col3:
        st.metric("🏥 Admissions", f"{financial_data['admission_count']:,}")
    with col4:
        st.metric("📊 Avg LOS", f"{financial_data['avg_los']:.1f} days")
    
    st.caption(f"🏥 Hospital Analytics Dashboard | Role: {user_role} | Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
