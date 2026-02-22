#!/usr/bin/env python3
"""
Test script to verify the Streamlit app structure and imports
"""

def test_imports():
    """Test that all required imports work"""
    try:
        import streamlit as st
        print("✅ Streamlit import successful")
    except ImportError as e:
        print(f"❌ Streamlit import failed: {e}")
        return False
    
    try:
        import pandas as pd
        print("✅ Pandas import successful")
    except ImportError as e:
        print(f"❌ Pandas import failed: {e}")
        return False
    
    try:
        import plotly.express as px
        import plotly.graph_objects as go
        print("✅ Plotly import successful")
    except ImportError as e:
        print(f"❌ Plotly import failed: {e}")
        return False
    
    try:
        from datetime import datetime, timedelta
        print("✅ Datetime import successful")
    except ImportError as e:
        print(f"❌ Datetime import failed: {e}")
        return False
    
    # Note: Snowpark context won't be available in local test
    print("✅ All basic imports successful")
    return True

def test_data_files():
    """Test that all required data files exist"""
    import os
    
    data_dir = "/Users/rbotha/Documents/Cursor_code/hospital_snowflake_demo/data"
    
    required_files = [
        'patient_demographics_large.csv',
        'patient_admissions_large.csv', 
        'medical_procedures_large.csv',
        'hospital_departments_complete.csv',
        'bed_inventory.csv',
        'bed_bookings.csv',
        'bed_availability.csv',
        'pharmacy_inventory.csv',
        'medication_orders.csv',
        'medication_dispensing.csv',
        'allied_health_services.csv'
    ]
    
    all_exist = True
    for file in required_files:
        file_path = os.path.join(data_dir, file)
        if os.path.exists(file_path):
            size = os.path.getsize(file_path)
            print(f"✅ {file} exists ({size:,} bytes)")
        else:
            print(f"❌ {file} missing")
            all_exist = False
    
    return all_exist

def test_app_syntax():
    """Test that the Streamlit app has valid Python syntax"""
    import ast
    
    app_file = "/Users/rbotha/Documents/Cursor_code/hospital_snowflake_demo/hospital_analytics_app.py"
    
    try:
        with open(app_file, 'r') as f:
            source_code = f.read()
        
        # Parse the syntax
        ast.parse(source_code)
        print("✅ Streamlit app syntax is valid")
        return True
    except SyntaxError as e:
        print(f"❌ Syntax error in Streamlit app: {e}")
        return False
    except Exception as e:
        print(f"❌ Error reading Streamlit app: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 50)
    print("Hospital Streamlit App - Structure Test")
    print("=" * 50)
    
    print("\n1. Testing imports...")
    imports_ok = test_imports()
    
    print("\n2. Testing data files...")
    files_ok = test_data_files()
    
    print("\n3. Testing app syntax...")
    syntax_ok = test_app_syntax()
    
    print("\n" + "=" * 50)
    print("TEST RESULTS")
    print("=" * 50)
    
    if imports_ok and files_ok and syntax_ok:
        print("🎉 ALL TESTS PASSED!")
        print("✅ Streamlit app is ready for deployment")
        print("✅ All data files generated successfully")
        print("✅ App structure is valid")
        
        print("\nNext steps:")
        print("1. Upload files to Snowflake stage")
        print("2. Run SQL deployment script (09_deploy_streamlit_app.sql)")
        print("3. Access app through Snowflake web interface")
        
        return True
    else:
        print("❌ SOME TESTS FAILED")
        print("Please fix the issues before deploying")
        return False

if __name__ == "__main__":
    main()
