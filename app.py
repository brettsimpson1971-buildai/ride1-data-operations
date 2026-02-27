import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

# Database connection helper
def get_conn():
    return psycopg2.connect(st.secrets["postgres"]["url"])

st.set_page_config(page_title="RIDE 1 DATA OPERATIONS", layout="wide")

# Sidebar Navigation
st.sidebar.image("https://ride1dashboard-x46xojtyqjqte9xoql5xbj.streamlit.app/~/+/media/70888888888888888888888888888888888888888888888888888888.png", width=200)
operation = st.sidebar.radio("Select Operation:", ["Upload Inventory", "Upload Activity Log", "View Database Stats", "NUKE"])

# --- OPERATION 1: UPLOAD MASTER INVENTORY ---
if operation == "Upload Inventory":
    st.header("📦 Upload Master Inventory")
    st.info("Use this for the initial 1M+ SKU Master List.")
    uploaded_file = st.file_uploader("Choose Master Inventory CSV", type="csv")
    
    if uploaded_file:
        df_preview = pd.read_csv(uploaded_file, nrows=5)
        st.write("Preview:", df_preview)
        
        col_map = {}
        cols = df_preview.columns.tolist()
        col_map['part_number'] = st.selectbox("DB: part_number", cols, index=0 if 'part_number' in cols else 0)
        col_map['quantity_on_hand'] = st.selectbox("DB: quantity_on_hand", cols, index=1 if 'quantity_on_hand' in cols else 0)
        col_map['location_bin'] = st.selectbox("DB: location_bin", cols, index=2 if 'location_bin' in cols else 0)

        if st.button("🚀 START MASTER IMPORT"):
            try:
                conn = get_conn()
                cur = conn.cursor()
                st.warning("Wiping existing inventory...")
                cur.execute("TRUNCATE TABLE inventory;")
                conn.commit()
                
                uploaded_file.seek(0)
                reader = pd.read_csv(uploaded_file, chunksize=50000)
                
                total_rows = 0
                progress_bar = st.progress(0)
                status_text = st.empty()

                for chunk in reader:
                    data_to_insert = chunk[[col_map['part_number'], col_map['quantity_on_hand'], col_map['location_bin']]].values.tolist()
                    execute_values(cur, "INSERT INTO inventory (part_number, quantity_on_hand, location_bin) VALUES %s", data_to_insert)
                    conn.commit()
                    total_rows += len(chunk)
                    status_text.text(f"Committed {total_rows:,} rows...")
                
                st.success(f"Done! {total_rows:,} Master SKUs imported.")
                st.balloons()
                cur.close()
                conn.close()
            except Exception as e:
                st.error(f"Stream Failure: {e}")

# --- OPERATION 2: UPLOAD ACTIVITY LOG (THE FORENSIC DATA) ---
elif operation == "Upload Activity Log":
    st.header("🕵️ Upload Daily Activity Log")
    st.info("Use this for daily DMS exports or Forensic Demo files.")
    uploaded_file = st.file_uploader("Choose Activity Log CSV", type="csv")
    
    if uploaded_file:
        df_preview = pd.read_csv(uploaded_file, nrows=5)
        st.write("Preview:", df_preview)
        
        # Auto-mapping for the forensic columns
        cols = df_preview.columns.tolist()
        
        if st.button("🔍 IMPORT ACTIVITY LOG"):
            try:
                conn = get_conn()
                cur = conn.cursor()
                
                uploaded_file.seek(0)
                # Activity logs are usually smaller, but we still use chunks for safety
                reader = pd.read_csv(uploaded_file, chunksize=10000)
                
                total_rows = 0
                for chunk in reader:
                    # Ensure column names match the CSV I generated for you
                    data_to_insert = chunk[['part_number', 'description', 'quantity', 'employee_id', 'movement_type', 'location_bin', 'variance_amount', 'severity_level', 'timestamp']].values.tolist()
                    
                    execute_values(cur, """
                        INSERT INTO receiving_log 
                        (part_number, description, quantity, employee_id, movement_type, location_bin, variance_amount, severity_level, timestamp) 
                        VALUES %s
                    """, data_to_insert)
                    conn.commit()
                    total_rows += len(chunk)
                
                st.success(f"Successfully imported {total_rows} activity records.")
                cur.close()
                conn.close()
            except Exception as e:
                st.error(f"Import Failure: {e}")

# --- OPERATION 3: VIEW STATS ---
elif operation == "View Database Stats":
    st.header("📊 Database Health")
    try:
        conn = get_conn()
        inv_count = pd.read_sql("SELECT count(*) FROM inventory", conn).iloc[0,0]
        log_count = pd.read_sql("SELECT count(*) FROM receiving_log", conn).iloc[0,0]
        st.metric("Master Inventory SKUs", f"{inv_count:,}")
        st.metric("Activity Log Records", f"{log_count:,}")
        conn.close()
    except Exception as e:
        st.error(f"Error: {e}")

# --- OPERATION 4: NUKE ---
elif operation == "NUKE":
    st.header("☢️ Danger Zone")
    target = st.selectbox("Select Table to Wipe:", ["inventory", "receiving_log"])
    if st.button(f"CONFIRM WIPE {target}"):
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(f"TRUNCATE TABLE {target};")
            conn.commit()
            st.success(f"{target} has been cleared.")
            cur.close()
            conn.close()
        except Exception as e:
            st.error(f"Nuke Failed: {e}")
