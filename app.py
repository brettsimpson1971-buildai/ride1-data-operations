import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

st.set_page_config(page_title="RIDE 1 | DATA OPERATIONS", layout="wide")

if "password_correct" not in st.session_state:
    st.session_state["password_correct"] = False

def check_password():
    def password_entered():
        if st.session_state["password"] == "ZAPTASK-RIDE1":
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False
    if not st.session_state["password_correct"]:
        st.title("🔐 ZAPTASK A.I. SECURE GATEWAY")
        st.text_input("Enter Admin Password:", type="password", on_change=password_entered, key="password")
        return False
    return True

if check_password():
    def get_conn():
        return psycopg2.connect(st.secrets["postgres"]["url"])

    # RAW LOGO URL
    LOGO_URL = "https://raw.githubusercontent.com/brettsimpson1971-buildai/ride1dashboard/main/Screenshot%202026-02-28%20164500.png"

    # Sidebar
    st.sidebar.image(LOGO_URL, use_container_width=True)
    st.sidebar.title("DATA OPERATIONS")
    if st.sidebar.button("🔒 LOGOUT"):
        st.session_state["password_correct"] = False
        st.rerun()

    operation = st.sidebar.radio("Select Operation:", ["Upload Activity Log", "Upload Master Inventory", "NUKE"])
    
    # Header (Restoring your professional layout)
    st.title("RIDE 1: DATA OPERATIONS CENTER")
    st.divider()
    
    st.header(f"📦 {operation}")
    uploaded_file = st.file_uploader("Choose CSV File", type="csv")
    
    if uploaded_file:
        if st.button("🚀 START IMPORT"):
            try:
                conn = get_conn()
                cur = conn.cursor()
                if operation == "Upload Master Inventory":
                    cur.execute("TRUNCATE TABLE inventory;")
                    conn.commit()
                
                reader = pd.read_csv(uploaded_file, chunksize=10000)
                for chunk in reader:
                    chunk = chunk.fillna("")
                    data = chunk.values.tolist()
                    if operation == "Upload Master Inventory":
                        execute_values(cur, "INSERT INTO inventory (part_number, quantity_on_hand, location_bin) VALUES %s ON CONFLICT (part_number) DO UPDATE SET quantity_on_hand = EXCLUDED.quantity_on_hand", data)
                    else:
                        execute_values(cur, "INSERT INTO receiving_log (part_number, description, quantity, employee_id, movement_type, location_bin, variance_amount, severity_level, timestamp) VALUES %s", data)
                    conn.commit()
                st.success("✅ Import Complete! Data is now live in the Command Center.")
                conn.close()
            except Exception as e:
                st.error(f"Upload Error: {e}")
