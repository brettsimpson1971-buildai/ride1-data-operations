import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ---------- CONFIG ----------
st.set_page_config(page_title="ZAPTASK A.I. | DATA OPERATIONS", layout="wide")

# ---------- PERSISTENT LOGIN ----------
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
        st.text_input("Enter Admin Password to Access Data Operations:", type="password", on_change=password_entered, key="password")
        if "password_correct" in st.session_state and not st.session_state["password_correct"]:
            st.error("❌ Invalid Password.")
        return False
    return True

if check_password():
    # ---------- DB CONNECTION ----------
    def get_conn():
        return psycopg2.connect(st.secrets["postgres"]["url"])

    # Sidebar
    st.sidebar.image("https://raw.githubusercontent.com/brettsimpson1971/ride1-dashboard/main/logo.png", width=200)
    st.sidebar.title("DATA OPERATIONS")
    if st.sidebar.button("🔒 LOGOUT"):
        st.session_state["password_correct"] = False
        st.rerun()

    operation = st.sidebar.radio("Select Operation:", ["Upload Activity Log", "Upload Master Inventory", "NUKE"])

    # --- UPLOAD LOGIC ---
    st.header(f"📦 {operation}")
    uploaded_file = st.file_uploader("Choose CSV File", type="csv")
    
    if uploaded_file:
        if st.button("🚀 START IMPORT"):
            try:
                conn = get_conn()
                cur = conn.cursor()
                if operation == "Upload Master Inventory":
                    cur.execute("TRUNCATE TABLE inventory;")
                
                reader = pd.read_csv(uploaded_file, chunksize=50000)
                for chunk in reader:
                    if operation == "Upload Master Inventory":
                        execute_values(cur, "INSERT INTO inventory (part_number, quantity_on_hand, location_bin) VALUES %s ON CONFLICT (part_number) DO UPDATE SET quantity_on_hand = EXCLUDED.quantity_on_hand", chunk[['part_number', 'quantity_on_hand', 'location_bin']].values.tolist())
                    else:
                        execute_values(cur, "INSERT INTO receiving_log (part_number, description, quantity, employee_id, movement_type, location_bin, variance_amount, severity_level, timestamp) VALUES %s", chunk[['part_number', 'description', 'quantity', 'employee_id', 'movement_type', 'location_bin', 'variance_amount', 'severity_level', 'timestamp']].values.tolist())
                    conn.commit()
                st.success("Import Complete!")
                conn.close()
            except Exception as e:
                st.error(f"Upload Error: {e}")
