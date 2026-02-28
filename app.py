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

    LOGO_URL = "https://raw.githubusercontent.com/brettsimpson1971-buildai/ride1dashboard/main/Screenshot%202026-02-28%20164500.png"

    st.sidebar.image(LOGO_URL, use_container_width=True)
    st.sidebar.title("DATA OPERATIONS")
    operation = st.sidebar.radio("Select Operation:", ["Upload Activity Log", "Upload Master Inventory", "NUKE"])
    
    st.title("RIDE 1: DATA OPERATIONS CENTER")
    st.info("💡 Lightspeed Compatibility Mode: Active")
    st.divider()
    
    uploaded_file = st.file_uploader("Choose Lightspeed CSV Export", type="csv")
    
    if uploaded_file:
        # 1. PREVIEW THE FILE
        df = pd.read_csv(uploaded_file)
        st.write("### 🔍 Step 1: Preview Lightspeed Data")
        st.dataframe(df.head(5))
        
        st.write("### 🛠️ Step 2: Map Lightspeed Columns to System")
        cols = df.columns.tolist()
        
        # 2. DYNAMIC MAPPING (This saves the project if Lightspeed changes headers)
        if operation == "Upload Master Inventory":
            c1, c2, c3 = st.columns(3)
            p_col = c1.selectbox("Which column is Part Number?", cols, index=cols.index('part_number') if 'part_number' in cols else 0)
            q_col = c2.selectbox("Which column is Quantity?", cols, index=cols.index('quantity_on_hand') if 'quantity_on_hand' in cols else 0)
            l_col = c3.selectbox("Which column is Location/Bin?", cols, index=cols.index('location_bin') if 'location_bin' in cols else 0)
        else:
            c1, c2, c3, c4 = st.columns(4)
            p_col = c1.selectbox("Part Number", cols, index=cols.index('part_number') if 'part_number' in cols else 0)
            d_col = c2.selectbox("Description", cols, index=cols.index('description') if 'description' in cols else 0)
            q_col = c3.selectbox("Quantity", cols, index=cols.index('quantity') if 'quantity' in cols else 0)
            e_col = c4.selectbox("Employee/User", cols, index=cols.index('employee_id') if 'employee_id' in cols else 0)
            
            c5, c6, c7, c8 = st.columns(4)
            m_col = c5.selectbox("Movement Type", cols, index=cols.index('movement_type') if 'movement_type' in cols else 0)
            b_col = c6.selectbox("Bin/Location", cols, index=cols.index('location_bin') if 'location_bin' in cols else 0)
            v_col = c7.selectbox("Variance", cols, index=cols.index('variance_amount') if 'variance_amount' in cols else 0)
            t_col = c8.selectbox("Timestamp/Date", cols, index=cols.index('timestamp') if 'timestamp' in cols else 0)

        # 3. START IMPORT
        if st.button("🚀 START SMART IMPORT"):
            try:
                conn = get_conn()
                cur = conn.cursor()
                
                if operation == "Upload Master Inventory":
                    cur.execute("TRUNCATE TABLE inventory;")
                    # Re-map the dataframe to our standard names
                    final_df = df[[p_col, q_col, l_col]].copy()
                    final_df.columns = ['part_number', 'quantity_on_hand', 'location_bin']
                    data = final_df.fillna("").values.tolist()
                    execute_values(cur, "INSERT INTO inventory (part_number, quantity_on_hand, location_bin) VALUES %s", data)
                else:
                    # Re-map Activity Log
                    final_df = df[[p_col, d_col, q_col, e_col, m_col, b_col, v_col, t_col]].copy()
                    # Add a dummy severity column if missing
                    final_df['severity'] = 'MEDIUM'
                    final_df.columns = ['part_number', 'description', 'quantity', 'employee_id', 'movement_type', 'location_bin', 'variance_amount', 'timestamp', 'severity_level']
                    data = final_df.fillna("").values.tolist()
                    execute_values(cur, "INSERT INTO receiving_log (part_number, description, quantity, employee_id, movement_type, location_bin, variance_amount, timestamp, severity_level) VALUES %s", data)
                
                conn.commit()
                st.success(f"✅ Success! {len(final_df)} rows imported from Lightspeed.")
                conn.close()
            except Exception as e:
                st.error(f"Import Error: {e}")
