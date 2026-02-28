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
    st.divider()
    
    uploaded_file = st.file_uploader("Choose CSV File", type="csv")
    
    if uploaded_file:
        df = pd.read_csv(uploaded_file)
        st.write("### 🔍 Step 1: Preview Data")
        st.dataframe(df.head(5))
        
        cols = df.columns.tolist()
        # Mapping Logic (Simplified for this update)
        p_col = 'part_number' if 'part_number' in cols else cols[0]
        q_col = 'quantity' if 'quantity' in cols else ('quantity_on_hand' if 'quantity_on_hand' in cols else cols[2])
        
        if st.button("🚀 START LIVE IMPORT"):
            try:
                conn = get_conn()
                cur = conn.cursor()
                
                if operation == "Upload Master Inventory":
                    cur.execute("TRUNCATE TABLE inventory;")
                    data = df[['part_number', 'quantity_on_hand', 'location_bin']].fillna("").values.tolist()
                    execute_values(cur, "INSERT INTO inventory (part_number, quantity_on_hand, location_bin) VALUES %s", data)
                else:
                    # 1. Log the Activity
                    data = df.fillna("").values.tolist()
                    execute_values(cur, """
                        INSERT INTO receiving_log 
                        (part_number, description, quantity, employee_id, movement_type, location_bin, variance_amount, severity_level, timestamp) 
                        VALUES %s
                    """, data)
                    
                    # 2. UPDATE MASTER INVENTORY BALANCE (The "Live" Part)
                    # This adds the quantity from the CSV to the existing balance in the inventory table
                    for index, row in df.iterrows():
                        cur.execute("""
                            INSERT INTO inventory (part_number, quantity_on_hand, location_bin)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (part_number) 
                            DO UPDATE SET quantity_on_hand = inventory.quantity_on_hand + EXCLUDED.quantity_on_hand
                        """, (row[p_col], row[q_col], row.get('location_bin', 'UNKNOWN')))
                
                conn.commit()
                st.success(f"✅ Success! Master Inventory Balance has been updated.")
                conn.close()
            except Exception as e:
                st.error(f"Import Error: {e}")
