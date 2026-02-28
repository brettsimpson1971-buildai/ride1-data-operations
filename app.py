import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ---------- DB CONNECTION ----------

def get_conn():
    return psycopg2.connect(st.secrets["postgres"]["url"])

st.set_page_config(page_title="RIDE 1 DATA OPERATIONS", layout="wide")

# ---------- SIDEBAR & NAV ----------

st.sidebar.title("RIDE 1 DATA OPERATIONS")
operation = st.sidebar.radio(
    "Select Operation:",
    ["Upload Inventory", "Upload Activity Log", "View Inventory", "NUKE"]
)

# ---------- OPERATION 1: UPLOAD MASTER INVENTORY ----------

if operation == "Upload Inventory":
    st.header("📦 Upload Master Inventory")
    st.info("Use this for the initial 1M+ SKU Master List. This handles duplicates automatically.")

    uploaded_file = st.file_uploader("Choose Inventory CSV", type="csv")

    if uploaded_file is not None:
        df_preview = pd.read_csv(uploaded_file, nrows=5)
        st.write("Preview of file:", df_preview)

        cols = df_preview.columns.tolist()
        part_col = st.selectbox("DB: part_number", cols, index=cols.index("part_number") if "part_number" in cols else 0)
        qty_col = st.selectbox("DB: quantity_on_hand", cols, index=cols.index("quantity_on_hand") if "quantity_on_hand" in cols else 0)
        bin_col = st.selectbox("DB: location_bin", cols, index=cols.index("location_bin") if "location_bin" in cols else 0)

        if st.button("🚀 START STREAMED IMPORT"):
            try:
                conn = get_conn()
                cur = conn.cursor()
                
                st.warning("Wiping existing inventory for clean master list...")
                cur.execute("TRUNCATE TABLE inventory;")
                conn.commit()

                uploaded_file.seek(0)
                reader = pd.read_csv(uploaded_file, chunksize=50000)

                total_rows = 0
                status_text = st.empty()
                progress_bar = st.progress(0)

                for i, chunk in enumerate(reader):
                    data_to_insert = chunk[[part_col, qty_col, bin_col]].values.tolist()
                    
                    execute_values(cur, """
                        INSERT INTO inventory (part_number, quantity_on_hand, location_bin) 
                        VALUES %s
                        ON CONFLICT (part_number) 
                        DO UPDATE SET 
                            quantity_on_hand = EXCLUDED.quantity_on_hand,
                            location_bin = EXCLUDED.location_bin;
                    """, data_to_insert)
                    
                    conn.commit()
                    total_rows += len(chunk)
                    status_text.markdown(f"### ✅ Committed {total_rows:,} rows...")
                    progress_bar.progress(min(total_rows / 2000000, 1.0))
                
                st.success(f"Done! {total_rows:,} inventory rows imported.")
                st.balloons()
                cur.close()
                conn.close()
            except Exception as e:
                st.error(f"Stream Failure: {e}")

# ---------- OPERATION 2: UPLOAD ACTIVITY LOG (SALES & RECEIVING) ----------

elif operation == "Upload Activity Log":
    st.header("🕵️ Upload Daily Activity Log")
    st.info("Upload the daily CSV from the DMS. This handles SALES, RECEIVING, and ADJUSTMENTS.")

    uploaded_file = st.file_uploader("Choose Activity Log CSV", type="csv")

    if uploaded_file is not None:
        df_preview = pd.read_csv(uploaded_file, nrows=5)
        st.write("Preview:", df_preview)

        if st.button("🔍 IMPORT ACTIVITY LOG"):
            try:
                conn = get_conn()
                cur = conn.cursor()
                
                uploaded_file.seek(0)
                cols_to_use = ['part_number', 'description', 'quantity', 'employee_id', 'movement_type', 'location_bin', 'variance_amount', 'severity_level', 'timestamp']
                
                reader = pd.read_csv(uploaded_file, chunksize=10000)
                total_rows = 0
                status_text = st.empty()

                for chunk in reader:
                    data_to_insert = chunk[cols_to_use].values.tolist()
                    execute_values(cur, """
                        INSERT INTO receiving_log 
                        (part_number, description, quantity, employee_id, movement_type, location_bin, variance_amount, severity_level, timestamp) 
                        VALUES %s
                    """, data_to_insert)
                    conn.commit()
                    total_rows += len(chunk)
                    status_text.markdown(f"### 🔍 Logged {total_rows:,} activity records...")
                
                st.success(f"Successfully imported {total_rows} activity records.")
                st.balloons()
                cur.close()
                conn.close()
            except Exception as e:
                st.error(f"Import Failure: {e}")

# ---------- OPERATION 3: VIEW INVENTORY ----------

elif operation == "View Inventory":
    st.header("📊 View Inventory Snapshot")
    try:
        conn = get_conn()
        total_rows = pd.read_sql("SELECT COUNT(*) FROM inventory;", conn).iloc[0, 0]
        total_logs = pd.read_sql("SELECT COUNT(*) FROM receiving_log;", conn).iloc[0, 0]
        
        col1, col2 = st.columns(2)
        col1.metric("Total Inventory Rows", f"{total_rows:,}")
        col2.metric("Total Activity Log Rows", f"{total_logs:,}")
        
        st.subheader("Latest 50 Activity Records")
        df_logs = pd.read_sql("SELECT * FROM receiving_log ORDER BY timestamp DESC LIMIT 50;", conn)
        st.dataframe(df_logs)
        
        conn.close()
    except Exception as e:
        st.error(f"Error loading stats: {e}")

# ---------- OPERATION 4: NUKE (WITH SAFETY LOCK) ----------

elif operation == "NUKE":
    st.header("☢️ Danger Zone: Wipe Database")
    st.warning("This action is permanent. A password is required to proceed.")
    
    target = st.selectbox("Select table to wipe:", ["inventory", "receiving_log"])
    password_input = st.text_input("Enter Admin Password to Unlock:", type="password")
    
    if st.button(f"CONFIRM WIPE {target.upper()}"):
        if password_input == "RIDE1-ADMIN":
            try:
                conn = get_conn()
                cur = conn.cursor()
                cur.execute(f"TRUNCATE TABLE {target};")
                conn.commit()
                st.success(f"SUCCESS: {target} has been cleared.")
                cur.close()
                conn.close()
            except Exception as e:
                st.error(f"Nuke Failed: {e}")
        else:
            st.error("❌ INVALID PASSWORD. Access Denied.")
