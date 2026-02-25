import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# 1. Page Setup
st.set_page_config(page_title="RIDE 1 DATA OPERATIONS", layout="wide")

# 2. DB Helpers
def get_conn():
    return psycopg2.connect(
        host=st.secrets["DB_HOST"],
        database=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        port=st.secrets["DB_PORT"]
    )

def run_command(query, params=None):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"DB Error: {str(e)}")
        return False

def fast_bulk_insert(df, table, column_map):
    """Uses execute_values for high-speed batch insertion"""
    conn = get_conn()
    cur = conn.cursor()
    try:
        # Prepare the data
        cols = list(column_map.values())
        # Filter DF to only mapped columns and rename them to match DB
        upload_df = df[list(column_map.keys())].copy()
        data_tuples = [tuple(x) for x in upload_df.to_numpy()]
        
        col_names = ", ".join(cols)
        query = f"INSERT INTO {table} ({col_names}) VALUES %s"
        
        execute_values(cur, query, data_tuples)
        conn.commit()
        success = len(data_tuples)
        errors = 0
    except Exception as e:
        st.error(f"Batch Insert Failed: {str(e)}")
        success = 0
        errors = len(df)
    finally:
        cur.close()
        conn.close()
    return success, errors

# 3. SIDEBAR
with st.sidebar:
    st.image("https://cdn.abacus.ai/images/8f44384a-1116-4c71-b3e6-67356cf217cd.png", use_container_width=True)
    st.markdown("---")
    st.markdown("### NAVIGATION")
    page = st.radio("Select Operation:", [
        "Upload Receiving Log",
        "Upload Parts Master",
        "Upload Inventory",
        "View Database Tables",
        "NUKE & RESET (Admin)"
    ])

# 4. HEADER
st.title("RIDE 1: DATA OPERATIONS CENTER")
st.markdown("---")

# ============================================================
# PAGE: Upload Inventory (Focusing on this for the fix)
# ============================================================
if page == "Upload Inventory":
    st.subheader("Upload Inventory CSV")
    uploaded = st.file_uploader("Choose a CSV file", type="csv", key="inventory")
    if uploaded:
        df = pd.read_csv(uploaded)
        st.write(f"Preview: {len(df)} rows detected")
        st.dataframe(df.head(5), use_container_width=True)

        st.markdown("### Map Your Columns")
        csv_cols = ["-- skip --"] + list(df.columns)
        col_part = st.selectbox("part_number", csv_cols, index=1 if len(csv_cols)>1 else 0)
        col_qty = st.selectbox("quantity_on_hand", csv_cols, index=3 if len(csv_cols)>3 else 0)
        col_bin = st.selectbox("location_bin (optional)", csv_cols, index=4 if len(csv_cols)>4 else 0)
        col_upd = st.selectbox("last_updated (optional)", csv_cols, index=5 if len(csv_cols)>5 else 0)

        mode = st.radio("Import Mode:", ["Append", "Replace (wipe table first)"])

        if st.button("🚀 START HIGH-SPEED IMPORT"):
            column_map = {}
            if col_part != "-- skip --": column_map[col_part] = "part_number"
            if col_qty != "-- skip --": column_map[col_qty] = "quantity_on_hand"
            if col_bin != "-- skip --": column_map[col_bin] = "location_bin"
            if col_upd != "-- skip --": column_map[col_upd] = "last_updated"

            with st.spinner("Processing 50,000 rows... please wait."):
                if "Replace" in mode:
                    run_command("DELETE FROM inventory;")
                
                success, errors = fast_bulk_insert(df, "inventory", column_map)
                if success > 0:
                    st.success(f"SUCCESS: {success:,} rows inserted into Inventory.")
                    st.balloons()

# ============================================================
# PAGE: View Database Tables
# ============================================================
elif page == "View Database Tables":
    st.subheader("Live Database Viewer")
    table = st.selectbox("Select Table:", ["receiving_log", "parts_master", "inventory"])
    
    if st.button("Refresh Table View"):
        try:
            conn = get_conn()
            # Get total count
            count_df = pd.read_sql(f"SELECT COUNT(*) as total FROM {table};", conn)
            total = count_df['total'].iloc[0]
            # Get sample
            df = pd.read_sql(f"SELECT * FROM {table} ORDER BY 1 DESC LIMIT 50;", conn)
            conn.close()
            st.metric(f"Total Rows in {table}", f"{total:,}")
            st.dataframe(df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Error: {str(e)}")

# (Keeping other pages simplified for brevity, but you can add them back)
elif page == "NUKE & RESET (Admin)":
    st.subheader("NUKE & RESET")
    admin_pass = st.text_input("Password:", type="password")
    if st.button("WIPE ALL DATA"):
        if admin_pass == st.secrets["ADMIN_PASSWORD"]:
            run_command("DELETE FROM inventory; DELETE FROM receiving_log; DELETE FROM parts_master;")
            st.success("Database Wiped.")
        else:
            st.error("Wrong Password")
