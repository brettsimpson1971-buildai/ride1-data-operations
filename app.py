import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

st.set_page_config(page_title="RIDE 1 DATA OPERATIONS", layout="wide")

# ---------- DB HELPERS ----------
def get_conn():
    return psycopg2.connect(
        host=st.secrets["DB_HOST"],
        database=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        port=st.secrets["DB_PORT"],
    )

def run_command(sql, params=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    cur.close()
    conn.close()

def chunked_bulk_insert(df, table, column_map, chunk_size=50000):
    """Inserts data in chunks to handle millions of rows safely"""
    conn = get_conn()
    cur = conn.cursor()
    total_inserted = 0
    
    try:
        # Prepare the dataframe
        df_to_insert = df[list(column_map.keys())].copy()
        df_to_insert.columns = list(column_map.values())
        cols = list(df_to_insert.columns)
        col_names = ", ".join(cols)
        sql = f"INSERT INTO {table} ({col_names}) VALUES %s"

        # Process in chunks
        for i in range(0, len(df_to_insert), chunk_size):
            chunk = df_to_insert.iloc[i : i + chunk_size]
            values = [tuple(x) for x in chunk.to_numpy()]
            execute_values(cur, sql, values)
            conn.commit()
            total_inserted += len(values)
            st.write(f"✅ Progress: {total_inserted:,} / {len(df):,} rows...")

        return total_inserted, 0
    except Exception as e:
        st.error(f"Bulk insert failed at row {total_inserted}: {e}")
        return total_inserted, len(df) - total_inserted
    finally:
        cur.close()
        conn.close()

# ---------- SIDEBAR NAV ----------
with st.sidebar:
    st.image("https://cdn.abacus.ai/images/8f44384a-1116-4c71-b3e6-67356cf217cd.png", use_container_width=True)
    st.markdown("---")
    page = st.radio("Select Operation:", ["Upload Inventory", "View Inventory", "NUKE (Inventory only)"])

st.title("RIDE 1: DATA OPERATIONS CENTER")
st.markdown("---")

# ---------- PAGE: UPLOAD INVENTORY ----------
if page == "Upload Inventory":
    st.subheader("Industrial Inventory Uploader (Supports 1M+ Rows)")
    uploaded = st.file_uploader("Choose CSV file", type="csv")
    
    if uploaded:
        df = pd.read_csv(uploaded)
        st.write(f"📂 File Loaded: {len(df):,} rows detected")
        st.dataframe(df.head(5), use_container_width=True)

        st.markdown("### Map Columns")
        csv_cols = ["-- skip --"] + list(df.columns)
        col_part = st.selectbox("DB: part_number", csv_cols, index=csv_cols.index("part_number") if "part_number" in df.columns else 1)
        col_qty = st.selectbox("DB: quantity_on_hand", csv_cols, index=csv_cols.index("quantity_on_hand") if "quantity_on_hand" in df.columns else 1)
        col_bin = st.selectbox("DB: location_bin (optional)", csv_cols, index=csv_cols.index("location_bin") if "location_bin" in df.columns else 0)
        col_upd = st.selectbox("DB: last_updated (optional)", csv_cols, index=csv_cols.index("last_updated") if "last_updated" in df.columns else 0)

        mode = st.radio("Import Mode", ["Append", "Replace (wipe inventory first)"], index=1)

        if st.button("🚀 START INDUSTRIAL IMPORT"):
            column_map = {col_part: "part_number", col_qty: "quantity_on_hand"}
            if col_bin != "-- skip --": column_map[col_bin] = "location_bin"
            if col_upd != "-- skip --": column_map[col_upd] = "last_updated"

            if "Replace" in mode:
                run_command("DELETE FROM inventory;")
            
            with st.spinner("Processing large dataset..."):
                success, errors = chunked_bulk_insert(df, "inventory", column_map)
                if success > 0:
                    st.success(f"COMPLETED: {success:,} rows successfully imported.")
                    st.balloons()

# ---------- PAGE: VIEW INVENTORY ----------
elif page == "View Inventory":
    st.subheader("Live Database Stats")
    try:
        conn = get_conn()
        count_df = pd.read_sql("SELECT COUNT(*) AS total_rows, COALESCE(SUM(quantity_on_hand),0) AS total_qty FROM inventory;", conn)
        sample_df = pd.read_sql("SELECT * FROM inventory ORDER BY 1 DESC LIMIT 50;", conn)
        conn.close()
        st.metric("Total Rows", f"{int(count_df['total_rows'].iloc[0]):,}")
        st.metric("Total Quantity", f"{int(count_df['total_qty'].iloc[0]):,}")
        st.dataframe(sample_df, use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")

# ---------- PAGE: NUKE ----------
elif page == "NUKE (Inventory only)":
    st.subheader("DANGER ZONE")
    if st.button("WIPE ALL INVENTORY DATA"):
        run_command("DELETE FROM inventory;")
        st.success("Inventory table cleared.")
