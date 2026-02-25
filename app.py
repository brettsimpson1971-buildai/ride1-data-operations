import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import io

st.set_page_config(page_title="RIDE 1 DATA OPERATIONS", layout="wide")

# ---------- DB HELPERS ----------
def get_conn():
    return psycopg2.connect(
        host=st.secrets["DB_HOST"],
        database=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        port=st.secrets["DB_PORT"],
        connect_timeout=10 # Prevent hanging forever
    )

def run_command(sql, params=None):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Command Failed: {e}")
        return False

# ---------- SIDEBAR NAV ----------
with st.sidebar:
    st.image("https://cdn.abacus.ai/images/8f44384a-1116-4c71-b3e6-67356cf217cd.png", use_container_width=True)
    st.markdown("---")
    page = st.radio("Select Operation:", ["Upload Inventory", "View Inventory", "NUKE (Inventory only)"])

st.title("RIDE 1: DATA OPERATIONS CENTER")
st.markdown("---")

# ---------- PAGE: UPLOAD INVENTORY ----------
if page == "Upload Inventory":
    st.subheader("Industrial Stream-Loader (2M+ Row Support)")
    
    uploaded = st.file_uploader("Choose CSV file", type="csv")
    
    if uploaded:
        # We read just the header first to get column names for mapping
        header_df = pd.read_csv(uploaded, nrows=5)
        uploaded.seek(0) # Reset file pointer to start
        
        st.write(f"📂 File Detected. Previewing first 5 rows:")
        st.dataframe(header_df, use_container_width=True)

        st.markdown("### Map Columns")
        csv_cols = ["-- skip --"] + list(header_df.columns)
        col_part = st.selectbox("DB: part_number", csv_cols, index=csv_cols.index("part_number") if "part_number" in header_df.columns else 1)
        col_qty = st.selectbox("DB: quantity_on_hand", csv_cols, index=csv_cols.index("quantity_on_hand") if "quantity_on_hand" in header_df.columns else 1)
        col_bin = st.selectbox("DB: location_bin", csv_cols, index=csv_cols.index("location_bin") if "location_bin" in header_df.columns else 0)
        col_upd = st.selectbox("DB: last_updated", csv_cols, index=csv_cols.index("last_updated") if "last_updated" in header_df.columns else 0)

        if st.button("🚀 START STREAMED IMPORT"):
            column_map = {col_part: "part_number", col_qty: "quantity_on_hand"}
            if col_bin != "-- skip --": column_map[col_bin] = "location_bin"
            if col_upd != "-- skip --": column_map[col_upd] = "last_updated"

            # 1. Wipe table first
            st.info("Wiping existing inventory...")
            run_command("DELETE FROM inventory;")
            
            # 2. Stream the file in chunks
            total_inserted = 0
            chunk_size = 50000
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                conn = get_conn()
                cur = conn.cursor()
                
                # Use pandas chunking to keep memory low
                for chunk in pd.read_csv(uploaded, chunksize=chunk_size):
                    # Filter and rename
                    upload_chunk = chunk[list(column_map.keys())].copy()
                    upload_chunk.columns = list(column_map.values())
                    
                    # Convert to tuples
                    data_tuples = [tuple(x) for x in upload_chunk.to_numpy()]
                    
                    # Batch Insert
                    cols = ", ".join(list(upload_chunk.columns))
                    query = f"INSERT INTO inventory ({cols}) VALUES %s"
                    execute_values(cur, query, data_tuples)
                    conn.commit()
                    
                    total_inserted += len(data_tuples)
                    status_text.text(f"Processing: {total_inserted:,} rows committed...")
                
                cur.close()
                conn.close()
                st.success(f"🏁 FINAL SUCCESS: {total_inserted:,} rows imported.")
                st.balloons()
                
            except Exception as e:
                st.error(f"Stream Failure: {e}")

# ---------- PAGE: VIEW INVENTORY ----------
elif page == "View Inventory":
    st.subheader("Live Database Stats")
    try:
        conn = get_conn()
        count_df = pd.read_sql("SELECT COUNT(*) AS total_rows, COALESCE(SUM(quantity_on_hand),0) AS total_qty FROM inventory;", conn)
        sample_df = pd.read_sql("SELECT * FROM inventory ORDER BY id DESC LIMIT 50;", conn)
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
