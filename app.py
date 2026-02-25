import streamlit as st
import pandas as pd
import psycopg2
from io import StringIO

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

def bulk_insert(df, table, column_map):
    conn = get_conn()
    cur = conn.cursor()
    success = 0
    errors = 0
    for _, row in df.iterrows():
        try:
            cols = list(column_map.values())
            vals = [row[k] for k in column_map.keys()]
            placeholders = ", ".join(["%s"] * len(cols))
            col_names = ", ".join(cols)
            cur.execute(f"INSERT INTO {table} ({col_names}) VALUES ({placeholders});", vals)
            success += 1
        except Exception:
            errors += 1
    conn.commit()
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
st.caption("Upload CSVs, manage inventory data, and prepare the system for live deployment.")
st.markdown("---")

# ============================================================
# PAGE: Upload Receiving Log
# ============================================================
if page == "Upload Receiving Log":
    st.subheader("Upload Receiving Log CSV")
    st.info("Required columns: part_number, description, quantity. Optional: employee_id, movement_type, variance_amount, severity_level, timestamp")

    uploaded = st.file_uploader("Choose a CSV file", type="csv", key="receiving")
    if uploaded:
        df = pd.read_csv(uploaded)
        st.write(f"Preview: {len(df)} rows detected")
        st.dataframe(df.head(10), use_container_width=True)

        st.markdown("### Map Your Columns")
        st.caption("Match your CSV column names to the database fields.")

        csv_cols = ["-- skip --"] + list(df.columns)

        col_part = st.selectbox("part_number", csv_cols, index=csv_cols.index(df.columns[0]) if len(df.columns) > 0 else 0)
        col_desc = st.selectbox("description", csv_cols, index=csv_cols.index(df.columns[1]) if len(df.columns) > 1 else 0)
        col_qty = st.selectbox("quantity", csv_cols, index=csv_cols.index(df.columns[2]) if len(df.columns) > 2 else 0)
        col_emp = st.selectbox("employee_id (optional)", csv_cols)
        col_move = st.selectbox("movement_type (optional)", csv_cols)
        col_var = st.selectbox("variance_amount (optional)", csv_cols)
        col_sev = st.selectbox("severity_level (optional)", csv_cols)
        col_ts = st.selectbox("timestamp (optional)", csv_cols)

        mode = st.radio("Import Mode:", ["Append (add to existing data)", "Replace (wipe table first)"])

        if st.button("Import to Database"):
            column_map = {}
            if col_part != "-- skip --": column_map[col_part] = "part_number"
            if col_desc != "-- skip --": column_map[col_desc] = "description"
            if col_qty != "-- skip --": column_map[col_qty] = "quantity"
            if col_emp != "-- skip --": column_map[col_emp] = "employee_id"
            if col_move != "-- skip --": column_map[col_move] = "movement_type"
            if col_var != "-- skip --": column_map[col_var] = "variance_amount"
            if col_sev != "-- skip --": column_map[col_sev] = "severity_level"
            if col_ts != "-- skip --": column_map[col_ts] = "timestamp"

            if "part_number" not in column_map.values():
                st.error("You must map at least the part_number column.")
            else:
                if "Replace" in mode:
                    run_command("DELETE FROM receiving_log;")
                    st.warning("Existing receiving_log data wiped.")

                success, errors = bulk_insert(df, "receiving_log", column_map)
                st.success(f"Import complete: {success} rows inserted, {errors} errors.")

# ============================================================
# PAGE: Upload Parts Master
# ============================================================
elif page == "Upload Parts Master":
    st.subheader("Upload Parts Master CSV")
    st.info("Required columns: part_number, description. Optional: category, unit_cost, supplier")

    uploaded = st.file_uploader("Choose a CSV file", type="csv", key="parts")
    if uploaded:
        df = pd.read_csv(uploaded)
        st.write(f"Preview: {len(df)} rows detected")
        st.dataframe(df.head(10), use_container_width=True)

        st.markdown("### Map Your Columns")
        csv_cols = ["-- skip --"] + list(df.columns)

        col_part = st.selectbox("part_number", csv_cols)
        col_desc = st.selectbox("description", csv_cols)
        col_cat = st.selectbox("category (optional)", csv_cols)
        col_cost = st.selectbox("unit_cost (optional)", csv_cols)
        col_sup = st.selectbox("supplier (optional)", csv_cols)

        mode = st.radio("Import Mode:", ["Append (add to existing data)", "Replace (wipe table first)"])

        if st.button("Import to Database"):
            column_map = {}
            if col_part != "-- skip --": column_map[col_part] = "part_number"
            if col_desc != "-- skip --": column_map[col_desc] = "description"
            if col_cat != "-- skip --": column_map[col_cat] = "category"
            if col_cost != "-- skip --": column_map[col_cost] = "unit_cost"
            if col_sup != "-- skip --": column_map[col_sup] = "supplier"

            if "part_number" not in column_map.values():
                st.error("You must map at least the part_number column.")
            else:
                if "Replace" in mode:
                    run_command("DELETE FROM parts_master;")
                    st.warning("Existing parts_master data wiped.")

                success, errors = bulk_insert(df, "parts_master", column_map)
                st.success(f"Import complete: {success} rows inserted, {errors} errors.")

# ============================================================
# PAGE: Upload Inventory
# ============================================================
elif page == "Upload Inventory":
    st.subheader("Upload Inventory CSV")
    st.info("Required columns: part_number, quantity_on_hand. Optional: location_bin, last_updated")

    uploaded = st.file_uploader("Choose a CSV file", type="csv", key="inventory")
    if uploaded:
        df = pd.read_csv(uploaded)
        st.write(f"Preview: {len(df)} rows detected")
        st.dataframe(df.head(10), use_container_width=True)

        st.markdown("### Map Your Columns")
        csv_cols = ["-- skip --"] + list(df.columns)

        col_part = st.selectbox("part_number", csv_cols)
        col_qty = st.selectbox("quantity_on_hand", csv_cols)
        col_bin = st.selectbox("location_bin (optional)", csv_cols)
        col_upd = st.selectbox("last_updated (optional)", csv_cols)

        mode = st.radio("Import Mode:", ["Append (add to existing data)", "Replace (wipe table first)"])

        if st.button("Import to Database"):
            column_map = {}
            if col_part != "-- skip --": column_map[col_part] = "part_number"
            if col_qty != "-- skip --": column_map[col_qty] = "quantity_on_hand"
            if col_bin != "-- skip --": column_map[col_bin] = "location_bin"
            if col_upd != "-- skip --": column_map[col_upd] = "last_updated"

            if "part_number" not in column_map.values():
                st.error("You must map at least the part_number column.")
            else:
                if "Replace" in mode:
                    run_command("DELETE FROM inventory;")
                    st.warning("Existing inventory data wiped.")

                success, errors = bulk_insert(df, "inventory", column_map)
                st.success(f"Import complete: {success} rows inserted, {errors} errors.")

# ============================================================
# PAGE: View Database Tables
# ============================================================
elif page == "View Database Tables":
    st.subheader("Live Database Viewer")

    table = st.selectbox("Select Table:", ["receiving_log", "parts_master", "inventory"])
    limit = st.slider("Rows to show:", 10, 500, 50)

    try:
        conn = get_conn()
        df = pd.read_sql(f"SELECT * FROM {table} ORDER BY 1 DESC LIMIT {limit};", conn)
        conn.close()
        st.write(f"{len(df)} rows from `{table}`")
        st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Could not load table: {str(e)}")

# ============================================================
# PAGE: NUKE & RESET (Admin Only)
# ============================================================
elif page == "NUKE & RESET (Admin)":
    st.subheader("NUKE & RESET: Admin Only")
    st.error("WARNING: This will permanently delete ALL data from the selected tables. This cannot be undone.")

    admin_pass = st.text_input("Enter Admin Password:", type="password")
    tables_to_nuke = st.multiselect(
        "Select tables to wipe:",
        ["receiving_log", "parts_master", "inventory"]
    )

    if st.button("EXECUTE NUKE"):
        if admin_pass != st.secrets.get("ADMIN_PASSWORD", "ride1admin"):
            st.error("Incorrect password. Access denied.")
        elif not tables_to_nuke:
            st.warning("Select at least one table.")
        else:
            for t in tables_to_nuke:
                run_command(f"DELETE FROM {t};")
            st.success(f"Tables wiped: {', '.join(tables_to_nuke)}. System is now on a clean slate.")
            st.balloons()
