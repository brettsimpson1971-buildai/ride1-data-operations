import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------
st.set_page_config(page_title="RIDE 1 DATA OPERATIONS", layout="wide")

# ---------------------------------------------------------
# DB HELPERS
# ---------------------------------------------------------
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

def fast_bulk_insert(df, table, column_map):
    """
    High‑speed bulk insert using execute_values.
    column_map: {csv_col_name: db_col_name}
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        # Reorder and rename columns to match DB
        df_to_insert = df[list(column_map.keys())].copy()
        df_to_insert.columns = list(column_map.values())
        cols = list(df_to_insert.columns)

        values = [tuple(x) for x in df_to_insert.to_numpy()]
        col_names = ", ".join(cols)
        sql = f"INSERT INTO {table} ({col_names}) VALUES %s"

        execute_values(cur, sql, values)
        conn.commit()
        success = len(values)
        errors = 0
    except Exception as e:
        st.error(f"Bulk insert failed: {e}")
        success = 0
        errors = len(df)
    finally:
        cur.close()
        conn.close()

    return success, errors

# ---------------------------------------------------------
# SIDEBAR NAV
# ---------------------------------------------------------
with st.sidebar:
    st.image(
        "https://cdn.abacus.ai/images/8f44384a-1116-4c71-b3e6-67356cf217cd.png",
        use_container_width=True,
    )
    st.markdown("---")
    page = st.radio(
        "Select Operation:",
        ["Upload Inventory", "View Inventory", "NUKE (Inventory only)"],
    )

st.title("RIDE 1: DATA OPERATIONS CENTER")
st.markdown("---")

# ---------------------------------------------------------
# PAGE: UPLOAD INVENTORY
# ---------------------------------------------------------
if page == "Upload Inventory":
    st.subheader("Upload Inventory CSV into `inventory` table")

    uploaded = st.file_uploader("Choose CSV file", type="csv")
    if uploaded:
        df = pd.read_csv(uploaded)
        st.write(f"Detected {len(df)} rows")
        st.dataframe(df.head(5), use_container_width=True)

        st.markdown("### Map Columns to `inventory` schema")

        # EXPECTED CSV COLUMNS in mock_inventory.csv:
        # part_number, description, quantity_on_hand, location_bin, last_updated

        csv_cols = ["-- skip --"] + list(df.columns)

        # ***** THIS IS THE MAPPING YOU WANT *****
        col_part = st.selectbox(
            "DB: part_number  ←  CSV column",
            csv_cols,
            index=csv_cols.index("part_number") if "part_number" in df.columns else 1,
        )
        col_qty = st.selectbox(
            "DB: quantity_on_hand  ←  CSV column",
            csv_cols,
            index=csv_cols.index("quantity_on_hand") if "quantity_on_hand" in df.columns else 1,
        )
        col_bin = st.selectbox(
            "DB: location_bin (optional)  ←  CSV column",
            csv_cols,
            index=csv_cols.index("location_bin") if "location_bin" in df.columns else 0,
        )
        col_upd = st.selectbox(
            "DB: last_updated (optional)  ←  CSV column",
            csv_cols,
            index=csv_cols.index("last_updated") if "last_updated" in df.columns else 0,
        )

        mode = st.radio(
            "Import Mode",
            ["Append", "Replace (wipe inventory first)"],
            index=1,
        )

        if st.button("🚀 IMPORT INVENTORY NOW"):
            # Build the mapping dict: CSV column -> DB column
            column_map = {}
            if col_part != "-- skip --":
                column_map[col_part] = "part_number"
            if col_qty != "-- skip --":
                column_map[col_qty] = "quantity_on_hand"
            if col_bin != "-- skip --":
                column_map[col_bin] = "location_bin"
            if col_upd != "-- skip --":
                column_map[col_upd] = "last_updated"

            # Safety check
            if "part_number" not in column_map.values() or "quantity_on_hand" not in column_map.values():
                st.error("You must map at least part_number and quantity_on_hand.")
            else:
                if "Replace" in mode:
                    st.warning("Wiping existing inventory…")
                    run_command("DELETE FROM inventory;")

                with st.spinner("Importing rows into inventory…"):
                    success, errors = fast_bulk_insert(df, "inventory", column_map)

                st.success(f"Inserted {success} rows into inventory. Errors: {errors}")


# ---------------------------------------------------------
# PAGE: VIEW INVENTORY
# ---------------------------------------------------------
elif page == "View Inventory":
    st.subheader("View `inventory` table")

    try:
        conn = get_conn()
        count_df = pd.read_sql(
            "SELECT COUNT(*) AS total_rows, "
            "COALESCE(SUM(quantity_on_hand),0) AS total_qty "
            "FROM inventory;",
            conn,
        )
        sample_df = pd.read_sql(
            "SELECT * FROM inventory ORDER BY 1 DESC LIMIT 50;", conn
        )
        conn.close()

        total_rows = int(count_df["total_rows"].iloc[0] or 0)
        total_qty = int(count_df["total_qty"].iloc[0] or 0)

        st.metric("Total rows in inventory", f"{total_rows:,}")
        st.metric("Total quantity_on_hand", f"{total_qty:,}")
        st.dataframe(sample_df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Error reading inventory: {e}")


# ---------------------------------------------------------
# PAGE: NUKE INVENTORY
# ---------------------------------------------------------
elif page == "NUKE (Inventory only)":
    st.subheader("NUKE INVENTORY TABLE")
    st.error("WARNING: This will delete ALL rows from `inventory`.")

    pwd = st.text_input("Type NUKE to confirm:", type="password")

    if st.button("NUKE NOW"):
        if pwd == "NUKE":
            run_command("DELETE FROM inventory;")
            st.success("All rows deleted from inventory.")
        else:
            st.error("Confirmation text incorrect. Type exactly: NUKE")
