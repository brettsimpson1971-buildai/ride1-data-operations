import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ---------- DB CONNECTION ----------

def get_conn():
    # Uses Streamlit secrets:
    # [postgres]
    # url = "postgresql://postgres:1%23Biggainz@database-2.ckfqmyysk121.us-east-1.rds.amazonaws.com:5432/postgres"
    return psycopg2.connect(st.secrets["postgres"]["url"])


st.set_page_config(page_title="RIDE 1 DATA OPERATIONS", layout="wide")

# ---------- SIDEBAR & NAV ----------

# Logo – update this URL if your logo path/name is different
st.sidebar.image(
    "https://raw.githubusercontent.com/brettsimpson1971/ride1-dashboard/main/logo.png",
    width=200,
)
st.sidebar.title("RIDE 1 OPS")

operation = st.sidebar.radio(
    "Select Operation:",
    ["Upload Inventory", "Upload Activity Log", "View Database Stats", "NUKE"],
)

# ---------- OPERATION 1: UPLOAD MASTER INVENTORY ----------

if operation == "Upload Inventory":
    st.header("📦 Upload Master Inventory")
    st.info(
        "Use this for the initial 1M+ SKU Master List.\n\n"
        "WARNING: This will WIPE the current inventory table and replace it."
    )

    uploaded_file = st.file_uploader("Choose Master Inventory CSV", type="csv")

    if uploaded_file is not None:
        df_preview = pd.read_csv(uploaded_file, nrows=5)
        st.write("Preview of file:", df_preview)

        cols = df_preview.columns.tolist()

        part_col = st.selectbox(
            "DB: part_number column",
            cols,
            index=cols.index("part_number") if "part_number" in cols else 0,
        )
        qty_col = st.selectbox(
            "DB: quantity_on_hand column",
            cols,
            index=cols.index("quantity_on_hand") if "quantity_on_hand" in cols else 0,
        )
        bin_col = st.selectbox(
            "DB: location_bin column",
            cols,
            index=cols.index("location_bin") if "location_bin" in cols else 0,
        )

        if st.button("🚀 START MASTER IMPORT"):
            try:
                conn = get_conn()
                cur = conn.cursor()

                st.warning("Wiping existing inventory table…")
                cur.execute("TRUNCATE TABLE inventory;")
                conn.commit()

                uploaded_file.seek(0)
                reader = pd.read_csv(uploaded_file, chunksize=50000)

                total_rows = 0
                progress_bar = st.progress(0)
                status_text = st.empty()

                for i, chunk in enumerate(reader, start=1):
                    data_to_insert = chunk[[part_col, qty_col, bin_col]].values.tolist()

                    execute_values(
                        cur,
                        """
                        INSERT INTO inventory (part_number, quantity_on_hand, location_bin)
                        VALUES %s
                        """,
                        data_to_insert,
                    )
                    conn.commit()

                    total_rows += len(chunk)
                    status_text.text(f"Committed {total_rows:,} rows so far…")
                    # crude progress estimate (not perfect, but better than 0)
                    progress_bar.progress(min(1.0, i * 0.05))

                st.success(f"Done! {total_rows:,} master inventory rows imported.")
                st.balloons()

                cur.close()
                conn.close()

            except Exception as e:
                st.error(f"Stream Failure during inventory import: {e}")

# ---------- OPERATION 2: UPLOAD ACTIVITY LOG (FORENSIC) ----------

elif operation == "Upload Activity Log":
    st.header("🕵️ Upload Daily Activity Log")
    st.info(
        "Use this for daily DMS exports or the forensic demo CSV.\n\n"
        "NOTE: This ADDS rows into receiving_log. It does NOT wipe the log."
    )

    uploaded_file = st.file_uploader("Choose Activity Log CSV", type="csv")

    if uploaded_file is not None:
        df_preview = pd.read_csv(uploaded_file, nrows=5)
        st.write("Preview of file:", df_preview)

        required_cols = [
            "part_number",
            "description",
            "quantity",
            "employee_id",
            "movement_type",
            "location_bin",
            "variance_amount",
            "severity_level",
            "timestamp",
        ]

        missing = [c for c in required_cols if c not in df_preview.columns]
        if missing:
            st.error(f"Missing required columns in CSV: {missing}")
        else:
            if st.button("🔍 IMPORT ACTIVITY LOG"):
                try:
                    conn = get_conn()
                    cur = conn.cursor()

                    uploaded_file.seek(0)
                    reader = pd.read_csv(uploaded_file, chunksize=10000)

                    total_rows = 0
                    progress_bar = st.progress(0)
                    status_text = st.empty()

                    for i, chunk in enumerate(reader, start=1):
                        data_to_insert = chunk[required_cols].values.tolist()

                        execute_values(
                            cur,
                            """
                            INSERT INTO receiving_log
                            (part_number, description, quantity, employee_id,
                             movement_type, location_bin, variance_amount,
                             severity_level, timestamp)
                            VALUES %s
                            """,
                            data_to_insert,
                        )
                        conn.commit()

                        total_rows += len(chunk)
                        status_text.text(f"Committed {total_rows:,} activity rows…")
                        progress_bar.progress(min(1.0, i * 0.1))

                    st.success(f"Successfully imported {total_rows:,} activity records.")
                    st.balloons()

                    cur.close()
                    conn.close()

                except Exception as e:
                    st.error(f"Import Failure for activity log: {e}")

# ---------- OPERATION 3: VIEW DATABASE STATS ----------

elif operation == "View Database Stats":
    st.header("📊 Database Health")

    try:
        conn = get_conn()

        inv_count = pd.read_sql("SELECT COUNT(*) FROM inventory;", conn).iloc[0, 0]
        log_count = pd.read_sql("SELECT COUNT(*) FROM receiving_log;", conn).iloc[0, 0]

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Master Inventory SKUs", f"{inv_count:,}")
        with col2:
            st.metric("Activity Log Records", f"{log_count:,}")

        st.subheader("Latest 10 Activity Records")
        latest_logs = pd.read_sql(
            "SELECT * FROM receiving_log ORDER BY timestamp DESC LIMIT 10;", conn
        )
        st.dataframe(latest_logs)

        conn.close()

    except Exception as e:
        st.error(f"Error while reading database stats: {e}")

# ---------- OPERATION 4: NUKE ----------

elif operation == "NUKE":
    st.header("☢️ Danger Zone – Truncate Tables")

    target = st.selectbox("Select table to wipe:", ["inventory", "receiving_log"])

    st.warning(
        "This will PERMANENTLY delete all rows from the selected table. "
        "There is no undo."
    )

    if st.button(f"CONFIRM WIPE {target.upper()}"):
        try:
            conn = get_conn()
            cur = conn.cursor()

            cur.execute(f"TRUNCATE TABLE {target};")
            conn.commit()

            st.success(f"Table '{target}' has been cleared.")
            cur.close()
            conn.close()

        except Exception as e:
            st.error(f"Nuke Failed: {e}")
