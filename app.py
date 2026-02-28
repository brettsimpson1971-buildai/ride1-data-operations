import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ---------- CONFIG & SECRETS ----------
st.set_page_config(page_title="ZAPTASK A.I. | SECURE OPS", layout="wide")

# Simple Login Logic
def check_password():
    """Returns True if the user had the correct password."""
    def password_entered():
        """Checks whether a password entered by the user is correct."""
        # CHANGED PASSWORD TO: ZAPTASK-RIDE1
        if st.session_state["password"] == "ZAPTASK-RIDE1":
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state or not st.session_state["password_correct"]:
        # Show input for password.
        st.title("🔐 ZAPTASK A.I. SECURE GATEWAY")
        st.text_input(
            "Enter Admin Password to Access Ride 1 Operations:", 
            type="password", 
            on_change=password_entered, 
            key="password"
        )
        if "password_correct" in st.session_state and not st.session_state["password_correct"]:
            st.error("❌ Invalid Password. Access Denied.")
        
        st.info("Unauthorized access is strictly prohibited and monitored.")
        return False
    else:
        # Password correct.
        return True

if check_password():
    # ---------- EVERYTHING BELOW ONLY RUNS IF LOGGED IN ----------

    def get_conn():
        return psycopg2.connect(st.secrets["postgres"]["url"])

    # Sidebar Navigation
    st.sidebar.image("https://raw.githubusercontent.com/brettsimpson1971/ride1-dashboard/main/logo.png", width=200)
    st.sidebar.title("ZAPTASK A.I. OPS")
    
    # ADDED LOGOUT BUTTON
    if st.sidebar.button("🔒 LOGOUT"):
        st.session_state["password_correct"] = False
        st.rerun()

    operation = st.sidebar.radio(
        "Select Operation:",
        ["Upload Inventory", "Upload Activity Log", "View Inventory", "NUKE"]
    )

    # --- OPERATION 1: UPLOAD MASTER INVENTORY ---
    if operation == "Upload Inventory":
        st.header("📦 Upload Master Inventory")
        st.info("Use this for the initial 1M+ SKU Master List. This handles duplicates automatically.")
        uploaded_file = st.file_uploader("Choose Inventory CSV", type="csv")
        if uploaded_file is not None:
            df_preview = pd.read_csv(uploaded_file, nrows=5)
            st.write("Preview:", df_preview)
            cols = df_preview.columns.tolist()
            part_col = st.selectbox("DB: part_number", cols, index=cols.index("part_number") if "part_number" in cols else 0)
            qty_col = st.selectbox("DB: quantity_on_hand", cols, index=cols.index("quantity_on_hand") if "quantity_on_hand" in cols else 0)
            bin_col = st.selectbox("DB: location_bin", cols, index=cols.index("location_bin") if "location_bin" in cols else 0)

            if st.button("🚀 START STREAMED IMPORT"):
                try:
                    conn = get_conn()
                    cur = conn.cursor()
                    st.warning("Wiping existing inventory...")
                    cur.execute("TRUNCATE TABLE inventory;")
                    conn.commit()
                    uploaded_file.seek(0)
                    reader = pd.read_csv(uploaded_file, chunksize=50000)
                    total_rows = 0
                    status_text = st.empty()
                    for i, chunk in enumerate(reader):
                        data_to_insert = chunk[[part_col, qty_col, bin_col]].values.tolist()
                        execute_values(cur, """
                            INSERT INTO inventory (part_number, quantity_on_hand, location_bin) 
                            VALUES %s
                            ON CONFLICT (part_number) DO UPDATE SET 
                                quantity_on_hand = EXCLUDED.quantity_on_hand,
                                location_bin = EXCLUDED.location_bin;
                        """, data_to_insert)
                        conn.commit()
                        total_rows += len(chunk)
                        status_text.markdown(f"### ✅ Committed {total_rows:,} rows...")
                    st.success(f"Done! {total_rows:,} inventory rows imported.")
                    st.balloons()
                    cur.close()
                    conn.close()
                except Exception as e:
                    st.error(f"Stream Failure: {e}")

    # --- OPERATION 2: UPLOAD ACTIVITY LOG ---
    elif operation == "Upload Activity Log":
        st.header("🕵️ Upload Daily Activity Log")
        uploaded_file = st.file_uploader("Choose Activity Log CSV", type="csv")
        if uploaded_file is not None:
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

    # --- OPERATION 3: VIEW INVENTORY ---
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

    # --- OPERATION 4: NUKE ---
    elif operation == "NUKE":
        st.header("☢️ Danger Zone")
        target = st.selectbox("Select table to wipe:", ["inventory", "receiving_log"])
        if st.button(f"CONFIRM WIPE {target.upper()}"):
            try:
                conn = get_conn()
                cur = conn.cursor()
                cur.execute(f"TRUNCATE TABLE {target};")
                conn.commit()
                st.success(f"{target} has been cleared.")
                cur.close()
                conn.close()
            except Exception as e:
                st.error(f"Nuke Failed: {e}")
