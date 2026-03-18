#!/usr/bin/env python3
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re, datetime
import traceback

# ---------- CONFIG ----------
st.set_page_config(page_title="Ride 1 Command Center", layout="wide")
# Update this URL if needed
engine = create_engine("postgresql://ride1admin@127.0.0.1:5432/ride1", pool_pre_ping=True)

# ---------- HELPERS ----------
def normalize_token(s: str) -> str:
    if s is None: return ""
    return re.sub(r'[^a-z0-9]', '', str(s).lower())

def find_best_column(actual_cols, candidates):
    norm_map = {normalize_token(c): c for c in actual_cols}
    for cand in candidates:
        nc = normalize_token(cand)
        if nc in norm_map:
            return norm_map[nc]
    # loose substring match
    for cand in candidates:
        nc = normalize_token(cand)
        for ac_norm, ac in norm_map.items():
            if nc in ac_norm or ac_norm in nc:
                return ac
    return None

def clean_numeric_series(s):
    if s is None:
        return None
    return pd.to_numeric(s.astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')

def remove_repeated_headers(df):
    if df.empty:
        return df
    header_vals = list(df.columns)
    header_norm = [str(x).strip().lower() for x in header_vals]
    keep_mask = []
    for _, row in df.iterrows():
        row_vals = [str(x).strip().lower() for x in row.fillna('').astype(str).tolist()]
        keep_mask.append(row_vals != header_norm)
    return df[pd.Series(keep_mask).values]

def safe_identifier(name):
    """Allow only safe SQL identifiers (letters, digits, underscore), starting with letter/underscore."""
    if not isinstance(name, str):
        return False
    return re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name) is not None

# ---------- LOGIN ----------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

def login_screen():
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        st.title("🔐 Forensic Command Center")
        u = st.text_input("Username").strip().lower()
        p = st.text_input("Password", type="password").strip()
        if st.button("Login", use_container_width=True):
            # Replace with your auth if different
            if u == "ride1" and p == "shawn!2025":
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Invalid credentials.")

if not st.session_state["authenticated"]:
    login_screen()
    st.stop()

# ---------- SMART MAPPING ----------
SHWAN_LOGIC = {
    'part_number': ['Part Number', 'part_number', 'partnumber', 'sku', 'part'],
    'description': ['Description', 'description', 'desc'],
    'quantity': ['Qty', 'Quantity', 'quantity', 'qty'],
    'price': ['Price', 'price', 'sale price', 'unit price'],
    'cost': ['Cost', 'cost', 'unit cost'],
    'tx_type': ['Source', 'Tx Type', 'Transaction Type', 'Type', 'source'],
    'user_name': ['User', 'Employee', 'user_name'],
    'location': ['Location', 'Store', 'location'],
    'created_at': ['Created At', 'created_at', 'created', 'timestamp', 'date']
}

def auto_map_from_columns(cols):
    mapping = {}
    for key, variants in SHWAN_LOGIC.items():
        found = find_best_column(cols, variants)
        mapping[key] = found
    return mapping

# ---------- SIDEBAR ----------
with st.sidebar:
    st.title("Navigation")
    page = st.radio("Go to:", ["Command Center", "Master Inventory Upload", "Daily Upload", "Leak Detector", "⚠️ NUKE"])
    if st.button("Logout"):
        st.session_state["authenticated"] = False
        st.rerun()

# ---------- PAGES ----------
if page == "Command Center":
    st.title("🚨 RIDE 1 | FORENSIC COMMAND CENTER")
    col1, col2 = st.columns(2)
    with col1:
        try:
            sku = pd.read_sql("SELECT COUNT(*) FROM inventory", engine).iloc[0,0]
            st.metric("Total SKUs in Database", f"{int(sku):,}")
        except Exception:
            st.metric("Total SKUs in Database", "N/A")
    with col2:
        try:
            savings = pd.read_sql("SELECT COALESCE(SUM(recovered_amount),0) FROM leak_cases", engine).iloc[0,0] or 0
            st.metric("Total Zaptask Savings", f"${float(savings):,.2f}")
        except Exception:
            st.metric("Total Zaptask Savings", "N/A")
    st.info("System ready. Monitoring Sales, Shipping, and Receiving.")

elif page == "Master Inventory Upload":
    st.title("📥 Master Inventory Upload")
    uploaded = st.file_uploader("Upload Master CSV", type="csv")
    if uploaded:
        df_raw = pd.read_csv(io.StringIO(uploaded.getvalue().decode('utf-8')), dtype=str)
        df_raw = remove_repeated_headers(df_raw)
        df_raw.columns = [str(c).strip() for c in df_raw.columns]

        pn_candidates = [c for c in df_raw.columns if normalize_token(c) in ('partnumber','part','sku')]
        if pn_candidates:
            pn_col = pn_candidates[0]
            df_raw = df_raw[~df_raw[pn_col].isin(['0', '1', 'nan', None, ''])]
        else:
            df_raw = df_raw.dropna(how='all')

        st.write("### 🛠 Verify Column Mapping")
        auto_map = auto_map_from_columns(list(df_raw.columns))
        final_map = {}
        cols = st.columns(3)
        for i, db_c in enumerate(auto_map.keys()):
            csv_list = ["-- Skip --"] + list(df_raw.columns)
            idx = csv_list.index(auto_map[db_c]) if (auto_map[db_c] in csv_list) else 0
            with cols[i % 3]:
                final_map[db_c] = st.selectbox(f"DB: {db_c}", csv_list, index=idx)

        if st.button("🚀 START MASTER IMPORT"):
            try:
                rename_dict = {v: k for k, v in final_map.items() if v != "-- Skip --"}
                df_import = df_raw[list(rename_dict.keys())].rename(columns=rename_dict)
                df_import.columns = [c.strip().lower().replace(' ', '_') for c in df_import.columns]

                for c in ['quantity', 'cost', 'price', 'adj_qty', 'adj_amount', 'qty_after_adj']:
                    if c in df_import.columns:
                        df_import[c] = clean_numeric_series(df_import[c]).fillna(0)

                if 'part_number' in df_import.columns:
                    df_import = df_import.drop_duplicates(subset=['part_number'], keep='last')

                with engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE inventory"))
                df_import.to_sql('inventory', engine, if_exists='append', index=False, chunksize=10000)
                st.success("✅ Master Inventory Updated!")
            except Exception as e:
                st.error(f"Error: {e}")

elif page == "Daily Upload":
    st.title("📋 Daily Transaction Upload")
    st.write("Upload Sales, Shipping, and Receiving logs here.")
    daily_file = st.file_uploader("Upload Combined CSV", type="csv")
    if daily_file:
        df_daily = pd.read_csv(io.StringIO(daily_file.getvalue().decode('utf-8')), dtype=str)
        df_daily = remove_repeated_headers(df_daily)
        df_daily.columns = [str(c).strip() for c in df_daily.columns]

        auto_map = auto_map_from_columns(list(df_daily.columns))
        st.write("### 🛠 Verify Daily Mapping")
        final_map = {}
        cols = st.columns(3)
        for i, db_c in enumerate(auto_map.keys()):
            csv_list = ["-- Skip --"] + list(df_daily.columns)
            idx = csv_list.index(auto_map[db_c]) if (auto_map[db_c] in csv_list) else 0
            with cols[i % 3]:
                final_map[db_c] = st.selectbox(f"Daily: {db_c}", csv_list, index=idx)

        if st.button("✅ CONFIRM DAILY IMPORT"):
            try:
                rename_dict = {v: k for k, v in final_map.items() if v != "-- Skip --"}
                df_import = df_daily[list(rename_dict.keys())].rename(columns=rename_dict)
                df_import.columns = [c.strip().lower().replace(' ', '_') for c in df_import.columns]

                for c in ['quantity', 'cost', 'price', 'adj_qty', 'adj_amount']:
                    if c in df_import.columns:
                        df_import[c] = clean_numeric_series(df_import[c]).fillna(0)

                if 'created_at' not in df_import.columns:
                    df_import['created_at'] = datetime.datetime.utcnow()

                df_import.to_sql('daily_log', engine, if_exists='append', index=False)
                st.success(f"✅ {len(df_import)} transactions added to history!")
            except Exception as e:
                st.error(f"Error: {e}")

elif page == "Leak Detector":
    st.title("🔍 Leak Detector (Forensics)")

    # First, fetch column lists (show even if later query works)
    try:
        cols_daily = pd.read_sql("SELECT column_name FROM information_schema.columns WHERE table_name = 'daily_log' ORDER BY ordinal_position", engine)
        cols_inv = pd.read_sql("SELECT column_name FROM information_schema.columns WHERE table_name = 'inventory' ORDER BY ordinal_position", engine)
        cols_daily_list = list(cols_daily['column_name'])
        cols_inv_list = list(cols_inv['column_name'])
    except Exception as e_cols:
        cols_daily_list = []
        cols_inv_list = []
        st.warning("Could not fetch table column lists automatically.")
        st.code(str(e_cols))

    st.write("Detected columns (daily_log):", cols_daily_list or "N/A")
    st.write("Detected columns (inventory):", cols_inv_list or "N/A")
    st.divider()

    # Allow user to pick which columns represent part_number in each table (fixes UndefinedColumn)
    st.write("Choose the join columns (if the default fails):")
    col_daily_choice = st.selectbox("daily_log part column", ["-- auto --"] + cols_daily_list, index=0)
    col_inv_choice = st.selectbox("inventory part column", ["-- auto --"] + cols_inv_list, index=0)
    auto_map_btn = st.button("Auto-map likely part columns")

    if auto_map_btn:
        guessed_daily = find_best_column(cols_daily_list, SHWAN_LOGIC['part_number'])
        guessed_inv = find_best_column(cols_inv_list, SHWAN_LOGIC['part_number'])
        if guessed_daily:
            col_daily_choice = guessed_daily
        if guessed_inv:
            col_inv_choice = guessed_inv
        st.success(f"Auto-mapped: daily -> {col_daily_choice or 'none'}, inventory -> {col_inv_choice or 'none'}")
        # keep in session state picks so next run uses them
        st.session_state['_leak_daily_col'] = col_daily_choice
        st.session_state['_leak_inv_col'] = col_inv_choice

    # allow persisting picks
    if '_leak_daily_col' in st.session_state and col_daily_choice == "-- auto --":
        col_daily_choice = st.session_state['_leak_daily_col']
    if '_leak_inv_col' in st.session_state and col_inv_choice == "-- auto --":
        col_inv_choice = st.session_state['_leak_inv_col']

    # determine final column names (use defaults if None)
    default_daily_part = 'part_number'
    default_inv_part = 'part_number'
    final_daily_col = col_daily_choice if (col_daily_choice and col_daily_choice != "-- auto --") else default_daily_part
    final_inv_col = col_inv_choice if (col_inv_choice and col_inv_choice != "-- auto --") else default_inv_part

    # Validate identifiers
    if not safe_identifier(final_daily_col) or not safe_identifier(final_inv_col):
        st.error(f"Invalid column names chosen: {final_daily_col}, {final_inv_col}. Only letters, digits, and underscores allowed.")
        st.stop()

    # Build query with chosen column names
    leak_query = f"""
    SELECT 
        d.{final_daily_col} as part_number, d.description, d.tx_type,
        d.cost as daily_cost, i.{final_inv_col} as inv_join_col, i.cost as master_cost,
        d.price as daily_price, i.price as master_price,
        d.quantity as daily_qty, d.user_name, d.created_at
    FROM daily_log d
    JOIN inventory i ON d.{final_daily_col} = i.{final_inv_col}
    LEFT JOIN leak_cases c ON d.{final_daily_col} = c.part_number AND c.status = 'resolved'
    WHERE c.part_number IS NULL
    AND (
        (d.tx_type = 'receive' AND ABS(CAST(d.cost AS NUMERIC) - i.cost) > 0.01)
        OR (d.tx_type = 'sale' AND CAST(d.price AS NUMERIC) < i.price)
        OR (d.tx_type = 'adjust' AND CAST(d.quantity AS NUMERIC) < 0)
    )
    ORDER BY d.created_at DESC
    """

    run_btn = st.button("Run Leak Query")
    if run_btn:
        try:
            leaks = pd.read_sql(text(leak_query), engine)
            st.success(f"Query executed successfully — {len(leaks)} rows returned.")
            if not leaks.empty:
                def color_leaks(row):
                    try:
                        if row['tx_type'] == 'adjust': return ['background-color: #ff4b4b; color: white'] * len(row)
                        if row['tx_type'] == 'sale' and float(row.get('daily_price', 0) or 0) < float(row.get('master_price', 0) or 0):
                            return ['background-color: #ffa500; color: black'] * len(row)
                    except: pass
                    return [''] * len(row)

                st.dataframe(leaks.style.apply(color_leaks, axis=1), use_container_width=True)
                st.divider()
                selected = st.selectbox("Select part to investigate:", ["--"] + list(leaks['part_number'].astype(str).unique()))
                if selected and selected != "--":
                    st.write("Recent history for this part:")
                    try:
                        hist = pd.read_sql(text("SELECT * FROM daily_log WHERE {col} = :p ORDER BY created_at DESC".format(col=final_daily_col)), engine, params={"p": selected})
                        st.dataframe(hist, use_container_width=True)
                    except Exception as e_hist:
                        st.error("Could not fetch history for part.")
                        st.code(str(e_hist))
            else:
                st.info("No active discrepancies found.")
        except Exception as e:
            # Show full error and column lists; don't crash
            st.error("Leak query failed with a database error. Details below.")
            st.code(str(e))
            with st.expander("Full traceback"):
                tb = traceback.format_exc()
                st.text(tb)

            st.info("Detected table columns (daily_log / inventory):")
            st.write("daily_log:", cols_daily_list or "N/A")
            st.write("inventory:", cols_inv_list or "N/A")

            st.markdown("""
            Suggested actions:
            - If daily_log's part column is named differently (e.g., `Part Number`, `part number`, `partnum`, `sku`), select it in the dropdown above and re-run.
            - Use 'Auto-map' to let the app guess likely columns.
            - If you want me to add tolerant fallback logic to handle many header formats automatically, I can add that (requires a full-file change).
            """)
            # Save the error for troubleshooting
            st.session_state['_last_leak_error'] = str(e)

    # Allow rerun with previously chosen mapping quickly
    if st.button("Re-run with previous picks"):
        st.experimental_rerun()

elif page == "⚠️ NUKE":
    st.title("☢️ RESET")
    if st.button("WIPE ALL DATA"):
        try:
            with engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE inventory"))
                conn.execute(text("TRUNCATE TABLE daily_log"))
                conn.execute(text("TRUNCATE TABLE leak_cases"))
            st.success("System Reset.")
        except Exception as e:
            st.error(f"Reset failed: {e}")

else:
    st.write("Page not found.")
