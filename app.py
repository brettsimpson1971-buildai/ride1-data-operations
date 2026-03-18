#!/usr/bin/env python3
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re, datetime

# ---------- CONFIG ----------
st.set_page_config(page_title="Ride 1 Command Center", layout="wide")
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

# ---------- LOGIN ----------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

def login_screen():
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        st.image("https://assets.zyrosite.com/46uOcFMIrXbOQmGo/logo.png-354IavGq7CUmvHlz.png", width=300)
        st.title("🔐 Forensic Command Center")
        u = st.text_input("Username").strip().lower()
        p = st.text_input("Password", type="password").strip()
        if st.button("Login", use_container_width=True):
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
    st.image("https://assets.zyrosite.com/46uOcFMIrXbOQmGo/logo.png-354IavGq7CUmvHlz.png", width=200)
    # NOTE: Leak Detector removed per request
    page = st.radio("Navigation", ["Command Center", "Master Inventory Upload", "Daily Upload", "⚠️ NUKE"])
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
            savings = pd.read_sql("SELECT SUM(recovered_amount) FROM leak_cases", engine).iloc[0,0] or 0
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

        # drop obviously junk rows if identifiable
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

# Leak Detector removed entirely per your request — no queries, no crashes.

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
