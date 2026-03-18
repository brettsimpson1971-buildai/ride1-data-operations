cat > ~/ride1dashboard/app.py <<'EOF'
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
    return None

def clean_numeric_series(s):
    if s is None: return None
    return pd.to_numeric(s.astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')

def remove_repeated_headers(df):
    if df.empty: return df
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

# ---------- SMART MAPPING LOGIC ----------
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
        except: st.metric("Total SKUs in Database", "N/A")
    with col2:
        try:
            savings = pd.read_sql("SELECT SUM(recovered_amount) FROM leak_cases", engine).iloc[0,0] or 0
            st.metric("Total Zaptask Savings", f"${float(savings):,.2f}")
        except: st.metric("Total Zaptask Savings", "N/A")
    st.info("System ready. Monitoring Sales, Shipping, and Receiving.")

elif page == "Master Inventory Upload":
    st.title("📥 Master Inventory Upload")
    uploaded = st.file_uploader("Upload Master CSV", type="csv")
    if uploaded:
        df_raw = pd.read_csv(io.StringIO(uploaded.getvalue().decode('utf-8')), dtype=str)
        df_raw = remove_repeated_headers(df_raw)
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
                for c in ['quantity', 'cost', 'price']:
                    if c in df_import.columns:
                        df_import[c] = clean_numeric_series(df_import[c]).fillna(0)
                with engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE inventory"))
                df_import.to_sql('inventory', engine, if_exists='append', index=False)
                st.success("✅ Master Inventory Updated!")
            except Exception as e:
                st.error(f"Error: {e}")

elif page == "Daily Upload":
    st.title("📋 Daily Transaction Upload")
    daily_file = st.file_uploader("Upload Combined CSV", type="csv")
    if daily_file:
        df_daily = pd.read_csv(io.StringIO(daily_file.getvalue().decode('utf-8')), dtype=str)
        df_daily = remove_repeated_headers(df_daily)
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
                for c in ['quantity', 'cost', 'price']:
                    if c in df_import.columns:
                        df_import[c] = clean_numeric_series(df_import[c]).fillna(0)
                if 'created_at' not in df_import.columns:
                    df_import['created_at'] = datetime.datetime.utcnow()
                df_import.to_sql('daily_log', engine, if_exists='append', index=False)
                st.success(f"✅ {len(df_import)} transactions added!")
            except Exception as e:
                st.error(f"Error: {e}")

elif page == "⚠️ NUKE":
    st.title("☢️ RESET")
    if st.button("WIPE ALL DATA"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE inventory; TRUNCATE TABLE daily_log; TRUNCATE TABLE leak_cases;"))
        st.success("System Reset.")
EOF

# RESTART COMMANDS
pkill -9 -f streamlit
nohup python -m streamlit run ~/ride1dashboard/app.py --server.port 8501 --server.address 0.0.0.0 > streamlit.log 2>&1 &
