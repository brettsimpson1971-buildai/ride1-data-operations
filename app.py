import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re

# ---------- CONFIG ----------
st.set_page_config(page_title="Ride 1 Command Center", layout="wide")
engine = create_engine("postgresql://ride1admin@127.0.0.1:5432/ride1", pool_pre_ping=True)

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

# ---------- UTILITIES ----------
def normalize_col(col):
    s = str(col).strip().lower()
    s = re.sub(r'[%#@\\\/&]', '_', s)
    s = re.sub(r'[\s\-\u2013]+', '_', s)
    s = re.sub(r'[^a-z0-9_]', '', s)
    return s.strip('_')

def get_smart_mapping(csv_cols, db_cols):
    mapping = {}
    csv_map = {normalize_col(c): c for c in csv_cols}
    shawn_logic = {
        'part_number': 'part_number',
        'description': 'description',
        'source': 'source',
        'cat': 'cat',
        'quantity': 'qty',
        'cost': 'cost',
        'price': 'price',
        'margin': 'margin',
        'margin_pct': 'margin_',
        'adj_qty': 'adj_qty',
        'adj_amount': 'adj_amount',
        'qty_after_adj': 'qty_after_adj'
    }
    for db_c in db_cols:
        target = shawn_logic.get(db_c)
        if target and target in csv_map:
            mapping[db_c] = csv_map[target]
        else:
            mapping[db_c] = "-- Skip --"
    return mapping

# ---------- SIDEBAR ----------
with st.sidebar:
    st.image("https://assets.zyrosite.com/46uOcFMIrXbOQmGo/logo.png-354IavGq7CUmvHlz.png", width=200)
    page = st.radio("Navigation", [
        "Command Center",
        "Master Inventory Upload",
        "Daily Upload",
        "Leak Detector",
        "⚠️ NUKE"
    ])
    if st.button("Logout"):
        st.session_state["authenticated"] = False
        st.rerun()

# ---------- PAGES ----------
if page == "Command Center":
    st.title("🚨 RIDE 1 | FORENSIC COMMAND CENTER")
    sku = pd.read_sql("SELECT COUNT(*) FROM inventory", engine).iloc[0,0]
    st.metric("Total SKUs in Database", f"{int(sku):,}")
    st.info("System ready. Use sidebar to upload inventory or daily logs.")

elif page == "Master Inventory Upload":
    st.title("📥 Master Inventory Upload")
    st.write("Upload the full master inventory CSV (Ride1_Combined_Sync.csv).")
    uploaded = st.file_uploader("Upload Master CSV", type="csv")
    if uploaded:
        df_raw = pd.read_csv(io.StringIO(uploaded.getvalue().decode('utf-8')), dtype=str)
        
        # --- CLEAN JUNK ROWS ---
        # Drop rows where Part Number is '0', '1', or contains 'Supplier Code'
        if 'Part Number' in df_raw.columns:
            df_raw = df_raw[~df_raw['Part Number'].isin(['0', '1'])]
            df_raw = df_raw[~df_raw['Part Number'].str.contains('Supplier Code', na=False)]

        with engine.connect() as conn:
            res = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='inventory'"))
            db_cols = [r[0] for r in res.fetchall() if r[0] not in ['id']]
        
        st.write("### 🛠 Verify Column Mapping")
        suggestions = get_smart_mapping(list(df_raw.columns), db_cols)
        final_map = {}
        cols = st.columns(3)
        for i, db_c in enumerate(db_cols):
            csv_list = ["-- Skip --"] + list(df_raw.columns)
            sugg = suggestions.get(db_c, "-- Skip --")
            idx = csv_list.index(sugg) if sugg in csv_list else 0
            with cols[i % 3]:
                final_map[db_c] = st.selectbox(f"DB: {db_c}", csv_list, index=idx)
        
        if st.button("🚀 START BULLETPROOF IMPORT", use_container_width=True):
            try:
                # 1. Prepare and Rename
                rename_dict = {v: k for k, v in final_map.items() if v != "-- Skip --"}
                df_import = df_raw[list(rename_dict.keys())].rename(columns=rename_dict)
                
                # 2. Deduplicate (Keep Last)
                original_count = len(df_import)
                df_import = df_import.drop_duplicates(subset=['part_number'], keep='last')
                deduped_count = len(df_import)
                
                # 3. Clean numeric columns
                num_cols = ['quantity', 'cost', 'price', 'margin', 'adj_qty', 'adj_amount', 'qty_after_adj']
                for c in num_cols:
                    if c in df_import.columns:
                        df_import[c] = pd.to_numeric(df_import[c].str.replace('[$,]', '', regex=True), errors='coerce').fillna(0)

                # 4. TRUNCATE (Separate Transaction)
                with engine.connect() as conn:
                    conn.execute(text("TRUNCATE TABLE inventory"))
                    conn.commit()

                # 5. INSERT
                st.info(f"Importing {deduped_count:,} unique SKUs (Dropped {original_count - deduped_count} duplicates)...")
                df_import.to_sql('inventory', engine, if_exists='append', index=False, chunksize=10000)
                
                st.success(f"✅ Successfully imported {len(df_import):,} rows!")
                st.balloons()
            except Exception as e:
                st.error(f"Import Failed: {e}")

elif page == "Daily Upload":
    st.title("📋 Daily Upload")
    st.write("Upload today's receiving log CSV to update inventory.")
    daily_file = st.file_uploader("Upload Daily CSV", type="csv")
    if daily_file:
        df_daily = pd.read_csv(io.StringIO(daily_file.getvalue().decode('utf-8')), dtype=str)
        st.write(f"### Preview — {len(df_daily):,} rows detected")
        st.dataframe(df_daily.head(20))
        if st.button("✅ CONFIRM DAILY IMPORT", use_container_width=True):
            try:
                with engine.connect() as conn:
                    conn.execute(text("TRUNCATE TABLE daily_log"))
                    conn.commit()
                df_daily.to_sql('daily_log', engine, if_exists='append', index=False, chunksize=5000)
                st.success(f"✅ Daily log imported — {len(df_daily):,} rows added!")
            except Exception as e:
                st.error(f"Daily Import Failed: {e}")

elif page == "Leak Detector":
    st.title("🔍 Leak Detector")
    st.info("Coming soon — cross-reference daily logs against master inventory to find discrepancies.")

elif page == "⚠️ NUKE":
    st.title("☢️ RESET")
    if st.button("WIPE ALL DATA"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE inventory"))
            conn.commit()
        st.success("Inventory cleared.")
