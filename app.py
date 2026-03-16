import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re, os, json

# ---------- CONFIG ----------
st.set_page_config(page_title='Ride 1 Command Center', layout='wide')
engine = create_engine("postgresql://ride1admin@127.0.0.1:5432/ride1", pool_pre_ping=True)

# ---------- DATABASE MIGRATION (EXPAND DB) ----------
def migrate_db():
    """Adds missing columns from Shawn's file to the database automatically."""
    new_cols = {
        "margin": "NUMERIC",
        "margin_pct": "TEXT",
        "adj_qty": "NUMERIC",
        "adj_amount": "NUMERIC",
        "qty_after_adj": "NUMERIC",
        "source": "TEXT",
        "cat": "TEXT"
    }
    with engine.begin() as conn:
        # Check existing columns
        res = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='inventory'"))
        existing = [r[0] for r in res.fetchall()]
        
        for col, col_type in new_cols.items():
            if col not in existing:
                try:
                    conn.execute(text(f"ALTER TABLE inventory ADD COLUMN {col} {col_type}"))
                except Exception as e:
                    st.warning(f"Could not add column {col}: {e}")

# Run migration on startup
migrate_db()

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
    """Hardcoded logic for Shawn's specific file format."""
    mapping = {}
    csv_map = {normalize_col(c): c for c in csv_cols}
    
    # Direct matches for Shawn's file
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
    page = st.radio("Navigation", ["Command Center", "Inventory Upload", "Leak Detector", "⚠️ NUKE"])
    if st.button("Logout"):
        st.session_state["authenticated"] = False
        st.rerun()

# ---------- PAGES ----------
if page == "Command Center":
    st.title("🚨 RIDE 1 | FORENSIC COMMAND CENTER")
    sku = pd.read_sql("SELECT COUNT(*) FROM inventory", engine).iloc[0,0]
    st.metric("Total SKUs in Database", f"{int(sku):,}")
    st.success("Database expanded to support Shawn's Master Inventory format.")

elif page == "Inventory Upload":
    st.title("📥 Master Inventory Upload")
    uploaded = st.file_uploader("Upload Shawn's CSV (Ride1_Combined_Sync.csv)", type="csv")
    
    if uploaded:
        df_raw = pd.read_csv(io.StringIO(uploaded.getvalue().decode('utf-8')), dtype=str)
        
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

        if st.button("🚀 START 2M ROW IMPORT", use_container_width=True):
            try:
                # Prepare data
                rename_dict = {v: k for k, v in final_map.items() if v != "-- Skip --"}
                df_import = df_raw[list(rename_dict.keys())].rename(columns=rename_dict)
                
                # Clean numeric columns
                num_cols = ['quantity', 'cost', 'price', 'margin', 'adj_qty', 'adj_amount', 'qty_after_adj']
                for c in num_cols:
                    if c in df_import.columns:
                        df_import[c] = pd.to_numeric(df_import[c].str.replace('[$,]', '', regex=True), errors='coerce').fillna(0)

                with engine.begin() as conn:
                    st.info("Wiping old inventory...")
                    conn.execute(text("TRUNCATE TABLE inventory"))
                    st.info(f"Streaming {len(df_import):,} rows to PostgreSQL...")
                    df_import.to_sql('inventory', engine, if_exists='append', index=False, chunksize=15000)
                
                st.success(f"✅ Successfully imported {len(df_import):,} rows!")
                st.balloons()
            except Exception as e:
                st.error(f"Import Failed: {e}")

elif page == "⚠️ NUKE":
    st.title("☢️ RESET")
    if st.button("WIPE ALL DATA"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE inventory"))
        st.success("Inventory cleared.")
