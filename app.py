import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re, datetime

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

# ---------- SMART MAPPING LOGIC ----------
def get_shawn_mapping(csv_cols):
    """Automatically matches Shawn's CSV headers to our Database columns"""
    mapping = {}
    # Key: DB Column Name | Value: Shawn's CSV Header variants
    logic = {
        'part_number': ['Part Number', 'part_number', 'SKU'],
        'description': ['Description', 'description', 'Desc'],
        'quantity': ['Qty', 'Quantity', 'quantity'],
        'price': ['Price', 'price', 'MSRP'],
        'cost': ['Cost', 'cost', 'Unit Cost'],
        'margin': ['Margin', 'margin', 'Profit'],
        'margin_pct': ['Margin %', 'margin_pct', 'Margin Percent'],
        'adj_qty': ['Adj Qty', 'adj_qty'],
        'adj_amount': ['Adj Amount', 'adj_amount'],
        'qty_after_adj': ['Qty After Adj', 'qty_after_adj'],
        'source': ['Source', 'source'],
        'cat': ['Cat', 'cat', 'Category']
    }
    
    for db_col, variants in logic.items():
        match = next((c for c in csv_cols if c in variants), "-- Skip --")
        mapping[db_col] = match
    return mapping

# ---------- SIDEBAR ----------
with st.sidebar:
    st.image("https://assets.zyrosite.com/46uOcFMIrXbOQmGo/logo.png-354IavGq7CUmvHlz.png", width=200)
    page = st.radio("Navigation", ["Command Center", "Master Inventory Upload", "Daily Upload", "Leak Detector", "⚠️ NUKE"])
    if st.button("Logout"):
        st.session_state["authenticated"] = False
        st.rerun()

# ---------- PAGES ----------
if page == "Command Center":
    st.title("🚨 RIDE 1 | FORENSIC COMMAND CENTER")
    sku = pd.read_sql("SELECT COUNT(*) FROM inventory", engine).iloc[0,0]
    st.metric("Total SKUs in Database", f"{int(sku):,}")
    st.info("System ready. Monitoring 222k+ records for margin leaks.")

elif page == "Master Inventory Upload":
    st.title("📥 Master Inventory Upload")
    uploaded = st.file_uploader("Upload Master CSV", type="csv")
    if uploaded:
        df_raw = pd.read_csv(io.StringIO(uploaded.getvalue().decode('utf-8')), dtype=str)
        
        # Clean junk rows immediately
        if 'Part Number' in df_raw.columns:
            df_raw = df_raw[~df_raw['Part Number'].isin(['0', '1', 'nan', None])]
            df_raw = df_raw[~df_raw['Part Number'].str.contains('Supplier Code', na=False)]

        with engine.connect() as conn:
            res = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='inventory'"))
            db_cols = [r[0] for r in res.fetchall() if r[0] not in ['id']]
        
        st.write("### 🛠 Verify Column Mapping")
        auto_map = get_shawn_mapping(list(df_raw.columns))
        final_map = {}
        cols = st.columns(3)
        
        for i, db_c in enumerate(db_cols):
            csv_list = ["-- Skip --"] + list(df_raw.columns)
            default_val = auto_map.get(db_c, "-- Skip --")
            default_idx = csv_list.index(default_val) if default_val in csv_list else 0
            with cols[i % 3]:
                final_map[db_c] = st.selectbox(f"DB: {db_c}", csv_list, index=default_idx)

        if st.button("🚀 START IMPORT", use_container_width=True):
            try:
                rename_dict = {v: k for k, v in final_map.items() if v != "-- Skip --"}
                df_import = df_raw[list(rename_dict.keys())].rename(columns=rename_dict)
                df_import = df_import.drop_duplicates(subset=['part_number'], keep='last')
                
                num_cols = ['quantity', 'cost', 'price', 'margin', 'adj_qty', 'adj_amount', 'qty_after_adj']
                for c in num_cols:
                    if c in df_import.columns:
                        df_import[c] = pd.to_numeric(df_import[c].str.replace('[$,]', '', regex=True), errors='coerce').fillna(0)
                
                with engine.connect() as conn:
                    conn.execute(text("TRUNCATE TABLE inventory"))
                    conn.commit()
                
                df_import.to_sql('inventory', engine, if_exists='append', index=False, chunksize=10000)
                st.success(f"✅ Successfully imported {len(df_import):,} unique SKUs!")
                st.balloons()
            except Exception as e:
                st.error(f"Error: {e}")

elif page == "Daily Upload":
    st.title("📋 Daily Upload")
    daily_file = st.file_uploader("Upload Daily CSV", type="csv")
    if daily_file:
        df_daily = pd.read_csv(io.StringIO(daily_file.getvalue().decode('utf-8')), dtype=str)
        if st.button("✅ CONFIRM DAILY IMPORT"):
            try:
                df_daily.to_sql('daily_log', engine, if_exists='append', index=False)
                st.success("✅ Daily log added to history!")
            except Exception as e:
                st.error(f"Error: {e}")

elif page == "Leak Detector":
    st.title("🔍 Forensic Leak Detector")
    query = """
    SELECT 
        d."Part Number" as part_number, d."Description" as description,
        d."Cost" as daily_cost, i.cost as master_cost,
        d."Price" as daily_price, i.price as master_price,
        d."Adj Qty" as adj_qty, d.created_at
    FROM daily_log d
    JOIN inventory i ON d."Part Number" = i.part_number
    LEFT JOIN leak_cases c ON d."Part Number" = c.part_number AND c.status = 'resolved'
    WHERE c.part_number IS NULL
    AND (
        ABS(CAST(REPLACE(REPLACE(d."Cost", '$', ''), ',', '') AS NUMERIC) - i.cost) > 0.01
        OR ABS(CAST(REPLACE(REPLACE(d."Price", '$', ''), ',', '') AS NUMERIC) - i.price) > 0.01
        OR CAST(REPLACE(REPLACE(d."Adj Qty", '$', ''), ',', '') AS NUMERIC) < 0
    )
    ORDER BY d.created_at DESC
    """
    leaks = pd.read_sql(query, engine)
    
    if not leaks.empty:
        def color_leaks(row):
            try:
                d_cost = float(str(row['daily_cost']).replace('$','').replace(',',''))
                m_cost = float(row['master_cost'])
                cost_diff = abs(d_cost - m_cost)
                adj = float(str(row['adj_qty']).replace('$','').replace(',',''))
                if adj < 0 or cost_diff > 50: return ['background-color: #ff4b4b; color: white; font-weight: bold'] * len(row)
                if cost_diff > 5: return ['background-color: #ffa500; color: black'] * len(row)
            except: pass
            return [''] * len(row)

        st.subheader(f"⚠️ Found {len(leaks)} Active Discrepancies")
        st.dataframe(leaks.style.apply(color_leaks, axis=1), use_container_width=True)
        
        st.divider()
        selected_part = st.selectbox("🔍 Select a Part Number to Investigate:", ["-- Select --"] + list(leaks['part_number'].unique()))
        
        if selected_part != "-- Select --":
            col_a, col_b = st.columns([2,1])
            with col_a:
                st.write(f"### 🕵️‍♂️ Case File: {selected_part}")
                hist = pd.read_sql(text("SELECT * FROM daily_log WHERE \"Part Number\" = :p ORDER BY created_at DESC"), engine, params={"p": selected_part})
                st.dataframe(hist, use_container_width=True)
            with col_b:
                st.write("### 🛠 Actions")
                if st.button(f"✅ Resolve & Archive {selected_part}", use_container_width=True):
                    with engine.connect() as conn:
                        conn.execute(text("INSERT INTO leak_cases (part_number, status, resolved_at) VALUES (:p, 'resolved', NOW())"), {"p": selected_part})
                        conn.commit()
                    st.success(f"Case {selected_part} resolved!")
                    st.rerun()
    else:
        st.success("✅ No active leaks detected.")

elif page == "⚠️ NUKE":
    st.title("☢️ RESET")
    if st.button("WIPE ALL DATA"):
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE inventory"), text("TRUNCATE TABLE daily_log"), text("TRUNCATE TABLE leak_cases"))
            conn.commit()
        st.success("System Reset.")
