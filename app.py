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

# ---------- SMART MAPPING ----------
def get_shawn_mapping(csv_cols):
    logic = {
        'part_number': ['Part Number', 'part_number', 'SKU'],
        'description': ['Description', 'description'],
        'quantity': ['Qty', 'Quantity', 'quantity'],
        'price': ['Price', 'price'],
        'cost': ['Cost', 'cost'],
        'tx_type': ['Tx Type', 'Transaction Type', 'Type'],
        'user_name': ['User', 'Employee', 'user_name'],
        'location': ['Location', 'Store', 'location']
    }
    mapping = {}
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
    col1, col2 = st.columns(2)
    with col1:
        sku = pd.read_sql("SELECT COUNT(*) FROM inventory", engine).iloc[0,0]
        st.metric("Total SKUs in Database", f"{int(sku):,}")
    with col2:
        savings = pd.read_sql("SELECT SUM(recovered_amount) FROM leak_cases", engine).iloc[0,0] or 0
        st.metric("Total Zaptask Savings", f"${float(savings):,.2f}")
    st.info("System ready. Monitoring Sales, Shipping, and Receiving.")

elif page == "Master Inventory Upload":
    st.title("📥 Master Inventory Upload")
    uploaded = st.file_uploader("Upload Master CSV", type="csv")
    if uploaded:
        df_raw = pd.read_csv(io.StringIO(uploaded.getvalue().decode('utf-8')), dtype=str)
        # Clean junk
        df_raw = df_raw[~df_raw['Part Number'].isin(['0', '1', 'nan', None])]
        
        st.write("### 🛠 Verify Column Mapping")
        auto_map = get_shawn_mapping(list(df_raw.columns))
        final_map = {}
        cols = st.columns(3)
        for i, db_c in enumerate(auto_map.keys()):
            csv_list = ["-- Skip --"] + list(df_raw.columns)
            idx = csv_list.index(auto_map[db_c]) if auto_map[db_c] in csv_list else 0
            with cols[i % 3]:
                final_map[db_c] = st.selectbox(f"DB: {db_c}", csv_list, index=idx)

        if st.button("🚀 START MASTER IMPORT"):
            try:
                rename_dict = {v: k for k, v in final_map.items() if v != "-- Skip --"}
                df_import = df_raw[list(rename_dict.keys())].rename(columns=rename_dict)
                df_import = df_import.drop_duplicates(subset=['part_number'], keep='last')
                
                # Numeric cleaning
                for c in ['quantity', 'cost', 'price']:
                    if c in df_import.columns:
                        df_import[c] = pd.to_numeric(df_import[c].str.replace('[$,]', '', regex=True), errors='coerce').fillna(0)
                
                with engine.connect() as conn:
                    conn.execute(text("TRUNCATE TABLE inventory"))
                    conn.commit()
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
        auto_map = get_shawn_mapping(list(df_daily.columns))
        
        st.write("### 🛠 Verify Daily Mapping")
        final_map = {}
        cols = st.columns(3)
        for i, db_c in enumerate(auto_map.keys()):
            csv_list = ["-- Skip --"] + list(df_daily.columns)
            idx = csv_list.index(auto_map[db_c]) if auto_map[db_c] in csv_list else 0
            with cols[i % 3]:
                final_map[db_c] = st.selectbox(f"Daily: {db_c}", csv_list, index=idx)

        if st.button("✅ CONFIRM DAILY IMPORT"):
            try:
                rename_dict = {v: k for k, v in final_map.items() if v != "-- Skip --"}
                df_import = df_daily[list(rename_dict.keys())].rename(columns=rename_dict)
                # We APPEND here to keep history
                df_import.to_sql('daily_log', engine, if_exists='append', index=False)
                st.success(f"✅ {len(df_import)} transactions added to history!")
            except Exception as e:
                st.error(f"Error: {e}")

elif page == "Leak Detector":
    st.title("🔍 Forensic Leak Detector")
    
    # Advanced Query: Checks Sales vs Price and Receiving vs Cost
    query = """
    SELECT 
        d.part_number, d.description, d.tx_type,
        d.cost as daily_cost, i.cost as master_cost,
        d.price as daily_price, i.price as master_price,
        d.quantity as daily_qty, d.user_name, d.created_at
    FROM daily_log d
    JOIN inventory i ON d.part_number = i.part_number
    LEFT JOIN leak_cases c ON d.part_number = c.part_number AND c.status = 'resolved'
    WHERE c.part_number IS NULL
    AND (
        (d.tx_type = 'receive' AND ABS(CAST(d.cost AS NUMERIC) - i.cost) > 0.01)
        OR (d.tx_type = 'sale' AND CAST(d.price AS NUMERIC) < i.price)
        OR (d.tx_type = 'adjust' AND CAST(d.quantity AS NUMERIC) < 0)
    )
    ORDER BY d.created_at DESC
    """
    leaks = pd.read_sql(query, engine)
    
    if not leaks.empty:
        def color_leaks(row):
            try:
                if row['tx_type'] == 'adjust': return ['background-color: #ff4b4b; color: white'] * len(row)
                if row['tx_type'] == 'sale' and float(row['daily_price']) < float(row['master_price']):
                    return ['background-color: #ffa500; color: black'] * len(row)
            except: pass
            return [''] * len(row)

        st.subheader(f"⚠️ Found {len(leaks)} Active Discrepancies")
        st.dataframe(leaks.style.apply(color_leaks, axis=1), use_container_width=True)
        
        st.divider()
        selected_part = st.selectbox("🔍 Select Part to Investigate:", ["-- Select --"] + list(leaks['part_number'].unique()))
        
        if selected_part != "-- Select --":
            col_a, col_b = st.columns([2,1])
            row = leaks[leaks['part_number'] == selected_part].iloc[0]
            
            # Calculate Recovery
            qty = float(row['daily_qty']) if row['daily_qty'] else 1
            recovery = abs(float(row['daily_price']) - float(row['master_price'])) * abs(qty)

            with col_a:
                st.write(f"### 🕵️‍♂️ Case File: {selected_part}")
                hist = pd.read_sql(text("SELECT * FROM daily_log WHERE part_number = :p ORDER BY created_at DESC"), engine, params={"p": selected_part})
                st.dataframe(hist, use_container_width=True)
            
            with col_b:
                st.write("### 🛠 Actions")
                st.metric("Potential Recovery", f"${recovery:,.2f}")
                if st.button(f"✅ Resolve & Bank Savings", use_container_width=True):
                    with engine.connect() as conn:
                        conn.execute(text("INSERT INTO leak_cases (part_number, status, resolved_at, recovered_amount) VALUES (:p, 'resolved', NOW(), :s)"), {"p": selected_part, "s": recovery})
                        conn.commit()
                    st.success("Case resolved!")
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
