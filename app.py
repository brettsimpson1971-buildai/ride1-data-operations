import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re, datetime

# ---------- CONFIG ----------
st.set_page_config(page_title="Ride 1 Command Center", layout="wide")
# Using 127.0.0.1 for local database connection
engine = create_engine("postgresql://ride1admin@127.0.0.1:5432/ride1", pool_pre_ping=True)

# ---------- LOGIN ----------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

def login_screen():
    st.markdown("<br><br>", unsafe_allow_html=True)
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

# ---------- SMART UTILITIES (The "No-Crash" Engine) ----------
def normalize_col(col):
    """Standardizes column names for matching."""
    s = str(col).strip().lower()
    s = re.sub(r'[^a-z0-9_]', '_', s)
    return s.strip('_')

def smart_map(df, target_table):
    """Maps CSV columns to DB columns and fills missing ones with 0/None."""
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table}'"))
        db_cols = [r[0] for r in res.fetchall() if r[0] != 'id']
    
    # Dictionary of common Lightspeed synonyms
    synonyms = {
        'part_number': ['part', 'part_no', 'part#', 'sku', 'item', 'partnum', 'part_number'],
        'quantity': ['qty', 'qoh', 'quantity_on_hand', 'count', 'units', 'on_hand', 'quantity'],
        'price': ['price', 'msrp', 'retail', 'sell_price'],
        'cost': ['cost', 'unit_cost', 'net_price'],
        'description': ['description', 'desc', 'item_name'],
        'location_bin': ['bin', 'location', 'loc', 'bin_number'],
        'adj_qty': ['adj_qty', 'adjustment_qty', 'adj_quantity', 'adjustment']
    }
    
    rename_map = {}
    for csv_col in df.columns:
        norm_csv = normalize_col(csv_col)
        for db_col, syns in synonyms.items():
            if norm_csv in [normalize_col(s) for s in syns] and db_col in db_cols:
                rename_map[csv_col] = db_col
                break
    
    df = df.rename(columns=rename_map)
    
    # ✅ CRITICAL FIX: Force 'qty' to 'quantity' to match your existing DB schema
    if 'qty' in df.columns and 'quantity' not in df.columns:
        df = df.rename(columns={'qty': 'quantity'})
    
    # Ensure all DB columns exist in the dataframe (Fixes the crash from missing columns)
    for col in db_cols:
        if col not in df.columns:
            df[col] = 0 if col in ['quantity', 'cost', 'price', 'adj_qty', 'adj_amount'] else None
            
    # Clean numeric columns (Remove $, %, and commas)
    num_cols = ['quantity', 'cost', 'price', 'margin', 'adj_qty', 'adj_amount', 'qty_after_adj']
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace('[$,%]', '', regex=True), errors='coerce').fillna(0)
            
    return df[db_cols]

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
        st.metric("Total Zaptask Savings", f"${float(savings):,.2f}", delta="Recovered Revenue", delta_color="normal")
    
    st.divider()
    st.info("System ready. Monitoring records for margin leaks and inventory variances.")

elif page == "Master Inventory Upload":
    st.title("📥 Master Inventory Upload")
    st.warning("This will WIPE the current inventory and replace it with the new file.")
    uploaded = st.file_uploader("Upload Master CSV", type="csv")
    if uploaded and st.button("🚀 START MASTER IMPORT"):
        try:
            with engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE inventory"))
            
            # Streamed processing for large files
            for chunk in pd.read_csv(io.StringIO(uploaded.getvalue().decode('utf-8', errors='ignore')), chunksize=50000):
                chunk_mapped = smart_map(chunk, 'inventory')
                chunk_mapped.to_sql('inventory', engine, if_exists='append', index=False)
            
            st.success("✅ Master Inventory Updated Successfully!")
            st.rerun()
        except Exception as e:
            st.error(f"Master Upload Error: {e}")

elif page == "Daily Upload":
    st.title("📋 Daily DMS Sync")
    st.write("Upload the daily export from Lightspeed to audit for leaks.")
    daily_file = st.file_uploader("Upload Daily CSV", type="csv")
    if daily_file and st.button("✅ RUN FORENSIC AUDIT"):
        try:
            df_daily = pd.read_csv(io.StringIO(daily_file.getvalue().decode('utf-8', errors='ignore')))
            # Smart Map handles missing columns like "Adj Qty" automatically
            df_mapped = smart_map(df_daily, 'daily_log')
            df_mapped.to_sql('daily_log', engine, if_exists='append', index=False)
            st.success("✅ Daily log added successfully! Check Leak Detector for results.")
        except Exception as e:
            st.error(f"Daily Upload Error: {e}")

elif page == "Leak Detector":
    st.title("🔍 Forensic Leak Detector")
    
    query = """
    SELECT 
        d.part_number, d.description,
        d.cost as daily_cost, i.cost as master_cost,
        d.price as daily_price, i.price as master_price,
        d.quantity as daily_qty, d.adj_qty, d.created_at
    FROM daily_log d
    JOIN inventory i ON d.part_number = i.part_number
    LEFT JOIN leak_cases c ON d.part_number = c.part_number AND c.status = 'resolved'
    WHERE c.part_number IS NULL
    AND (
        ABS(d.cost - i.cost) > 0.01
        OR ABS(d.price - i.price) > 0.01
        OR d.adj_qty < 0
    )
    ORDER BY d.created_at DESC
    """
    try:
        leaks = pd.read_sql(query, engine)
        
        if not leaks.empty:
            st.subheader(f"⚠️ Found {len(leaks)} Active Discrepancies")
            st.dataframe(leaks, use_container_width=True)
            
            selected_part = st.selectbox("🔍 Select a Part Number to Investigate:", ["-- Select --"] + list(leaks['part_number'].unique()))
            
            if selected_part != "-- Select --":
                row = leaks[leaks['part_number'] == selected_part].iloc[0]
                qty = float(row['daily_qty']) if row['daily_qty'] else 1
                total_win = (abs(row['daily_cost'] - row['master_cost']) + abs(row['daily_price'] - row['master_price'])) * qty

                col_a, col_b = st.columns([2,1])
                with col_a:
                    st.write(f"### 🕵️‍♂️ Case File: {selected_part}")
                    hist = pd.read_sql(text("SELECT * FROM daily_log WHERE part_number = :p ORDER BY created_at DESC"), engine, params={"p": selected_part})
                    st.dataframe(hist, use_container_width=True)
                
                with col_b:
                    st.write("### 🛠 Actions")
                    st.metric("Potential Recovery", f"${total_win:,.2f}")
                    if st.button(f"✅ Resolve & Bank Savings", use_container_width=True):
                        with engine.begin() as conn:
                            conn.execute(text("INSERT INTO leak_cases (part_number, status, resolved_at, recovered_amount) VALUES (:p, 'resolved', NOW(), :s)"), {"p": selected_part, "s": total_win})
                        st.success(f"Case {selected_part} resolved!")
                        st.rerun()
        else:
            st.success("✅ No active leaks detected.")
    except Exception as e:
        st.error(f"Leak Detector Error: {e}")

elif page == "⚠️ NUKE":
    st.title("☢️ RESET SYSTEM")
    st.error("This will delete ALL inventory, logs, and cases. This cannot be undone.")
    if st.button("WIPE ALL DATA"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE inventory, daily_log, leak_cases"))
        st.success("System Reset.")
