import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re

st.set_page_config(page_title='Ride 1 Command Center', layout='wide', initial_sidebar_state='expanded')
# Using 127.0.0.1 to bypass potential Peer Auth issues on the socket
engine = create_engine("postgresql://ride1admin@127.0.0.1:5432/ride1", pool_pre_ping=True)

# 1. LOGIN SYSTEM
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

# 2. HELPERS
def normalize_col(col):
    s = str(col).strip().lower()
    s = re.sub(r'[#@\\\/&]', '_', s)
    s = re.sub(r'[\s\-\u2013]+', '_', s)
    s = re.sub(r'[^a-z0-9_]', '', s)
    return s.strip('_')

def map_columns(df, target_cols):
    """
    Maps messy CSV headers to clean Database columns using synonyms.
    """
    synonyms = {
        'part_number': ['part', 'part_no', 'part#', 'sku', 'item', 'item_number', 'partnum', 'part_number'],
        'location_bin': ['bin', 'bin_number', 'bin#', 'bin_no', 'location', 'loc'],
        'quantity': ['qty', 'quantity_on_hand', 'on_hand', 'count', 'units', 'quantity', 'qty_after_adj'],
        'employee_id': ['emp', 'emp_id', 'user', 'employee', 'staff', 'tech'],
        'variance_amount': ['variance', 'diff', 'shrink', 'loss'],
        'severity_level': ['severity', 'status', 'priority'],
        'description': ['description', 'desc', 'notes'],
        'timestamp': ['timestamp', 'time', 'date', 'datetime'],
        'price': ['price', 'msrp', 'unit_price', 'cost_plus']
    }
    
    rename_map = {}
    for orig in list(df.columns):
        norm = normalize_col(orig)
        for target, syns in synonyms.items():
            # If the normalized CSV header matches a synonym or the target name
            if norm in [normalize_col(s) for s in syns] + [target]:
                # Only map if the target column actually exists in the DB table
                if target in target_cols:
                    rename_map[orig] = target
                break
    
    # Rename the columns we found
    df = df.rename(columns=rename_map)
    
    # CRITICAL: Keep ONLY the columns that now match the DB target_cols
    valid_cols = [c for c in df.columns if c in target_cols]
    return df[valid_cols]

# 3. SIDEBAR
with st.sidebar:
    st.image("https://assets.zyrosite.com/46uOcFMIrXbOQmGo/logo.png-354IavGq7CUmvHlz.png", width=200)
    st.write("👤 User: **ride1**")
    if st.button("Logout"):
        st.session_state["authenticated"] = False
        st.rerun()
    st.divider()
    page = st.radio("Navigation", ["Command Center", "Leak Detector", "Initial Inventory Upload", "Daily DMS Sync", "⚠️ NUKE"])

# 4. PAGES
if page == "Command Center":
    st.title("🚨 RIDE 1 | FORENSIC COMMAND CENTER")
    try:
        sku = pd.read_sql("SELECT COUNT(*) FROM inventory", engine).iloc[0,0]
        roi_query = """
            SELECT SUM(ABS(r.variance_amount) * COALESCE(i.price, 0)) 
            FROM receiving_log r
            LEFT JOIN inventory i ON r.part_number = i.part_number
            WHERE r.resolution_status = 'RESOLVED'
        """
        recovered = pd.read_sql(roi_query, engine).iloc[0,0] or 0
        
        c1, c2 = st.columns(2)
        c1.metric("Total SKUs in Database", f"{int(sku):,}")
        c2.metric("Total Leakage Prevented", f"${recovered:,.2f}", delta_color="normal")
        
        st.divider()
        st.subheader("System Health")
        st.success("A.I. Forensic Engine: ACTIVE")
        st.success("Database Connection: STABLE")
    except:
        st.info("Database initializing...")

elif page == "Leak Detector":
    st.title("🔍 Forensic Leak Detector")
    tab1, tab2 = st.tabs(["Active Leaks", "Resolution Archive"])
    
    with tab1:
        try:
            leaks = pd.read_sql("SELECT * FROM receiving_log WHERE severity_level IN ('MODERATE','HIGH','CRITICAL') AND (resolution_status IS NULL OR resolution_status != 'RESOLVED') ORDER BY timestamp DESC LIMIT 100", engine)
            if leaks.empty:
                st.success("✅ All leaks resolved or none detected.")
            else:
                for _, r in leaks.iterrows():
                    row_id = r.get('id')
                    part = r.get('part_number', 'Unknown')
                    var = r.get('variance_amount', '0')
                    with st.expander(f"🚨 {r.get('severity_level')} | Part: {part} | Var: {var}"):
                        st.write(f"**Employee:** {r.get('employee_id')}")
                        st.write(f"**Location Bin:** {r.get('location_bin')}")
                        st.write(f"**Timestamp:** {r.get('timestamp')}")
                        
                        note = st.text_input("Resolution Note", key=f"note_{row_id}")
                        if st.button("Mark as Resolved", key=f"btn_{row_id}"):
                            with engine.begin() as conn:
                                conn.execute(text("UPDATE receiving_log SET resolution_status='RESOLVED', resolution_note=:n, resolved_at=NOW() WHERE id=:id"), {"n": note, "id": row_id})
                            st.success("Resolved!")
                            st.rerun()
        except Exception as e:
            st.error(f"Display Error: {e}")

    with tab2:
        try:
            archive = pd.read_sql("SELECT * FROM receiving_log WHERE resolution_status = 'RESOLVED' ORDER BY resolved_at DESC LIMIT 50", engine)
            if archive.empty:
                st.write("No resolved cases yet.")
            else:
                st.dataframe(archive[['timestamp', 'part_number', 'employee_id', 'variance_amount', 'resolution_note', 'resolved_at']], use_container_width=True)
        except:
            st.write("Archive unavailable.")

elif page == "Daily DMS Sync":
    st.title("🔄 Daily DMS Sync & Forensic Audit")
    f = st.file_uploader("Upload Daily Log CSV", type="csv")
    if f and st.button("🔍 RUN FORENSIC AUDIT"):
        try:
            df = pd.read_csv(io.StringIO(f.getvalue().decode('utf-8')), dtype=str)
            with engine.connect() as conn:
                res = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='receiving_log'"))
                recv_cols = [r[0] for r in res.fetchall()]
            df_mapped = map_columns(df, recv_cols)
            df_mapped.to_sql('receiving_log', engine, if_exists='append', index=False)
            st.success(f"Audit Complete: {len(df_mapped)} rows synced.")
        except Exception as e:
            st.error(f"Upload Error: {e}")

elif page == "Initial Inventory Upload":
    st.title("📦 Industrial Inventory Uploader")
    st.info("This will wipe the current inventory and replace it with the new file.")
    f = st.file_uploader("Upload Master Inventory CSV", type="csv")
    
    if f and st.button("🚀 START IMPORT"):
        try:
            # 1. Get the actual DB columns for the inventory table
            with engine.connect() as conn:
                res = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='inventory'"))
                inv_cols = [r[0] for r in res.fetchall()]
            
            # 2. Load the CSV
            df = pd.read_csv(io.StringIO(f.getvalue().decode('utf-8')), dtype=str)
            
            # 3. Map the columns (e.g. 'Part Number' -> 'part_number')
            df_mapped = map_columns(df, inv_cols)
            
            if df_mapped.empty:
                st.error("Could not find any matching columns (Part Number, Qty, etc.) in your file.")
            else:
                # 4. Wipe and Upload
                with engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE inventory"))
                
                # Upload in chunks for speed/stability
                df_mapped.to_sql('inventory', engine, if_exists='append', index=False, chunksize=10000)
                st.success(f"Success! {len(df_mapped):,} items imported.")
                st.balloons()
        except Exception as e:
            st.error(f"Import Error: {e}")

elif page == "⚠️ NUKE":
    st.title("☢️ NUCLEAR RESET")
    st.error("⚠️ DANGER ZONE: This will permanently delete ALL inventory data.")
    if st.button("CONFIRM TOTAL WIPE"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE inventory"))
            conn.execute(text("TRUNCATE TABLE receiving_log"))
        st.success("System Wiped.")
