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
    Drops duplicate columns after renaming to avoid conflicts.
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
    used_targets = set()
    for orig in list(df.columns):
        norm = normalize_col(orig)
        for target, syns in synonyms.items():
            if norm in [normalize_col(s) for s in syns] + [target]:
                if target in target_cols and target not in used_targets:
                    rename_map[orig] = target
                    used_targets.add(target)
                break
    
    df = df.rename(columns=rename_map)
    df = df.loc[:, ~df.columns.duplicated()]
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

elif page in ["Initial Inventory Upload", "Daily DMS Sync"]:
    st.title(f"📥 {page}")
    target_table = 'inventory' if page == "Initial Inventory Upload" else 'receiving_log'
    
    f = st.file_uploader(f"Upload {target_table.replace('_',' ').title()} CSV", type="csv")
    
    if f:
        df_raw = pd.read_csv(io.StringIO(f.getvalue().decode('utf-8')), dtype=str)
        st.write("### 🛠 Step 1: Map your Columns")
        st.info("We've tried to match them automatically. Please confirm below.")
        
        # Get DB Columns
        with engine.connect() as conn:
            res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table}'"))
            db_cols = [r[0] for r in res.fetchall() if r[0] not in ['id', 'resolved_at', 'resolution_status', 'resolution_note']]

        # Mapping UI
        mapping = {}
        csv_cols = ["-- Skip --"] + list(df_raw.columns)
        
        cols = st.columns(len(db_cols) // 2 + 1)
        for i, db_c in enumerate(db_cols):
            # Try to find a smart default
            default_idx = 0
            norm_db = normalize_col(db_c)
            for j, csv_c in enumerate(csv_cols):
                if normalize_col(csv_c) == norm_db or any(s in normalize_col(csv_c) for s in [norm_db, 'part', 'qty', 'price']):
                    default_idx = j
                    break
            
            with cols[i % (len(db_cols) // 2 + 1)]:
                mapping[db_c] = st.selectbox(f"Database: {db_c}", csv_cols, index=default_idx)

        if st.button("🚀 START IMPORT", use_container_width=True):
            try:
                # Create the mapped dataframe
                final_mapping = {v: k for k, v in mapping.items() if v != "-- Skip --"}
                
                # Check for duplicates in mapping
                if len(set(final_mapping.keys())) != len(final_mapping.keys()):
                    st.error("Error: You mapped the same CSV column to multiple database fields.")
                    st.stop()

                df_final = df_raw[list(final_mapping.keys())].rename(columns=final_mapping)
                
                with engine.begin() as conn:
                    if page == "Initial Inventory Upload":
                        conn.execute(text("TRUNCATE TABLE inventory"))
                    
                    df_final.to_sql(target_table, engine, if_exists='append', index=False, chunksize=10000)
                
                st.success(f"Success! {len(df_final):,} rows imported into {target_table}.")
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
