import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re

st.set_page_config(page_title='Ride 1 Command Center', layout='wide', initial_sidebar_state='expanded')
engine = create_engine("postgresql://ride1admin@localhost:5432/ride1", pool_pre_ping=True)

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

def map_columns(df, recv_cols):
    synonyms = {
        'part_number': ['part', 'part_no', 'part#', 'sku', 'item', 'item_number', 'partnum'],
        'location_bin': ['bin', 'bin_number', 'bin#', 'bin_no', 'location', 'loc'],
        'quantity': ['qty', 'quantity_on_hand', 'on_hand', 'count', 'units', 'quantity'],
        'employee_id': ['emp', 'emp_id', 'user', 'employee', 'staff', 'tech'],
        'variance_amount': ['variance', 'diff', 'shrink', 'loss'],
        'severity_level': ['severity', 'status', 'priority'],
        'description': ['description', 'desc', 'notes'],
        'timestamp': ['timestamp', 'time', 'date', 'datetime']
    }
    recv_norm_map = { normalize_col(c): c for c in recv_cols }
    rename_map = {}
    for orig in list(df.columns):
        norm = normalize_col(orig)
        for target, syns in synonyms.items():
            if norm in [normalize_col(s) for s in syns] + [target]:
                if normalize_col(target) in recv_norm_map:
                    rename_map[orig] = recv_norm_map[normalize_col(target)]
                break
    return df.rename(columns=rename_map)

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
            final_cols = [c for c in df_mapped.columns if c in recv_cols]
            df_mapped[final_cols].to_sql('receiving_log', engine, if_exists='append', index=False)
            st.success(f"Audit Complete: {len(df_mapped)} rows synced.")
        except Exception as e:
            st.error(f"Upload Error: {e}")

elif page == "Initial Inventory Upload":
    st.title("📦 Industrial Inventory Uploader")
    f = st.file_uploader("CSV", type="csv")
    if f and st.button("🚀 START IMPORT"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE inventory"))
        for chunk in pd.read_csv(io.StringIO(f.getvalue().decode()), chunksize=50000):
            chunk.to_sql('inventory', engine, if_exists='append', index=False)
        st.success("Done")

elif page == "⚠️ NUKE":
    st.title("☢️ NUCLEAR RESET")
    st.error("⚠️ DANGER ZONE: This will permanently delete ALL inventory data. This action CANNOT be undone.")
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🗑️ Nuke Inventory Only")
        st.write("Deletes all records from the `inventory` table only. Leak history is preserved.")
        confirm1 = st.text_input("Type CONFIRM to nuke inventory", key="confirm_inv").strip()
        if st.button("🔴 NUKE INVENTORY", use_container_width=True):
            if confirm1 == "CONFIRM":
                try:
                    with engine.begin() as conn:
                        conn.execute(text("TRUNCATE TABLE inventory"))
                    st.success("✅ Inventory wiped. Ready for a fresh upload.")
                    st.balloons()
                except Exception as e:
                    st.error(f"Nuke Failed: {e}")
            else:
                st.error("❌ Type CONFIRM exactly to proceed.")
    
    with col2:
        st.subheader("💣 Nuke Everything")
        st.write("Deletes ALL records from BOTH `inventory` AND `receiving_log`. Full factory reset.")
        confirm2 = st.text_input("Type CONFIRM to nuke everything", key="confirm_all").strip()
        if st.button("💣 NUKE ALL DATA", use_container_width=True):
            if confirm2 == "CONFIRM":
                try:
                    with engine.begin() as conn:
                        conn.execute(text("TRUNCATE TABLE inventory"))
                        conn.execute(text("TRUNCATE TABLE receiving_log"))
                    st.success("✅ All data wiped. Full factory reset complete.")
                    st.balloons()
                except Exception as e:
                    st.error(f"Nuke Failed: {e}")
            else:
                st.error("❌ Type CONFIRM exactly to proceed.")
