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

# small heuristic to pick preferred CSV column among duplicates for a given DB target
def choose_preferred(csv_cols, db_col):
    # prefer columns containing 'after', 'adj', 'adjust', or exact db_col match
    prefs = ['after', 'adj', 'adjust', 'final', 'qty_after', 'after_adj']
    norm_db = normalize_col(db_col)
    # exact normalized match
    for c in csv_cols:
        if normalize_col(c) == norm_db:
            return c
    # prefer ones containing preference keywords
    for p in prefs:
        for c in csv_cols:
            if p in normalize_col(c):
                return c
    # fallback to first
    return csv_cols[0]

def map_columns(df, target_cols):
    """
    Old-style automatic mapping fallback (keeps first mapping for each target).
    This is still used if user chooses not to use the mapping UI.
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

# Combined, robust mapping/upload page for both Inventory and Receiving Log
elif page in ["Initial Inventory Upload", "Daily DMS Sync"]:
    st.title(f"📥 {page}")
    target_table = 'inventory' if page == "Initial Inventory Upload" else 'receiving_log'
    
    f = st.file_uploader(f"Upload {target_table.replace('_',' ').title()} CSV", type="csv")
    
    if f:
        try:
            df_raw = pd.read_csv(io.StringIO(f.getvalue().decode('utf-8')), dtype=str)
        except Exception as e:
            st.error(f"Could not read CSV: {e}")
            st.stop()

        st.write("### 🛠 Step 1: Map your Columns")
        st.info("We've pre-selected smart matches. Confirm or change them. Avoid mapping multiple DB fields to the same CSV column.")

        # Get DB Columns (exclude some internal/reserved columns)
        with engine.connect() as conn:
            res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table}'"))
            db_cols = [r[0] for r in res.fetchall() if r[0] not in ['id', 'resolved_at', 'resolution_status', 'resolution_note']]

        # Mapping UI: present DB columns with selectbox of CSV columns (or -- Skip --)
        mapping = {}
        csv_cols = ["-- Skip --"] + list(df_raw.columns)

        # layout columns (2 per column group)
        group_count = max(1, (len(db_cols) // 2) + 1)
        cols = st.columns(group_count)
        for i, db_c in enumerate(db_cols):
            # Try to determine a smart default index
            default_idx = 0
            norm_db = normalize_col(db_c)
            for j, csv_c in enumerate(csv_cols):
                if csv_c == "-- Skip --":
                    continue
                if normalize_col(csv_c) == norm_db:
                    default_idx = j
                    break
                # heuristics: part, qty, price
                if any(k in normalize_col(csv_c) for k in ['part', 'qty', 'price']) and any(k in norm_db for k in ['part', 'qty', 'price']):
                    default_idx = j
                    break

            with cols[i % group_count]:
                mapping[db_c] = st.selectbox(f"DB: {db_c}", csv_cols, index=default_idx, key=f"map_{db_c}")

        # Action: start import (with duplicate detection & auto-resolve)
        if st.button("🚀 START IMPORT", use_container_width=True):
            try:
                # mapping: db_col -> selected csv col (or "-- Skip --")
                # Build final_mapping: csv_col -> db_col for non-skip selections
                final_mapping = {v: k for k, v in mapping.items() if v != "-- Skip --"}

                # Detect duplicates where multiple CSV columns map to same DB column.
                # Actually 'final_mapping' keys are CSV names and values are DB names; we need db->list(csv)
                duplicates = {}
                for csv_col, db_col in final_mapping.items():
                    duplicates.setdefault(db_col, []).append(csv_col)
                dupes = {db: srcs for db, srcs in duplicates.items() if len(srcs) > 1}

                if dupes:
                    # Show clear message listing conflicts
                    err_lines = []
                    for db_col, srcs in dupes.items():
                        err_lines.append(f"{db_col}: {', '.join(srcs)}")
                    st.error("Mapping error — multiple CSV columns mapped to the same DB field:")
                    for line in err_lines:
                        st.write(f"- {line}")
                    st.info("Option A: Fix the mapping above (select a single CSV column per DB field).")
                    st.info("Option B: Auto-resolve duplicates (keep preferred column per DB field).")

                    if st.button("Auto-resolve duplicates (preferred heuristics)"):
                        # Resolve for each duplicated db_col, choose a preferred csv column
                        resolved_db_to_csv = {}
                        for db_col, srcs in duplicates.items():
                            preferred = choose_preferred(srcs, db_col)
                            resolved_db_to_csv[db_col] = preferred
                        # For non-duplicated mappings, copy as-is
                        for db_col, srcs in duplicates.items():
                            pass
                        for csv_col, db_col in final_mapping.items():
                            if db_col not in resolved_db_to_csv:
                                resolved_db_to_csv[db_col] = csv_col
                        # Build final_mapping csv->db from resolved_db_to_csv
                        final_mapping = {csv: db for db, csv in resolved_db_to_csv.items()}

                    else:
                        st.stop()

                # After resolution, final_mapping keys = CSV columns, values = DB columns
                csv_keys = list(final_mapping.keys())
                if not csv_keys:
                    st.error("No CSV columns selected for import. Please map at least one column.")
                    st.stop()

                # Build DataFrame and rename columns to DB names
                df_final = df_raw[csv_keys].rename(columns=final_mapping)

                # Safety: ensure unique column names in df_final
                if df_final.columns.duplicated().any():
                    st.error("Internal error: duplicate column names still present after mapping. Please re-check the mapping.")
                    st.stop()

                # Optionally: quick sanity checks (e.g., ensure part_number exists for inventory)
                if target_table == 'inventory' and 'part_number' not in df_final.columns:
                    st.error("Inventory import requires a 'part_number' mapping. Please map the Part Number column.")
                    st.stop()

                # Write to DB
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
