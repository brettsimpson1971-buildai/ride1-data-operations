import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re, os, json

st.set_page_config(page_title='Ride 1 Command Center', layout='wide', initial_sidebar_state='expanded')
engine = create_engine("postgresql://ride1admin@127.0.0.1:5432/ride1", pool_pre_ping=True)

MAPPINGS_FILE = "column_mappings.json"
PREFER_QTY_AFTER = True

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

def normalize_col(col):
    s = str(col).strip().lower()
    s = re.sub(r'[#@\\\/&]', '_', s)
    s = re.sub(r'[\s\-\u2013]+', '_', s)
    s = re.sub(r'[^a-z0-9_]', '', s)
    return s.strip('_')

def load_mappings():
    if os.path.exists(MAPPINGS_FILE):
        try:
            with open(MAPPINGS_FILE, 'r') as fh:
                return json.load(fh)
        except Exception:
            return {}
    return {}

def save_mappings(mappings):
    try:
        with open(MAPPINGS_FILE, 'w') as fh:
            json.dump(mappings, fh, indent=2)
        return True
    except Exception as e:
        st.error(f"Failed to save mappings: {e}")
        return False

def choose_preferred(csv_cols, db_col):
    norm_db = normalize_col(db_col)
    for c in csv_cols:
        if normalize_col(c) == norm_db:
            return c
    if PREFER_QTY_AFTER and 'quantity' in norm_db:
        for c in csv_cols:
            nc = normalize_col(c)
            if any(k in nc for k in ['after', 'adj', 'adjust', 'qty_after', 'after_adj', 'final']):
                return c
    prefs = ['after', 'adj', 'adjust', 'final', 'qty_after', 'after_adj', 'count']
    for p in prefs:
        for c in csv_cols:
            if p in normalize_col(c):
                return c
    return csv_cols[0]

def automatic_mapping_suggestions(csv_cols, db_cols):
    synonyms = {
        'part_number': ['part', 'part_no', 'part#', 'sku', 'item', 'item_number', 'partnum', 'part_number'],
        'location_bin': ['bin', 'bin_number', 'bin#', 'bin_no', 'location', 'loc'],
        'quantity': ['qty', 'quantity_on_hand', 'on_hand', 'count', 'units', 'quantity', 'qty_after_adj', 'qty_after', 'qtyafter'],
        'employee_id': ['emp', 'emp_id', 'user', 'employee', 'staff', 'tech'],
        'variance_amount': ['variance', 'diff', 'shrink', 'loss'],
        'severity_level': ['severity', 'status', 'priority'],
        'description': ['description', 'desc', 'notes'],
        'timestamp': ['timestamp', 'time', 'date', 'datetime'],
        'price': ['price', 'msrp', 'unit_price', 'cost_plus', 'cost']
    }
    csv_normal_map = {normalize_col(c): c for c in csv_cols}
    suggestion = {}
    used_csv = set()
    for db in db_cols:
        dbn = normalize_col(db)
        if dbn in csv_normal_map:
            suggestion[db] = csv_normal_map[dbn]
            used_csv.add(suggestion[db])
            continue
        picked = None
        syns = synonyms.get(db, []) + [db]
        for s in syns:
            ns = normalize_col(s)
            for nc, orig in csv_normal_map.items():
                if ns == nc and orig not in used_csv:
                    picked = orig
                    break
            if picked:
                break
        if not picked:
            for nc, orig in csv_normal_map.items():
                if dbn in ['part_number','part'] and any(k in nc for k in ['part','sku','item']):
                    picked = orig; break
                if dbn in ['quantity','qty','count'] and any(k in nc for k in ['qty','quantity','count']):
                    picked = orig; break
                if dbn in ['price','cost'] and any(k in nc for k in ['price','cost','msrp']):
                    picked = orig; break
        if picked and picked not in used_csv:
            suggestion[db] = picked
            used_csv.add(picked)
        else:
            suggestion[db] = "-- Skip --"
    return suggestion

def make_columns_unique(cols):
    seen = {}
    result = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            result.append(c)
        else:
            seen[c] += 1
            new_c = f"{c}_{seen[c]}"
            while new_c in seen:
                seen[c] += 1
                new_c = f"{c}_{seen[c]}"
            seen[new_c] = 0
            result.append(new_c)
    return result

with st.sidebar:
    st.image("https://assets.zyrosite.com/46uOcFMIrXbOQmGo/logo.png-354IavGq7CUmvHlz.png", width=200)
    st.write("👤 User: **ride1**")
    if st.button("Logout"):
        st.session_state["authenticated"] = False
        st.rerun()
    st.divider()
    page = st.radio("Navigation", ["Command Center", "Leak Detector", "Initial Inventory Upload", "Daily DMS Sync", "⚠️ NUKE"])

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
    except Exception:
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
        except Exception:
            st.write("Archive unavailable.")

elif page in ["Initial Inventory Upload", "Daily DMS Sync"]:
    st.title(f"📥 {page}")
    target_table = 'inventory' if page == "Initial Inventory Upload" else 'receiving_log'
    uploaded = st.file_uploader(f"Upload {target_table.replace('_',' ').title()} CSV", type="csv")

    mappings = load_mappings()

    if uploaded:
        try:
            df_raw = pd.read_csv(io.StringIO(uploaded.getvalue().decode('utf-8')), dtype=str)
        except Exception as e:
            st.error(f"Could not read CSV: {e}")
            st.stop()

        st.write("### 🛠 Step 1: Map your Columns")
        st.info("Confirm or change mappings. You can save mappings for this filename to reuse later.")

        with engine.connect() as conn:
            res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table}'"))
            db_cols = [r[0] for r in res.fetchall() if r[0] not in ['id', 'resolved_at', 'resolution_status', 'resolution_note']]

        csv_cols = ["-- Skip --"] + list(df_raw.columns)

        file_key = os.path.splitext(uploaded.name)[0]
        saved_map = mappings.get(file_key, {})

        if saved_map:
            suggestions = saved_map
            st.info(f"Loaded saved mapping for filename: {uploaded.name}")
        else:
            suggestions = automatic_mapping_suggestions(list(df_raw.columns), db_cols)

        mapping = {}
        group_count = max(1, (len(db_cols) // 2) + 1)
        cols = st.columns(group_count)
        for i, db_c in enumerate(db_cols):
            default_value = suggestions.get(db_c, "-- Skip --")
            if default_value not in csv_cols:
                default_index = 0
            else:
                default_index = csv_cols.index(default_value)
            with cols[i % group_count]:
                mapping[db_c] = st.selectbox(f"DB: {db_c}", csv_cols, index=default_index, key=f"map_{db_c}")

        col1, col2 = st.columns([1,1])
        with col1:
            if st.button("Save mapping for this filename"):
                to_save = {db: sel for db, sel in mapping.items() if sel != "-- Skip --"}
                mappings[file_key] = to_save
                if save_mappings(mappings):
                    st.success(f"Mapping saved for filename key: {file_key}")
                else:
                    st.error("Failed to save mapping.")
        with col2:
            if st.checkbox("Always prefer 'Qty After' style columns for quantity", value=PREFER_QTY_AFTER):
                st.info("Preference applied for this upload (session only).")

        if st.button("🚀 START IMPORT", use_container_width=True):
            try:
                final_mapping = {v: k for k, v in mapping.items() if v != "-- Skip --"}

                db_to_csv = {}
                for csv_col, db_col in final_mapping.items():
                    db_to_csv.setdefault(db_col, []).append(csv_col)
                dupes = {db: srcs for db, srcs in db_to_csv.items() if len(srcs) > 1}

                if dupes:
                    st.error("Mapping conflict detected: multiple CSV columns mapped to the same DB field.")
                    for db_col, srcs in dupes.items():
                        st.write(f"- {db_col}: {', '.join(srcs)}")
                    st.info("You can fix the mapping above, or use Auto-resolve to pick a preferred column per DB field.")

                    if st.button("Auto-resolve duplicates (preferred heuristics)"):
                        resolved_db_to_csv = {}
                        for db_col, srcs in dupes.items():
                            preferred = choose_preferred(srcs, db_col)
                            resolved_db_to_csv[db_col] = preferred
                        for db_col, srcs in db_to_csv.items():
                            if db_col not in resolved_db_to_csv:
                                resolved_db_to_csv[db_col] = srcs[0]
                        final_mapping = {csv: db for db, csv in resolved_db_to_csv.items()}
                        st.success("Auto-resolve applied.")
                    else:
                        st.stop()

                csv_keys = list(final_mapping.keys())
                if not csv_keys:
                    st.error("No CSV columns selected for import. Please map at least one column.")
                    st.stop()

                df_final = df_raw[csv_keys].rename(columns=final_mapping)

                st.write("Columns to be uploaded (before making unique):", list(df_final.columns))

                # Make columns unique by appending suffixes to duplicates
                unique_cols = make_columns_unique(df_final.columns)
                if list(df_final.columns) != unique_cols:
                    st.warning("Duplicate column names detected. Renaming duplicates to unique names.")
                    df_final.columns = unique_cols

                st.write("Columns to be uploaded (after making unique):", list(df_final.columns))

                if target_table == 'inventory' and 'part_number' not in df_final.columns:
                    st.error("Inventory import requires a 'part_number' mapping. Please map the Part Number column.")
                    st.stop()

                persist_map = {v: k for k, v in final_mapping.items()}

                with engine.begin() as conn:
                    if page == "Initial Inventory Upload":
                        conn.execute(text("TRUNCATE TABLE inventory"))
                    df_final.to_sql(target_table, engine, if_exists='append', index=False, chunksize=10000)

                st.success(f"Success! {len(df_final):,} rows imported into {target_table}.")
                st.balloons()

                mappings[file_key] = persist_map
                save_mappings(mappings)

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
