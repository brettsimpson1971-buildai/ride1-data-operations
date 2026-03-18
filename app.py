#!/usr/bin/env python3
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re, datetime

# ---------- CONFIG ----------
st.set_page_config(page_title="Ride 1 Command Center", layout="wide")
engine = create_engine("postgresql://ride1admin@127.0.0.1:5432/ride1", pool_pre_ping=True)

# ---------- HELPERS ----------
def normalize_token(s: str) -> str:
    if s is None: return ""
    return re.sub(r'[^a-z0-9]', '', s.lower())

def find_best_column(actual_cols, candidates):
    """
    actual_cols: list of actual column names (exact as in DB/CSV)
    candidates: list of desired logical names (variants)
    returns the actual column name (exact) or None
    """
    norm_map = {normalize_token(c): c for c in actual_cols}
    for cand in candidates:
        nc = normalize_token(cand)
        # exact normalized match
        if nc in norm_map:
            return norm_map[nc]
    # try substring matching (loose)
    for cand in candidates:
        nc = normalize_token(cand)
        for ac_norm, ac in norm_map.items():
            if nc in ac_norm or ac_norm in nc:
                return ac
    return None

def get_table_columns(table_name):
    q = text("SELECT column_name FROM information_schema.columns WHERE table_name = :t")
    with engine.connect() as conn:
        res = conn.execute(q, {"t": table_name}).fetchall()
    return [r[0] for r in res]

def quote_ident(name):
    # If already a safe unquoted identifier, return as-is; else quote with double quotes and escape double quotes.
    if name is None:
        return None
    if re.match(r'^[a-z_][a-z0-9_]*$', name):
        return name
    return '"' + name.replace('"', '""') + '"'

def clean_numeric_series(s):
    if s is None:
        return None
    return pd.to_numeric(s.astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce')

def remove_repeated_headers(df):
    # Remove rows that are identical to the header row repeated inside the file
    if df.empty:
        return df
    header_vals = list(df.columns)
    # check each row if it matches header strings (case-insensitive)
    mask = []
    for _, row in df.iterrows():
        row_vals = [str(x).strip().lower() for x in row.fillna('').astype(str).tolist()]
        header_norm = [str(x).strip().lower() for x in header_vals]
        mask.append(row_vals != header_norm)
    return df[pd.Series(mask).values]

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
SHWAN_LOGIC = {
    'part_number': ['Part Number', 'part_number', 'partnumber', 'sku'],
    'description': ['Description', 'description', 'desc'],
    'quantity': ['Qty', 'Quantity', 'quantity', 'qty'],
    'price': ['Price', 'price', 'sale price', 'unit price'],
    'cost': ['Cost', 'cost', 'unit cost'],
    'tx_type': ['Source', 'Tx Type', 'Transaction Type', 'Type', 'source'],
    'user_name': ['User', 'Employee', 'user_name'],
    'location': ['Location', 'Store', 'location'],
    'created_at': ['Created At', 'created_at', 'created', 'timestamp', 'date']
}

def auto_map_from_columns(cols):
    mapping = {}
    for key, variants in SHWAN_LOGIC.items():
        found = find_best_column(cols, variants)
        mapping[key] = found
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
        # remove repeated header rows and rows that are empty
        df_raw = remove_repeated_headers(df_raw)
        # normalize column whitespace
        df_raw.columns = [c.strip() for c in df_raw.columns]

        # drop obviously junk rows
        pn_candidates = [c for c in df_raw.columns if normalize_token(c) in ('partnumber','partnumber','part')]
        if pn_candidates:
            pn_col = pn_candidates[0]
            df_raw = df_raw[~df_raw[pn_col].isin(['0', '1', 'nan', None, ''])]
        else:
            # fallback: drop rows with all null
            df_raw = df_raw.dropna(how='all')

        st.write("### 🛠 Verify Column Mapping")
        auto_map = auto_map_from_columns(list(df_raw.columns))
        final_map = {}
        cols = st.columns(3)
        for i, db_c in enumerate(auto_map.keys()):
            csv_list = ["-- Skip --"] + list(df_raw.columns)
            idx = csv_list.index(auto_map[db_c]) if (auto_map[db_c] in csv_list) else 0
            with cols[i % 3]:
                final_map[db_c] = st.selectbox(f"DB: {db_c}", csv_list, index=idx)

        if st.button("🚀 START MASTER IMPORT"):
            try:
                rename_dict = {v: k for k, v in final_map.items() if v != "-- Skip --"}
                df_import = df_raw[list(rename_dict.keys())].rename(columns=rename_dict)

                # normalize target columns names (lowercase, underscores)
                df_import.columns = [c.strip().lower().replace(' ', '_') for c in df_import.columns]

                # Best-effort numeric cleaning
                for c in ['quantity', 'cost', 'price', 'adj_qty', 'adj_amount', 'qty_after_adj']:
                    if c in df_import.columns:
                        df_import[c] = clean_numeric_series(df_import[c]).fillna(0)

                # Reduce duplicates keeping last
                if 'part_number' in df_import.columns:
                    df_import = df_import.drop_duplicates(subset=['part_number'], keep='last')
                # write to db (truncate then append)
                with engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE inventory"))
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
        df_daily = remove_repeated_headers(df_daily)
        df_daily.columns = [c.strip() for c in df_daily.columns]

        auto_map = auto_map_from_columns(list(df_daily.columns))
        st.write("### 🛠 Verify Daily Mapping")
        final_map = {}
        cols = st.columns(3)
        for i, db_c in enumerate(auto_map.keys()):
            csv_list = ["-- Skip --"] + list(df_daily.columns)
            idx = csv_list.index(auto_map[db_c]) if (auto_map[db_c] in csv_list) else 0
            with cols[i % 3]:
                final_map[db_c] = st.selectbox(f"Daily: {db_c}", csv_list, index=idx)

        if st.button("✅ CONFIRM DAILY IMPORT"):
            try:
                rename_dict = {v: k for k, v in final_map.items() if v != "-- Skip --"}
                df_import = df_daily[list(rename_dict.keys())].rename(columns=rename_dict)

                # normalize column names to stable names
                df_import.columns = [c.strip().lower().replace(' ', '_') for c in df_import.columns]

                # clean numeric-ish fields
                for c in ['quantity', 'cost', 'price', 'adj_qty', 'adj_amount']:
                    if c in df_import.columns:
                        df_import[c] = clean_numeric_series(df_import[c]).fillna(0)

                # add created_at if missing
                if 'created_at' not in df_import.columns:
                    df_import['created_at'] = datetime.datetime.utcnow()

                # append to daily_log (keep history)
                df_import.to_sql('daily_log', engine, if_exists='append', index=False)
                st.success(f"✅ {len(df_import)} transactions added to history!")
            except Exception as e:
                st.error(f"Error: {e}")

elif page == "Leak Detector":
    st.title("🔍 Forensic Leak Detector")

    # Discover actual column names in daily_log and inventory
    inv_cols = get_table_columns('inventory')
    daily_cols = get_table_columns('daily_log')

    # Map to actual column names (if the table already has normalized columns we'll use those)
    mapping_inv = auto_map_from_columns(inv_cols)
    mapping_daily = auto_map_from_columns(daily_cols)

    # define actual column references (quoted if needed)
    def actual(col_key, mapping, fallback=None):
        col = mapping.get(col_key) or fallback
        return quote_ident(col) if col else None

    d_part = actual('part_number', mapping_daily)
    d_desc = actual('description', mapping_daily)
    d_tx = actual('tx_type', mapping_daily)
    d_cost = actual('cost', mapping_daily)
    d_price = actual('price', mapping_daily)
    d_qty = actual('quantity', mapping_daily)
    d_user = actual('user_name', mapping_daily)
    d_created = actual('created_at', mapping_daily) or 'd.created_at'

    i_part = actual('part_number', mapping_inv)
    i_cost = actual('cost', mapping_inv) or 'i.cost'
    i_price = actual('price', mapping_inv) or 'i.price'

    # If we couldn't find part_number in daily_log, show a clear message and stop
    if d_part is None:
        st.error("Leak Detector: could not find a Part Number column in daily_log. Run a master import first or inspect your database columns.")
        cols = get_table_columns('daily_log')
        st.write("daily_log columns found:", cols)
        st.stop()

    # Build SQL using the resolved identifiers
    # Use REGEXP_REPLACE to strip $ and commas for numeric casts where necessary
    q = f"""
    SELECT
      d.{d_part} as part_number,
      {('d.' + d_desc) if d_desc else 'NULL as description'},
      {('d.' + d_tx) if d_tx else "'receive' as tx_type"},
      {('d.' + d_cost) if d_cost else 'NULL as daily_cost'},
      i.{i_cost if i_cost and i_cost != 'i.cost' else 'cost'} as master_cost,
      {('d.' + d_price) if d_price else 'NULL as daily_price'},
      i.{i_price if i_price and i_price != 'i.price' else 'price'} as master_price,
      {('d.' + d_qty) if d_qty else 'NULL as daily_qty'},
      {('d.' + d_user) if d_user else 'NULL as user_name'},
      {('d.' + d_created) if d_created else 'd.created_at as created_at'}
    FROM daily_log d
    JOIN inventory i ON d.{d_part} = i.{quote_ident('part_number')}
    LEFT JOIN leak_cases c ON d.{d_part} = c.{quote_ident('part_number')} AND c.status = 'resolved'
    WHERE c.{quote_ident('part_number')} IS NULL
      AND (
        ({('d.' + d_tx) if d_tx else "'receive'"} = 'receive'
          AND ABS(CAST(REGEXP_REPLACE(COALESCE({('d.' + d_cost) if d_cost else "''"},''), '[$,]', '', 'g') AS NUMERIC) - i.{quote_ident('cost')}) > 0.01)
        OR ({('d.' + d_tx) if d_tx else "'sale'"} = 'sale'
          AND CAST(REGEXP_REPLACE(COALESCE({('d.' + d_price) if d_price else "''"},''), '[$,]', '', 'g') AS NUMERIC) < i.{quote_ident('price')})
        OR ({('d.' + d_tx) if d_tx else "'adjust'"} = 'adjust'
          AND CAST(REGEXP_REPLACE(COALESCE({('d.' + d_qty) if d_qty else "0"},''), '[$,]', '', 'g') AS NUMERIC) < 0)
      )
    ORDER BY {('d.' + d_created) if d_created else 'd.created_at'} DESC
    """

    try:
        leaks = pd.read_sql(text(q), engine)
    except Exception as e:
        st.error(f"SQL Error: {e}")
        st.code(q)
        st.stop()

    if not leaks.empty:
        def color_leaks(row):
            try:
                if row.get('tx_type') == 'adjust': return ['background-color: #ff4b4b; color: white'] * len(row)
                if row.get('tx_type') == 'sale' and float(row.get('daily_price') or 0) < float(row.get('master_price') or 0):
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
            qty = float(row.get('daily_qty') or 1)
            recovery = 0.0
            try:
                recovery = abs(float(row.get('daily_price') or 0) - float(row.get('master_price') or 0)) * abs(qty)
            except:
                recovery = 0.0

            with col_a:
                st.write(f"### 🕵️‍♂️ Case File: {selected_part}")
                # Pull history using paramized query to handle whichever column name exists in DB
                # We'll pass the raw selected_part as parameter
                hist_query = f"SELECT * FROM daily_log WHERE {d_part} = :p ORDER BY {d_created} DESC"
                hist = pd.read_sql(text(hist_query), engine, params={"p": selected_part})
                st.dataframe(hist, use_container_width=True)

            with col_b:
                st.write("### 🛠 Actions")
                st.metric("Potential Recovery", f"${recovery:,.2f}")
                if st.button(f"✅ Resolve & Bank Savings", use_container_width=True):
                    with engine.begin() as conn:
                        conn.execute(text("INSERT INTO leak_cases (part_number, status, resolved_at, recovered_amount) VALUES (:p, 'resolved', NOW(), :s)"), {"p": selected_part, "s": recovery})
                    st.success("Case resolved!")
                    st.rerun()
    else:
        st.success("✅ No active leaks detected.")

elif page == "⚠️ NUKE":
    st.title("☢️ RESET")
    if st.button("WIPE ALL DATA"):
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE inventory"))
            conn.execute(text("TRUNCATE TABLE daily_log"))
            conn.execute(text("TRUNCATE TABLE leak_cases"))
        st.success("System Reset.")
