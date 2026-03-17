import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re

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

# ---------- UTILITIES ----------
def normalize_col(col):
    s = str(col).strip().lower()
    s = re.sub(r'[%#@\\\/&]', '_', s)
    s = re.sub(r'[\s\-\u2013]+', '_', s)
    s = re.sub(r'[^a-z0-9_]', '', s)
    return s.strip('_')

# ---------- SIDEBAR ----------
with st.sidebar:
    st.image("https://assets.zyrosite.com/46uOcFMIrXbOQmGo/logo.png-354IavGq7CUmvHlz.png", width=200)
    page = st.radio("Navigation", [
        "Command Center",
        "Master Inventory Upload",
        "Daily Upload",
        "Leak Detector",
        "⚠️ NUKE"
    ])
    if st.button("Logout"):
        st.session_state["authenticated"] = False
        st.rerun()

# ---------- PAGES ----------
if page == "Command Center":
    st.title("🚨 RIDE 1 | FORENSIC COMMAND CENTER")
    sku = pd.read_sql("SELECT COUNT(*) FROM inventory", engine).iloc[0,0]
    st.metric("Total SKUs in Database", f"{int(sku):,}")
    st.info("System ready. Use sidebar to upload inventory or daily logs.")

elif page == "Master Inventory Upload":
    st.title("📥 Master Inventory Upload")
    uploaded = st.file_uploader("Upload Master CSV", type="csv")
    if uploaded:
        df_raw = pd.read_csv(io.StringIO(uploaded.getvalue().decode('utf-8')), dtype=str)
        with engine.connect() as conn:
            res = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='inventory'"))
            db_cols = [r[0] for r in res.fetchall() if r[0] not in ['id']]
        
        # Mapping logic (simplified for brevity, same as before)
        st.write("### 🛠 Verify Column Mapping")
        final_map = {}
        cols = st.columns(3)
        for i, db_c in enumerate(db_cols):
            with cols[i % 3]:
                final_map[db_c] = st.selectbox(f"DB: {db_c}", ["-- Skip --"] + list(df_raw.columns))

        if st.button("🚀 START IMPORT"):
            try:
                rename_dict = {v: k for k, v in final_map.items() if v != "-- Skip --"}
                df_import = df_raw[list(rename_dict.keys())].rename(columns=rename_dict)
                df_import = df_import.drop_duplicates(subset=['part_number'], keep='last')
                
                # Clean numbers
                num_cols = ['quantity', 'cost', 'price', 'margin', 'adj_qty', 'adj_amount', 'qty_after_adj']
                for c in num_cols:
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
    st.title("📋 Daily Upload")
    daily_file = st.file_uploader("Upload Daily CSV", type="csv")
    if daily_file:
        df_daily = pd.read_csv(io.StringIO(daily_file.getvalue().decode('utf-8')), dtype=str)
        if st.button("✅ CONFIRM DAILY IMPORT"):
            try:
                # We DON'T truncate here anymore, we APPEND so we have history
                df_daily.to_sql('daily_log', engine, if_exists='append', index=False)
                st.success("✅ Daily log added to history!")
            except Exception as e:
                st.error(f"Error: {e}")

elif page == "Leak Detector":
    st.title("🔍 Forensic Leak Detector")
    
    # 1. Show High-Level Discrepancies
    query = """
    SELECT 
        d."Part Number" as part_number,
        COUNT(*) as instances,
        MAX(d.created_at) as last_seen
    FROM daily_log d
    JOIN inventory i ON d."Part Number" = i.part_number
    GROUP BY d."Part Number"
    ORDER BY last_seen DESC
    """
    leaks = pd.read_sql(query, engine)
    
    if not leaks.empty:
        st.subheader("Suspect Parts Found")
        selected_part = st.selectbox("Select a Part Number to Drill Down:", leaks['part_number'])
        
        if selected_part:
            st.write(f"### 🕵️‍♂️ History for {selected_part}")
            history_query = text("SELECT * FROM daily_log WHERE \"Part Number\" = :p ORDER BY created_at DESC")
            history_df = pd.read_sql(history_query, engine, params={"p": selected_part})
            st.table(history_df)
    else:
        st.info("No discrepancies found in history.")

elif page == "⚠️ NUKE":
    st.title("☢️ RESET")
    if st.button("WIPE ALL DATA"):
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE inventory"))
            conn.execute(text("TRUNCATE TABLE daily_log"))
            conn.commit()
        st.success("System Reset.")
