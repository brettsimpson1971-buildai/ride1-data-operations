import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import io, re

# ---------- CONFIG ----------
st.set_page_config(page_title="Ride 1 Data Operations", layout="wide")
# Use your external database URL here (the one you put in Streamlit Secrets)
engine = create_engine(st.secrets["postgres"]["url"], pool_pre_ping=True)

# ---------- SMART UTILITIES ----------
def normalize_col(col):
    s = str(col).strip().lower()
    s = re.sub(r'[^a-z0-9_]', '_', s)
    return s.strip('_')

def smart_map(df, target_table):
    with engine.connect() as conn:
        res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{target_table}'"))
        db_cols = [r[0] for r in res.fetchall() if r[0] != 'id']
    
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
    
    # FORCE FIX: Ensure we use 'quantity' to match your DB
    if 'qty' in df.columns and 'quantity' not in df.columns:
        df = df.rename(columns={'qty': 'quantity'})

    for col in db_cols:
        if col not in df.columns:
            df[col] = 0 if col in ['quantity', 'cost', 'price', 'adj_qty', 'adj_amount'] else None
            
    num_cols = ['quantity', 'cost', 'price', 'margin', 'adj_qty', 'adj_amount', 'qty_after_adj']
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace('[$,%]', '', regex=True), errors='coerce').fillna(0)
            
    return df[db_cols]

# ---------- APP UI ----------
st.title("🚀 Ride 1 | Data Operations")
st.write("Use this tool to bulk-upload the Master Inventory (222k+ rows).")

uploaded = st.file_uploader("Upload Master CSV", type="csv")

if uploaded:
    if st.button("🚀 START MASTER IMPORT"):
        try:
            with engine.begin() as conn:
                conn.execute(text("TRUNCATE TABLE inventory"))
            
            # Streamed processing for large files
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Read file to get total rows for progress
            total_rows = sum(1 for line in uploaded) - 1
            uploaded.seek(0) # Reset file pointer
            
            rows_imported = 0
            for chunk in pd.read_csv(uploaded, chunksize=50000):
                chunk_mapped = smart_map(chunk, 'inventory')
                chunk_mapped.to_sql('inventory', engine, if_exists='append', index=False)
                rows_imported += len(chunk)
                progress = min(rows_imported / total_rows, 1.0)
                progress_bar.progress(progress)
                status_text.text(f"Imported {rows_imported:,} / {total_rows:,} rows...")
            
            st.success(f"✅ Successfully imported {rows_imported:,} rows!")
        except Exception as e:
            st.error(f"Import Error: {e}")
