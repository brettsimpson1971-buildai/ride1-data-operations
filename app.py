import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ---------- DB CONNECTION ----------

def get_conn():
    # Uses Streamlit secrets:
    # [postgres]
    # url = "postgresql://postgres:1%23Biggainz@database-2.ckfqmyysk121.us-east-1.rds.amazonaws.com:5432/postgres"
    return psycopg2.connect(st.secrets["postgres"]["url"])


st.set_page_config(page_title="RIDE 1 DATA OPERATIONS", layout="wide")

# ---------- SIDEBAR & NAV ----------

# Logo – update this URL if your logo path
