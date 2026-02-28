import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime

# ---------- CONFIG ----------
st.set_page_config(page_title="ZAPTASK A.I. | COMMAND CENTER", layout="wide")

# ---------- PERSISTENT LOGIN ----------
if "password_correct" not in st.session_state:
    st.session_state["password_correct"] = False

def check_password():
    def password_entered():
        if st.session_state["password"] == "ZAPTASK-RIDE1":
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False
    if not st.session_state["password_correct"]:
        st.title("🔐 ZAPTASK A.I. SECURE GATEWAY")
        st.text_input("Enter Admin Password:", type="password", on_change=password_entered, key="password")
        return False
    return True

if check_password():
    def get_conn():
        return psycopg2.connect(st.secrets["postgres"]["url"])

    # Sidebar with LOGO
    st.sidebar.image("https://raw.githubusercontent.com/brettsimpson1971/ride1-dashboard/main/logo.png", width=200)
    st.sidebar.title("COMMAND CENTER")
    if st.sidebar.button("🔒 LOGOUT"):
        st.session_state["password_correct"] = False
        st.rerun()
    
    if st.sidebar.button("🔄 SYNC LATEST DATA"):
        st.cache_data.clear()
        st.rerun()

    try:
        conn = get_conn()
        
        # Metrics
        inv_stats = pd.read_sql("SELECT COUNT(*) as total_skus, SUM(quantity_on_hand) as total_parts, SUM(quantity_on_hand * 25) as est_value FROM inventory", conn)
        
        # Leak Query - Looking for UNRESOLVED leaks
        leaks_df = pd.read_sql("""
            SELECT * FROM receiving_log 
            WHERE resolution_status IS NULL OR resolution_status = 'Unresolved'
            ORDER BY timestamp DESC
        """, conn)
        
        st.title("Forensic Audit & Loss Prevention")
        st.divider()

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("TOTAL PARTS", f"{int(inv_stats['total_parts'][0] or 0):,}", f"{int(inv_stats['total_skus'][0] or 0):,} SKUs")
        m2.metric("EST. VALUE", f"${(inv_stats['est_value'][0] or 0):,.2f}")
        m3.metric("ACCESSORIES", f"{int((inv_stats['total_parts'][0] or 0) * 0.4):,}")
        m4.metric("ACTIVE LEAKS", len(leaks_df), delta_color="inverse")

        st.divider()

        st.subheader("🚨 LEAK DETECTOR: SUSPICIOUS VARIANCE")
        if not leaks_df.empty:
            st.dataframe(leaks_df, use_container_width=True)
            for index, row in leaks_df.iterrows():
                with st.expander(f"CASE #{row['id']} | {row['part_number']} - {row['description']}"):
                    st.write(f"**Employee:** {row['employee_id']} | **Movement:** {row['movement_type']} | **Variance:** {row['variance_amount']}")
                    verdict = st.selectbox("Set Verdict:", ["Unresolved", "Theft", "Error", "Damaged"], key=f"v_{row['id']}")
                    if st.button("SUBMIT VERDICT", key=f"b_{row['id']}"):
                        cur = conn.cursor()
                        cur.execute("UPDATE receiving_log SET resolution_status = %s, resolved_at = NOW() WHERE id = %s", (verdict, row['id']))
                        conn.commit()
                        st.success("Verdict Logged.")
                        st.rerun()
        else:
            st.success("✅ No active leaks detected. System is balanced.")

        conn.close()
    except Exception as e:
        st.error(f"Dashboard Error: {e}")
