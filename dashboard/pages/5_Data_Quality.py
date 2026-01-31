import streamlit as st
import pandas as pd
import os
from utils.db import run_query
from datetime import datetime

st.set_page_config(page_title="Data Quality & Trust", layout="wide")

PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'fleetintel360')
SCHEMA = f"{PROJECT_ID}.fleet_intel_staging"

st.title("🧪 Data Quality & Trust Panel")
st.caption("Monitoring Pipeline Latency, Table Sync, and Business Rule Validity")

# 1. SYNCED DATA FETCHING (Adapted for BigQuery)

# A. Get the Global Latest Date
max_date_res = run_query(f"SELECT MAX(activity_date) FROM `{SCHEMA}.fct_fleet_performance`")
last_data_date = max_date_res.iloc[0,0] if not max_date_res.empty else None

# B. Dynamic Thresholds (Using BigQuery or hardcoded fallbacks)
# We pull these from rpt_driver_rankings or set operational standards
TEMP_LIMIT = 100.0   # Engine Overheat threshold
FATIGUE_LIMIT = 0.7  # High Fatigue threshold

# C. Latency Calculation
latency_query = f"SELECT DATE_DIFF(CURRENT_DATE(), MAX(activity_date), DAY) as days_lag FROM `{SCHEMA}.fct_fleet_performance`"
latency_df = run_query(latency_query)
external_lag = int(latency_df["days_lag"][0]) if not latency_df.empty else 99

# D. Consolidated Leads (Unified Vehicle & Driver Anomalies)
leads_sql = f"""
    SELECT 
        'Vehicle' as category,
        CAST(vehicle_id AS STRING) as entity_id, 
        'Overheat' as issue_type,
        CAST(overheat_events AS STRING) as value
    FROM `{SCHEMA}.fct_fleet_performance` 
    WHERE overheat_events > 0
    AND activity_date = (SELECT MAX(activity_date) FROM `{SCHEMA}.fct_fleet_performance`)

    UNION ALL

    SELECT 
        'Driver' as category,
        driver_name as entity_id, 
        'High Fatigue' as issue_type,
        ROUND(avg_fatigue, 2) || '' as value
    FROM `{SCHEMA}.fct_fleet_performance` 
    WHERE avg_fatigue > {FATIGUE_LIMIT}
    AND activity_date = (SELECT MAX(activity_date) FROM `{SCHEMA}.fct_fleet_performance`)
"""
leads_df = run_query(leads_sql)
validity_count = len(leads_df)

# E. Schema Check
null_check = run_query(f"SELECT COUNT(*) FROM `{SCHEMA}.fct_fleet_performance` WHERE driver_id IS NULL").iloc[0,0]

# 2. ENHANCED HEALTH LOGIC (The v1 Logic you loved)
health_score = "Healthy"
if external_lag > 1 or validity_count > 0:
    health_score = "Degraded"
if external_lag > 3 or validity_count > 10:
    health_score = "Critical"

status_map = {"Healthy": "🟢", "Degraded": "🟠", "Critical": "🔴"}
st.markdown(f"## {status_map[health_score]} System Status: {health_score}")

# 3. KPI DISPLAY
c1, c2, c3, c4 = st.columns(4)

c1.metric(label="Pipeline Latency", value=f"{external_lag}d Lag", 
          delta="Optimal" if external_lag <= 1 else "Delayed", 
          delta_color="normal" if external_lag <= 1 else "inverse") 

c2.metric("Validation Alerts", validity_count, 
          delta="Risk Detected" if validity_count > 0 else "Clear", delta_color="inverse")

c3.metric("Integrity Check", f"{null_check} Nulls", 
          delta="Passed" if null_check == 0 else "Fail", delta_color="normal" if null_check == 0 else "inverse")

display_date = last_data_date.strftime('%Y-%m-%d') if hasattr(last_data_date, 'strftime') else str(last_data_date)
c4.metric("Last Data Sync", display_date)

st.divider()

# 4. TRENDS & AUDIT LOG
left, right = st.columns([3, 2])

with left:
    st.subheader("📊 Data Ingestion Volumetrics")
    volume_sql = f"""
        SELECT activity_date as date_label, COUNT(*) as record_count
        FROM `{SCHEMA}.fct_fleet_performance` 
        GROUP BY 1 ORDER BY 1 DESC LIMIT 14
    """
    volume_df = run_query(volume_sql).sort_values("date_label")
    st.area_chart(volume_df.set_index("date_label")["record_count"], color="#4db6ac")



with right:
    st.subheader("📝 Investigation Leads")
    st.markdown("_High-risk telemetry requiring immediate verification._")
    if not leads_df.empty:
        st.dataframe(leads_df, use_container_width=True, hide_index=True)
        csv = leads_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Export Audit Leads", csv, "dq_investigation.csv", "text/csv", use_container_width=True)
    else:
        st.success("All telemetry matches safety thresholds.")

# 5. TRUST TIP
st.divider()
if health_score == "Healthy":
    st.success("💡 **Data Trust:** The sync between BigQuery and this dashboard is verified and healthy.")
else:
    st.warning(f"💡 **Data Trust:** Found {validity_count} business rule violations. Check Driver Risk page for specifics.")