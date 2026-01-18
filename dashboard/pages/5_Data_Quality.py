import streamlit as st
import pandas as pd
from utils.db import run_query
from datetime import datetime

st.set_page_config(page_title="Data Quality & Trust", layout="wide")

st.title("🧪 Data Quality & Trust Panel")
st.caption("Monitoring Pipeline Latency, Table Sync, and Business Rule Validity")

# 1. SYNCED DATA FETCHING

# A. Get the Global Latest Date (Source of Truth)
max_date_res = run_query("SELECT MAX(date_key) FROM mart.fact_vehicle_daily_metrics")
last_data_date = max_date_res.iloc[0,0] if not max_date_res.empty else None

# B. Fetch Thresholds
thresholds = run_query("SELECT metric_name, warning_threshold, critical_threshold FROM mart.alert_thresholds")

def get_thresh(metric, type='warning'):
    try:
        return thresholds.loc[thresholds['metric_name'] == metric, f'{type}_threshold'].values[0]
    except:
        return 95.0 if 'temp' in metric else 0.65

TEMP_WARN = get_thresh('engine_temp_c', 'warning')
FATIGUE_WARN = get_thresh('avg_fatigue_index', 'warning')

# C. Latency Calculation
latency_df = run_query("SELECT DATEDIFF('day', MAX(date_key), CURRENT_DATE) as days_lag FROM mart.fact_vehicle_daily_metrics")
external_lag = int(latency_df["days_lag"][0]) if not latency_df.empty else 99

# D. Consolidated Leads
leads_df = run_query(f"""
    SELECT 
        'Vehicle' as category,
        vehicle_id as entity_id, 
        'High Temp' as issue_type,
        ROUND(avg_engine_temp_c, 1) || '°C' as value
    FROM mart.fact_vehicle_daily_metrics 
    WHERE avg_engine_temp_c > {TEMP_WARN}
    AND date_key = (SELECT MAX(date_key) FROM mart.fact_vehicle_daily_metrics)

    UNION ALL

    SELECT 
        'Driver' as category,
        driver_id as entity_id, 
        'High Fatigue' as issue_type,
        ROUND(avg_fatigue_index, 2) as value
    FROM mart.fact_driver_daily_metrics 
    WHERE avg_fatigue_index > {FATIGUE_WARN}
    AND date_key = (SELECT MAX(date_key) FROM mart.fact_driver_daily_metrics)
""")

validity_count = len(leads_df)
null_check = run_query("SELECT count(*) FROM mart.fact_vehicle_daily_metrics WHERE vehicle_id IS NULL").iloc[0,0]

# 2. ENHANCED HEALTH LOGIC
health_score = "Healthy"
if external_lag > 1 or validity_count > 0:
    health_score = "Degraded"
if external_lag > 3 or validity_count > 10:
    health_score = "Critical"

status_map = {"Healthy": "🟢", "Degraded": "🟠", "Critical": "🔴"}
st.markdown(f"## {status_map[health_score]} System Status: {health_score}")

# 3. KPI DISPLAY
c1, c2, c3, c4 = st.columns(4)

c1.metric(label="External Latency", value=f"{external_lag}d Lag", 
          delta="Optimal" if external_lag == 0 else "Behind", 
          delta_color="normal") 

c2.metric("Operational Alerts", validity_count, 
          delta="Risk Detected" if validity_count > 0 else "Clear", delta_color="inverse")

c3.metric("Schema Integrity", f"{null_check} Nulls", 
          delta="Clean" if null_check == 0 else "Error")

display_date = last_data_date.strftime('%Y-%m-%d') if hasattr(last_data_date, 'strftime') else str(last_data_date)
c4.metric("Last Data Date", display_date)

st.divider()

# 4. TRENDS & AUDIT LOG
left, right = st.columns([3, 2])

with left:
    st.subheader("📊 Ingestion Volume")
    volume_df = run_query("""
        SELECT date_key::DATE as date_label, COUNT(*) as record_count, date_key
        FROM mart.fact_vehicle_daily_metrics 
        GROUP BY 1, 3 ORDER BY 3 DESC LIMIT 14
    """).sort_values("date_key")
    st.bar_chart(volume_df.set_index("date_label")["record_count"], color="#00d4ff")

with right:
    st.subheader("📝 Data Trust Audit Log")
    if not leads_df.empty:
        st.dataframe(leads_df, use_container_width=True, hide_index=True)
        csv = leads_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Investigation Leads", csv, "leads.csv", "text/csv", use_container_width=True)
    else:
        st.success("✅ All telemetry matches safety thresholds.")

# 5. TRUST TIP
st.divider()
if health_score == "Healthy":
    st.success("💡 **Trust Tip:** Data is synced and operating within business thresholds.")
else:
    st.warning(f"💡 **Trust Tip:** Found {validity_count} violations. This matches the Executive Health pulse.")