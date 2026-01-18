# dashboard/pages/2_Driver_Risk.py
import streamlit as st
import pandas as pd
from utils.db import run_query
from components.filters import driver_filter, date_filter

st.set_page_config(page_title="Driver Risk Monitor", layout="wide")

st.title("Driver Risk Monitor")
st.caption("Deep-dive into safety compliance and fatigue patterns")

# FILTERS
col1, col2 = st.columns(2)
with col1:
    selected_driver = driver_filter()
with col2:
    start_date, end_date = date_filter()

if not selected_driver:
    st.info("Select a driver to begin analysis.")
    st.stop()

# DATA FETCHING
summary_sql = f"""
SELECT
    COUNT(DISTINCT date_key)           AS active_days,
    AVG(avg_fatigue_index)             AS avg_fatigue,
    SUM(speeding_events)               AS total_speeding,
    MAX(max_continuous_hours)          AS max_hours,
    SUM(fraud_alerts_count)            AS fraud_alerts
FROM mart.fact_driver_daily_metrics
WHERE driver_id = '{selected_driver}'
  AND date_key BETWEEN '{start_date}' AND '{end_date}'
"""
summary = run_query(summary_sql).iloc[0]

# KPI TOP BAR
c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Days Active", int(summary["active_days"]))
c2.metric("Avg Fatigue", f"{summary['avg_fatigue']:.2f}", 
          delta="Risk" if summary['avg_fatigue'] > 0.65 else "Safe", 
          delta_color="inverse")
c3.metric("Total Speeding", int(summary["total_speeding"]))
c4.metric("Longest Shift", f"{summary['max_hours']:.1f}h", 
          delta="Violation" if summary['max_hours'] > 8 else "Compliant", 
          delta_color="inverse")
c5.metric("Fraud Alerts", int(summary["fraud_alerts"]))

st.divider()

# TREND ANALYSIS
left, right = st.columns(2)

with left:
    st.subheader("🧠 Fatigue & Continuous Driving")
    trend_sql = f"""
    SELECT date_key::DATE as date, avg_fatigue_index, max_continuous_hours
    FROM mart.fact_driver_daily_metrics
    WHERE driver_id = '{selected_driver}' AND date_key BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date_key
    """
    trend_df = run_query(trend_sql)
    st.line_chart(trend_df, x="date", y=["avg_fatigue_index", "max_continuous_hours"])

with right:
    st.subheader("🚦 Speeding Profile")
    speed_sql = f"""
    SELECT date_key::DATE as date, speeding_events
    FROM mart.fact_driver_daily_metrics
    WHERE driver_id = '{selected_driver}' AND date_key BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date_key
    """
    st.bar_chart(run_query(speed_sql), x="date", y="speeding_events", color="#ff4b4b")

st.divider()

# POLICY VIOLATION LOG 
st.subheader("📋 Compliance Audit Log")

audit_sql = f"""
SELECT 
    date_key,
    ROUND(avg_fatigue_index, 2) as fatigue,
    max_continuous_hours as shift_hrs,
    speeding_events as speeding,
    fraud_alerts_count as fraud
FROM mart.fact_driver_daily_metrics
WHERE driver_id = '{selected_driver}'
  AND date_key BETWEEN '{start_date}' AND '{end_date}'
  AND (avg_fatigue_index > 0.7 OR max_continuous_hours > 8 OR speeding_events > 10 OR fraud_alerts_count > 0)
ORDER BY date_key DESC
"""
audit_df = run_query(audit_sql)

if audit_df.empty:
    st.success(f"Driver {selected_driver} is fully compliant for this period.")
else:
    audit_df['date_key'] = pd.to_datetime(audit_df['date_key']).dt.date
    
    st.warning(f"Detected {len(audit_df)} days with policy violations.")
    st.dataframe(
        audit_df.style.highlight_max(axis=0, color='rgba(255, 0, 0, 0.2)'), 
        use_container_width=True,
        hide_index=True
    )

# MAINTENANCE BOX
with st.expander("ℹ️ Driver Safety Guidelines"):
    st.write("""
    - **Fatigue:** Any index above 0.70 requires mandatory 1-hour rest.
    - **Shifts:** Maximum continuous driving hours capped at 8.0h.
    - **Speeding:** More than 10 events per day triggers a formal review.
    """)