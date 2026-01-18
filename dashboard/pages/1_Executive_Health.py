# dashboard/pages/1_Executive_Health.py
import streamlit as st
import pandas as pd
from utils.db import run_query
from components.kpis import get_executive_kpis

st.set_page_config(page_title="Executive Daily Health", layout="wide")

# HEADER
st.title("🚛 Fleet Executive Pulse")
st.caption("Real-time operational health and weekly performance trends")

# 1. FETCH THRESHOLDS (Centralized Truth)
thresholds = run_query("SELECT metric_name, warning_threshold, critical_threshold FROM mart.alert_thresholds")

def get_thresh(metric, type='warning'):
    try:
        return thresholds.loc[thresholds['metric_name'] == metric, f'{type}_threshold'].values[0]
    except:
        return 90.0 if 'temp' in metric else 0.7 # Fallbacks

TEMP_WARN = get_thresh('engine_temp_c', 'warning')
FATIGUE_WARN = get_thresh('avg_fatigue_index', 'warning')

# 2. KPI TOP BAR
kpis = get_executive_kpis()

# Dynamic Overheat Query
overheat_sql = f"""
    SELECT COUNT(DISTINCT vehicle_id) 
    FROM mart.fact_vehicle_daily_metrics 
    WHERE avg_engine_temp_c > {TEMP_WARN} 
    AND date_key = (SELECT MAX(date_key) FROM mart.fact_vehicle_daily_metrics)
"""
overheat_val = run_query(overheat_sql).iloc[0, 0]

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Drivers Active", kpis["active_drivers"])
c2.metric("Fleet Fatigue", f"{kpis['avg_fatigue_index']:.2f}", 
          delta="Risk" if kpis['avg_fatigue_index'] > FATIGUE_WARN else "Normal", delta_color="inverse")
c3.metric("Weekly Profit", f"${kpis['net_profit']:,.0f}")
c4.metric("Overheating Assets", overheat_val, 
          delta="Critical" if overheat_val > 0 else "Clear", delta_color="inverse")
c5.metric("Fraud Events", kpis["fraud_alerts"], 
          delta="Review" if kpis["fraud_alerts"] > 0 else "OK", delta_color="inverse")

st.divider()

# 3. WEEKLY TRENDS (7-Day View)
st.subheader("🗓️ Last 7 Days Performance")
t_col1, t_col2 = st.columns(2)

with t_col1:
    st.write("**Net Profit Trend**")
    profit_df = run_query("""
        SELECT date_key, SUM(net_profit) AS profit
        FROM mart.fact_driver_daily_metrics
        WHERE date_key >= (SELECT MAX(date_key) FROM mart.fact_driver_daily_metrics) - INTERVAL 7 DAY
        GROUP BY 1 ORDER BY 1
    """)
    st.area_chart(profit_df, x="date_key", y="profit", color="#2ecc71")

with t_col2:
    st.write("**Safety & Health Incidents**")
    safety_df = run_query(f"""
        SELECT 
            date_key, 
            SUM(speeding_events) as speeding,
            COUNT(CASE WHEN avg_engine_temp_c > {TEMP_WARN} THEN 1 END) as overheating
        FROM mart.fact_vehicle_daily_metrics
        WHERE date_key >= (SELECT MAX(date_key) FROM mart.fact_vehicle_daily_metrics) - INTERVAL 7 DAY
        GROUP BY 1 ORDER BY 1
    """)
    st.line_chart(safety_df, x="date_key", y=["speeding", "overheating"])

st.divider()

# 4. OPERATIONAL ATTENTION
st.subheader("🚨 Priority Action Items")
a_col1, a_col2 = st.columns([2, 1])

with a_col1:
    st.write("**Drivers Requiring Intervention**")
    risky_drivers_sql = f"""
    SELECT
        driver_id,
        ROUND(avg_fatigue_index, 2) AS fatigue,
        speeding_events AS speeding,
        fraud_alerts_count AS fraud
    FROM mart.fact_driver_daily_metrics
    WHERE date_key = (SELECT MAX(date_key) FROM mart.fact_driver_daily_metrics)
    AND (avg_fatigue_index > {FATIGUE_WARN} OR speeding_events > 5 OR fraud_alerts_count > 0)
    ORDER BY fatigue DESC
    LIMIT 5
    """
    st.dataframe(run_query(risky_drivers_sql), use_container_width=True, hide_index=True)

with a_col2:
    st.write("**Asset Status Distribution**")
    status_df = run_query("""
    SELECT 
        CASE WHEN telemetry_events > 0 THEN 'Active' ELSE 'Idle/Ghost' END as status,
        COUNT(vehicle_id) as count
    FROM mart.fact_vehicle_daily_metrics
    WHERE date_key = (SELECT MAX(date_key) FROM mart.fact_vehicle_daily_metrics)
    GROUP BY 1
    """)
    if not status_df.empty:
        st.bar_chart(status_df.set_index("status"), color="#00d4ff")
    else:
        st.info("No asset data for today.")

st.divider()
st.info("💡 **Executive Tip:** Fatigue risk thresholds are pulled from the `alert_thresholds` master table.")