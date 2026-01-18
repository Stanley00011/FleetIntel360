# dashboard/pages/3_Vehicle_Monitor.py
import streamlit as st
import pandas as pd
from utils.db import run_query
from components.filters import vehicle_filter, date_filter

# Page config
st.set_page_config(page_title="Vehicle Monitor", layout="wide")

st.title("Vehicle Monitor")
st.caption("Operational health and diagnostic risk trends")

# 1. FILTERS
col1, col2 = st.columns(2)
with col1:
    selected_vehicle = vehicle_filter()
with col2:
    start_date, end_date = date_filter()

if not selected_vehicle:
    st.info("Select a vehicle to begin analysis.")
    st.stop()

# 2. DYNAMIC THRESHOLD FETCHING
thresholds_df = run_query("""
    SELECT metric_name, warning_threshold, critical_threshold 
    FROM mart.alert_thresholds 
    WHERE metric_name IN ('engine_temp_c', 'battery_voltage')
""")

temp_warn = thresholds_df.loc[thresholds_df['metric_name'] == 'engine_temp_c', 'warning_threshold'].values[0] if not thresholds_df.empty else 95.0
temp_crit = thresholds_df.loc[thresholds_df['metric_name'] == 'engine_temp_c', 'critical_threshold'].values[0] if not thresholds_df.empty else 105.0
batt_warn = thresholds_df.loc[thresholds_df['metric_name'] == 'battery_voltage', 'warning_threshold'].values[0] if not thresholds_df.empty else 11.8

# 3. DATA FETCHING
summary_sql = f"""
SELECT
    COUNT(DISTINCT date_key)           AS active_days,
    AVG(avg_speed_kph)                 AS avg_speed,
    MAX(max_speed_kph)                 AS max_speed,
    AVG(avg_engine_temp_c)             AS avg_engine_temp,
    AVG(avg_battery_voltage)           AS avg_battery,
    SUM(speeding_events)               AS total_speeding
FROM mart.fact_vehicle_daily_metrics
WHERE vehicle_id = '{selected_vehicle}'
  AND date_key BETWEEN '{start_date}' AND '{end_date}'
"""
summary_df = run_query(summary_sql)

is_ghost = summary_df.empty or summary_df.iloc[0]["active_days"] == 0

if is_ghost:
    st.warning(f"⚠️ **{selected_vehicle}** is currently inactive or in maintenance.")
    summary = pd.Series({"active_days":0, "avg_speed":0, "max_speed":0, "avg_engine_temp":0, "avg_battery":0, "total_speeding":0})
else:
    summary = summary_df.iloc[0]

# 4. KPI TOP BAR
c1, c2, c3, c4, c5, c6 = st.columns(6)

c1.metric("Days Active", int(summary["active_days"]))
c2.metric("Avg Speed", f"{summary['avg_speed']:.1f} kph")
c3.metric("Max Speed", f"{summary['max_speed']:.1f} kph")

temp_val = summary['avg_engine_temp']
temp_status = "normal"
if temp_val >= temp_crit: temp_status = "inverse"
elif temp_val >= temp_warn: temp_status = "off"

c4.metric("Avg Engine Temp", f"{temp_val:.1f}°C", 
          delta="OVERHEAT" if temp_val > temp_warn else "Optimal", 
          delta_color=temp_status)

batt_val = summary['avg_battery']
c5.metric("Battery Voltage", f"{batt_val:.2f}V", 
          delta="LOW" if batt_val < batt_warn else "Healthy", 
          delta_color="inverse" if batt_val < batt_warn else "normal")

c6.metric("Total Speeding", int(summary["total_speeding"]))

st.divider()

# 5. DIAGNOSTIC TRENDS
left, right = st.columns(2)
with left:
    st.subheader("🌡️ Engine Temperature Trend")
    engine_df = run_query(f"""
        SELECT date_key, avg_engine_temp_c 
        FROM mart.fact_vehicle_daily_metrics 
        WHERE vehicle_id = '{selected_vehicle}' 
        AND date_key BETWEEN '{start_date}' AND '{end_date}' 
        ORDER BY date_key
    """)
    if not engine_df.empty:
        # Clean date for chart hover/X-axis
        engine_df['date_key'] = pd.to_datetime(engine_df['date_key']).dt.date
        st.area_chart(engine_df, x="date_key", y="avg_engine_temp_c", color="#ff8c00")

with right:
    st.subheader("⚡ Speeding & Stress Correlation")
    stress_df = run_query(f"""
        SELECT date_key, speeding_rate, avg_speed_kph 
        FROM mart.fact_vehicle_daily_metrics 
        WHERE vehicle_id = '{selected_vehicle}' 
        AND date_key BETWEEN '{start_date}' AND '{end_date}' 
        ORDER BY date_key
    """)
    if not stress_df.empty:
        # Clean date for chart hover/X-axis
        stress_df['date_key'] = pd.to_datetime(stress_df['date_key']).dt.date
        st.line_chart(stress_df, x="date_key", y=["speeding_rate", "avg_speed_kph"])

st.divider()

# 6. MAINTENANCE & RISK FLAGS
st.subheader("🛠️ Maintenance Recommendations")
risk_triggered = False

if summary["avg_engine_temp"] >= temp_crit:
    st.error(f"**CRITICAL OVERHEAT:** {selected_vehicle} is averaging {summary['avg_engine_temp']:.1f}°C (Threshold: {temp_crit}°C). Immediate inspection required.")
    risk_triggered = True
elif summary["avg_engine_temp"] >= temp_warn:
    st.warning(f"**Temperature Warning:** Above {temp_warn}°C. Monitor coolant levels.")
    risk_triggered = True

if summary["avg_battery"] < batt_warn:
    st.warning(f"**Electrical Warning:** Battery at {summary['avg_battery']:.2f}V (Warning Threshold: {batt_warn}V).")
    risk_triggered = True

if not risk_triggered and not is_ghost:
    st.success(f"Asset **{selected_vehicle}** is operating within all nominal parameters.")

st.divider()

# 7. DATA TABLE (FIXED DATE CLEANING)
st.subheader("📋 Detailed Operational Log")
log_df = run_query(f"""
    SELECT 
        date_key, 
        avg_speed_kph, 
        max_speed_kph, 
        avg_engine_temp_c, 
        avg_battery_voltage, 
        speeding_events 
    FROM mart.fact_vehicle_daily_metrics 
    WHERE vehicle_id = '{selected_vehicle}' 
    AND date_key BETWEEN '{start_date}' AND '{end_date}' 
    ORDER BY date_key DESC
""")

if not log_df.empty:
    log_df['date_key'] = pd.to_datetime(log_df['date_key']).dt.date
    st.dataframe(log_df, use_container_width=True, hide_index=True)

    csv_log = log_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Export Operational Log (CSV)",
        data=csv_log,
        file_name=f"logs_{selected_vehicle}_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )