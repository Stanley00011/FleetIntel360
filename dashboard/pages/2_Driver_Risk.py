import streamlit as st
import pandas as pd
import os
import altair as alt
from utils.db import run_query

st.set_page_config(page_title="Driver Risk Mission Control", layout="wide")
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'fleetintel360')
SCHEMA = f"{PROJECT_ID}.fleet_intel_staging"

st.title("🚦 Driver Risk Mission Control")
st.caption("Detailed behavioral audit: Identifying the 'Why' behind performance scores.")

# 1. SMART SELECTOR & DRILL-DOWN
driver_list = run_query(f"SELECT driver_id, driver_name FROM `{SCHEMA}.dim_drivers` ORDER BY driver_name")
selected_id = st.selectbox("Select Driver to Audit", driver_list['driver_id'], 
                           format_func=lambda x: driver_list[driver_list['driver_id']==x]['driver_name'].iloc[0])

# 2. EXECUTIVE KPIs FOR INDIVIDUAL
audit_data = run_query(f"SELECT * FROM `{SCHEMA}.rpt_driver_rankings` WHERE driver_id = '{selected_id}'").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("MTD Profit", f"${audit_data['total_profit']:,.2f}")
c2.metric("Fatigue Index", f"{audit_data['avg_fatigue_score']:.2f}")
c3.metric("Speeding Events", audit_data['total_speeding_incidents'])
# Color-coded performance metric
p_color = "inverse" if audit_data['performance_score'] < 0 else "normal"
c4.metric("Performance Score", f"{audit_data['performance_score']:.2f}", delta="Needs Review" if audit_data['performance_score'] < 0 else "Optimal", delta_color=p_color)

st.divider()

# 3. PERFORMANCE DECOMPOSITION (VISUAL MATH)
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("📊 Score Decomposition")
    st.markdown("_How behavioral penalties impact the bottom line_")
    # Breakdown logic: (Profit/100) vs (Fatigue*50) vs (Speeding*10)
    math_data = pd.DataFrame({
        'Component': ['Profit Contribution (+)', 'Fatigue Penalty (-)', 'Speeding Penalty (-)'],
        'Impact': [audit_data['total_profit']/100, -(audit_data['avg_fatigue_score']*50), -(audit_data['total_speeding_incidents']*10)]
    })
    
    decomp_chart = alt.Chart(math_data).mark_bar().encode(
        x=alt.X('Impact:Q', title="Score Contribution"),
        y=alt.Y('Component:N', sort='-x'),
        color=alt.condition(alt.datum.Impact > 0, alt.value("#2ecc71"), alt.value("#e74c3c")),
        tooltip=['Component', 'Impact']
    ).properties(height=300)
    st.altair_chart(decomp_chart, use_container_width=True)



with col_b:
    st.subheader("📅 14-Day Safety Timeline")
    st.markdown("_Correlation between fatigue and speeding events_")
    trend = run_query(f"""
        SELECT activity_date, avg_fatigue, speeding_events 
        FROM `{SCHEMA}.fct_fleet_performance` 
        WHERE driver_id = '{selected_id}' 
        ORDER BY activity_date ASC LIMIT 14
    """)
    
    base = alt.Chart(trend).encode(x='activity_date:T')
    line = base.mark_line(color='#e74c3c', strokeWidth=3).encode(y=alt.Y('avg_fatigue:Q', title="Fatigue Level"))
    bars = base.mark_bar(opacity=0.3, color='#3498db').encode(y=alt.Y('speeding_events:Q', title="Speeding Incidents"))
    
    st.altair_chart((bars + line).resolve_scale(y='independent'), use_container_width=True)

st.divider()

# 4. AUDIT LOG TABLE
st.subheader("🕵️ Recent Trip Anomalies")
anomaly_sql = f"""
    SELECT activity_date, daily_profit, fraud_alerts, speeding_events, overheat_events
    FROM `{SCHEMA}.fct_fleet_performance`
    WHERE driver_id = '{selected_id}'
    ORDER BY activity_date DESC LIMIT 10
"""
st.dataframe(run_query(anomaly_sql), use_container_width=True, hide_index=True)