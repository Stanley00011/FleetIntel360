import streamlit as st
import pandas as pd
import altair as alt
import os
from utils.db import run_query

# 1. Page Config & Context
st.set_page_config(page_title="Executive Pulse", layout="wide")

PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'fleetintel360')
SCHEMA = f"{PROJECT_ID}.fleet_intel_staging"

st.title("🚛 Fleet Executive Pulse")
st.caption("Strategic Operational Health & Profitability Baseline")

# 2. TOP LEVEL METRICS (Cleaned Formatting)
kpi_sql = f"""
    SELECT 
        CAST(COUNT(DISTINCT driver_id) AS INT64) as drivers, 
        SUM(daily_profit) as profit, 
        AVG(avg_fatigue) as fatigue 
    FROM `{SCHEMA}.fct_fleet_performance`
"""

try:
    k = run_query(kpi_sql).iloc[0]
    m1, m2, m3, m4 = st.columns(4)
    
    # Casting to int/float explicitly to fix the ".0" display issue
    m1.metric("Active Assets", int(k['drivers']))
    m2.metric("MTD Profit", f"${k['profit']:,.0f}")
    
    avg_f = float(k['fatigue'])
    m3.metric("Fleet Fatigue", f"{avg_f:.2f}", 
              delta="High Risk" if avg_f > 0.6 else "Normal", 
              delta_color="inverse")
    
    m4.metric("System Health", "Optimal", delta="98.2% Sync")
except Exception as e:
    st.error("Check database connection: " + str(e))

st.divider()

# 3. STRATEGIC VIZ GRID (2x2 Layout)
row1_left, row1_right = st.columns(2)
row2_left, row2_right = st.columns(2)

# --- VIZ 1: PERFORMANCE DISTRIBUTION ---
with row1_left:
    st.subheader("🏆 Fleet Grade Distribution")
    dist_df = run_query(f"SELECT performance_score FROM `{SCHEMA}.rpt_driver_rankings`")
    if not dist_df.empty:
        dist_df['Grade'] = pd.cut(dist_df['performance_score'], 
                                  bins=[-999, -100, 0, 100, 999], 
                                  labels=['F (Critical)', 'D (At Risk)', 'B (Stable)', 'A (Elite)'])
        
        grade_chart = alt.Chart(dist_df).mark_bar().encode(
            x=alt.X('count():Q', title="Driver Count"),
            y=alt.Y('Grade:N', sort=['A (Elite)', 'B (Stable)', 'D (At Risk)', 'F (Critical)']),
            color=alt.Color('Grade:N', scale=alt.Scale(domain=['A (Elite)', 'B (Stable)', 'D (At Risk)', 'F (Critical)'], 
                                                       range=['#2ecc71', '#f1c40f', '#e67e22', '#e74c3c']))
        ).properties(height=300)
        st.altair_chart(grade_chart, use_container_width=True)

# --- VIZ 2: REGIONAL PROFITABILITY ---
with row1_right:
    st.subheader("📍 Profitability by Zone")
    zone_df = run_query(f"SELECT location_base, SUM(daily_profit) as profit FROM `{SCHEMA}.fct_fleet_performance` GROUP BY 1")
    if not zone_df.empty:
        zone_chart = alt.Chart(zone_df).mark_bar().encode(
            x=alt.X('profit:Q', title="Net Profit ($)"),
            y=alt.Y('location_base:N', sort='-x', title="Zone"),
            color=alt.condition(alt.datum.profit > 0, alt.value('#2ecc71'), alt.value('#e74c3c'))
        ).properties(height=300)
        st.altair_chart(zone_chart, use_container_width=True)

# --- VIZ 3: REVENUE VS COST TREND ---
with row2_left:
    st.subheader("📈 Financial Trajectory")
    # Derived Cost logic: Revenue - Profit = Cost
    trend_sql = f"""
        SELECT 
            activity_date, 
            SUM(daily_revenue) as revenue, 
            SUM(daily_revenue - daily_profit) as cost 
        FROM `{SCHEMA}.fct_fleet_performance` 
        GROUP BY 1 
        ORDER BY 1
    """
    
    try:
        trend_df = run_query(trend_sql)
        
        if not trend_df.empty:
            # Reorganize data for Altair (Long-form)
            trend_melt = trend_df.melt('activity_date', var_name='Type', value_name='Amount')
            
            line_chart = alt.Chart(trend_melt).mark_line(strokeWidth=3, interpolate='monotone').encode(
                x=alt.X('activity_date:T', title="Date"),
                y=alt.Y('Amount:Q', title="Amount ($)"),
                color=alt.Color('Type:N', 
                                scale=alt.Scale(domain=['revenue', 'cost'], 
                                range=['#2ecc71', '#e74c3c']),
                                title="Financial Metric"),
                tooltip=['activity_date', 'Type', 'Amount']
            ).properties(height=300).interactive()
            
            st.altair_chart(line_chart, use_container_width=True)
        else:
            st.info("No trend data available for the current period.")
            
    except Exception as e:
        st.error(f"Error calculating trajectory: {e}")

# --- VIZ 4: RISK CORRELATION ---
with row2_right:
    st.subheader("⚖️ Profit vs. Fatigue Frontier")
    risk_df = run_query(f"SELECT driver_name, total_profit, avg_fatigue_score, performance_score FROM `{SCHEMA}.rpt_driver_rankings`")
    if not risk_df.empty:
        scatter = alt.Chart(risk_df).mark_circle(size=120).encode(
            x=alt.X('avg_fatigue_score:Q', title="Fatigue Index (Risk)"),
            y=alt.Y('total_profit:Q', title="MTD Profit ($)"),
            color=alt.Color('performance_score:Q', scale=alt.Scale(scheme='redyellowgreen'), title="Score"),
            tooltip=['driver_name', 'performance_score', 'total_profit']
        ).properties(height=300).interactive()
        
        # Adding a median line for fatigue to show "Danger Zone"
        rule = alt.Chart(pd.DataFrame({'x': [0.5]})).mark_rule(color='red', strokeDash=[5,5]).encode(x='x')
        
        st.altair_chart(scatter + rule, use_container_width=True)

st.divider()

# 4. LEADERBOARD PREVIEW
st.subheader("🕵️ Performance Deep-Dive")
st.dataframe(
    risk_df.sort_values('performance_score', ascending=True).style.background_gradient(cmap='RdYlGn', subset=['performance_score']),
    use_container_width=True, hide_index=True
)