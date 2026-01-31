import streamlit as st
import pandas as pd
import os
import altair as alt
from utils.db import run_query

st.set_page_config(page_title="Finance & Compliance Audit", layout="wide")
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'fleetintel360')
SCHEMA = f"{PROJECT_ID}.fleet_intel_staging"

st.title("💰 Finance & Compliance Mission Control")
st.caption("P&L Integrity, Revenue Leakage, and Fraud Surveillance")

# 1. TOP-LINE FINANCIAL KPIs
kpi_sql = f"""
    SELECT 
        SUM(daily_revenue) as rev, 
        SUM(daily_profit) as profit,
        SUM(daily_revenue - daily_profit) as expenses,
        SUM(fraud_alerts) as fraud
    FROM `{SCHEMA}.fct_fleet_performance`
"""
k = run_query(kpi_sql).iloc[0]

m1, m2, m3, m4 = st.columns(4)
m1.metric("Gross Revenue", f"${k['rev']:,.2f}")
m2.metric("Total OPEX", f"${k['expenses']:,.2f}", delta="Derived Cost", delta_color="off")
m3.metric("Net Profit", f"${k['profit']:,.2f}", delta=f"{(k['profit']/k['rev']*100):.1f}% Margin")
m4.metric("Financial Risk (Fraud)", int(k['fraud']), delta="Audit Required" if k['fraud'] > 0 else "Clean", delta_color="inverse")

st.divider()

# 2. FINANCIAL ANALYSIS GRID (Visualizing Leakage)
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("📉 Revenue vs. Profit Waterfall (MTD)")
    # Finance logic: Visualizing the gap between what we make and what we keep
    waterfall_data = pd.DataFrame({
        'Step': ['1. Gross Revenue', '2. Operating Expenses', '3. Net Profit'],
        'Amount': [k['rev'], -k['expenses'], k['profit']],
        'Type': ['Positive', 'Negative', 'Total']
    })
    
    waterfall = alt.Chart(waterfall_data).mark_bar().encode(
        x=alt.X('Step:N', sort=None, title=None),
        y=alt.Y('Amount:Q', title="Value ($)"),
        color=alt.Color('Type:N', scale=alt.Scale(domain=['Positive', 'Negative', 'Total'], range=['#2ecc71', '#e74c3c', '#3498db']), legend=None),
        tooltip=['Step', 'Amount']
    ).properties(height=400)
    st.altair_chart(waterfall, use_container_width=True)

with col_right:
    st.subheader("📍 Profit Margin by Operational Zone")
    zone_data = run_query(f"""
        SELECT 
            location_base as zone, 
            SUM(daily_profit) / NULLIF(SUM(daily_revenue), 0) * 100 as margin_pct,
            SUM(daily_profit) as total_profit
        FROM `{SCHEMA}.fct_fleet_performance` 
        GROUP BY 1 ORDER BY margin_pct DESC
    """)
    
    margin_chart = alt.Chart(zone_data).mark_bar().encode(
        x=alt.X('margin_pct:Q', title="Operating Margin (%)"),
        y=alt.Y('zone:N', sort='-x', title="Zone"),
        color=alt.condition(alt.datum.margin_pct > 15, alt.value("#2ecc71"), alt.value("#f1c40f")),
        tooltip=['zone', 'margin_pct', 'total_profit']
    ).properties(height=400)
    st.altair_chart(margin_chart, use_container_width=True)

st.divider()

# 3. COMPLIANCE & RISK (Quadrant Analysis)
st.subheader("⚖️ Unit Economics: Profitability vs. Risk Score")
st.markdown("_Finance teams look for high-profit drivers with high-risk scores (Sustainability risk)._")

risk_df = run_query(f"""
    SELECT driver_name, total_profit, performance_score, avg_fatigue_score 
    FROM `{SCHEMA}.rpt_driver_rankings`
""")

scatter = alt.Chart(risk_df).mark_circle(size=100).encode(
    x=alt.X('performance_score:Q', title="Compliance/Safety Score"),
    y=alt.Y('total_profit:Q', title="Net Profit Contribution ($)"),
    color=alt.condition(alt.datum.total_profit < 0, alt.value("#e74c3c"), alt.value("#2ecc71")),
    tooltip=['driver_name', 'performance_score', 'total_profit']
).properties(height=450).interactive()

# Add a zero-line to show who is costing the company money
zero_line = alt.Chart(pd.DataFrame({'y': [0]})).mark_rule(color='white', strokeDash=[5,5]).encode(y='y')
st.altair_chart(scatter + zero_line, use_container_width=True)



st.divider()

# 4. LEAKAGE AUDIT & FRAUD LOG
st.subheader("🕵️ Loss Investigation & Fraud Audit")
audit_col, fraud_col = st.columns([2, 1])

with audit_col:
    st.markdown("**Bottom 10 Performers (Revenue Leakage)**")
    leakage_df = run_query(f"""
        SELECT driver_name, location_base, total_profit, performance_score 
        FROM `{SCHEMA}.rpt_driver_rankings` 
        WHERE total_profit < 0 
        ORDER BY total_profit ASC LIMIT 10
    """)
    st.dataframe(leakage_df.style.background_gradient(cmap='Reds', subset=['total_profit']), use_container_width=True, hide_index=True)

with fraud_col:
    st.markdown("**Fraud Concentration by Zone**")
    fraud_data = run_query(f"SELECT location_base, SUM(fraud_alerts) as alerts FROM `{SCHEMA}.fct_fleet_performance` GROUP BY 1 HAVING alerts > 0")
    if not fraud_data.empty:
        st.altair_chart(alt.Chart(fraud_data).mark_arc(innerRadius=60).encode(
            theta='alerts:Q', color='location_base:N'
        ), use_container_width=True)
    else:
        st.success("No active fraud alerts.")