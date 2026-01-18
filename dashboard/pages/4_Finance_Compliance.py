# dashboard/pages/4_Finance_Compliance.py

import streamlit as st
import pandas as pd
from utils.db import run_query
from components.filters import date_filter

# Page config
st.set_page_config(page_title="Finance & Compliance", layout="wide")

st.title("💰 Finance & Compliance")
st.caption("Monitoring fleet profitability against regulatory and fraud risk")

# FILTERS
start_date, end_date = date_filter()

# FINANCIAL KPIs
kpi_sql = f"""
SELECT
    SUM(total_revenue)      AS total_revenue,
    SUM(total_cost)         AS total_cost,
    SUM(net_profit)         AS net_profit,
    SUM(fraud_alerts_count) AS fraud_alerts
FROM mart.fact_driver_daily_metrics
WHERE date_key BETWEEN '{start_date}' AND '{end_date}'
"""
kpi_res = run_query(kpi_sql)

if not kpi_res.empty:
    kpis = kpi_res.iloc[0]
    # Calculate margin safely
    margin = (kpis['net_profit'] / kpis['total_revenue'] * 100) if kpis['total_revenue'] > 0 else 0
else:
    kpis = {'total_revenue':0, 'total_cost':0, 'net_profit':0, 'fraud_alerts':0}
    margin = 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue", f"${kpis['total_revenue']:,.0f}")
col2.metric("Net Profit", f"${kpis['net_profit']:,.0f}", delta=f"{margin:.1f}% Margin")
col3.metric("Fleet Costs", f"${kpis['total_cost']:,.0f}", delta_color="inverse")
col4.metric("Fraud Alerts", int(kpis["fraud_alerts"]), delta="High Risk" if kpis["fraud_alerts"] > 5 else "Normal", delta_color="inverse")

st.divider()

# TREND ANALYSIS
left, right = st.columns(2)

with left:
    st.subheader("📈 Revenue vs. Cost Over Time")
    trend_sql = f"""
        SELECT date_key, SUM(total_revenue) AS revenue, SUM(total_cost) AS cost
        FROM mart.fact_driver_daily_metrics
        WHERE date_key BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1 ORDER BY 1
    """
    st.area_chart(run_query(trend_sql).set_index("date_key"))

with right:
    st.subheader("🚩 Fraud Alert Velocity")
    fraud_trend_sql = f"""
        SELECT date_key, SUM(fraud_alerts_count) AS alerts
        FROM mart.fact_driver_daily_metrics
        WHERE date_key BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1 ORDER BY 1
    """
    st.bar_chart(run_query(fraud_trend_sql).set_index("date_key"), color="#ff4b4b")

st.divider()

# ADVANCED RISK ANALYSIS
st.subheader("🎯 Risk vs. Reward (Driver Profiling)")
correlation_sql = f"""
    SELECT 
        driver_id, 
        SUM(net_profit) as total_profit, 
        SUM(fraud_alerts_count) as total_fraud,
        AVG(avg_fatigue_index) as avg_fatigue
    FROM mart.fact_driver_daily_metrics
    WHERE date_key BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1
"""
corr_df = run_query(correlation_sql)

if not corr_df.empty:
    st.scatter_chart(
        corr_df,
        x="total_profit",
        y="total_fraud",
        size="avg_fatigue",
        color="driver_id",
        use_container_width=True
    )
    st.caption("Bubble size represents average driver fatigue. Look for high-profit/high-fraud outliers.")

st.divider()

# LOSS & FRAUD AUDIT
c1, c2 = st.columns(2)

with c1:
    st.subheader("📉 Loss-Making Drivers")
    loss_sql = f"""
        SELECT driver_id, SUM(net_profit) as profit, SUM(total_revenue) as rev
        FROM mart.fact_driver_daily_metrics
        WHERE date_key BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1 HAVING SUM(net_profit) < 0 ORDER BY profit ASC
    """
    st.dataframe(run_query(loss_sql), use_container_width=True)

with c2:
    st.subheader("🕵️ High Fraud Drivers")
    fraud_audit_sql = f"""
        SELECT driver_id, SUM(fraud_alerts_count) as alerts, AVG(avg_fatigue_index) as fatigue
        FROM mart.fact_driver_daily_metrics
        WHERE date_key BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1 HAVING SUM(fraud_alerts_count) > 0 ORDER BY alerts DESC
    """
    st.dataframe(run_query(fraud_audit_sql), use_container_width=True)