import streamlit as st
import pandas as pd
from utils.db import run_query
from utils.formatting import format_int

# 1. Page Config
st.set_page_config(
    page_title="Fleet Intelligence Dashboard",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🚛 Fleet Intelligence Platform")
st.markdown("### Operational Mission Control")

# 2. Sidebar Metrics
st.sidebar.header("Fleet Inventory")
try:
    fleet_stats = run_query("""
        SELECT 
            COUNT(*) as total_vehicles,
            COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_count,
            COUNT(CASE WHEN last_seen_at IS NULL THEN 1 END) as ghost_count
        FROM mart.dim_vehicle
    """)
    
    st.sidebar.metric("Total Fleet Size", format_int(fleet_stats['total_vehicles'][0]))
    st.sidebar.metric("Active Assets", format_int(fleet_stats['active_count'][0]))
    st.sidebar.metric("Inactive/Maint.", format_int(fleet_stats['ghost_count'][0]))
except Exception as e:
    st.sidebar.error("Stats unavailable")

# 3. TOP ROW: Quick Fleet Pulse
st.subheader("System Snapshots")
c1, c2, c3 = st.columns(3)

# Data Freshness Check
freshness_res = run_query("SELECT MAX(date_key) FROM mart.fact_vehicle_daily_metrics")
latest_date = freshness_res.iloc[0,0]

formatted_date = latest_date.strftime('%b %d, %Y') if hasattr(latest_date, 'strftime') else str(latest_date)
c1.info(f"📅 **Data Freshness:** {formatted_date}")

# Critical Alert Summary
alert_count = run_query("""
    SELECT COUNT(DISTINCT vehicle_id) FROM mart.fact_vehicle_daily_metrics 
    WHERE avg_engine_temp_c > 100 AND date_key = (SELECT MAX(date_key) FROM mart.fact_vehicle_daily_metrics)
""").iloc[0,0]
c2.error(f"🚨 **Critical Alerts:** {alert_count} Vehicles")

# Profit Today
profit_today = run_query("""
    SELECT SUM(net_profit) FROM mart.fact_driver_daily_metrics 
    WHERE date_key = (SELECT MAX(date_key) FROM mart.fact_driver_daily_metrics)
""").iloc[0,0]
c3.success(f"💰 **Today's Net Profit:** ${profit_today:,.0f}" if profit_today else "💰 Profit: $0")

st.divider()

# Fleet Composition & Readiness
st.subheader("Fleet Composition")
chart_col1, chart_col2 = st.columns([2, 1])

with chart_col1:
    status_dist_sql = """
        SELECT status, COUNT(*) as count 
        FROM mart.dim_driver 
        GROUP BY 1
    """
    status_dist = run_query(status_dist_sql)
    st.bar_chart(status_dist.set_index("status"), color="#4db6ac")

with chart_col2:
    st.write("**Operational Readiness**")
    total_v = fleet_stats['total_vehicles'][0]
    active_perc = (fleet_stats['active_count'][0] / total_v * 100) if total_v > 0 else 0
    st.write(f"✅ {active_perc:.0f}% of fleet is mission-ready.")
    
    if alert_count > 0:
        st.error(f"⚠️ Action Required: {alert_count} assets need immediate inspection.")
    else:
        st.success("👍 All active assets are within safety limits.")

# 4. Driver Connectivity (CLEAN DATE LOGIC)
st.subheader("Driver Operational Status")
try:
    # Updated SQL to cast timestamp to DATE for a cleaner UI
    drivers = run_query("""
        SELECT 
            driver_id, 
            status as current_status, 
            CASE 
                WHEN last_seen_at IS NULL THEN 'N/A'
                ELSE last_seen_at::DATE::VARCHAR -- Casts timestamp to Date only
            END as last_activity
        FROM mart.dim_driver
        ORDER BY last_seen_at DESC NULLS LAST
    """)

    def style_status(val):
        color = 'gray'
        if val == 'ACTIVE': color = '#2ecc71'
        elif val == 'INACTIVE': color = '#e74c3c'
        return f'color: {color}; font-weight: bold'

    st.dataframe(
        drivers.style.map(style_status, subset=['current_status']),
        use_container_width=True,
        hide_index=True
    )
except Exception as e:
    st.error(f"Error loading drivers: {e}")

# 5. Live Alert Feed
st.subheader("Critical Fleet Issues (Latest Run)")
try:
    alerts = run_query("""
        SELECT 
            vehicle_id, 
            avg_engine_temp_c as temp,
            speeding_events as speeding,
            CASE WHEN avg_engine_temp_c > 110 THEN 'CRITICAL' ELSE 'WARNING' END as severity
        FROM mart.fact_vehicle_daily_metrics 
        WHERE (avg_engine_temp_c > 100 OR speeding_events > 15)
          AND date_key = (SELECT MAX(date_key) FROM mart.fact_vehicle_daily_metrics)
    """)

    if not alerts.empty:
        st.dataframe(
            alerts.style.background_gradient(cmap='YlOrRd', subset=['temp']),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.success("✅ Fleet is operating within normal safety parameters.")
except Exception as e:
    st.error(f"Alert feed error: {e}")

st.divider()
st.info("💡 **Navigation:** Use the sidebar deep-dive into Risk, Finance, and Data Quality modules.")