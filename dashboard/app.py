import streamlit as st
import pandas as pd
from utils.db import run_query
from utils.formatting import format_int, format_currency
import os
import altair as alt

# 1. Page Config
st.set_page_config(
    page_title="Fleet Intelligence Platform",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded"
)

PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'fleetintel360')
SCHEMA = f"{PROJECT_ID}.fleet_intel_staging"

# 2. Sidebar Metrics & GLOBAL LEADERBOARD
st.sidebar.header("Fleet Inventory")
try:
    fleet_stats = run_query(f"""
        SELECT 
            CAST(COUNT(*) AS INT64) as total_vehicles,
            CAST(COUNTIF(operational_status = 'ACTIVE') AS INT64) as active_count,
            CAST(COUNTIF(operational_status != 'ACTIVE') AS INT64) as ghost_count
        FROM `{SCHEMA}.dim_vehicles`
    """)
    
    # Explicitly cast to int() to strip the .0
    st.sidebar.metric("Total Fleet Size", int(fleet_stats['total_vehicles'][0]))
    st.sidebar.metric("Active Assets", int(fleet_stats['active_count'][0]))
    st.sidebar.metric("Inactive/Maint.", int(fleet_stats['ghost_count'][0]))
except Exception as e:
    st.sidebar.error("Stats unavailable")

st.sidebar.divider()
st.sidebar.header("🏆 Driver Leaderboard")

# GLOBAL LEADERBOARD LOGIC
try:
    leaderboard_sql = f"""
        SELECT driver_name, performance_score, total_profit
        FROM `{SCHEMA}.rpt_driver_rankings`
        ORDER BY performance_score DESC
    """
    lb_df = run_query(leaderboard_sql)

    if not lb_df.empty:
        with st.sidebar.expander("⭐ Top 3 Performers", expanded=True):
            for i, row in lb_df.head(3).iterrows():
                st.markdown(f"**{row['driver_name']}**")
                # Format score to 0 decimal places
                st.caption(f"Score: {row['performance_score']:.0f} | Profit: ${row['total_profit']:,.0f}")
        
        with st.sidebar.expander("🚨 Critical Interventions", expanded=True):
            for i, row in lb_df.tail(3).iterrows():
                st.markdown(f"**{row['driver_name']}** :red[({row['performance_score']:.0f})]")
                st.caption(f"Audit Required - Loss: ${row['total_profit']:,.0f}")
except:
    st.sidebar.warning("Leaderboard syncing...")

# 3. TOP ROW: Quick Fleet Pulse
st.title("🚛 Fleet Intelligence Platform")
st.markdown("### Operational Mission Control")

st.subheader("System Snapshots")
c1, c2, c3 = st.columns(3)

# Data Freshness
try:
    freshness_res = run_query(f"SELECT MAX(event_timestamp) FROM `{SCHEMA}.fct_latest_vehicle_stats`")
    latest_date = freshness_res.iloc[0,0]
    formatted_date = latest_date.strftime('%b %d, %Y') if hasattr(latest_date, 'strftime') else str(latest_date)
    c1.info(f"📅 **Data Freshness:** {formatted_date}")
except:
    c1.info("📅 **Data Freshness:** N/A")

# Alerts
try:
    alert_res = run_query(f"""
        SELECT CAST(COUNT(DISTINCT vehicle_id) AS INT64) as alert_count 
        FROM `{SCHEMA}.fct_latest_vehicle_stats` 
        WHERE engine_temp_c > 100 OR speed_kph > 110
    """)
    # Force to int to remove .0
    alert_val = int(alert_res['alert_count'][0])
    c2.error(f"🚨 **Critical Alerts:** {alert_val} Vehicles")
except:
    c2.error("🚨 **Critical Alerts:** 0")

# Profit Today
try:
    profit_today = run_query(f"""
        SELECT SUM(daily_profit) FROM `{SCHEMA}.fct_fleet_performance` 
        WHERE activity_date = (SELECT MAX(activity_date) FROM `{SCHEMA}.fct_fleet_performance`)
    """).iloc[0,0]
    c3.success(f"💰 **Today's Net Profit:** ${profit_today:,.0f}" if profit_today else "💰 Profit: $0")
except:
    c3.success("💰 Profit: $0")

st.divider()

# 4. Fleet Composition & Map
st.subheader("Fleet Composition & Real-Time Tracking")
chart_col1, map_col = st.columns([1, 2])

with chart_col1:
    status_dist = run_query(f"SELECT operational_status as status, COUNT(*) as count FROM `{SCHEMA}.dim_vehicles` GROUP BY 1")
    if not status_dist.empty:
        st.bar_chart(status_dist.set_index("status"), color="#4db6ac")
    
    # Cast to int for clean percentage calculation
    total_v = int(fleet_stats['total_vehicles'][0]) if not fleet_stats.empty else 1
    active_v = int(fleet_stats['active_count'][0]) if not fleet_stats.empty else 0
    active_perc = (active_v / total_v * 100)
    st.write(f"✅ **{active_perc:.0f}%** mission-ready.")
    st.progress(active_perc / 100)

with map_col:
    map_data = run_query(f"""
    SELECT latitude, longitude, vehicle_id 
    FROM `{SCHEMA}.fct_latest_vehicle_stats`
    WHERE latitude BETWEEN 6.30 AND 6.75 
      AND longitude BETWEEN 3.20 AND 3.70 
      AND latitude != 0 AND longitude != 0
    """)
    st.map(map_data, latitude='latitude', longitude='longitude', size=20)

st.divider()

# 5. STRATEGIC ZONE ANALYSIS
st.subheader("🌐 Zone Operational Efficiency")
col_x, col_y = st.columns(2)

with col_x:
    zone_perf_sql = f"""
        SELECT location_base, SUM(daily_profit) as zone_profit, AVG(avg_fatigue) as zone_fatigue
        FROM `{SCHEMA}.fct_fleet_performance`
        GROUP BY 1 ORDER BY 2 DESC
    """
    zone_data = run_query(zone_perf_sql)
    if not zone_data.empty:
        st.write("**Total Profit by Zone**")
        st.bar_chart(zone_data.set_index("location_base")["zone_profit"], color="#2ecc71")

with col_y:
    st.write("**Zone Risk (Fatigue) vs. Reward (Profit)**")
    if not zone_data.empty:
        scatter = alt.Chart(zone_data).mark_circle(size=200).encode(
            x=alt.X('zone_fatigue:Q', title="Avg Fatigue (Risk)"),
            y=alt.Y('zone_profit:Q', title="Total Profit (Reward)"),
            color=alt.Color('location_base:N', legend=None),
            tooltip=['location_base', 'zone_profit', 'zone_fatigue']
        ).properties(height=300).interactive()
        st.altair_chart(scatter, use_container_width=True)

st.divider()

# 6. Driver Status & Audit Table
st.subheader("Driver Performance Audit")
try:
    drivers_audit = run_query(f"""
        SELECT 
            driver_name, 
            location_base, 
            performance_score,
            total_profit,
            avg_fatigue_score
        FROM `{SCHEMA}.rpt_driver_rankings`
        ORDER BY performance_score ASC
    """)

    def style_performance(val):
        color = '#e74c3c' if val < 0 else '#2ecc71'
        return f'color: {color}; font-weight: bold'

    st.dataframe(
        drivers_audit.style.map(style_performance, subset=['performance_score'])
                    .background_gradient(cmap='Reds', subset=['avg_fatigue_score'])
                     .format({
                         "total_profit": "${:,.2f}", 
                         "performance_score": "{:.0f}" # Strips decimal in table
                     }),
        use_container_width=True,
        hide_index=True
    )
except Exception as e:
    st.error(f"Error loading audit table: {e}")

st.info("💡 **Navigation:** Use the sidebar to deep-dive into Risk, Finance, and Data Quality modules.")