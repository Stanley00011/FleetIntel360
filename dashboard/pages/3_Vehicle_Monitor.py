import streamlit as st
import pandas as pd
import os
import altair as alt
from utils.db import run_query
from utils.formatting import format_int

st.set_page_config(page_title="Vehicle Health Monitor", layout="wide")

PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'fleetintel360')
SCHEMA = f"{PROJECT_ID}.fleet_intel_staging"

st.title("🚛 Vehicle Health & Maintenance")
st.caption("Predictive diagnostics and real-time asset tracking")

# 1. SELECTOR & GLOBAL STATUS
# Pulling both ID and Status to handle maintenance logic
v_options = run_query(f"SELECT vehicle_id, operational_status, vehicle_type FROM `{SCHEMA}.dim_vehicles` ORDER BY 1")
selected_vehicle = st.selectbox("Select Asset ID", v_options['vehicle_id'])

# Get metadata for selected vehicle
v_meta = v_options[v_options['vehicle_id'] == selected_vehicle].iloc[0]

if selected_vehicle:
    # 2. STATUS OVERLAY
    if v_meta['operational_status'] != 'ACTIVE':
        st.warning(f"🚨 **ASSET OFFLINE:** This {v_meta['vehicle_type']} is currently marked as **{v_meta['operational_status']}**.")
    else:
        st.success(f"✅ **ASSET ONLINE:** {v_meta['vehicle_type']} is mission-ready.")

    # 3. PREDICTIVE SCORECARD (Using fct_maintenance_predictions)
    maint_sql = f"""
        SELECT *, 
               (100 - (days_since_service * 0.2) - (total_overheats * 5)) as health_score
        FROM `{SCHEMA}.fct_maintenance_predictions` 
        WHERE vehicle_id = '{selected_vehicle}'
    """
    maint_df = run_query(maint_sql)
    
    if not maint_df.empty:
        m = maint_df.iloc[0]
        score = max(0, min(100, m['health_score']))
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Health Score", f"{int(score)}%", delta=f"{m['maintenance_priority']}")
        col2.metric("Last Service", f"{m['days_since_service']} days ago")
        col3.metric("Lifetime Overheats", m['total_overheats'])
        col4.metric("Tire Status", "🚨 LOW PSI" if m['low_tire_pressure'] else "✅ NOMINAL")

    st.divider()

    # 4. TELEMETRY & MAP (With Lagos Water Protection)
    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.subheader("📡 Real-Time Diagnostic Feed")
        tel_sql = f"""
            SELECT engine_temp_c, fuel_level, speed_kph, zone_name, is_speeding_in_zone, latitude, longitude
            FROM `{SCHEMA}.fct_latest_vehicle_stats`
            WHERE vehicle_id = '{selected_vehicle}'
        """
        t_df = run_query(tel_sql)
        
        if not t_df.empty:
            t = t_df.iloc[0]
            
            # Gauge-style display
            st.progress(t['fuel_level']/100, text=f"Fuel Level: {t['fuel_level']}%")
            
            t1, t2 = st.columns(2)
            t1.metric("Engine Temp", f"{t['engine_temp_c']}°C", 
                      delta="HIGH" if t['engine_temp_c'] > 105 else None, delta_color="inverse")
            t2.metric("Current Speed", f"{int(t['speed_kph'])} km/h")
            
            st.write(f"📍 **Zone:** {t['zone_name'] if pd.notna(t['zone_name']) else 'Unmapped Area'}")
            if t['is_speeding_in_zone'] is True:
                st.error("🚨 CRITICAL: Speeding violation detected in current zone.")

    with col_right:
        st.subheader("📍 Precise Asset Location")
        # Protection logic: Don't show map if coords are 0 or far outside Lagos
        if t_df.empty or t['latitude'] < 6.0 or t['latitude'] == 0:
            st.info("GPS Signal Unavailable: Asset may be inside a maintenance bay or shielded.")
        else:
            map_data = pd.DataFrame({'lat': [t['latitude']], 'lon': [t['longitude']]})
            st.map(map_data, zoom=13)

    st.divider()

    # 5. HARDWARE STRESS CHARTS
    st.subheader("📉 7-Day Stress Analysis")
    history_sql = f"""
        SELECT activity_date, avg_speed, overheat_events, daily_profit
        FROM `{SCHEMA}.fct_fleet_performance`
        WHERE vehicle_id = '{selected_vehicle}'
        ORDER BY activity_date ASC
    """
    h_df = run_query(history_sql)
    
    if not h_df.empty:
        # Complex Chart: Speed vs Overheats
        base = alt.Chart(h_df).encode(x='activity_date:T')
        
        line = base.mark_line(color='#5276A7').encode(
            y=alt.Y('avg_speed:Q', title='Average Speed (km/h)')
        )
        
        bar = base.mark_bar(opacity=0.3, color='#F4B400').encode(
            y=alt.Y('overheat_events:Q', title='Overheat Incidents')
        )
        
        st.altair_chart(alt.layer(bar, line).resolve_scale(y='independent'), use_container_width=True)
        st.caption("Yellow bars represent engine stress (overheats); Blue line represents average speed.")