# dashboard/components/kpis.py
from utils.db import run_query
import os

PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'fleetintel360')
SCHEMA = f"{PROJECT_ID}.fleet_intel_staging"

def get_executive_kpis():
    # Updated to use 'avg_fatigue' as defined in your .sql file
    driver_sql = f"""
    SELECT 
        COUNT(DISTINCT driver_id) AS active_drivers,
        AVG(avg_fatigue)          AS avg_fatigue, 
        SUM(daily_profit)         AS net_profit
    FROM `{SCHEMA}.fct_fleet_performance`
    WHERE activity_date = (SELECT MAX(activity_date) FROM `{SCHEMA}.fct_fleet_performance`)
    """
    
    # Updated to use 'engine_temp_c' and 'speed_kph' from fct_latest_vehicle_stats.sql
    vehicle_sql = f"""
    SELECT 
        COUNTIF(engine_temp_c > 110) AS overheat_alerts,
        COUNTIF(speed_kph > 100)      AS speeding_alerts
    FROM `{SCHEMA}.fct_latest_vehicle_stats`
    """
    
    d_df = run_query(driver_sql)
    v_df = run_query(vehicle_sql)
    
    has_data = not d_df.empty and d_df["active_drivers"].iloc[0] is not None

    return {
        "active_drivers": int(d_df["active_drivers"].iloc[0]) if has_data else 0,
        "avg_fatigue": float(d_df["avg_fatigue"].iloc[0]) if has_data else 0.0,
        "net_profit": float(d_df["net_profit"].iloc[0]) if has_data else 0.0,
        "safety_alerts": int(v_df["overheat_alerts"].iloc[0] + v_df["speeding_alerts"].iloc[0]) if not v_df.empty else 0
    }