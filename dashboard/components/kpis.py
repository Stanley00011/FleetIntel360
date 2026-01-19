# dashboard/components/kpis.py
from utils.db import run_query

def get_executive_kpis():
    driver_sql = """
    SELECT 
        COUNT(DISTINCT driver_id) AS active_drivers,
        AVG(avg_fatigue_index)    AS avg_fatigue,
        SUM(net_profit)           AS net_profit,
        SUM(fraud_alerts_count)   AS fraud_alerts
    FROM mart.fact_driver_daily_metrics
    WHERE date_key = CURRENT_DATE
    """
    
    vehicle_sql = """
    SELECT 
        SUM(speeding_events) AS total_speeding
    FROM mart.fact_vehicle_daily_metrics
    WHERE date_key = CURRENT_DATE
    """
    
    d_df = run_query(driver_sql)
    v_df = run_query(vehicle_sql)
    
    # Use .get() or check if empty to prevent iloc errors
    has_driver_data = not d_df.empty and d_df["active_drivers"].iloc[0] is not None
    has_vehicle_data = not v_df.empty and v_df["total_speeding"].iloc[0] is not None

    return {
        "active_drivers": int(d_df["active_drivers"].iloc[0]) if has_driver_data else 0,
        "avg_fatigue_index": float(d_df["avg_fatigue"].iloc[0]) if has_driver_data else 0.0,
        "net_profit": float(d_df["net_profit"].iloc[0]) if has_driver_data else 0.0,
        "fraud_alerts": int(d_df["fraud_alerts"].iloc[0]) if has_driver_data else 0,
        "total_speeding_events": int(v_df["total_speeding"].iloc[0]) if has_vehicle_data else 0
    }

def get_readiness_kpis():
    # fix NULLs = Ghost Assets
    sql = """
    SELECT 
        COUNT(*) FILTER (WHERE last_seen_at IS NULL) as ghost_count
    FROM mart.dim_vehicle
    """
    df = run_query(sql)
    return {"ghost_count": int(df["ghost_count"].iloc[0])}