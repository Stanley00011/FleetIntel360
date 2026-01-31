-- models/marts/fct_fleet_performance.sql

WITH telemetry_daily AS (
    SELECT 
        driver_id,
        vehicle_id,
        DATE(event_timestamp) as activity_date,
        AVG(speed_kph) as avg_speed,
        COUNTIF(is_speeding = True) as speeding_events,
        COUNTIF(engine_temp_c > 110) as overheat_events
    FROM `fleetintel360`.`fleet_intel_staging`.`stg_telemetry`
    GROUP BY 1, 2, 3
),

health_daily AS (
    SELECT 
        driver_id,
        DATE(event_timestamp) as activity_date,
        AVG(fatigue_index) as avg_fatigue,
        SUM(CAST(took_breaks AS INT64)) as total_breaks
    FROM `fleetintel360`.`fleet_intel_staging`.`stg_health`
    GROUP BY 1, 2
),

finance_daily AS (
    SELECT 
        driver_id,
        business_date as activity_date,
        SUM(revenue) as daily_revenue,
        SUM(total_cost) as daily_cost,
        SUM(net_profit) as daily_profit,
        SUM(fraud_alerts_count) as total_fraud_alerts 
    FROM `fleetintel360`.`fleet_intel_staging`.`stg_finance`
    GROUP BY 1, 2
)

SELECT 
    t.activity_date,
    t.driver_id,
    t.vehicle_id,
    d.driver_name,
    d.location_base,
    t.avg_speed,
    t.speeding_events,
    t.overheat_events,
    COALESCE(h.avg_fatigue, 0) as avg_fatigue,
    COALESCE(f.daily_revenue, 0) as daily_revenue,
    COALESCE(f.daily_profit, 0) as daily_profit,
    COALESCE(f.total_fraud_alerts, 0) as fraud_alerts
FROM telemetry_daily t
LEFT JOIN health_daily h 
    ON t.driver_id = h.driver_id 
    AND t.activity_date = h.activity_date
LEFT JOIN finance_daily f 
    ON t.driver_id = f.driver_id 
    AND t.activity_date = f.activity_date
JOIN `fleetintel360`.`fleet_intel_staging`.`dim_drivers` d 
    ON t.driver_id = d.driver_id