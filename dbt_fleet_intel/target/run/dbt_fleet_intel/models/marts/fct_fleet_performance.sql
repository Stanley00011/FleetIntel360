
  
    

    create or replace table `fleetintel360`.`fleet_intel_staging`.`fct_fleet_performance`
      
    
    

    
    OPTIONS()
    as (
      -- models/marts/fct_fleet_performance.sql

WITH telemetry AS (
    SELECT 
        driver_id,
        vehicle_id,
        DATE(event_timestamp) as activity_date,
        AVG(speed_kph) as avg_speed,
        MAX(speed_kph) as max_speed,
        COUNTIF(is_speeding = True) as speeding_events
    FROM `fleetintel360`.`fleet_intel_staging`.`stg_telemetry`
    GROUP BY 1, 2, 3
),

health AS (
    SELECT 
        driver_id,
        DATE(event_timestamp) as activity_date,
        AVG(fatigue_index) as avg_fatigue,
        MAX(continuous_hours) as max_continuous_hours,
        SUM(CAST(took_breaks AS INT64)) as total_breaks
    FROM `fleetintel360`.`fleet_intel_staging`.`stg_health`
    GROUP BY 1, 2
),

finance AS (
    SELECT 
        driver_id,
        business_date as activity_date,
        SUM(revenue) as daily_revenue,
        SUM(total_cost) as daily_cost,
        SUM(net_profit) as daily_profit
    FROM `fleetintel360`.`fleet_intel_staging`.`stg_finance`
    GROUP BY 1, 2
)

SELECT 
    t.activity_date,
    t.driver_id,
    t.vehicle_id,
    t.avg_speed,
    t.speeding_events,
    h.avg_fatigue,
    h.max_continuous_hours,
    f.daily_revenue,
    f.daily_profit,
    -- New Functional Logic: Efficiency Metric
    CASE 
        WHEN f.daily_revenue > 0 THEN (f.daily_profit / f.daily_revenue) * 100 
        ELSE 0 
    END as profit_margin_pct
FROM telemetry t
LEFT JOIN health h ON t.driver_id = h.driver_id AND t.activity_date = h.activity_date
LEFT JOIN finance f ON t.driver_id = f.driver_id AND t.activity_date = f.activity_date
    );
  