-- warehouse/sql/alerts/alert_data_freshness.sql
SELECT
    'SYSTEM'                     AS entity_id,
    'system'                     AS entity_type,
    'data_freshness_days_lag'    AS metric_name,
    -- Compare the fleet's last update to the global last update
    -- This detects if a specific group (ACTIVE drivers) is falling behind the rest of the system
    (SELECT MAX(date_key) FROM mart.fact_driver_daily_metrics) - MAX(f.date_key) AS metric_value,
    'WARNING'                    AS severity,
    'Data freshness SLA breach for active fleet' AS description
FROM mart.fact_driver_daily_metrics f
JOIN mart.dim_driver d ON f.driver_id = d.driver_id
WHERE d.status = 'ACTIVE' 
HAVING metric_value > 1;

