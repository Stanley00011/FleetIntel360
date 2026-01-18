-- warehouse/sql/alerts/driver_fatigue_alerts.sql
-- DRIVER FATIGUE ALERTS

-- warehouse/sql/alerts/driver_fatigue_alerts.sql
SELECT
    d.driver_id                         AS entity_id,
    'driver'                            AS entity_type,
    'avg_fatigue_index'                 AS metric_name,
    ROUND(d.avg_fatigue_index, 1)       AS metric_value,
    CASE
        WHEN d.avg_fatigue_index >= t.critical_threshold THEN 'CRITICAL'
        ELSE 'WARNING'
    END                                 AS severity,
    t.description
FROM mart.fact_driver_daily_metrics d
JOIN mart.dim_driver dim ON d.driver_id = dim.driver_id
JOIN mart.alert_thresholds t ON t.metric_name = 'avg_fatigue_index'
WHERE d.date_key = (SELECT MAX(date_key) FROM mart.fact_driver_daily_metrics)
  AND dim.status = 'ACTIVE' -- <--- Ignore retired drivers
  AND d.avg_fatigue_index >= t.warning_threshold;