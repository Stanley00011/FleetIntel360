-- warehouse/sql/alerts/alert_vehicle_risk.sql
SELECT
    v.vehicle_id                 AS entity_id,
    'vehicle'                    AS entity_type,
    'engine_temp'                AS metric_name,
    v.avg_engine_temp_c          AS metric_value, 
    CASE
        WHEN v.avg_engine_temp_c >= t.critical_threshold THEN 'CRITICAL'
        ELSE 'WARNING'
    END                          AS severity,
    t.description
FROM mart.fact_vehicle_daily_metrics v
JOIN mart.alert_thresholds t ON t.metric_name = 'engine_temp_c'
WHERE v.date_key = (SELECT MAX(date_key) FROM mart.fact_vehicle_daily_metrics)
  AND v.avg_engine_temp_c >= t.warning_threshold;
