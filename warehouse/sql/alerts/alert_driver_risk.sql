-- warehouse/sql/alerts/alert_driver_risk.sql
SELECT
    vehicle_id                   AS entity_id,
    'vehicle'                    AS entity_type,
    'vehicle_risk'               AS metric_name,
    avg_engine_temp_c            AS metric_value,
    CASE
        WHEN avg_engine_temp_c > 100 THEN 'CRITICAL'
        ELSE 'WARNING'
    END                          AS severity,
    'Engine temp or speeding anomaly' AS description
FROM mart.fact_vehicle_daily_metrics
WHERE d.date_key = (SELECT MAX(date_key) FROM fact_vehicle_daily_metrics)
  AND (
        avg_engine_temp_c > 95
     OR speeding_rate > 0.10
  );
