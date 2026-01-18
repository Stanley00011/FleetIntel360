-- warehouse/sql/alerts/alert_fraud.sql
SELECT
    driver_id                    AS entity_id,
    'driver'                     AS entity_type,
    'fraud_alerts'               AS metric_name,
    fraud_alerts_count           AS metric_value,
    'CRITICAL'                   AS severity,
    'Fraud signals detected'     AS description
FROM mart.fact_driver_daily_metrics
WHERE date_key = (SELECT MAX(date_key) FROM mart.fact_driver_daily_metrics)
  AND fraud_alerts_count > 0;
