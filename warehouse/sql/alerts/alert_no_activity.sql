-- warehouse/sql/alerts/alert_no_activity.sql
SELECT
    'SYSTEM'                     AS entity_id,
    'system'                     AS entity_type,
    'no_driver_activity'         AS metric_name,
    0                             AS metric_value,
    'CRITICAL'                   AS severity,
    'No active drivers today'    AS description
FROM mart.fact_driver_daily_metrics
WHERE d.date_key = (SELECT MAX(date_key) FROM mart.fact_driver_daily_metrics)
HAVING COUNT(DISTINCT driver_id) = 0;

