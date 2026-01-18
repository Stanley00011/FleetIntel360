-- DQ: Null checks
SELECT
    COUNT(*) AS bad_rows
FROM mart.fact_driver_daily_metrics
WHERE driver_id IS NULL
   OR date_key IS NULL;