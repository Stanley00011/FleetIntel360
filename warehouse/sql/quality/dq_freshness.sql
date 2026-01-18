-- DQ: Data freshness
-- warehouse/sql/quality/dq_freshness.sql
SELECT
    MAX(date_key) AS latest_data_point,
    CURRENT_DATE - MAX(date_key) AS days_of_lag,
    CASE 
        WHEN (CURRENT_DATE - MAX(date_key)) > 1 THEN 'OUTDATED'
        ELSE 'FRESH'
    END AS status
FROM mart.fact_driver_daily_metrics;