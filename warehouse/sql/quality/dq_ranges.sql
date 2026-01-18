-- DQ: Range violations
SELECT *
FROM mart.fact_driver_daily_metrics
WHERE date_key = (SELECT MAX(date_key) FROM mart.fact_driver_daily_metrics)
  AND (
    avg_speed_kph > 180
    OR total_shift_hours > 24
    OR max_continuous_hours > 12
    OR avg_fatigue_index > 1
    OR net_profit < -1000
  );