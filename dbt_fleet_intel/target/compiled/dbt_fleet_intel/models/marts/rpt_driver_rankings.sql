SELECT 
    driver_id,
    driver_name,
    location_base,
    SUM(daily_profit) as total_profit,
    AVG(avg_fatigue) as avg_fatigue_score,
    COUNT(speeding_events) as total_speeding_incidents,
    -- Logic: Performance Score
    -- (Profit / 100) - (Fatigue * 50) - (Violations * 10)
    ROUND((SUM(daily_profit) / 100) - (AVG(avg_fatigue) * 50) - (SUM(speeding_events) * 10), 2) as performance_score
FROM `fleetintel360`.`fleet_intel_staging`.`fct_fleet_performance`
WHERE activity_date >= DATE_TRUNC(CURRENT_DATE(), MONTH)
GROUP BY 1, 2, 3
ORDER BY performance_score DESC