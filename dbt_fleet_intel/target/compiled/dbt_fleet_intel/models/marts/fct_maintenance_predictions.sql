WITH vehicle_stress AS (
    -- Get total overheat events and mileage proxy (count of telemetry)
    SELECT 
        vehicle_id,
        COUNTIF(engine_temp_c > 110) as total_overheats,
        COUNT(*) as activity_count
    FROM `fleetintel360`.`fleet_intel_staging`.`stg_telemetry`
    GROUP BY 1
),
latest_status AS (
    -- Get the most recent tire and fuel data
    SELECT * FROM `fleetintel360`.`fleet_intel_staging`.`fct_latest_vehicle_stats`
)

SELECT 
    v.vehicle_id,
    v.vehicle_type,
    v.days_since_service,
    s.total_overheats,
    l.low_tire_pressure,
    -- The Maintenance Logic
    CASE 
        WHEN v.days_since_service > 180 OR s.total_overheats > 10 THEN 'URGENT: Schedule Now'
        WHEN v.days_since_service > 90 OR s.total_overheats > 5 THEN 'Action Required'
        WHEN l.low_tire_pressure = True THEN 'Check Tires'
        ELSE 'Healthy'
    END as maintenance_priority
FROM `fleetintel360`.`fleet_intel_staging`.`dim_vehicles` v
LEFT JOIN vehicle_stress s ON v.vehicle_id = s.vehicle_id
LEFT JOIN latest_status l ON v.vehicle_id = l.vehicle_id
WHERE v.operational_status = 'ACTIVE'