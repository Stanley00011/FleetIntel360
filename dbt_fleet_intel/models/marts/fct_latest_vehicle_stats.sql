WITH latest_telemetry AS (
    SELECT 
        t.*,
        z.zone_name,
        z.max_speed_kph as zone_limit
    FROM {{ ref('stg_telemetry') }} t
    LEFT JOIN {{ ref('seed_speed_zones') }} z 
        ON t.latitude BETWEEN z.lat_min AND z.lat_max
        AND t.longitude BETWEEN z.lon_min AND z.lon_max
    QUALIFY ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY event_timestamp DESC) = 1
)
SELECT 
    l.vehicle_id,
    l.driver_id,
    l.latitude,
    l.longitude,
    l.speed_kph,
    l.zone_name,
    (l.speed_kph > l.zone_limit) as is_speeding_in_zone,
    l.fuel_level,
    l.engine_temp_c,
    -- If your stg_telemetry already parsed these, use the names directly. 
    -- If not, we use the raw JSON field (usually called 'data_json' or 'body' in staging)
    l.tire_psi_fl < 28 as low_tire_pressure, 
    l.event_timestamp
FROM latest_telemetry l