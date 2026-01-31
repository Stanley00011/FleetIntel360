WITH latest_telemetry AS (
    SELECT * FROM {{ ref('fct_latest_vehicle_stats') }}
),
latest_health AS (
    -- We qualify by driver to get their most recent vitals
    SELECT * FROM {{ ref('stg_health') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY event_timestamp DESC) = 1
),
latest_finance AS (
    -- We qualify by driver to get their most recent financial status
    SELECT * FROM {{ ref('stg_finance') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY ingested_at DESC) = 1
)

SELECT 
    t.vehicle_id, 
    t.driver_id,
    'CRITICAL' as severity,
    CASE 
        WHEN t.engine_temp_c > 110 THEN 'OVERHEAT: ' || CAST(round(t.engine_temp_c, 1) AS STRING) || '°C'
        WHEN t.is_speeding_in_zone = True THEN 'SPEEDING: Zone Violation'
        WHEN h.fatigue_index > 0.8 THEN 'FATIGUE: High Risk Driver (' || t.driver_id || ')'
        WHEN f.fraud_alerts_count > 0 THEN 'FINANCE: Fraud detected for ' || t.driver_id
        ELSE 'SYSTEM: General Alert'
    END as alert_message,


    CURRENT_TIMESTAMP() as alert_time
FROM latest_telemetry t
LEFT JOIN latest_health h ON t.driver_id = h.driver_id
LEFT JOIN latest_finance f ON t.driver_id = f.driver_id
WHERE (
    t.engine_temp_c > 110 
    OR h.fatigue_index > 0.8 
    OR f.fraud_alerts_count > 0 
    OR t.is_speeding_in_zone = True
)