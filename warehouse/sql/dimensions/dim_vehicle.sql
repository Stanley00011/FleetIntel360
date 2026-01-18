-- warehouse/sql/dimensions/dim_vehicle.sql
-- 1. Create a temp table from your staged VEHICLE master config
CREATE OR REPLACE TEMP TABLE stg_master_vehicles AS 
SELECT * FROM read_json_auto('warehouse/staging/dim_vehicles.jsonl');

-- 2. Merge status and calculate type for Vehicles
MERGE INTO mart.dim_vehicle AS target
USING stg_master_vehicles AS source
ON target.vehicle_id = source.vehicle_id
WHEN MATCHED THEN
    UPDATE SET 
        status = source.status,
        vehicle_type = CASE 
            WHEN source.vehicle_id LIKE 'BUS%' THEN 'BUS'
            WHEN source.vehicle_id LIKE 'CAR%' THEN 'CAR'
            ELSE 'UNKNOWN'
        END
WHEN NOT MATCHED THEN
    INSERT (vehicle_id, vehicle_type, status, first_seen_at, last_seen_at)
    VALUES (
        source.vehicle_id, 
        CASE 
            WHEN source.vehicle_id LIKE 'BUS%' THEN 'BUS'
            WHEN source.vehicle_id LIKE 'CAR%' THEN 'CAR'
            ELSE 'UNKNOWN'
        END, 
        source.status, 
        NULL, 
        NULL
    );

-- Set everyone that has no telemetry to NULL
UPDATE mart.dim_vehicle 
SET first_seen_at = NULL, 
    last_seen_at = NULL
WHERE vehicle_id NOT IN (SELECT DISTINCT vehicle_id FROM mart.fact_vehicle_telemetry);

-- Final check
UPDATE mart.dim_vehicle 
SET first_seen_at = NULL, 
    last_seen_at = NULL 
WHERE year(last_seen_at) <= 1970;