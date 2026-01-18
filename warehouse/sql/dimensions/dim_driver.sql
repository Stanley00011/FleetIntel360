-- Dimension: Driver
-- 1. Create a master list with the latest activity already calculated
CREATE OR REPLACE TEMP TABLE driver_master_calc AS
WITH latest_activity AS (
    SELECT driver_id, MAX(event_timestamp) as actual_last_seen
    FROM mart.fact_driver_shifts
    GROUP BY 1
)
SELECT 
    m.driver_id,
    m.status,
    la.actual_last_seen
FROM read_json_auto('warehouse/staging/dim_drivers.jsonl') m
LEFT JOIN latest_activity la ON m.driver_id = la.driver_id;

-- 2. Use a single MERGE that handles everything at once
MERGE INTO mart.dim_driver AS target
USING driver_master_calc AS source
ON target.driver_id = source.driver_id
WHEN MATCHED THEN
    UPDATE SET 
        status = source.status,
        -- Only update the timestamp if real data is found 
        last_seen_at = COALESCE(source.actual_last_seen, target.last_seen_at)
WHEN NOT MATCHED THEN
    INSERT (driver_id, status, first_seen_at, last_seen_at)
    VALUES (source.driver_id, source.status, source.actual_last_seen, source.actual_last_seen);

-- 3. Final safety check: if a date is 1970 or earlier, it's a mistake.
UPDATE mart.dim_driver 
SET first_seen_at = NULL, last_seen_at = NULL 
WHERE year(last_seen_at) <= 1970 OR last_seen_at IS NULL;