
  
    

    create or replace table `fleetintel360`.`fleet_intel_staging`.`dim_vehicles`
      
    
    

    
    OPTIONS()
    as (
      SELECT 
    vehicle_id,
    vehicle_type,
    status as operational_status,
    CAST(last_service_date AS DATE) as last_service_date,
    -- Business Logic: Days since last service
    DATE_DIFF(CURRENT_DATE(), CAST(last_service_date AS DATE), DAY) as days_since_service
FROM `fleetintel360`.`fleet_intel_staging`.`seed_vehicles`
    );
  