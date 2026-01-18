# check_results.py 
import duckdb

con = duckdb.connect("warehouse/analytics/analytics.duckdb")

query = """
SELECT 
    d.status,
    COUNT(d.driver_id) as driver_count,
    COALESCE(SUM(s.shift_hours), 0) as total_fleet_hours
FROM mart.dim_driver d
LEFT JOIN mart.fact_driver_shifts s ON d.driver_id = s.driver_id
GROUP BY d.status;
"""

print(con.execute(query).df())