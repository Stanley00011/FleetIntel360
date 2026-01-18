import duckdb
from pathlib import Path

DB_PATH = "warehouse/analytics/analytics.duckdb"

def run_sql(sql_file: str, fetch_results: bool = False):
    sql_path = Path(sql_file)

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    print(f"\n Running: {sql_path}")

    con = duckdb.connect(DB_PATH)
    sql = sql_path.read_text()

    try:
        if fetch_results:
            result = con.execute(sql).fetchdf()
            print("Success (DQ Results)")
            print(result)
            return result
        else:
            # con.execute(sql)
            # print("Success")
            con.sql(sql) 
            print("Success")
    except Exception as e:
        print("Failed")
        raise e
    finally:
        con.close()

if __name__ == "__main__":

    # SETUP (run daily, but only does work on Day 1)
    run_sql("warehouse/sql/schema.sql") 
    run_sql("warehouse/sql/seed/alert_thresholds.sql")
    run_sql("warehouse/sql/dimensions/dim_date.sql") 

    # INCREMENTAL RAW DATA (Appends new logs)
    run_sql("warehouse/sql/facts/fact_vehicle_telemetry.sql")
    run_sql("warehouse/sql/facts/fact_driver_shifts.sql")
    run_sql("warehouse/sql/facts/fact_daily_finance.sql")

    # UPDATE MASTER RECORDS (Updates 'Last Seen' timestamps)
    run_sql("warehouse/sql/dimensions/dim_driver.sql")
    run_sql("warehouse/sql/dimensions/dim_vehicle.sql")

    # RECOMPUTE AGGREGATES (The core of the dashboard)
    run_sql("warehouse/sql/facts/fact_driver_daily_metrics.sql") 
    run_sql("warehouse/sql/facts/fact_vehicle_daily_metrics.sql")

    # VALIDATE
    run_sql("warehouse/sql/quality/dq_nulls.sql", fetch_results=True)
    run_sql("warehouse/sql/quality/dq_ranges.sql", fetch_results=True)

    print("\n Warehouse build completed successfully")

