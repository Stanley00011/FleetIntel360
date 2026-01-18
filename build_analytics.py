import duckdb
import logging
from pathlib import Path

# BASE_DIR is /app/ inside the container
BASE_DIR = Path(__file__).resolve().parent

DB_PATH = BASE_DIR / "warehouse" / "analytics" / "analytics.duckdb"
SCHEMA_PATH = BASE_DIR / "warehouse" / "sql" / "schema.sql"
STAGING_PATH = BASE_DIR / "warehouse" / "staging"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def build_gold_layer():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"CRITICAL: Schema file not found at {SCHEMA_PATH}")

    # Open connection ONCE
    con = duckdb.connect(str(DB_PATH))
    
    try:
        logger.info("Connection to DuckDB successful. Starting load...")
        
        # 1. Initialize Schema
        with open(SCHEMA_PATH, 'r') as f:
            con.execute(f.read())

        # 2. Load Dimensions
        logger.info("Loading Drivers Dimension...")
        con.execute(f"INSERT OR REPLACE INTO mart.dim_driver SELECT * FROM read_json_auto('{STAGING_PATH}/dim_drivers.jsonl')")

        # 3. Load Fact Tables (This moves the Jan 18 data!)
        logger.info("Loading Fact Tables with Upsert logic...")
        
        # Driver Health
        con.execute(f"""
            INSERT OR REPLACE INTO mart.fact_driver_shifts 
            SELECT *, CAST(timestamp AS DATE) as date_key 
            FROM read_json_auto('{STAGING_PATH}/driver_health_staged.jsonl')
        """)

        # IMPORTANT: (Vehicle Telemetry)
        con.execute(f"""
            INSERT OR REPLACE INTO mart.fact_vehicle_daily_metrics 
            SELECT * FROM read_json_auto('{STAGING_PATH}/vehicles_staged.jsonl')
        """)

        # 4. Final Quality Check
        count = con.execute("SELECT count(*) FROM mart.dim_driver").fetchone()[0]
        logger.info(f"Analytics Layer Ready. Total Drivers in Mart: {count}")

    finally:
        # Close connection ONLY after all work is done
        con.close()
        logger.info("Database connection closed safely.")

if __name__ == "__main__":
    build_gold_layer()