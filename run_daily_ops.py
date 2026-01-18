# run_daily_ops.py
import subprocess
import logging
import argparse
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def main():
    # Setup argument parsing
    parser = argparse.ArgumentParser()
    # keep orchestrator's flag as --date for simplicity
    parser.add_argument("--date", type=str, help="Date in YYYY-MM-DD format", default=str(date.today()))
    args = parser.parse_args()

    target_date = args.date
    logger.info(f"Starting FleetIntel360 Daily Operations for {target_date}")

    try:
        # STEP 1: Run Simulation 
        # FIXED: Changed --date to --start-date and added --days 1
        logger.info(f"Step 1/2: Generating Daily Raw Data for {target_date}...")
        subprocess.run([
            "python", "-m", "simulator.run_simulation", 
            "--start-date", target_date,  # Matches the simulator's requirements
            "--days", "1"
        ], check=True)

        # STEP 2: Run the full staging pipeline (staging -> DuckDB -> Alerts)
        logger.info("Step 2/2: Executing Staging Pipeline & Analytics Refresh...")
        subprocess.run(["python", "run_staging.py"], check=True)

        logger.info(f"Operations complete. Dashboard and Alerts updated for {target_date}.")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Daily Operations Failed: {e}")

if __name__ == "__main__":
    main()