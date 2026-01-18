# run_staging.py
import subprocess
import logging
import os 

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def run_script(script_name):
    logger.info(f"--- Running {script_name} ---")
    try:
        subprocess.run(["python", script_name], check=True, env=os.environ.copy())
    except subprocess.CalledProcessError as e:
        logger.error(f"Error occurred while running {script_name}: {e}")
        return False
    return True

def main():
    # 1. ALWAYS run Dimensions first 
    scripts = [
        "stage_master_data.py",      
        "stage_driver_health.py",    
        "stage_vehicles.py",          
        "stage_finance.py",
        "build_analytics.py",
        "run_sql.py",                
        "run_alerts.py"        
    ]

    for script in scripts:
        if not run_script(script):
            logger.error("Staging pipeline failed early.")
            break
    else:
        logger.info("ALL STAGING SCRIPTS COMPLETED SUCCESSFULLY")

if __name__ == "__main__":
    main()