import os
import time
from dotenv import load_dotenv
import requests
from google.cloud import bigquery
from datetime import datetime


# This looks for a .env file and loads the variables
script_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(script_dir, '.env'))
# --- CONFIGURATION ---
# We use a specific variable name for Phase 4 to avoid conflict with v1

WEBHOOK_URL = os.getenv('CLOUD_SLACK_WEBHOOK') 
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET = "fleet_intel_staging"

if not WEBHOOK_URL:
    print("Error: CLOUD_SLACK_WEBHOOK environment variable not found.")
    print("Please run: export CLOUD_SLACK_WEBHOOK='https://hooks.slack.com/...'")
    exit()

def send_slack_alert(alert_data):
    """Formats and sends the alert to Slack."""
    message = {
        "text": f"🚨 *PHASE 4 ALERT* | {alert_data['severity']}", # Added 'Phase 4' tag
        "attachments": [
            {
                "color": "#FF0000",
                "fields": [
                    {"title": "Vehicle", "value": alert_data['vehicle_id'], "short": True},
                    {"title": "Driver", "value": alert_data['driver_id'], "short": True},
                    {"title": "Issue", "value": alert_data['alert_message'], "short": False},
                    {"title": "Time", "value": str(alert_data['alert_time']), "short": False}
                ]
            }
        ]
    }
    try:
        response = requests.post(WEBHOOK_URL, json=message)
        if response.status_code == 200:
            print(f"Alert sent for {alert_data['vehicle_id']}")
        else:
            print(f"Failed to send alert: {response.text}")
    except Exception as e:
        print(f"Connection Error: {e}")

def check_alerts():
    """Queries BigQuery for the latest critical alerts."""
    # EXPLICITLY pass the project_id here to avoid the "non-empty" error
    client = bigquery.Client(project=PROJECT_ID)
    
    # Tip: Using a formatted string with backticks for BigQuery tables
    query = f"""
        SELECT 
            vehicle_id, 
            driver_id, 
            severity, 
            alert_message, 
            alert_time 
        FROM `{PROJECT_ID}.{DATASET}.alerts_slack`
        WHERE alert_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        ORDER BY alert_time DESC
        LIMIT 5
    """
    
    try:
        # Run the query
        query_job = client.query(query)  
        results = query_job.result() # This is where the 400 error usually happens
        
        count = 0
        for row in results:
            send_slack_alert(dict(row))
            count += 1
        
        if count == 0:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔍 No new critical alerts in {DATASET}.")
            
    except Exception as e:
        print(f" BigQuery Error: {e}")

if __name__ == "__main__":
    print("Cloud Alert Monitor (v2) Started...")
    print(f"Listening on project: {PROJECT_ID}")
    while True:
        check_alerts()
        time.sleep(60)