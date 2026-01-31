import os
import requests
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# CONFIG
WEBHOOK_URL = os.getenv('CLOUD_SLACK_WEBHOOK') 
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
KEY_JSON = os.getenv('GCP_SA_KEY') 
DATASET = "fleet_intel_staging"

def get_bq_client():
    """Initializes BigQuery client."""
    try:
        if KEY_JSON:
            info = json.loads(KEY_JSON)
            credentials = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=credentials, project=PROJECT_ID)
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        print(f"BQ Init Error: {e}")
        return None

def build_slack_payload(alert_data):
    """
    RIGOR: Matches dbt schema. 
    Includes Vehicle ID and Driver ID for full traceability.
    """
    return {
        "blocks": [
            {
                "type": "header", 
                "text": {"type": "plain_text", "text": "🚛 Fleet Security Notification"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Severity:*\n`{alert_data['severity']}`"},
                    {"type": "mrkdwn", "text": f"*Vehicle:*\n`{alert_data['vehicle_id']}`"},
                    {"type": "mrkdwn", "text": f"*Driver:*\n{alert_data['driver_id']}"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{alert_data['alert_time']}"}
                ]
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Alert Details:*\n{alert_data['alert_message']}"}
            }
        ]
    }

def send_slack_alert(alert_data):
    """Sends the formatted alert to Slack."""
    if not WEBHOOK_URL:
        return
    payload = build_slack_payload(alert_data)
    try:
        requests.post(WEBHOOK_URL, json=payload, timeout=10)
    except Exception as e:
        print(f"Slack post failed: {e}")

def run_portfolio_audit():
    """Queries BigQuery and dispatches alerts with clear execution logging."""
    print(f"[{datetime.now()}] Starting Fleet Security Audit...")
    
    client = get_bq_client()
    if not client:
        print("Audit Aborted: Could not initialize BigQuery client.")
        return

    # Rigor: Querying the Mart we built in dbt
    query = f"""
        SELECT vehicle_id, driver_id, severity, alert_message, alert_time 
        FROM `{PROJECT_ID}.{DATASET}.alerts_slack`
        WHERE alert_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    """
    
    try:
        print(f"Scanning `{DATASET}.alerts_slack` for critical events...")
        query_job = client.query(query)
        results = query_job.result()
        
        # Count results for better log visibility
        row_count = 0
        for row in results:
            alert_dict = {
                "vehicle_id": row.vehicle_id, 
                "driver_id": row.driver_id,
                "severity": row.severity, 
                "alert_message": row.alert_message,
                "alert_time": str(row.alert_time)
            }
            send_slack_alert(alert_dict)
            print(f"Dispatched Alert: {row.vehicle_id} | {row.alert_message}")
            row_count += 1
        
        if row_count == 0:
            print("✨ Clean Audit: No critical anomalies detected in the last hour.")
        else:
            print(f"🏁 Audit Complete: {row_count} alerts sent to Slack.")

    except Exception as e:
        print(f"Audit Failed with Error: {e}")

if __name__ == "__main__":
    run_portfolio_audit()