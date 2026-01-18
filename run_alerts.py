import duckdb
import requests
from pathlib import Path
from dotenv import load_dotenv
import os

from slack_formatter import format_alert

# CLOUD-AWARE ENVIRONMENT LOADING
# 1. Try to get from system environment (GitHub Actions / Docker)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# 2. Fallback to .env for local development
if not SLACK_WEBHOOK_URL:
    load_dotenv(Path(__file__).parent / ".env")
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# 3. Validation
if not SLACK_WEBHOOK_URL:
    print("ERROR: SLACK_WEBHOOK_URL not found. Check GitHub Secrets or local .env")
else:
    print("Slack Webhook URL loaded successfully")

# Change path to be relative to the root for GitHub Actions compatibility
DB_PATH = os.path.join(os.getcwd(), "warehouse/analytics/analytics.duckdb")


def send_to_slack(payload):
    """Sends the formatted JSON payload to the Slack Webhook."""
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to send alert: {e}")


# Connect to DuckDB
con = duckdb.connect(DB_PATH)

ALERT_SQL_FILES = [
    "warehouse/sql/alerts/driver_fatigue_alerts.sql",
    "warehouse/sql/alerts/alert_vehicle_risk.sql",
    "warehouse/sql/alerts/alert_fraud.sql",
    "warehouse/sql/alerts/alert_data_freshness.sql",
]

print(f"Checking {len(ALERT_SQL_FILES)} alert queries...")

for sql_file in ALERT_SQL_FILES:
    if not os.path.exists(sql_file):
        print(f"Warning: SQL file not found: {sql_file}")
        continue

    sql = Path(sql_file).read_text()
    df = con.execute(sql).df()

    if df.empty:
        print(f"No results for: {os.path.basename(sql_file)}")
        continue

    print(f"ALERT FOUND in {os.path.basename(sql_file)}: Sending to Slack...")
    
    payload = format_alert(
        title="🚨 Fleet Alert",
        rows=df,
        severity_column="severity",
        entity_column="entity_id"
    )

    # Slack safety limit (max 50 blocks allowed)
    if "blocks" in payload and len(payload["blocks"]) > 45:
        payload["blocks"] = payload["blocks"][:45]

    send_to_slack(payload)

con.close()
print("Alerts run complete.")