import duckdb
import requests
from pathlib import Path
from dotenv import load_dotenv
import os

from slack_formatter import format_alert

# Load env variables
load_dotenv(Path(__file__).parent / ".env")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

DB_PATH = "warehouse/analytics/analytics.duckdb"


def send_to_slack(payload):
    response = requests.post(SLACK_WEBHOOK_URL, json=payload)
    response.raise_for_status()


# Connect to DuckDB
con = duckdb.connect(DB_PATH)

ALERT_SQL_FILES = [
    "warehouse/sql/alerts/driver_fatigue_alerts.sql",
    "warehouse/sql/alerts/alert_vehicle_risk.sql",
    "warehouse/sql/alerts/alert_fraud.sql",
    "warehouse/sql/alerts/alert_data_freshness.sql",
]

for sql_file in ALERT_SQL_FILES:
    sql = Path(sql_file).read_text()
    df = con.execute(sql).df()

    if df.empty:
        continue

    payload = format_alert(
        title="🚨 Fleet Alert",
        rows=df,
        severity_column="severity",
        entity_column="entity_id"
    )

    # Slack safety limit
    if len(payload.get("blocks", [])) > 45:
        payload["blocks"] = payload["blocks"][:45]

    send_to_slack(payload)

con.close()
print("Alerts run complete")
