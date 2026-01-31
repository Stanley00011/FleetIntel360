terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  # Relative path from infra/terraform/ to your root project folder
  credentials = file("../../service_account.json")
}

# --- DATASETS ---

resource "google_bigquery_dataset" "raw_data" {
  dataset_id  = "fleet_intel_raw"
  location    = "US"
  description = "Landing zone for Pub/Sub streaming data"
}

resource "google_bigquery_dataset" "staging_data" {
  dataset_id  = var.dataset_id # fleet_intel_staging
  location    = "US"
  description = "Transformation layer for dbt models"
}

# --- PUB/SUB TOPICS ---

resource "google_pubsub_topic" "telemetry" { name = "fleet-telemetry" }
resource "google_pubsub_topic" "health"    { name = "fleet-health" }
resource "google_pubsub_topic" "finance"   { name = "fleet-finance" }

# --- TABLES ---

resource "google_bigquery_table" "alerts_slack" {
  dataset_id = google_bigquery_dataset.staging_data.dataset_id
  table_id   = "alerts_slack"
  
  # Schema matches your dbt query exactly
  schema = <<EOF
[
  {"name": "vehicle_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "driver_id", "type": "STRING", "mode": "NULLABLE"},
  {"name": "severity", "type": "STRING", "mode": "NULLABLE"},
  {"name": "alert_message", "type": "STRING", "mode": "NULLABLE"},
  {"name": "alert_time", "type": "TIMESTAMP", "mode": "NULLABLE"}
]
EOF

  # Prevents Terraform from deleting the table if dbt is managing the data
  deletion_protection = false 
}