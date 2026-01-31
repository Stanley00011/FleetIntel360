variable "project_id" {
  description = "The GCP Project ID"
  type        = string
}

variable "region" {
  default = "us-central1"
}

variable "dataset_id" {
  default = "fleet_intel_staging"
}