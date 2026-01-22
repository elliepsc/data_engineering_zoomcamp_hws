variable "project_id" {
  description = "Your GCP Project ID"
  type        = string
  # Replace with your actual project ID
  default     = "your-gcp-project-id"
}

variable "region" {
  description = "Region for GCP resources"
  type        = string
  default     = "us-central1"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  type        = string
  default     = "ny_taxi"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket Name"
  type        = string
  default     = "data-lake"
}
