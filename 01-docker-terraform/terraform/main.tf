terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# GCS Bucket for Data Lake
resource "google_storage_bucket" "data_lake_bucket" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true
  
  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "ny_taxi_dataset" {
  dataset_id                  = var.bq_dataset_name
  friendly_name               = "NY Taxi Dataset"
  description                 = "Dataset for NYC Taxi data - Data Engineering Zoomcamp"
  location                    = var.region
  default_table_expiration_ms = 3600000  # 1 hour

  labels = {
    env     = "dev"
    project = "de-zoomcamp"
  }
}
