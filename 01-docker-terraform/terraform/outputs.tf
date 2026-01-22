output "bucket_name" {
  description = "Name of the GCS bucket"
  value       = google_storage_bucket.data_lake_bucket.name
}

output "bucket_url" {
  description = "URL of the GCS bucket"
  value       = google_storage_bucket.data_lake_bucket.url
}

output "bigquery_dataset_id" {
  description = "BigQuery Dataset ID"
  value       = google_bigquery_dataset.ny_taxi_dataset.dataset_id
}

output "bigquery_dataset_location" {
  description = "BigQuery Dataset Location"
  value       = google_bigquery_dataset.ny_taxi_dataset.location
}
