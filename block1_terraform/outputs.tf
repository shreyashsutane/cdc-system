output "bigquery_dataset_id" {
  value       = google_bigquery_dataset.cdc_dataset.dataset_id
  description = "The ID of the BigQuery Dataset containing the CDC table."
}

output "bigquery_table_id" {
  value       = google_bigquery_table.cdc_table.table_id
  description = "The ID of the BigQuery Log Table."
}

output "cloud_run_function_url" {
  value       = google_cloudfunctions2_function.ingest_function.service_config[0].uri
  description = "The URL of the deployed Gen 2 Cloud Run Function."
}

output "eventarc_trigger_name" {
  value       = google_eventarc_trigger.firestore_trigger.name
  description = "The name of the Eventarc trigger."
}

output "ingestion_service_account_email" {
  value       = google_service_account.cdc_ingester_sa.email
  description = "The email of the ingestion service account."
}
