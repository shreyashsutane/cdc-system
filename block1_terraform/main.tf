# ==============================================================================
# BLOCK 1: INFRASTRUCTURE AS CODE (TERRAFORM)
# ==============================================================================

# Enable GCP Services
locals {
  services = [
    "firestore.googleapis.com",
    "eventarc.googleapis.com",
    "run.googleapis.com",
    "cloudfunctions.googleapis.com",
    "bigquery.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
    "cloudbuild.googleapis.com",
    "compute.googleapis.com",
    "eventarcpublishing.googleapis.com"
  ]
}

resource "google_project_service" "gcp_services" {
  for_each                   = toset(local.services)
  project                    = var.project_id
  service                    = each.key
  disable_on_destroy         = false
}

# ------------------------------------------------------------------------------
# BIGQUERY STORAGE PROVISIONING
# ------------------------------------------------------------------------------

# BigQuery Dataset
resource "google_bigquery_dataset" "cdc_dataset" {
  dataset_id                  = "cdc_logging"
  friendly_name               = "CDC Logging Dataset"
  description                 = "Dataset containing Change Data Capture logs from Cloud Datastore"
  location                    = var.region
  default_table_expiration_ms = null # Retention indefinitely unless cleaned up

  depends_on = [google_project_service.gcp_services]
}

# BigQuery Log Table Partitioned strictly by ingress 'execution_time'
resource "google_bigquery_table" "cdc_table" {
  dataset_id = google_bigquery_dataset.cdc_dataset.dataset_id
  table_id   = "datastore_mutations_ledger"
  description = "Ledger of mutations in Datastore/Firestore"

  time_partitioning {
    type  = "DAY"
    field = "execution_time"
  }

  schema = <<EOF
[
  {
    "name": "event_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique identifier of the mutation event"
  },
  {
    "name": "execution_time",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Ingress time when the change event was written to BigQuery"
  },
  {
    "name": "operation_type",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "The mutation operation: INSERT, UPDATE, or DELETE"
  },
  {
    "name": "entity_kind",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Datastore entity kind (document collection path)"
  },
  {
    "name": "entity_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Datastore entity key ID or name"
  },
  {
    "name": "changed_by",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Email address of the operator who triggered the change"
  },
  {
    "name": "old_value",
    "type": "JSON",
    "mode": "NULLABLE",
    "description": "JSON representation of the entity state before mutation"
  },
  {
    "name": "new_value",
    "type": "JSON",
    "mode": "NULLABLE",
    "description": "JSON representation of the entity state after mutation"
  }
]
EOF

  depends_on = [google_bigquery_dataset.cdc_dataset]
}

# ------------------------------------------------------------------------------
# IDENTITY & ACCESS MANAGEMENT (IAM)
# ------------------------------------------------------------------------------

# Least-Privilege Ingestion Service Account
resource "google_service_account" "cdc_ingester_sa" {
  account_id   = "cdc-ingester-sa"
  display_name = "CDC Ingestion Cloud Run Function Service Account"
  depends_on   = [google_project_service.gcp_services]
}

# Bind role roles/bigquery.dataEditor to function service account
resource "google_project_iam_member" "bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.cdc_ingester_sa.email}"
}

# Bind role roles/bigquery.jobUser to function service account
resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.cdc_ingester_sa.email}"
}

# Bind role roles/eventarc.eventReceiver to function service account
resource "google_project_iam_member" "eventarc_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.cdc_ingester_sa.email}"
}

# ------------------------------------------------------------------------------
# CLOUD RUN FUNCTION (GEN 2) PROVISIONING
# ------------------------------------------------------------------------------

# Storage Bucket for Cloud Function Source Code Codebase
resource "google_storage_bucket" "function_source_bucket" {
  name                        = "${var.project_id}-cdc-func-src"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  depends_on                  = [google_project_service.gcp_services]
}

# Compress function source code
data "archive_file" "function_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../block2_ingestion"
  output_path = "${path.module}/tmp/block2_ingestion.zip"
}

# Upload zipped source code to bucket
resource "google_storage_bucket_object" "function_zip_object" {
  name   = "source-${data.archive_file.function_zip.output_md5}.zip"
  bucket = google_storage_bucket.function_source_bucket.name
  source = data.archive_file.function_zip.output_path
}

# Cloud Run Function
resource "google_cloudfunctions2_function" "ingest_function" {
  name        = "firestore-cdc-ingester"
  location    = var.region
  description = "Parses Firestore/Datastore Eventarc mutations and ingests them into BigQuery"

  build_config {
    runtime     = "python311"
    entry_point = "firestore_cdc_ingester"
    source {
      storage_source {
        bucket = google_storage_bucket.function_source_bucket.name
        object = google_storage_bucket_object.function_zip_object.name
      }
    }
  }

  service_config {
    max_instance_count = 100
    min_instance_count = 0
    available_memory   = "256Mi"
    timeout_seconds    = 60
    service_account_email = google_service_account.cdc_ingester_sa.email
    ingress_settings   = "ALLOW_ALL"
  }

  depends_on = [
    google_project_service.gcp_services,
    google_storage_bucket_object.function_zip_object
  ]
}

# ------------------------------------------------------------------------------
# EVENTARC MUTATION TRIGGER
# ------------------------------------------------------------------------------

# Service account used by Eventarc trigger to invoke Cloud Run
resource "google_service_account" "eventarc_trigger_sa" {
  account_id   = "cdc-trigger-sa"
  display_name = "Eventarc Trigger Service Account"
  depends_on   = [google_project_service.gcp_services]
}

# Allow Eventarc Trigger Service Account to invoke the Cloud Run service behind the Cloud Function
resource "google_cloud_run_service_iam_member" "eventarc_invoker" {
  project  = var.project_id
  location = var.region
  service  = google_cloudfunctions2_function.ingest_function.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.eventarc_trigger_sa.email}"
}

# Grant Pub/Sub Publisher role to Eventarc system agent (needed for Custom/Direct Triggers in some regions)
resource "google_project_iam_member" "pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.eventarc_trigger_sa.email}"
}

# Eventarc Trigger tracking Firestore mutations via google.cloud.firestore.document.v1.written
resource "google_eventarc_trigger" "firestore_trigger" {
  name                    = "firestore-cdc-trigger"
  location                = var.region
  event_data_content_type = "application/protobuf"
  
  matching_criteria {
    attribute = "type"
    value     = "google.cloud.datastore.entity.v1.written"
  }

  matching_criteria {
    attribute = "database"
    value     = var.firestore_database
  }

  destination {
    cloud_run_service {
      service = google_cloudfunctions2_function.ingest_function.name
      region  = var.region
    }
  }

  service_account = google_service_account.eventarc_trigger_sa.email

  depends_on = [
    google_cloudfunctions2_function.ingest_function,
    google_cloud_run_service_iam_member.eventarc_invoker,
    google_project_iam_member.eventarc_service_agent,
    google_project_iam_member.trigger_eventarc_receiver
  ]
}

# ------------------------------------------------------------------------------
# SUPPLEMENTAL IAM FOR GEN 2 CLOUD FUNCTIONS BUILD COMPILING
# ------------------------------------------------------------------------------

data "google_project" "project" {
  project_id = var.project_id
}

# Allow Cloud Functions Service Agent to act as the custom execution service account
resource "google_service_account_iam_member" "gcf_sa_user" {
  service_account_id = google_service_account.cdc_ingester_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:service-${data.google_project.project.number}@gcf-admin-robot.iam.gserviceaccount.com"
}

# Allow Cloud Build Service Account to act as the custom execution service account
resource "google_service_account_iam_member" "gcb_sa_user" {
  service_account_id = google_service_account.cdc_ingester_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${data.google_project.project.number}@cloudbuild.gserviceaccount.com"
}

# Retrieve default Compute service account email dynamically
data "google_compute_default_service_account" "default" {
  project    = var.project_id
  depends_on = [google_project_service.gcp_services["compute.googleapis.com"]]
}

# Force creation of Eventarc service identity/agent
resource "google_project_service_identity" "eventarc_identity" {
  provider   = google-beta
  project    = var.project_id
  service    = "eventarc.googleapis.com"
  depends_on = [google_project_service.gcp_services["eventarc.googleapis.com"]]
}

# ------------------------------------------------------------------------------
# DEFAULT COMPUTE SERVICE ACCOUNT PERMISSIONS FOR CLOUD BUILD USE
# ------------------------------------------------------------------------------

# Grant Storage Object Viewer to the default Compute SA (used for reading function source zip)
resource "google_project_iam_member" "compute_sa_storage_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${data.google_compute_default_service_account.default.email}"
}

# Grant Log Writer to the default Compute SA (used for writing build logs)
resource "google_project_iam_member" "compute_sa_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${data.google_compute_default_service_account.default.email}"
}

# Grant Artifact Registry Writer to the default Compute SA (used for pushing compiled container image)
resource "google_project_iam_member" "compute_sa_registry_writer" {
  project = var.project_id
  role    = "roles/artifactregistry.writer"
  member  = "serviceAccount:${data.google_compute_default_service_account.default.email}"
}

# Grant BigQuery Job User to the default Compute SA (used by cdc-dashboard-gateway to run query jobs)
resource "google_project_iam_member" "compute_sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${data.google_compute_default_service_account.default.email}"
}

# Grant BigQuery Data Viewer to the default Compute SA (used by cdc-dashboard-gateway to read ledger data)
resource "google_project_iam_member" "compute_sa_bq_data_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${data.google_compute_default_service_account.default.email}"
}


# Grant Eventarc Service Agent role to the Eventarc Service Agent identity
resource "google_project_iam_member" "eventarc_service_agent" {
  project = var.project_id
  role    = "roles/eventarc.serviceAgent"
  member  = "serviceAccount:${google_project_service_identity.eventarc_identity.email}"
}

# Grant Eventarc Event Receiver to the Eventarc trigger service account
resource "google_project_iam_member" "trigger_eventarc_receiver" {
  project = var.project_id
  role    = "roles/eventarc.eventReceiver"
  member  = "serviceAccount:${google_service_account.eventarc_trigger_sa.email}"
}





