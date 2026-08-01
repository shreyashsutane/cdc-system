variable "project_id" {
  type        = string
  description = "The GCP Project ID where the resources will be deployed."
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "The GCP region to deploy resources to."
}

variable "firestore_database" {
  type        = string
  default     = "(default)"
  description = "The Firestore database name (use (default) for default database)."
}

variable "environment" {
  type        = string
  default     = "production"
  description = "Deployment environment name (e.g. production, staging, development)."
}
