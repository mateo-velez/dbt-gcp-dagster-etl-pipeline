variable "PROJECT_ID" {
  description = "Google Cloud project ID"
  type        = string
}

variable "REGION_ID" {
  description = "Google Cloud region ID where resources will be created"
  type        = string
  default     = "us-central1"
}

variable "GOOGLE_APPLICATION_CREDENTIALS" {
  description = "Path to the Google Cloud credentials file"
  type        = string
}


variable "RAW_BUCKET_NAME" {
  description = "The name of the raw bucket"
  type        = string
}

variable "STAGING_BUCKET_NAME" {
  description = "The name of the staging bucket"
  type        = string
}


variable "MISC_BUCKET_NAME" {
  description = "The name of the miscellaneous bucket"
  type        = string
}

variable "DBT_DATASET_ID" {
  description = "The ID of the dbt dataset"
  type        = string
}
