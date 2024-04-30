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


variable "BACKEND_BUCKET_NAME" {
  description = "The name of the backend bucket"
  type        = string
}

variable "BRONZE_BUCKET_NAME" {
  description = "The name of the bronze bucket"
  type        = string
}

variable "SILVER_BUCKET_NAME" {
  description = "The name of the silver bucket"
  type        = string
}

variable "GOLD_BUCKET_NAME" {
  description = "The name of the gold bucket"
  type        = string
}

variable "MISC_BUCKET_NAME" {
  description = "The name of the miscellaneous bucket"
  type        = string
}

variable "BRONZE_DATASET_ID" {
  description = "The ID of the bronze dataset"
  type        = string
}

variable "SILVER_DATASET_ID" {
  description = "The ID of the silver dataset"
  type        = string
}

variable "GOLD_DATASET_ID" {
  description = "The ID of the gold dataset"
  type        = string
}