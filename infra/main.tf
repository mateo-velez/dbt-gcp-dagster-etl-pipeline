terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = file(var.GOOGLE_APPLICATION_CREDENTIALS)
  project     = var.PROJECT_ID
  region      = var.REGION_ID
}

data "google_compute_network" "vpc" {
  name = "default"
}

terraform {
  backend "gcs" {}
}


resource "google_storage_bucket" "raw_bucket" {
  name          = var.RAW_BUCKET_NAME
  location      = var.REGION_ID
  force_destroy = true
  
}


resource "google_storage_bucket" "staging_bucket" {
  name          = var.STAGING_BUCKET_NAME
  location      = var.REGION_ID
  force_destroy = true
}


resource "google_storage_bucket" "misc_bucket" {
  name          = var.MISC_BUCKET_NAME
  location      = var.REGION_ID
  force_destroy = true
}

resource "google_bigquery_dataset" "dbt_dataset" {
  dataset_id = var.DBT_DATASET_ID
}
