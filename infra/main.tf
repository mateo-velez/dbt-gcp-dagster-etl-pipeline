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


resource "google_storage_bucket" "bronze_bucket" {
  name          = var.BRONZE_BUCKET_NAME
  location      = var.REGION_ID
  force_destroy = true
}

resource "google_storage_bucket" "silver_bucket" {
  name          = var.SILVER_BUCKET_NAME
  location      = var.REGION_ID
  force_destroy = true
}

resource "google_storage_bucket" "gold_bucket" {
  name          = var.GOLD_BUCKET_NAME
  location      = var.REGION_ID
  force_destroy = true
}


resource "google_storage_bucket" "misc_bucket" {
  name          = var.MISC_BUCKET_NAME
  location      = var.REGION_ID
  force_destroy = true
}

resource "google_bigquery_dataset" "bronze_dataset" {
  dataset_id = var.BRONZE_DATASET_ID
}

resource "google_bigquery_dataset" "silver_dataset" {
  dataset_id = var.SILVER_DATASET_ID
}

resource "google_bigquery_dataset" "gold_dataset" {
  dataset_id = var.GOLD_DATASET_ID
}