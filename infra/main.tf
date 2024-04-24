terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = vars.project_id
  region = vars.region_id
}

data "google_compute_network" "vpc" {
  name = "default"
}

terraform {
  backend "gcs" {}
}