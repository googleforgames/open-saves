terraform {
  backend "gcs" {
    bucket = "triton-state"
    prefix = "terraform/state"
  }
}

resource "google_storage_bucket" "triton-gcs-blob" {
  name          = "triton-dev-store"
  location      = "US"
  force_destroy = true
}

resource "google_storage_bucket" "triton-integration" {
  name          = "triton-integration"
  location      = "US"
  force_destroy = true
}
