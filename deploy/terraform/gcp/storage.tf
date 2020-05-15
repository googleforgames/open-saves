terraform {
  backend "gcs" {
    bucket = "triton-state"
    prefix = "terraform/state"
  }
}

resource "google_storage_bucket" "triton-gcs" {
  name          = "triton-dev-store"
  location      = "US"
  force_destroy = true
}
