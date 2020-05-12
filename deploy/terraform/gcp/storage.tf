resource "google_storage_bucket" "triton-gcs" {
  name          = "triton-dev-store"
  location      = "US"
  force_destroy = true
}
