provider "google" {
  project = "sincere-nirvana-340100"
  region  = "us-central1"
}

resource "google_storage_bucket" "data_bucket" {
  name          = "ml_ops_zoomcamp_final_project_data_bucket"
  location      = "US"
  force_destroy = true
}

output "bucket_url" {
  value = google_storage_bucket.data_bucket.url
}