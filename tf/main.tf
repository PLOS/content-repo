provider "google" {
  project = var.project
  region  = "us-east1"
  version = "3.25"
}

terraform {
  backend "gcs" {}
}

resource "google_storage_bucket" "crepo" {
  for_each = toset(var.buckets)
  name     = each.value
  location = "US"
}

resource "google_storage_bucket_iam_binding" "cloudfunctions" {
  for_each = toset(var.buckets)
  bucket = each.value
  role = "roles/storage.admin"
  members = [
    "serviceAccount:${var.project}@appspot.gserviceaccount.com",
    "serviceAccount:rhino-sa@${var.project}.iam.gserviceaccount.com",
    "serviceAccount:wombat-sa@${var.project}.iam.gserviceaccount.com"
  ]
}

resource "google_pubsub_topic" "corpus-migration" {
  name = "corpus-migration"
}

resource "google_pubsub_topic_iam_member" "counter-topic-member" {
  topic   = google_pubsub_topic.corpus-migration.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:rhino-sa@${var.project}.iam.gserviceaccount.com"
}

