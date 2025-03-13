/**
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

resource "google_firestore_index" "blob_status_updated_at" {
  collection  = "blob"
  query_scope = "COLLECTION_GROUP"
  api_scope   = "DATASTORE_MODE_API"
  fields {
    field_path = "Status"
    order      = "ASCENDING"
  }
  fields {
    field_path = "Timestamps.UpdatedAt"
    order      = "ASCENDING"
  }
}

resource "google_firestore_index" "default_indexed_properties" {
  collection  = "record"
  # ancestor = "ALL_ANCESTORS"
  query_scope = "COLLECTION_GROUP"
  api_scope   = "DATASTORE_MODE_API"
  fields {
    field_path = "Properties.prop1"
    order      = "ASCENDING"
  }
  fields {
    field_path = "Properties.prop1"
    order      = "DESCENDING"
  }

  fields {
    field_path = "Timestamps.CreatedAt"
    order      = "ASCENDING"
  }

  fields {
    field_path = "Timestamps.UpdatedAt"
    order      = "ASCENDING"
  }
}

resource "google_firestore_field" "blob_ttl" {
  collection = "blob"
  field = "ExpiresAt"
  ttl_config {} // Just being present will configure this field as TTL.
}

resource "google_firestore_field" "record_ttl" {
  collection = "record"
  field = "ExpiresAt"
  ttl_config {} // Just being present will configure this field as TTL.
}
