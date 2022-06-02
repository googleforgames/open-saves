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

resource "google_datastore_index" "blob_status_updated_at" {
  kind = "blob"
  properties {
    name      = "Status"
    direction = "ASCENDING"
  }
  properties {
    name      = "Timestamps.UpdatedAt"
    direction = "ASCENDING"
  }
}

resource "google_datastore_index" "chunk_status_updated_at" {
  kind = "chunk"
  properties {
    name      = "Status"
    direction = "ASCENDING"
  }
  properties {
    name      = "Timestamps.UpdatedAt"
    direction = "ASCENDING"
  }
}

resource "google_datastore_index" "default_indexed_properties" {
  kind     = "record"
  ancestor = "ALL_ANCESTORS"
  properties {
    name      = "Properties.prop1"
    direction = "ASCENDING"
  }
  properties {
    name      = "Properties.prop1"
    direction = "DESCENDING"
  }
}
