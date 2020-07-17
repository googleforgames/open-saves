// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metadb

import (
	"time"

	"github.com/google/uuid"
)

// timestamps keeps keeps when each record is created or updated as well as
// a randomly generated UUID to keep consistency under concurrent writes.
// It should be embedded to metadata entities such as Record and Store.
type timestamps struct {
	// CreatedAt is the timestamp of the record creation time
	// Automatically set by MetaDB
	CreatedAt time.Time
	// UpdatedAt is the timestamp of the last modification time
	// Automatically set and managed by MetaDB
	UpdatedAt time.Time
	// Signature is a UUID that is randomly created each time the record is updated
	// Automatically set and managed by MetaDB
	Signature uuid.UUID `datastore:",noindex"`
}

// NewTimestamps sets CreatedAt and UpdatedAt to time.Now() and Signature to uuid.New().
func (t *timestamps) NewTimestamps() {
	now := time.Now()
	t.CreatedAt = now
	t.UpdatedAt = now
	t.Signature = uuid.New()
}

// UpdateTimestamps updates the UpdatedAt and Signature fields with time.Now() and uuid.New().
func (t *timestamps) UpdateTimestamps() {
	t.UpdatedAt = time.Now()
	t.Signature = uuid.New()
}
