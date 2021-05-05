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

package timestamps

import (
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
)

const (
	signaturePropertyName = "Signature"

	// Precision is the effective precision of timestamps.
	// Currently it's 1 microsecond as it's what Datastore supports.
	// Datastore: 1 microsecond: https://cloud.google.com/datastore/docs/concepts/entities#date_and_time
	Precision = time.Millisecond
)

// Timestamps keeps keeps when each record is created or updated as well as
// a randomly generated UUID to keep consistency under concurrent writes.
// It should be embedded to metadata entities such as Record and Store.
type Timestamps struct {
	// CreatedAt is the timestamp of the record creation time
	// Automatically set by MetaDB
	CreatedAt time.Time
	// UpdatedAt is the timestamp of the last modification time
	// Automatically set and managed by MetaDB
	UpdatedAt time.Time
	// Signature is a UUID that is randomly created each time the record is updated
	// Automatically set and managed by MetaDB
	Signature uuid.UUID `datastore:"-"`
}

// Assert Timestamps implements the PropertyLoadSaver interface.
var _ datastore.PropertyLoadSaver = new(Timestamps)

// New returns a new Timestamps instance with
// CreatedAt and UpdatedAt set to time.Now() and Signature set to uuid.New().
func New() Timestamps {
	now := time.Now().UTC().Truncate(Precision)
	return Timestamps{
		CreatedAt: now,
		UpdatedAt: now,
		Signature: uuid.New(),
	}
}

// Update updates the UpdatedAt and Signature fields with time.Now() and uuid.New().
func (t *Timestamps) Update() {
	t.UpdatedAt = time.Now().UTC().Truncate(Precision)
	t.Signature = uuid.New()
}

// Load implements the Datastore PropertyLoadSaver interface and converts Datastore
// properties to corresponding struct fields.
func (t *Timestamps) Load(ps []datastore.Property) error {
	sig, ps, err := LoadUUID(ps, signaturePropertyName)
	if err != nil {
		return err
	}
	t.Signature = sig
	return datastore.LoadStruct(t, ps)
}

// Save implements the Datastore PropertyLoadSaver interface and converts the properties
// field in the struct to separate Datastore properties.
func (t *Timestamps) Save() ([]datastore.Property, error) {
	ps, err := datastore.SaveStruct(t)
	if err != nil {
		return nil, err
	}

	ps = append(ps, UUIDToDatastoreProperty(signaturePropertyName, t.Signature, true))
	return ps, nil
}
