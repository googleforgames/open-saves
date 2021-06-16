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

package blobref

import (
	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
)

// BlobRef is a metadata document to keep track of blobs stored in an external blob store.
type BlobRef struct {
	// Key is the primary key for the blob entry
	Key uuid.UUID `datastore:"-"`
	// Size is the byte size of the blob
	Size int64
	// Status is the current status of the blob
	Status
	// StoreKey is the key of the store that the blob belongs to
	StoreKey string
	// RecordKey is the key of the record that the blob belongs to
	// It can be non-existent (e.g. deleted already) but then the Status
	// should not be Blob StatusReady.
	RecordKey string

	// Timestamps keeps track of creation and modification times and stores a randomly
	// generated UUID to maintain consistency.
	Timestamps timestamps.Timestamps
}

// Assert Blob implements both PropertyLoadSave and KeyLoader.
var _ datastore.PropertyLoadSaver = new(BlobRef)
var _ datastore.KeyLoader = new(BlobRef)

// These functions need to be implemented here instead of the datastore package because
// go doesn't permit to define additional receivers in another package.
// Save and Load replicates the default behaviors, however, they are required
// for the KeyLoader interface.

// Save implements the Datastore PropertyLoadSaver interface and converts the properties
// field in the struct to separate Datastore properties.
func (b *BlobRef) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(b)
}

// Load implements the Datastore PropertyLoadSaver interface and converts Datstore
// properties to the Properties field.
func (b *BlobRef) Load(ps []datastore.Property) error {
	return datastore.LoadStruct(b, ps)
}

// LoadKey implements the KeyLoader interface and sets the value to the Key field.
func (b *BlobRef) LoadKey(k *datastore.Key) error {
	key, err := uuid.Parse(k.Name)
	if err == nil {
		b.Key = key
	}
	return err
}

// NewBlobRef creates a new BlobRef as follows:
//	- Set a new UUID to Key
//	- Initialize Size and ObjectName as specified
//	- Set Status to BlobRefStatusInitializing
//	- Set current time to Timestamps (both created and updated at)
func NewBlobRef(size int64, storeKey, recordKey string) *BlobRef {
	return &BlobRef{
		Key:        uuid.New(),
		Size:       size,
		Status:     StatusInitializing,
		StoreKey:   storeKey,
		RecordKey:  recordKey,
		Timestamps: timestamps.New(),
	}
}

// ObjectPath returns an object path for the backend blob storage.
func (b *BlobRef) ObjectPath() string {
	return b.Key.String()
}
