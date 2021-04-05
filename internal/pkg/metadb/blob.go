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
	"errors"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
)

// BlobRefStatus represents a current blob status.
//
// Life of a Blob
//
// [New Record created] --> [new BlobRef entity with BlobRefStatusInitializing]
//                         /               \
//                        / fail            \  success
//                       v                   v
//                [BlobRefStatusError]       [BlobRefStatusReady]
//                       |       x            |
//      Upload new blob  |        \ fail      | Record deleted or new blob uploaded
//            or         |         \          v
//     delete the record |          -----[BlobRefStatusPendingDeletion]
//                       v                  /
//  [Delete the blob entity] <-------------/   Garbage collection
type BlobRefStatus int16

const (
	// BlobRefStatusUnknown represents internal error.
	BlobRefStatusUnknown BlobRefStatus = iota
	// BlobRefStatusInitializing means the blob is currently being prepared, i.e.
	// being uploaded to the blob store.
	BlobRefStatusInitializing
	// BlobRefStatusReady means the blob is committed and ready for use.
	BlobRefStatusReady
	// BlobRefStatusPendingDeletion means the blob is no longer referenced by
	// any Record entities and needs to be deleted.
	BlobRefStatusPendingDeletion
	// BlobRefStatusError means the blob was not uploaded due to client or server
	// errors and the corresponding record needs to be updated (either by
	// retrying blob upload or deleting the entry).
	BlobRefStatusError
)

// BlobRef is a metadata document to keep track of blobs stored in an external blob store.
type BlobRef struct {
	// Key is the primary key for the blob entry
	Key uuid.UUID `datastore:"-"`
	// Size is the byte size of the blob
	Size int64
	// Status is the current status of the blob
	Status BlobRefStatus
	// StoreKey is the key of the store that the blob belongs to
	StoreKey string
	// RecordKey is the key of the record that the blob belongs to
	// It can be non-existent (e.g. deleted already) but then the Status
	// should not be Blob StatusReady.
	RecordKey string

	// Timestamps keeps track of creation and modification times and stores a randomly
	// generated UUID to maintain consistency.
	Timestamps Timestamps
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
	b := new(BlobRef)
	b.Key = uuid.New()
	b.Size = size
	b.Status = BlobRefStatusInitializing
	b.StoreKey = storeKey
	b.RecordKey = recordKey
	// Nanosecond is fine as it will not be returned to clients.
	b.Timestamps.NewTimestamps(time.Nanosecond)
	return b
}

// Ready changes Status to BlobRefStatusReady and updates Timestamps.
// It returns an error if the current Status is not BlobRefStatusInitializing.
func (b *BlobRef) Ready() error {
	if b.Status != BlobRefStatusInitializing {
		return errors.New("Ready was called when Status is not Initializing")
	}
	b.Status = BlobRefStatusReady
	b.Timestamps.UpdateTimestamps(time.Nanosecond)
	return nil
}

// MarkForDeletion marks the BlobRef as BlobRefStatusPendingDeletion and updates Timestamps.
// Returns an error if the current Status is not BlobRefStatusReady.
func (b *BlobRef) MarkForDeletion() error {
	if b.Status != BlobRefStatusInitializing && b.Status != BlobRefStatusReady {
		return errors.New("MarkForDeletion was called when Status is not either Initializing or Ready")
	}
	b.Status = BlobRefStatusPendingDeletion
	b.Timestamps.UpdateTimestamps(time.Nanosecond)
	return nil
}

// Fail marks the BlobRef as BlobRefStatusError and updates Timestamps.
// Any state can transition to BlobRefStatusError.
func (b *BlobRef) Fail() error {
	b.Status = BlobRefStatusError
	b.Timestamps.UpdateTimestamps(time.Nanosecond)
	return nil
}

// ObjectPath returns an object path for the backend blob storage.
func (b *BlobRef) ObjectPath() string {
	return b.Key.String()
}

// BlobRefCursor is a database cursor for BlobRef.
type BlobRefCursor interface {
	// Next advances the iterator and returns the next value.
	// Returns nil and an iterator.Done at the end of the iterator.
	Next() (*BlobRef, error)
}
