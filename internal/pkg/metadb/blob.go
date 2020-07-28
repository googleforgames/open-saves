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
	"cloud.google.com/go/datastore"
)

// BlobStatus represents the current blob status.
//
// Life of a Blob
//
// [New Record created] --> [new Blob entity with BlobStatusInitializing]
//                         /               \
//                        / fail            \  success
//                       v                   v
//                [BlobStatusError]       [BlobStatusReady]
//                       |                   |
//      Upload new blob  |                   | Record deleted or new blob uploaded
//            or         |                   v
//     delete the record |               [BlobStatusPendingDeletion]
//                       v                  /
//  [Delete the blob entity] <-------------/   New blob is ready
//
type BlobStatus int16

const (
	// BlobStatusUnknown represents internal error.
	BlobStatusUnknown = BlobStatus(iota)
	// BlobStatusInitializing means the blob is currently being prepared, i.e.
	// being uploaded to the blob store.
	BlobStatusInitializing
	// BlobStatusReady means the blob is committed and ready for use.
	BlobStatusReady
	// BlobStatusPendingDeletion means the blob is no longer referenced by
	// any Record entities and needs to be deleted.
	BlobStatusPendingDeletion
	// BlobStatusError means the blob was not uploaded due to client or server
	// errors and the corresponding record needs to be updated (either by retrying
	// blob upload or deleting the entry).s
	BlobStatusError
)

// Blob is a metadata document to keep track of blobs stored in an external blob store.
type Blob struct {
	// Key is the primary key for the blob entry
	Key string `datastore:"-"`
	// Size is the byte size of the blob
	Size int64
	// ObjectName represents the object name stored in the blob store.
	ObjectName string
	// Status is the current status of the blob
	Status BlobStatus

	// Timestamps keeps track of creation and modification times and stores a randomly
	// generated UUID to maintain consistency.
	Timestamps Timestamps
}

// Assert Blob implements both PropertyLoadSave and KeyLoader.
var _ datastore.PropertyLoadSaver = new(Blob)
var _ datastore.KeyLoader = new(Blob)

// These functions need to be implemented here instead of the datastore package because
// go doesn't permit to define additional receivers in another package.
// Save and Load replicates the default behaviors, however, they are required
// for the KeyLoader interface.

// Save implements the Datastore PropertyLoadSaver interface and converts the properties
// field in the struct to separate Datastore properties.
func (b *Blob) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(b)
}

// Load implements the Datastore PropertyLoadSaver interface and converts Datstore
// properties to the Properties field.
func (b *Blob) Load(ps []datastore.Property) error {
	return datastore.LoadStruct(b, ps)
}

// LoadKey implements the KeyLoader interface and sets the value to the Key field.
func (b *Blob) LoadKey(k *datastore.Key) error {
	b.Key = k.Name
	return nil
}
