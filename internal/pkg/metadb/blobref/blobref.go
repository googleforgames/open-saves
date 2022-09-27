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
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums"
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
	// Chunked is whether the BlobRef is chunked or not.
	Chunked bool
	// ChunkCount is the number of chunks that should be associated to the BlobRef.
	// It is set by either the client when starting a chunk upload or
	// the server when committing a chunked upload.
	ChunkCount int64

	// Checksums have checksums for each blob object associated with the BlobRef entity.
	// Record.{MD5,CRC32C} must be used for inline blobs, and
	// ChunkRef.{MD5,CRC32C} must be used for chunked blobs.
	checksums.Checksums `datastore:",flatten"`

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

// NewChunkedBlobRef creates a new BlobRef object with Size, Chunked, and
// ChunkCount set to 0, true, and chunkCount respectively.
// Other behaviors are the same as NewBlobRef
func NewChunkedBlobRef(storeKey, recordKey string, chunkCount int64) *BlobRef {
	b := NewBlobRef(0, storeKey, recordKey)
	b.Chunked = true
	b.ChunkCount = chunkCount
	return b
}

// ObjectPath returns an object path for the backend blob storage.
func (b *BlobRef) ObjectPath() string {
	return b.Key.String()
}

// ToProto returns a BlobMetadata representation of the object.
func (b *BlobRef) ToProto() *pb.BlobMetadata {
	return &pb.BlobMetadata{
		StoreKey:   b.StoreKey,
		RecordKey:  b.RecordKey,
		Size:       b.Size,
		Md5:        b.MD5,
		Chunked:    b.Chunked,
		ChunkCount: b.ChunkCount,
		Crc32C:     b.GetCRC32C(),
		HasCrc32C:  b.HasCRC32C,
	}
}
