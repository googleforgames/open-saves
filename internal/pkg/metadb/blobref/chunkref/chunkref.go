// Copyright 2021 Google LLC
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

package chunkref

import (
	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/cache"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"github.com/vmihailenco/msgpack/v5"
)

// ChunkRef is a metadata entity to keep track of chunks stored in an external blob store.
// It is a child entity of and always associated to a BlobRef.
type ChunkRef struct {
	// Key is the primary key of the ChunkRef.
	Key uuid.UUID `datastore:"-"`
	// BlobRef is the key of parent BlobRef.
	BlobRef uuid.UUID `datastore:"-"`
	// Number is the position of the chunk in the BlobRef.
	Number int32
	// Size is the byte size of the chunk.
	Size int32

	// Deprecated, present for backwards compatibility.
	// Status is the current status of the chunk.
	blobref.Status `datastore:",omitempty"`

	// Checksums contains checksums for the chunk object.
	checksums.Checksums `datastore:",flatten"`

	// Timestamps keeps track of creation and modification times and stores a randomly
	// generated UUID to maintain consistency.
	Timestamps timestamps.Timestamps
}

// Assert ChunkRef implements both PropertyLoadSave and KeyLoader.
var _ datastore.PropertyLoadSaver = new(ChunkRef)
var _ datastore.KeyLoader = new(ChunkRef)

// Assert ChunkRef implements Cacheable
var _ cache.Cacheable = new(ChunkRef)

func (c *ChunkRef) LoadKey(k *datastore.Key) error {
	if uuidKey, err := uuid.Parse(k.Name); err == nil {
		c.Key = uuidKey
	} else {
		return err
	}
	if k.Parent != nil {
		if uuidParent, err := uuid.Parse(k.Parent.Name); err == nil {
			c.BlobRef = uuidParent
		} else {
			return err
		}
	}
	return nil
}

// Save and Load replicates the default behaviors, however, they are required
// for the KeyLoader interface.

// Load implements the Datastore PropertyLoadSaver interface and converts Datastore
// properties to corresponding struct fields.
func (c *ChunkRef) Load(ps []datastore.Property) error {
	return datastore.LoadStruct(c, ps)
}

// Save implements the Datastore PropertyLoadSaver interface and converts struct fields
// to Datastore properties.
func (c *ChunkRef) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(c)
}

func (c *ChunkRef) ObjectPath() string {
	return c.Key.String()
}

// New creates a new ChunkRef instance with the input parameters.
func New(blobRef uuid.UUID, number int32) *ChunkRef {
	return &ChunkRef{
		Key:        uuid.New(),
		BlobRef:    blobRef,
		Number:     number,
		Timestamps: timestamps.New(),
	}
}

// CacheKey returns a cache key string to store in the cache.
// It returns a string representation of the uuid key.
func CacheKey(u uuid.UUID) string {
	return u.String()
}

// Cacheable implementations.

// CacheKey returns a cache key string to store in the cache.
// It returns a string representation of the uuid key.
func (c ChunkRef) CacheKey() string {
	return CacheKey(c.Key)
}

// EncodeBytes returns a serialized byte slice of the object.
func (c *ChunkRef) EncodeBytes() ([]byte, error) {
	b, err := msgpack.Marshal(c)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// DecodeBytes deserializes the byte slice given by by.
func (c *ChunkRef) DecodeBytes(by []byte) error {
	return msgpack.Unmarshal(by, c)
}

// ToProto converts returns a pb.ChunkMetadata representation of the
// ChunkRef object.
func (c *ChunkRef) ToProto() *pb.ChunkMetadata {
	return &pb.ChunkMetadata{
		SessionId: c.BlobRef.String(),
		Number:    int64(c.Number),
		Size:      int64(c.Size),
		Md5:       c.MD5,
		Crc32C:    c.GetCRC32C(),
		HasCrc32C: c.HasCRC32C,
	}
}
