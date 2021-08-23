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
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/stretchr/testify/assert"
)

func TestChunkRef_New(t *testing.T) {
	blobuuid := uuid.New()
	c := New(blobuuid, 42)
	assert.NotEqual(t, uuid.Nil, c.Key)
	assert.Equal(t, blobuuid, c.BlobRef)
	assert.Equal(t, int32(42), c.Number)
	assert.Equal(t, int32(0), c.Size)
	assert.Equal(t, blobref.StatusInitializing, c.Status)
	assert.NotEqual(t, uuid.Nil, c.Timestamps.Signature)
}

func TestChunkRef_ObjectPath(t *testing.T) {
	c := New(uuid.Nil, 0)
	assert.Equal(t, c.Key.String(), c.ObjectPath())
}

func TestChunkRef_SaveLoad(t *testing.T) {
	c := New(uuid.New(), 42)
	ps, err := c.Save()
	if err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	assert.Len(t, ps, 4)
	loaded := new(ChunkRef)
	if err := loaded.Load(ps); err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	// Reset Key and BlobRef as they are not loaded by Load()
	c.Key = uuid.Nil
	c.BlobRef = uuid.Nil
	assert.Equal(t, c, loaded)
}

func TestChunkRef_LoadKey(t *testing.T) {
	testUUID := uuid.New()
	c := new(ChunkRef)
	if err := c.LoadKey(datastore.NameKey("chunk", testUUID.String(), nil)); assert.NoError(t, err) {
		assert.Equal(t, testUUID, c.Key)
		assert.Equal(t, uuid.Nil, c.BlobRef)
	}

	c = new(ChunkRef)
	testBlobUUID := uuid.New()
	key := datastore.NameKey("chunk", testUUID.String(),
		datastore.NameKey("blobref", testBlobUUID.String(), nil))
	if err := c.LoadKey(key); assert.NoError(t, err) {
		assert.Equal(t, testUUID, c.Key)
		assert.Equal(t, testBlobUUID, c.BlobRef)
	}
}

func TestChunkRef_CacheKey(t *testing.T) {
	testUUID := uuid.New()
	assert.Equal(t, testUUID.String(), CacheKey(testUUID))
	c := New(uuid.Nil, 0)
	assert.Equal(t, c.Key.String(), c.CacheKey())
}

func TestChunkRef_EncodeDecodeBytes(t *testing.T) {
	c := New(uuid.New(), 42)
	encoded, err := c.EncodeBytes()
	if err != nil {
		t.Fatalf("EncodeBytes failed with error: %v", err)
	}
	assert.NotEmpty(t, encoded)

	decoded := new(ChunkRef)
	if err := decoded.DecodeBytes(encoded); err != nil {
		t.Fatalf("DecodeBytes failed with error: %v", err)
	}
	if assert.NotNil(t, decoded) {
		assert.Equal(t, c.BlobRef, decoded.BlobRef)
		assert.Equal(t, c.Key, decoded.Key)
		assert.Equal(t, c.Number, decoded.Number)
		assert.Equal(t, c.Size, decoded.Size)
		assert.Equal(t, c.Status, decoded.Status)
		assert.Equal(t, c.Timestamps.Signature, decoded.Timestamps.Signature)
		assert.True(t, c.Timestamps.CreatedAt.Equal(decoded.Timestamps.CreatedAt))
		assert.True(t, c.Timestamps.UpdatedAt.Equal(decoded.Timestamps.UpdatedAt))
	}
}

func TestChunkRef_ToProto(t *testing.T) {
	blobKey := uuid.New()
	c := New(blobKey, 42)
	c.Size = 12345
	proto := c.ToProto()
	if assert.NotNil(t, proto) {
		assert.Equal(t, blobKey.String(), proto.GetSessionId())
		assert.EqualValues(t, c.Size, proto.GetSize())
		assert.EqualValues(t, c.Number, proto.GetNumber())
	}
}
