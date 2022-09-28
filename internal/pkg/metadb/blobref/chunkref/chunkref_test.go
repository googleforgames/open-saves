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

package chunkref_test

import (
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref/chunkref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums/checksumstest"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"github.com/stretchr/testify/assert"
)

func TestChunkRef_New(t *testing.T) {
	blobuuid := uuid.New()
	c := chunkref.New(blobuuid, 42)
	assert.NotEqual(t, uuid.Nil, c.Key)
	assert.Equal(t, blobuuid, c.BlobRef)
	assert.Equal(t, int32(42), c.Number)
	assert.Equal(t, int32(0), c.Size)
	assert.Equal(t, blobref.StatusInitializing, c.Status)
	assert.NotEqual(t, uuid.Nil, c.Timestamps.Signature)
}

func TestChunkRef_ObjectPath(t *testing.T) {
	c := chunkref.New(uuid.Nil, 0)
	assert.Equal(t, c.Key.String(), c.ObjectPath())
}

func TestChunkRef_SaveLoad(t *testing.T) {
	c := chunkref.New(uuid.New(), 42)
	c.Checksums = checksumstest.RandomChecksums(t)
	c.Timestamps = timestamps.New()
	ps, err := c.Save()
	if err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	assert.Len(t, ps, 7)
	loaded := new(chunkref.ChunkRef)
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
	c := new(chunkref.ChunkRef)
	if err := c.LoadKey(datastore.NameKey("chunk", testUUID.String(), nil)); assert.NoError(t, err) {
		assert.Equal(t, testUUID, c.Key)
		assert.Equal(t, uuid.Nil, c.BlobRef)
	}

	c = new(chunkref.ChunkRef)
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
	assert.Equal(t, testUUID.String(), chunkref.CacheKey(testUUID))
	c := chunkref.New(uuid.Nil, 0)
	assert.Equal(t, c.Key.String(), c.CacheKey())
}

func TestChunkRef_EncodeDecodeBytes(t *testing.T) {
	c := chunkref.New(uuid.New(), 42)
	encoded, err := c.EncodeBytes()
	if err != nil {
		t.Fatalf("EncodeBytes failed with error: %v", err)
	}
	assert.NotEmpty(t, encoded)

	decoded := new(chunkref.ChunkRef)
	if err := decoded.DecodeBytes(encoded); err != nil {
		t.Fatalf("DecodeBytes failed with error: %v", err)
	}
	if diff := cmp.Diff(c, decoded); diff != "" {
		t.Errorf("DecodeBytes() = (-want, +got):\n%s", diff)
	}
}

func TestChunkRef_ToProto(t *testing.T) {
	blobKey := uuid.New()
	c := chunkref.New(blobKey, 42)
	c.Size = 12345
	c.Checksums = checksumstest.RandomChecksums(t)
	proto := c.ToProto()
	if assert.NotNil(t, proto) {
		assert.Equal(t, blobKey.String(), proto.GetSessionId())
		assert.EqualValues(t, c.Size, proto.GetSize())
		assert.EqualValues(t, c.Number, proto.GetNumber())
		checksumstest.AssertProtoEqual(t, c.Checksums, proto)
	}
}
