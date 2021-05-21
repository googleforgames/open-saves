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
	"github.com/stretchr/testify/assert"
)

func TestChunkRef_New(t *testing.T) {
	blobuuid := uuid.New()
	c := New(blobuuid, 42, 123450)
	assert.NotEqual(t, uuid.Nil, c.Key)
	assert.Equal(t, blobuuid, c.BlobRef)
	assert.Equal(t, 42, c.Number)
	assert.Equal(t, 123450, c.Size)
}

func TestChunkRef_ObjectPath(t *testing.T) {
	c := New(uuid.Nil, 0, 0)
	assert.Equal(t, c.Key.String(), c.ObjectPath())
}

func TestChunkRef_SaveLoad(t *testing.T) {
	c := New(uuid.New(), 42, 24)
	ps, err := c.Save()
	if err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	assert.Len(t, ps, 3)
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

func TestChunkRef_EncodeDecodeBytes(t *testing.T) {
	c := New(uuid.New(), 42, 24)
	encoded, err := c.EncodeBytes()
	if err != nil {
		t.Fatalf("EncodeBytes failed with error: %v", err)
	}
	assert.NotEmpty(t, encoded)

	decoded := new(ChunkRef)
	if err := decoded.DecodeBytes(encoded); err != nil {
		t.Fatalf("DecodeBytes failed with error: %v", err)
	}
	assert.Equal(t, c, decoded)
}
