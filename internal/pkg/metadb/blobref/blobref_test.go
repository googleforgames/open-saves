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
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums/checksumstest"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"github.com/stretchr/testify/assert"
)

func TestBlobRef_New(t *testing.T) {
	const (
		store      = "store"
		record     = "record"
		chunkCount = int64(42)
	)

	b := NewChunkedBlobRef(store, record, chunkCount)
	if assert.NotNil(t, b) {
		assert.Equal(t, store, b.StoreKey)
		assert.Equal(t, record, b.RecordKey)
		assert.NotEqual(t, uuid.Nil, b.Key)
		assert.Equal(t, chunkCount, b.ChunkCount)
	}
}

func TestBlobRef_LoadKey(t *testing.T) {
	key := uuid.MustParse("d13c289c-8845-485f-b582-c87342d5dade")
	blob := new(BlobRef)
	assert.NoError(t, blob.LoadKey(datastore.NameKey("blob", key.String(), nil)))
	assert.Equal(t, key, blob.Key)
}

func TestBlobRef_Save(t *testing.T) {
	const (
		size       = int64(123)
		objectName = "object name"
		store      = "store"
		record     = "record"
		chunkCount = int64(42)
	)

	blob := BlobRef{
		Size:       size,
		Status:     StatusInitializing,
		StoreKey:   store,
		RecordKey:  record,
		ChunkCount: chunkCount,
		Checksums:  checksumstest.RandomChecksums(t),
		Timestamps: timestamps.New(),
	}

	expected := []datastore.Property{
		{
			Name:  "Size",
			Value: size,
		},
		{
			Name:  "Status",
			Value: int64(StatusInitializing),
		},
		{
			Name:  "StoreKey",
			Value: store,
		},
		{
			Name:  "RecordKey",
			Value: record,
		},
		{
			Name:  "Chunked",
			Value: false,
		},
		{
			Name:  "ChunkCount",
			Value: chunkCount,
		},
	}
	actual, err := blob.Save()
	assert.NoError(t, err)
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected, actual[:len(expected)])
		if assert.Equal(t, len(expected)+3+1, len(actual)) {
			checksumstest.AssertPropertyListMatch(t, blob.Checksums, actual[len(expected):len(expected)+3])
			assert.Equal(t, "Timestamps", actual[len(expected)+3].Name)
		}
	}
}

func TestBlobRef_Load(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		ps   []datastore.Property
		want *BlobRef
	}{
		{
			name: "canonical",
			ps: []datastore.Property{
				{
					Name:  "Size",
					Value: int64(123),
				},
				{
					Name:  "Status",
					Value: int64(StatusReady),
				},
				{
					Name:  "StoreKey",
					Value: "store key",
				},
				{
					Name:  "RecordKey",
					Value: "record key",
				},
				{
					Name:  "Chunked",
					Value: true,
				},
				{
					Name:  "ChunkCount",
					Value: int64(551),
				},
			},
			want: &BlobRef{
				Size:       123,
				Status:     StatusReady,
				StoreKey:   "store key",
				RecordKey:  "record key",
				Chunked:    true,
				ChunkCount: 551,
				Checksums:  checksumstest.RandomChecksums(t),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cs := checksumstest.RandomChecksums(t)
			ps := append(tc.ps, checksumstest.ChecksumsToProperties(t, cs)...)
			got := &BlobRef{}
			if err := got.Load(ps); err != nil {
				t.Errorf("Load() failed: %v", err)
			}
			if diff := cmp.Diff(tc.want, got, cmpopts.IgnoreTypes(checksums.Checksums{})); diff != "" {
				t.Errorf("Load() = (-want, +got):\n%s", diff)
			}
			if diff := cmp.Diff(cs, got.Checksums); diff != "" {
				t.Errorf("Load() Checksums = (-want, +got):\n%s", diff)
			}
		})
	}
}

func TestBlobRef_GetObjectPath(t *testing.T) {
	blob := NewBlobRef(0, "", "")

	assert.Equal(t, blob.Key.String(), blob.ObjectPath())
}

func TestBlobRef_ToProto(t *testing.T) {
	const (
		size       = int64(123)
		store      = "store"
		record     = "record"
		chunkCount = int64(42)
	)
	b := NewChunkedBlobRef(store, record, chunkCount)
	b.Checksums = checksumstest.RandomChecksums(t)
	b.Size = size
	b.ChunkCount = chunkCount

	proto := b.ToProto()
	if assert.NotNil(t, proto) {
		assert.Equal(t, b.StoreKey, proto.GetStoreKey())
		assert.Equal(t, b.RecordKey, proto.GetRecordKey())
		assert.Equal(t, b.Size, proto.GetSize())
		assert.True(t, b.Chunked)
		assert.Equal(t, b.ChunkCount, chunkCount)
		checksumstest.AssertProtoEqual(t, b.Checksums, proto)
	}
}
