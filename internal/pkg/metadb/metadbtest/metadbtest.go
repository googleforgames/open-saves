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

package metadbtest

import (
	"testing"
	"time"

	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/blobref/chunkref"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/record"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/store"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/timestamps"
	"github.com/stretchr/testify/assert"
)

func assertEqualChecksums(t *testing.T, expected, actual checksums.Checksums, msgAndArgs ...interface{}) {
	t.Helper()
	if len(expected.MD5) == 0 {
		assert.Empty(t, actual.MD5, msgAndArgs...)
	} else {
		assert.Equal(t, expected.MD5, actual.MD5, msgAndArgs...)
	}
	assert.Equal(t, expected.CRC32C, actual.CRC32C, msgAndArgs...)
	assert.Equal(t, expected.HasCRC32C, actual.HasCRC32C, msgAndArgs...)
}

func assertTimestampsWithinDuration(t *testing.T, expected, actual *timestamps.Timestamps, delta time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	assert.WithinDuration(t, expected.CreatedAt, actual.CreatedAt, delta, msgAndArgs...)
	assert.WithinDuration(t, expected.UpdatedAt, actual.UpdatedAt, delta, msgAndArgs...)
	assert.Equal(t, expected.Signature, actual.Signature, msgAndArgs...)
}

// AssertEqualStore is equivalent to
// AssertEqualStoreWithinDuration(t, expected, actual, time.Duration(0), msgAndArgs...)
func AssertEqualStore(t *testing.T, expected, actual *store.Store, msgAndArgs ...interface{}) {
	t.Helper()
	AssertEqualStoreWithinDuration(t, expected, actual, time.Duration(0), msgAndArgs...)
}

// AssertEqualStoreWithinDuration compares each field in store.Store and asserts the timestamps
// are within delta.
func AssertEqualStoreWithinDuration(t *testing.T, expected, actual *store.Store, delta time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected.Key, actual.Key, msgAndArgs...)
		assert.Equal(t, expected.Name, actual.Name, msgAndArgs...)
		assert.Equal(t, expected.OwnerID, actual.OwnerID, msgAndArgs...)
		assert.ElementsMatch(t, expected.Tags, actual.Tags, msgAndArgs...)
		assertTimestampsWithinDuration(t, &expected.Timestamps, &actual.Timestamps, delta, msgAndArgs...)
	}
}

// AssertEqualRecord is equivalent to
// AssertEqualRecordWithinDuration(t, expected, actual, time.Duration(0), msgAndArgs...)
func AssertEqualRecord(t *testing.T, expected, actual *record.Record, msgAndArgs ...interface{}) {
	t.Helper()
	AssertEqualRecordWithinDuration(t, expected, actual, time.Duration(0), msgAndArgs...)
}

// AssertEqualRecordWithinDuration compares each field in record.Record and asserts the timestamps
// are within delta.
func AssertEqualRecordWithinDuration(t *testing.T, expected, actual *record.Record, delta time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected.Key, actual.Key, msgAndArgs...)
		assert.Equal(t, expected.Blob, actual.Blob, msgAndArgs...)
		assert.Equal(t, expected.BlobSize, actual.BlobSize, msgAndArgs...)
		assert.Equal(t, expected.Properties, actual.Properties, msgAndArgs...)
		assert.ElementsMatch(t, expected.Tags, actual.Tags, msgAndArgs...)
		assert.Equal(t, expected.OwnerID, actual.OwnerID, msgAndArgs...)
		assertEqualChecksums(t, expected.Checksums, actual.Checksums, msgAndArgs...)
		assertTimestampsWithinDuration(t, &expected.Timestamps, &actual.Timestamps, delta, msgAndArgs...)
	}
}

// AssertEqualBlobRef is equivalent to
// AssertEqualBlobRefWithinDuration(t, expected, actual, time.Duration(0), msgAndArgs...)
func AssertEqualBlobRef(t *testing.T, expected, actual *blobref.BlobRef, msgAndArgs ...interface{}) {
	t.Helper()
	AssertEqualBlobRefWithinDuration(t, expected, actual, time.Duration(0), msgAndArgs...)
}

// AssertEqualBlobRefWithinDuration compares each field in blobref.BlobRef and asserts the timestamps
// are within delta.
func AssertEqualBlobRefWithinDuration(t *testing.T, expected, actual *blobref.BlobRef, delta time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected.Key, actual.Key, msgAndArgs...)
		assert.Equal(t, expected.RecordKey, actual.RecordKey, msgAndArgs...)
		assert.Equal(t, expected.Size, actual.Size, msgAndArgs...)
		assert.Equal(t, expected.Status, actual.Status, msgAndArgs...)
		assert.Equal(t, expected.StoreKey, actual.StoreKey, msgAndArgs...)
		assert.Equal(t, expected.RecordKey, actual.RecordKey, msgAndArgs...)
		assert.Equal(t, expected.Chunked, actual.Chunked, msgAndArgs...)
		assertEqualChecksums(t, expected.Checksums, actual.Checksums, msgAndArgs...)
		assertTimestampsWithinDuration(t, &expected.Timestamps, &actual.Timestamps, delta, msgAndArgs...)
	}
}

// AssertEqualChunkRef is equivalent to
// AssertEqualChunkRefWithinDuration(t, expected, actual, time.Duration(0), msgAndArgs...)
func AssertEqualChunkRef(t *testing.T, expected, actual *chunkref.ChunkRef, msgAndArgs ...interface{}) {
	t.Helper()
	AssertEqualChunkRefWithinDuration(t, expected, actual, time.Duration(0), msgAndArgs...)
}

// AssertEqualChunkRefWithinDuration compares each field in chunkref.ChunkRef and asserts they are equal
// (timestamps are within delta).
func AssertEqualChunkRefWithinDuration(t *testing.T, expected, actual *chunkref.ChunkRef, delta time.Duration, msgAndArgs ...interface{}) {
	t.Helper()
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	if assert.NotNil(t, actual) {
		assert.Equal(t, expected.BlobRef, actual.BlobRef, msgAndArgs...)
		assert.Equal(t, expected.Key, actual.Key, msgAndArgs...)
		assert.Equal(t, expected.Number, actual.Number, msgAndArgs...)
		assert.Equal(t, expected.Size, actual.Size, msgAndArgs...)
		assert.Equal(t, expected.Status, actual.Status, msgAndArgs...)
		assertEqualChecksums(t, expected.Checksums, actual.Checksums, msgAndArgs...)
		assertTimestampsWithinDuration(t, &expected.Timestamps, &actual.Timestamps, delta, msgAndArgs...)
	}
}
