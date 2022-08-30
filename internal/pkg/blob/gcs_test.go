// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blob

import (
	"context"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "gocloud.dev/blob/memblob"
	"gocloud.dev/gcerrors"
)

// These unit tests use the in-memory driver instead of actual Cloud Storage.
// It provides the same interface and serves as a mock.
const testBucket = "mem://"

func getBucket(ctx context.Context, t *testing.T) *BlobGCP {
	t.Helper()

	gcs, err := NewBlobGCP(ctx, testBucket)
	if err != nil {
		t.Fatalf("Initializing bucket error: %v", err)
	}
	t.Cleanup(func() { assert.NoError(t, gcs.Close(), "Close should not fail.") })
	return gcs
}

// Test that the gs:// url works.
func TestGCS_OpenBucket(t *testing.T) {
	t.Parallel()

	// Any bucket name works as it doesn't actually send requests.
	gcs, err := NewBlobGCP(context.Background(), "gs://bucket")
	assert.NotNil(t, gcs)
	assert.NoError(t, err)
	assert.NoError(t, gcs.Close())
}

func TestGCS_PutGetDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	gcs := getBucket(ctx, t)
	const (
		filePath   = "get.txt"
		testString = "hello world"
	)

	// Should return gcerrors.NotFound
	got, err := gcs.Get(ctx, filePath)
	assert.Nil(t, got, "Get should return nil when object is not found.")
	assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err),
		"Get should return gcerrors.NotFound when object is not found.")

	err = gcs.Put(ctx, filePath, []byte(testString))
	assert.NoError(t, err, "Put should not return error.")

	got, err = gcs.Get(ctx, filePath)
	assert.NoError(t, err, "Get should not return error.")
	assert.Equal(t, []byte(testString), got, "Get should return the content of the object.")

	err = gcs.Delete(ctx, filePath)
	assert.NoError(t, err, "Delete should not return error.")

	// Check to see access to this file fails now that it has been deleted.
	got, err = gcs.Get(ctx, filePath)
	assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err),
		"Get should return gcerrors.NotFound after object has been deleted.")

	err = gcs.Delete(ctx, filePath)
	assert.Equal(t, gcerrors.NotFound, gcerrors.Code(err),
		"Delete should return gcerrors.NotFound when object is not found.")
}

func TestGCS_SimpleStreamTests(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	gcs := getBucket(ctx, t)
	const filePath = "simple-stream-tests.txt"
	testBlob := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit")

	writer, err := gcs.NewWriter(ctx, filePath)
	require.NotNilf(t, writer, "NewWriter(%q) should not return nil.", filePath)
	require.NoErrorf(t, err, "NewWriter(%q) should not return error.", filePath)

	n, err := writer.Write(testBlob[:10])
	assert.NoError(t, err)
	assert.Equal(t, 10, n)
	n, err = writer.Write(testBlob[10:])
	assert.NoError(t, err)
	assert.Equal(t, len(testBlob)-10, n)
	err = writer.Close()
	require.NoError(t, err, "writer.Close should not return error.")
	t.Cleanup(func() { assert.NoError(t, gcs.Delete(ctx, filePath)) })

	reader, err := gcs.NewReader(ctx, filePath)
	require.NotNilf(t, writer, "NewReader(%q) should not return nil.", filePath)
	require.NoErrorf(t, err, "NewReader(%q) should not return error.", filePath)

	err = iotest.TestReader(reader, testBlob)
	assert.NoError(t, err, "TestReader should not return error.")
	assert.NoError(t, reader.Close())

	const (
		offset = 3
		length = 5
	)
	rangeReader, err := gcs.NewRangeReader(ctx, filePath, offset, length)
	require.NotNilf(t, rangeReader, "NewRangeReader(%q, %q, %q) should not return nil.", filePath, offset, length)
	require.NoErrorf(t, err, "NewRangeReader(%q, %q, %q) should not error.", filePath, offset, length)

	iotest.TestReader(rangeReader, testBlob[offset:offset+length])
	assert.NoError(t, err, "TestReader should not return error.")
	assert.NoError(t, rangeReader.Close())
}
