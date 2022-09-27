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
	"fmt"
	"io"
	"testing"
	"testing/iotest"

	"github.com/google/go-cmp/cmp"
	_ "gocloud.dev/blob/memblob"
	"gocloud.dev/gcerrors"
)

// These unit tests use the in-memory driver instead of actual Cloud Storage.
// It provides the same interface and serves as a mock.
const testBucket = "mem://"

func mustGetBucket(ctx context.Context, t *testing.T) *BlobGCP {
	t.Helper()

	gcs, err := NewBlobGCP(ctx, testBucket)
	if err != nil {
		t.Fatalf("Initializing bucket error: %v", err)
	}
	t.Cleanup(func() {
		if err := gcs.Close(); err != nil {
			t.Errorf("Close() failed: %v", err)
		}
	})
	return gcs
}

// Test that the gs:// url works.
func TestGCS_GCPBucketSmokeTest(t *testing.T) {
	t.Parallel()

	// Any bucket name works as it doesn't actually send requests.
	gcs, err := NewBlobGCP(context.Background(), "gs://bucket")
	if err != nil {
		t.Errorf("NewBlobGCP() failed: %v", err)
	}
	if gcs == nil {
		t.Errorf("NewBlobGCP() = %v, want nil", gcs)
	} else {
		if err := gcs.Close(); err != nil {
			t.Errorf("Close() failed: %v", err)
		}
	}
}

func TestGCS_PutGetDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	gcs := mustGetBucket(ctx, t)
	const (
		filePath   = "get.txt"
		testString = "hello world"
	)

	// Should return gcerrors.NotFound
	got, err := gcs.Get(ctx, filePath)
	if got != nil {
		t.Errorf("Get() = %v, want nil", got)
	}
	if gcerrors.Code(err) != gcerrors.NotFound {
		t.Errorf("Get() = %v, want gcerrors.NotFound", err)
	}

	if err := gcs.Put(ctx, filePath, []byte(testString)); err != nil {
		t.Errorf("Put() failed: %v", err)
	}

	got, err = gcs.Get(ctx, filePath)
	if err != nil {
		t.Errorf("Get() failed: %v", err)
	}
	if diff := cmp.Diff([]byte(testString), got); diff != "" {
		t.Errorf("Get() = (-want, +got):\n%s", got)
	}

	if err := gcs.Delete(ctx, filePath); err != nil {
		t.Errorf("Delete() failed: %v", err)
	}

	// Check to see access to this file fails now that it has been deleted.
	_, err = gcs.Get(ctx, filePath)
	if gcerrors.Code(err) != gcerrors.NotFound {
		t.Errorf("Get() = %v, want = gcerrors.NotFound", err)
	}

	if err = gcs.Delete(ctx, filePath); gcerrors.Code(err) != gcerrors.NotFound {
		t.Errorf("Delete() = %v, want gcerrors.NotFound", err)
	}
}

func testReader(t *testing.T, name string, rd io.ReadCloser, b []byte) {
	t.Run(name, func(t *testing.T) {
		if rd == nil {
			t.Fatalf("rd = %v, want non-nil", rd)
		}
		if err := iotest.TestReader(rd, b); err != nil {
			t.Errorf("iotest.TestReader() failed: %v", err)
		}
		if err := rd.Close(); err != nil {
			t.Errorf("reader.Close() failed: %v", err)
		}
	})
}

func TestGCS_SimpleStreamTests(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	gcs := mustGetBucket(ctx, t)
	const filePath = "simple-stream-tests.txt"
	testBlob := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit")

	writer, err := gcs.NewWriter(ctx, filePath)
	if writer == nil {
		t.Fatalf("NewWriter(%q) = %v, want non-nil", filePath, writer)
	}
	if err != nil {
		t.Fatalf("NewWriter(%q) failed: %v", filePath, err)
	}

	for _, tc := range []struct {
		start, end, want int
	}{
		{0, 10, 10},
		{10, len(testBlob), len(testBlob) - 10},
	} {
		t.Run(fmt.Sprintf("Write [%v:%v]", tc.start, tc.end), func(t *testing.T) {
			got, err := writer.Write(testBlob[tc.start:tc.end])
			if err != nil {
				t.Errorf("Write() failed: %v", err)
			}
			if tc.want != got {
				t.Errorf("Write() = %v, want %v", got, tc.want)
			}
		})
	}
	if err := writer.Close(); err != nil {
		t.Errorf("writer.Close() failed: %v", err)
	}
	t.Cleanup(func() {
		if err := gcs.Delete(ctx, filePath); err != nil {
			t.Errorf("Delete() failed: %v", err)
		}
	})

	if reader, err := gcs.NewReader(ctx, filePath); err != nil {
		t.Errorf("NewReader(%q) failed: %v", filePath, err)
	} else {
		testReader(t, "NewReader", reader, testBlob)
	}

	const (
		offset = 3
		length = 5
	)
	if reader, err := gcs.NewRangeReader(ctx, filePath, offset, length); err != nil {
		t.Errorf("NewRangeReader(%q, %q, %q) failed: %v", filePath, offset, length, err)
	} else {
		testReader(t, "NewRangeReader", reader, testBlob[offset:offset+length])
	}
}
