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
	"bytes"
	"context"
	"testing"
)

func getBucket(t *testing.T) *BlobGCP {
	gcs, err := NewBlobGCP("gs://triton-integration")
	if err != nil {
		t.Fatalf("Initializing bucket error: %v", err)
	}
	return gcs
}

func TestGCS_OpenBucket(t *testing.T) {
	gcs, err := NewBlobGCP("gs://triton-integration")
	if err != nil {
		t.Fatalf("failed to initialize gcs backend: %v", err)
	}
	if gcs.bucket == nil {
		t.Fatalf("initialized gcs bucket but got nil")
	}
}

func TestGCS_PutFile(t *testing.T) {
	ctx := context.Background()
	gcs := getBucket(t)
	filePath := "put.txt"
	if err := gcs.Put(ctx, filePath, []byte("hello world")); err != nil {
		t.Fatalf("Put file(%q) in GCS got error: %v", filePath, err)
	}
}

func TestGCS_GetFile(t *testing.T) {
	ctx := context.Background()
	gcs := getBucket(t)
	filePath := "get.txt"

	if err := gcs.Put(ctx, filePath, []byte("hello world")); err != nil {
		t.Fatalf("Put file(%q) in GCS got error: %v", filePath, err)
	}

	got, err := gcs.Get(ctx, filePath)
	if err != nil {
		t.Fatalf("Get file(%q) from GCS got error: %v", filePath, err)
	}
	want := []byte("hello world")
	if !bytes.Equal(got, want) {
		t.Fatalf("Get file(%q) from GCS failed\ngot:  %s\nwant: %s", filePath, got, want)
	}
}

func TestGCS_Delete(t *testing.T) {
	ctx := context.Background()
	gcs := getBucket(t)
	filePath := "delete.txt"
	if err := gcs.Put(ctx, filePath, []byte("hello world")); err != nil {
		t.Fatalf("Put file(%q) in GCS got error: %v", filePath, err)
	}

	if err := gcs.Delete(ctx, filePath); err != nil {
		t.Fatalf("Delete file(%q) in GCS got error: %v", filePath, err)
	}

	// Check to see access to this file fails now that it has been deleted.
	if _, err := gcs.Get(ctx, filePath); err == nil {
		t.Fatalf("Get should fail after file has been deleted, got nil")
	}
}
