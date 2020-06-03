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

func TestGCS_Write(t *testing.T) {
	ctx := context.Background()
	gcs := getBucket(t)
	filePath := "write.txt"
	if err := gcs.Write(ctx, filePath, []byte("hello world")); err != nil {
		t.Fatalf("Write to GCS with file(%q) error: %v", filePath, err)
	}
}

func TestGCS_Read(t *testing.T) {
	ctx := context.Background()
	gcs := getBucket(t)
	filePath := "read.txt"

	got, err := gcs.Read(ctx, filePath)
	if err != nil {
		t.Fatalf("Read from GCS with file(%q) got error: %v", filePath, err)
	}
	want := []byte("hello world")
	if !bytes.Equal(got, want) {
		t.Fatalf("Read file(%q) from GCS, got: %b, want: %b", filePath, got, want)
	}
}

func TestGCS_Delete(t *testing.T) {
	ctx := context.Background()
	gcs := getBucket(t)
	filePath := "delete.txt"
	if err := gcs.Write(ctx, filePath, []byte("hello world")); err != nil {
		t.Fatalf("Write to GCS with file(%q) error: %v", filePath, err)
	}

	if err := gcs.Delete(ctx, filePath); err != nil {
		t.Fatalf("Delete file(%q) in GCS got error: %v", filePath, err)
	}
}
