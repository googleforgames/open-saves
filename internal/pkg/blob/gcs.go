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
	"io"
	"time"

	"gocloud.dev/blob"
	// Register the gocloud blob GCS driver
	_ "gocloud.dev/blob/gcsblob"
)

// BlobGCP is the GCP implementation of blob.BlobStore using Cloud Storage.
type BlobGCP struct {
	bucket *blob.Bucket
}

// Assert BlobGCP implements the Blob interface
var _ BlobStore = new(BlobGCP)

// NewBlobGCP returns a new BlobGCP instance.
func NewBlobGCP(ctx context.Context, bucketURL string) (*BlobGCP, error) {
	// blob.OpenBucket creates a *blob.Bucket from url.
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, err
	}

	// bucket is guaranteed not to be nil if OpenBucket succeeds.
	gcs := &BlobGCP{
		bucket: bucket,
	}

	return gcs, nil
}

// Put inserts a blob at the given path.
func (b *BlobGCP) Put(ctx context.Context, path string, data []byte) error {
	return b.bucket.WriteAll(ctx, path, data, nil)
}

// NewWriter creates a new object with path and returns an io.WriteCloser
// instance for the object. The object is not committed and visible until
// you close the writer.
func (b *BlobGCP) NewWriter(ctx context.Context, path string) (io.WriteCloser, error) {
	return b.bucket.NewWriter(ctx, path, nil)
}

// Get retrives the data given a blob path.
func (b *BlobGCP) Get(ctx context.Context, path string) ([]byte, error) {
	return b.bucket.ReadAll(ctx, path)
}

// NewReader is an alias to NewRangeReader(ctx, path, 0, -1), which creates
// a reader from the beginning of an object to EOF.
func (b *BlobGCP) NewReader(ctx context.Context, path string) (io.ReadCloser, error) {
	return b.NewRangeReader(ctx, path, 0, -1)
}

// NewRangeReader returns an io.ReadCloser instance for the object specified by path,
// beginning at the offset-th byte and length bytes long. length = -1 means until EOF.
// Make sure to close the reader after all operations to the reader.
func (b *BlobGCP) NewRangeReader(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	return b.bucket.NewRangeReader(ctx, path, offset, length, nil)
}

// Delete deletes the blob at the given path.
func (b *BlobGCP) Delete(ctx context.Context, path string) error {
	return b.bucket.Delete(ctx, path)
}

// Close releases any resources used by the instance.
func (b *BlobGCP) Close() error {
	return b.bucket.Close()
}

// SignUrl returns an open accessible signed url for the given blob key
func (b *BlobGCP) SignUrl(ctx context.Context, key string, ttlInSeconds int64, contentType string, method string) (string, error) {
	opts := &blob.SignedURLOptions{
		Expiry:                   time.Duration(ttlInSeconds) * time.Second,
		Method:                   method,
		ContentType:              contentType,
		EnforceAbsentContentType: false,
		BeforeSign:               nil,
	}
	return b.bucket.SignedURL(ctx, key, opts)
}
