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

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
)

type BlobGCP struct {
	bucket *blob.Bucket
}

func NewBlobGCP(bucketURL string) (*BlobGCP, error) {
	ctx := context.Background()
	// blob.OpenBucket creates a *blob.Bucket from url.
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, err
	}

	gcs := &BlobGCP{
		bucket: bucket,
	}

	return gcs, nil
}

func (b *BlobGCP) Put(ctx context.Context, path string, data []byte) error {
	if b.bucket == nil {
		return fmt.Errorf("could not find bucket for storage provider")
	}
	return b.bucket.WriteAll(ctx, path, data, nil)
}

func (b *BlobGCP) Get(ctx context.Context, path string) ([]byte, error) {
	if b.bucket == nil {
		return []byte{}, fmt.Errorf("could not find bucket for storage provider")
	}
	return b.bucket.ReadAll(ctx, path)
}

func (b *BlobGCP) Delete(ctx context.Context, path string) error {
	if b.bucket == nil {
		return fmt.Errorf("could not find bucket for storage provider")
	}
	return b.bucket.Delete(ctx, path)
}

func (b *BlobGCP) Close() error {
	if b.bucket != nil {
		return b.bucket.Close()
	}
	return fmt.Errorf("could not close storage handler without open bucket")
}
