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
)

// BlobStore is a public interface for Blob operations within Triton.
// Use one of the structs defined in the package.
// Currently available drivers:
// 	- BlobGCP: Google Cloud Storage
type BlobStore interface {
	Put(ctx context.Context, path string, data []byte) error

	// NewWriter creates a new object with path and returns an io.WriteCloser
	// instance for the object.
	// Make sure to close the writer after all operations to the writer.
	NewWriter(ctx context.Context, path string) (io.WriteCloser, error)
	Get(ctx context.Context, path string) ([]byte, error)

	// NewReader is an alias to NewRangeReader(ctx, path, 0, -1), which creates
	// a reader from the beginning of an object to EOF.
	NewReader(ctx context.Context, path string) (io.ReadCloser, error)

	// NewRangeReader returns an io.ReadCloser instance for the object specified by path,
	// beginning at the offset-th byte and length bytes long. length = -1 means until EOF.
	// Make sure to close the reader after all operations to the reader.
	NewRangeReader(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error)
	Delete(ctx context.Context, path string) error
}
