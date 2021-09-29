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

package checksums

import (
	"crypto/md5"
	"hash"
	"hash/crc32"
	"io"
	"sync"
)

var crc32cTable *crc32.Table
var tableInitOnce = new(sync.Once)

// Digest calculates MD5 and CRC32C values as you call Write and
// returns the values as a Checksums variable.
type Digest struct {
	md5    hash.Hash
	crc32c hash.Hash32
}

// Assert that Digest implements io.Writer.
var _ io.Writer = new(Digest)

// NewDigest creates a new instance of Digest.
func NewDigest() *Digest {
	tableInitOnce.Do(func() { crc32cTable = crc32.MakeTable(crc32.Castagnoli) })
	return &Digest{
		md5:    md5.New(),
		crc32c: crc32.New(crc32cTable),
	}
}

// Write adds more data to the running hashes.
// It never returns an error.
func (d *Digest) Write(p []byte) (int, error) {
	// Write never returns an error for Hash objects.
	// https://pkg.go.dev/hash#Hash
	d.crc32c.Write(p)
	d.md5.Write(p)
	return len(p), nil
}

// Reset resets the hash states.
func (d *Digest) Reset() {
	d.md5.Reset()
	d.crc32c.Reset()
}

// Checksums returns a new Checksums variable with the calculated hash values.
func (d *Digest) Checksums() Checksums {
	return Checksums{
		MD5:       d.md5.Sum(nil),
		CRC32C:    int32(d.crc32c.Sum32()),
		HasCRC32C: true,
	}
}
