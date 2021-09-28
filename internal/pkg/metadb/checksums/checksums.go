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

// Checksums is a struct for blob checksums.
type Checksums struct {
	// MD5 is the MD5 hash value of the associated blob object.
	MD5 []byte `datastore:",noindex"`
	// CRC32C is the CRC32C checksum of the associated blob object.
	// CRC32C uses the Castagnoli polynomial.
	// This needs to be int32 because Datastore doesn't support unsigned integers...
	// Use SetCRC32C() and GetCRC32C() for easier access.
	CRC32C int32 `datastore:",noindex"`
	// HasCRC32C indicates if there is a valid CRC32C value.
	HasCRC32C bool `datastore:",noindex"`
}

// SetCRC32C sets v to CRC32C and sets HasCRC32C to true.
func (c *Checksums) SetCRC32C(v uint32) {
	c.HasCRC32C = true
	c.CRC32C = int32(v)
}

// GetCRC32C returns CRC32C.
func (c *Checksums) GetCRC32C() uint32 {
	return uint32(c.CRC32C)
}

// ResetCRC32C clears the CRC32C value and sets HasCRC32C to false.
func (c *Checksums) ResetCRC32C() {
	c.CRC32C = 0 // 0 is a possible CRC32C value but would still help debugging.
	c.HasCRC32C = false
}

// ChecksumsProto represents an proto message that has checksum values.
type ChecksumsProto interface {
	GetHasCrc32C() bool
	GetCrc32C() uint32
	GetMd5() []byte
}
