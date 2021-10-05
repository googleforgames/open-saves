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

package checksums_test

import (
	"testing"

	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDigest_All(t *testing.T) {
	const testString = "Lorem ipsum dolor sit amet, consectetur adipiscing elit"

	d := checksums.NewDigest()
	require.NotNil(t, d)
	n, err := d.Write([]byte(testString))
	assert.Equal(t, len(testString), n)
	assert.NoError(t, err)

	checksums := d.Checksums()
	if assert.NotNil(t, checksums) {
		expectedMD5 := []byte{0xfc, 0x10, 0xa0, 0x8d, 0xf7, 0xfa, 0xfa, 0x38, 0x71, 0x16, 0x66, 0x46, 0x60, 0x9e, 0x1c, 0x95}
		expectedCRC32C := uint32(0xD07ABEB0)

		assert.Equal(t, expectedMD5, checksums.MD5)
		assert.Equal(t, expectedCRC32C, uint32(checksums.CRC32C))
		assert.True(t, checksums.HasCRC32C)
	}

	d.Reset()
	checksums = d.Checksums()
	if assert.NotNil(t, checksums) {
		expectedMD5 := []byte{0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42, 0x7e}
		assert.Equal(t, expectedMD5, checksums.MD5)
		assert.Zero(t, checksums.CRC32C)
		assert.True(t, checksums.HasCRC32C)
	}
}
