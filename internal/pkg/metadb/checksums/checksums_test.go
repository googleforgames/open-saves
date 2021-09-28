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
	"crypto/md5"
	"testing"

	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums/checksumstest"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestChecksums_CRC32C(t *testing.T) {
	var c checksums.Checksums
	c.SetCRC32C(uint32(0xfedcba98))
	assert.Equal(t, uint32(0xfedcba98), c.GetCRC32C())
	c.ResetCRC32C()
	assert.False(t, c.HasCRC32C)
	assert.Zero(t, c.CRC32C)
}

func TestChecksums_ValidateIfPresent(t *testing.T) {
	c := checksumstest.RandomChecksums(t)
	none := new(checksumstest.ChecksumsProtoImpl)
	assert.NoError(t, c.ValidateIfPresent(none))
	md5 := make([]byte, md5.Size)
	copy(md5, c.MD5)

	hasMD5 := &checksumstest.ChecksumsProtoImpl{
		Checksums: checksums.Checksums{
			MD5: md5,
		},
	}
	assert.NoError(t, c.ValidateIfPresent(hasMD5))

	hasCRC32C := &checksumstest.ChecksumsProtoImpl{
		Checksums: checksums.Checksums{
			CRC32C:    c.CRC32C,
			HasCRC32C: true,
		},
	}
	assert.NoError(t, c.ValidateIfPresent(hasCRC32C))

	hasBoth := &checksumstest.ChecksumsProtoImpl{
		Checksums: checksums.Checksums{
			MD5:       md5,
			CRC32C:    c.CRC32C,
			HasCRC32C: true,
		},
	}
	assert.NoError(t, c.ValidateIfPresent(hasBoth))

	// Tweak values so they don't match
	c.MD5[0] += 1
	c.CRC32C += 1

	assert.NoError(t, c.ValidateIfPresent(none))
	assert.Equal(t, codes.DataLoss, status.Code(c.ValidateIfPresent(hasMD5)))
	assert.Equal(t, codes.DataLoss, status.Code(c.ValidateIfPresent(hasCRC32C)))
	assert.Equal(t, codes.DataLoss, status.Code(c.ValidateIfPresent(hasBoth)))
}
