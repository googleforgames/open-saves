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

package checksumstest

import (
	"crypto/md5"
	"math/rand"
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/googleforgames/open-saves/internal/pkg/metadb/checksums"
	"github.com/stretchr/testify/assert"
)

// AssertPropertyListMatch asserts the properties in actual has the same values to expected.
func AssertPropertyListMatch(t *testing.T, expected checksums.Checksums, actual []datastore.Property) {
	t.Helper()
	ps, err := datastore.SaveStruct(&expected)
	if assert.NoError(t, err) {
		assert.ElementsMatch(t, ps, actual)
	}
}

// ChecksumsToProperties creates a list of datastore.Property with values in c.
func ChecksumsToProperties(t *testing.T, c checksums.Checksums) []datastore.Property {
	t.Helper()
	ps, err := datastore.SaveStruct(&c)
	assert.NoError(t, err)
	return ps
}

// RandomChecksums returns a randomly initialized Checksums for tests.
func RandomChecksums(t *testing.T) checksums.Checksums {
	t.Helper()
	md5 := make([]byte, md5.Size)
	rand.Read(md5)
	return checksums.Checksums{
		MD5:       md5,
		CRC32C:    int32(rand.Uint32()),
		HasCRC32C: true,
	}
}

// AssertProtoEquals checks actual has the same values as expected.
func AssertProtoEqual(t *testing.T, expected checksums.Checksums, actual checksums.ChecksumsProto) {
	t.Helper()
	assert.Equal(t, expected.MD5, actual.GetMd5())
	assert.Equal(t, expected.GetCRC32C(), actual.GetCrc32C())
	assert.Equal(t, expected.HasCRC32C, actual.GetHasCrc32C())
}

// ChecksumsProtoImpl is a wrapper of Checksums for tests.
type ChecksumsProtoImpl struct {
	checksums.Checksums
}

func (c *ChecksumsProtoImpl) GetMd5() []byte {
	return c.MD5
}

func (c *ChecksumsProtoImpl) GetCrc32C() uint32 {
	return c.GetCRC32C()
}

func (c *ChecksumsProtoImpl) GetHasCrc32C() bool {
	return c.HasCRC32C
}
