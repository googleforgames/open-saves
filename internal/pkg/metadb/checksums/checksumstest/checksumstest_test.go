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
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/assert"
)

func TestChecksumsTest_Random(t *testing.T) {
	c := RandomChecksums(t)
	if assert.NotNil(t, c) {
		assert.Len(t, c.MD5, md5.Size)
		assert.True(t, c.HasCRC32C)
	}
}

func TestChecksumsTest_AssertPropertyListMatch(t *testing.T) {
	c := RandomChecksums(t)
	ps, err := datastore.SaveStruct(&c)
	if assert.NoError(t, err) {
		AssertPropertyListMatch(t, c, ps)
	}
}

func TestChecksumsTest_AssertProtoEqual(t *testing.T) {
	c := RandomChecksums(t)
	p := &ChecksumsProtoImpl{Checksums: c}
	AssertProtoEqual(t, c, p)
}
