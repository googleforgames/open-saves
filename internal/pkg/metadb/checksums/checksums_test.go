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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChecksums_CRC32C(t *testing.T) {
	var c Checksums
	c.SetCRC32C(uint32(0xfedcba98))
	assert.Equal(t, uint32(0xfedcba98), c.GetCRC32C())
	c.ResetCRC32C()
	assert.False(t, c.HasCRC32C)
	assert.Zero(t, c.CRC32C)
}
