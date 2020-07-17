// Copyright 2020 Google LLC
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

package metadb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimestamps_NewTimestamps(t *testing.T) {
	beforeNew := time.Now()
	var ts timestamps
	ts.NewTimestamps()
	afterNew := time.Now()
	assert.NotNil(t, ts)

	// Check the uuid
	assert.NotEmpty(t, ts.Signature)

	assert.True(t, beforeNew.Before(ts.CreatedAt))
	assert.True(t, beforeNew.Before(ts.UpdatedAt))
	assert.True(t, afterNew.After(ts.CreatedAt))
	assert.True(t, afterNew.After(ts.UpdatedAt))
	assert.True(t, ts.CreatedAt.Equal(ts.UpdatedAt))
}

func TestTimestamps_Update(t *testing.T) {
	var ts timestamps
	ts.NewTimestamps()
	ocreated := ts.CreatedAt
	oupdated := ts.UpdatedAt
	osignature := ts.Signature
	assert.NotNil(t, ts)
	beforeUpdate := time.Now()
	ts.Update()
	afterUpdate := time.Now()

	assert.True(t, ocreated.Equal(ts.CreatedAt))
	assert.False(t, oupdated.Equal(ts.UpdatedAt))
	assert.True(t, beforeUpdate.Before(ts.UpdatedAt))
	assert.True(t, afterUpdate.After(ts.UpdatedAt))
	assert.NotEqual(t, osignature, ts.Signature)
}
