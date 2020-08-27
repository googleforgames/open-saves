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

package cache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedis_All(t *testing.T) {
	ctx := context.Background()

	// Use a local instance of Redis for tests. This
	// requires starting a redis server prior to test
	// invocation.
	r := NewRedis("localhost:6379")

	keys, err := r.ListKeys(ctx)
	assert.NoError(t, err)
	assert.Empty(t, keys)

	_, err = r.Get(ctx, "unknown")
	assert.NoError(t, err)

	assert.NoError(t, r.Set(ctx, "hello", "triton"))

	val, err := r.Get(ctx, "hello")
	assert.NoError(t, err)
	assert.Equal(t, val, "triton")

	keys, err = r.ListKeys(ctx)
	assert.NoError(t, err)
	assert.Equal(t, keys, []string{"hello"})

	assert.NoError(t, r.Delete(ctx, "hello"))
}
