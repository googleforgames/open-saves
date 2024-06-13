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

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedis_All(t *testing.T) {
	ctx := context.Background()

	// Use miniredis for tests.
	s := miniredis.RunT(t)
	r := NewRedis(s.Addr())
	require.NotNil(t, r)

	assert.NoError(t, r.FlushAll(ctx))

	keys, err := r.ListKeys(ctx)
	assert.NoError(t, err)
	assert.Empty(t, keys)

	_, err = r.Get(ctx, "unknown")
	assert.Error(t, err)

	by := []byte("byte")
	assert.NoError(t, r.Set(ctx, "hello", by, 0))

	val, err := r.Get(ctx, "hello")
	assert.NoError(t, err)
	assert.Equal(t, by, val)

	// test with TTL. The resolution is one millisecond.
	assert.NoError(t, r.Set(ctx, "withTTL", by, 1*time.Millisecond))
	s.FastForward(2 * time.Millisecond)
	val, err = r.Get(ctx, "withTTL")
	assert.ErrorIs(t, redis.Nil, err)
	assert.Nil(t, val)

	keys, err = r.ListKeys(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"hello"}, keys)

	assert.NoError(t, r.Delete(ctx, "hello"))

	assert.NoError(t, r.FlushAll(ctx))

	keys, err = r.ListKeys(ctx)
	assert.NoError(t, err)
	assert.Empty(t, keys)
}

// Test parsing of Redis addresses works as expected.
func TestRedisParseRedisAddress(t *testing.T) {
	type ParseRedisAddressFixture struct {
		address  string
		expected []string
	}

	fixture := []ParseRedisAddressFixture{
		// Single address.
		{
			address:  "redis:6379",
			expected: []string{"redis:6379"},
		},
		// Multiple addresses, no whitespaces.
		{
			address:  "redis-1:6379,redis-2:6379",
			expected: []string{"redis-1:6379", "redis-2:6379"},
		},
		// Multiple addresses, with whitespaces.
		{
			address:  "   redis-1:6379    ,    redis-2:6379   ",
			expected: []string{"redis-1:6379", "redis-2:6379"},
		},
	}

	for _, test := range fixture {
		result := parseRedisAddress(test.address)

		require.Equal(t, test.expected, result)
	}
}
