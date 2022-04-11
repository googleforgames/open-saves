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
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	mock_cache "github.com/googleforgames/open-saves/internal/pkg/cache/mock"
	"github.com/googleforgames/open-saves/internal/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestCache_Simple(t *testing.T) {
	ctrl := gomock.NewController(t)
	cacheable := mock_cache.NewMockCacheable(ctrl)
	driver := mock_cache.NewMockDriver(ctrl)
	const testCacheKey = "testcache/key"
	testBinary := []byte{0x42, 0x24, 0x00, 0x12}
	ctx := context.Background()

	cache := New(driver, &config.CacheConfig{DefaultTTL: 42 * time.Second})
	if cache == nil {
		t.Fatal("cache.New returned nil")
	}

	cacheable.EXPECT().CacheKey().Return(testCacheKey)
	cacheable.EXPECT().EncodeBytes().Return(testBinary, nil)
	driver.EXPECT().Set(ctx, testCacheKey, testBinary, 42*time.Second).Return(nil)

	assert.NoError(t, cache.Set(ctx, cacheable))

	cacheable.EXPECT().DecodeBytes(testBinary).Return(nil)
	driver.EXPECT().Get(ctx, testCacheKey).Return(testBinary, nil)

	assert.NoError(t, cache.Get(ctx, testCacheKey, cacheable))

	driver.EXPECT().Delete(ctx, testCacheKey).Return(nil)
	assert.NoError(t, cache.Delete(ctx, testCacheKey))

	driver.EXPECT().FlushAll(ctx).Return(nil)
	assert.NoError(t, cache.FlushAll(ctx))
}

func TestCache_TooBigToCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	cacheable := mock_cache.NewMockCacheable(ctrl)
	driver := mock_cache.NewMockDriver(ctrl)
	const testCacheKey = "testcache/key"
	ctx := context.Background()

	cache := New(driver, &config.CacheConfig{})
	if cache == nil {
		t.Fatal("cache.New returned nil")
	}
	cache.MaxSizeToCache = 100

	testBinaryBuf := new(bytes.Buffer)
	testBinaryBuf.Grow(cache.MaxSizeToCache + 1)
	for i := 0; i < cache.MaxSizeToCache; i++ {
		if err := testBinaryBuf.WriteByte(byte(i)); err != nil {
			t.Fatalf("test internal error: Buffer.WriteByte returned error: %v", err)
		}
	}

	testBytes := testBinaryBuf.Bytes()

	cacheable.EXPECT().EncodeBytes().Return(testBytes, nil)
	cacheable.EXPECT().CacheKey().Return(testCacheKey)
	driver.EXPECT().Set(ctx, testCacheKey, testBytes, time.Duration(0)).Return(nil)

	assert.NoError(t, cache.Set(ctx, cacheable))

	// Add one byte
	if err := testBinaryBuf.WriteByte(0x42); err != nil {
		t.Fatalf("test internal error: Buffer.WriteByte returned error: %v", err)
	}
	testBytes = testBinaryBuf.Bytes()

	cacheable.EXPECT().EncodeBytes().Return(testBytes, nil)
	cacheable.EXPECT().CacheKey().Return(testCacheKey)
	driver.EXPECT().Delete(ctx, testCacheKey).Return(nil)

	assert.NoError(t, cache.Set(ctx, cacheable))
}
