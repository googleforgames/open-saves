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
	"time"

	"github.com/googleforgames/open-saves/internal/pkg/config"
)

const defaultMaxSizeToCache int = 10 * 1024 * 1024 // 10 MB

// Cache defines operations for a cache service.
type Cache struct {
	driver         Driver
	MaxSizeToCache int
	Config         *config.CacheConfig
}

func New(driver Driver, config *config.CacheConfig) *Cache {
	return &Cache{
		driver:         driver,
		MaxSizeToCache: defaultMaxSizeToCache,
		Config:         config,
	}
}

// Set takes a Cacheable object, encodes it into binary, checks the length, and
// set the value into the cache if it's under MaxSizeToCache.
// If the object exceeds MaxSizeToCache, Set deletes the object from the cache.
func (c *Cache) Set(ctx context.Context, object Cacheable) error {
	encoded, err := object.EncodeBytes()
	if err != nil {
		return err
	}
	if len(encoded) > c.MaxSizeToCache {
		return c.Delete(ctx, object.CacheKey())
	}
	return c.driver.Set(ctx, object.CacheKey(), encoded, c.Config.DefaultTTL)
}

// Get takes a key string, and a Cacheable object. It fetches an object from the cache,
// and decodes the binary into dest if it is found. Otherwise it returns a error from
// Driver.Get.
func (c *Cache) Get(ctx context.Context, key string, dest Cacheable) error {
	stored, err := c.driver.Get(ctx, key)
	if err != nil {
		return err
	}
	return dest.DecodeBytes(stored)
}

// Deletes deletes an object identified by key from the cache.
func (c *Cache) Delete(ctx context.Context, key string) error {
	return c.driver.Delete(ctx, key)
}

// FlushAll clears all entries in the cache.
func (c *Cache) FlushAll(ctx context.Context) error {
	return c.driver.FlushAll(ctx)
}

// Cacheable is an interface that objects implement to support caching.
type Cacheable interface {
	// CacheKey returns a cache key string to manage cached entries.
	CacheKey() string
	// DecodeBytes deserializes the byte slice given by by.
	// It can assume ownership and destroy the content of by.
	DecodeBytes(by []byte) error
	// EncodeBytes returns a serialized byte slice of the object.
	EncodeBytes() ([]byte, error)
}

// Driver interface defines common operations for the cache store.
type Driver interface {
	Set(ctx context.Context, key string, value []byte, expiration time.Duration) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	ListKeys(ctx context.Context) ([]string, error)
	FlushAll(ctx context.Context) error
}
