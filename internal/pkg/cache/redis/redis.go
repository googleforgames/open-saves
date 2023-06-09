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
	"github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
	"time"

	"github.com/googleforgames/open-saves/internal/pkg/config"
)

// Redis is an implementation of the cache.Cache interface.
type Redis struct {
	c *redis.Client
}

// NewRedis creates a new Redis instance.
func NewRedis(address string) *Redis {
	cfg := &config.RedisConfig{
		Address: address,
	}

	return NewRedisWithConfig(cfg)
}

// NewRedis creates a new Redis instance.
func NewRedisWithConfig(cfg *config.RedisConfig) *Redis {
	o := &redis.Options{
		Addr:         cfg.Address,
		MinIdleConns: cfg.MinIdleConns,
		PoolSize:     cfg.PoolSize,
		IdleTimeout:  cfg.IdleTimeout,
	}

	c := redis.NewClient(o)
	c.AddHook(redisotel.NewTracingHook())

	return &Redis{
		c: c,
	}
}

// Set adds a key-value pair to the redis instance.
func (r *Redis) Set(ctx context.Context, key string, value []byte, expiration time.Duration) error {
	return r.c.Set(ctx, key, string(value), expiration).Err()
}

// Get retrieves the value for a given key.
func (r *Redis) Get(ctx context.Context, key string) ([]byte, error) {
	val, err := r.c.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	return []byte(val), nil
}

// Delete deletes the key from the redis instance.
func (r *Redis) Delete(ctx context.Context, key string) error {
	return r.c.Del(ctx, key).Err()
}

// FlushAll removes all key-value pairs from the redis instance.
func (r *Redis) FlushAll(ctx context.Context) error {
	return r.c.FlushAll(ctx).Err()
}

// ListKeys lists all the keys in the redis instance.
func (r *Redis) ListKeys(ctx context.Context) ([]string, error) {
	return r.c.Keys(ctx, "*").Result()
}
