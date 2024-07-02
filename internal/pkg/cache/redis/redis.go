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
	"github.com/googleforgames/open-saves/internal/pkg/config"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

// Knonw Redis connection modes.
const (
	RedisModeSingle = "single"
	RedisModeCluster = "cluster"
)

const (
	redisClusterSeparator = ","
)

// Redis is an implementation of the cache.Cache interface.
type Redis struct {
	c redis.UniversalClient
}

// NewRedis creates a new Redis instance.
func NewRedis(address string) *Redis {
	cfg := &config.RedisConfig{
		Address: address,
		RedisMode: RedisModeSingle,
	}

	return NewRedisWithConfig(cfg)
}

// NewRedisWithConfig creates a new Redis instance with configurable options.
func NewRedisWithConfig(cfg *config.RedisConfig) *Redis {
	var c redis.UniversalClient

	if cfg.RedisMode == RedisModeCluster {
		o := &redis.ClusterOptions{
			// When working with a standard Redis Cluster, it is expected the address being a list of addresses separated by commas (,)
			// When working with CGP MemoryStore Redis Cluster, it is expected the address being a single address - the discovery address.
			Addrs:           parseRedisCluster(cfg.Address),
			MinIdleConns:    cfg.MinIdleConns,
			PoolSize:        cfg.PoolSize,
			ConnMaxIdleTime: cfg.IdleTimeout,
			ConnMaxLifetime: cfg.MaxConnAge,
		}

		c = redis.NewClusterClient(o)
	} else {
		// By default, if no RedisMode is supplied, Single mode will be selected.
		// This is to be retro compatible with previous versions of OpenSaves.
		// In this case the address is expected to be a single address.
		o := &redis.Options{
			Addr:            cfg.Address,
			MinIdleConns:    cfg.MinIdleConns,
			PoolSize:        cfg.PoolSize,
			ConnMaxIdleTime: cfg.IdleTimeout,
			ConnMaxLifetime: cfg.MaxConnAge,
		}

		c = redis.NewClient(o)
	}

	err := redisotel.InstrumentMetrics(c)
	if err != nil {
		log.Errorf("got error adding metric instrumentation to redis client: %v", err)
	}

	err = redisotel.InstrumentTracing(c)
	if err != nil {
		log.Errorf("got error adding tracing instrumentation to redis client: %v", err)
	}

	return &Redis{
		c: c,
	}
}

// parseRedisCluster Parse the input Redis address by splitting the list of addresses separated by commas (,)
func parseRedisCluster(address string) []string {
	addresses := []string{}

	for _, foundAddr := range strings.Split(address, redisClusterSeparator) {
		addresses = append(addresses, strings.TrimSpace(foundAddr))
	}

	return addresses
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
