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
	"errors"
	"syscall"

	"github.com/gomodule/redigo/redis"
	"github.com/googleforgames/open-saves/internal/pkg/config"
)

const BrokenPipeRetries = 3

// Redis is an implementation of the cache.Cache interface.
type Redis struct {
	redisPool *redis.Pool
}

// NewRedis creates a new Redis instance.
func NewRedis(address string, opts ...redis.DialOption) *Redis {
	cfg := &config.RedisConfig{
		Address: address,
		Pool: config.RedisPool{
			MaxIdle:     500,
			MaxActive:   10000,
			IdleTimeout: 0,
			Wait:        false,
		},
	}

	return NewRedisWithConfig(cfg, opts...)
}

// NewRedis creates a new Redis instance.
func NewRedisWithConfig(cfg *config.RedisConfig, opts ...redis.DialOption) *Redis {
	rp := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", cfg.Address, opts...)
		},
		MaxIdle:     cfg.Pool.MaxIdle,
		MaxActive:   cfg.Pool.MaxActive,
		IdleTimeout: cfg.Pool.IdleTimeout,
		Wait:        cfg.Pool.Wait,
	}
	return &Redis{
		redisPool: rp,
	}
}

// Set adds a key-value pair to the redis instance.
func (r *Redis) Set(ctx context.Context, key string, value []byte) error {
	return retryOnBrokenPipe(BrokenPipeRetries, func() error {
		conn, err := r.redisPool.GetContext(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()

		_, err = conn.Do("SET", key, value)
		if err != nil {
			return err
		}
		return nil
	})
}

// Get retrieves the value for a given key.
func (r *Redis) Get(ctx context.Context, key string) ([]byte, error) {
	return retryOnBrokenPipeBytes(BrokenPipeRetries, func() ([]byte, error) {
		conn, err := r.redisPool.GetContext(ctx)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		val, err := redis.Bytes(conn.Do("GET", key))
		if err != nil {
			return nil, err
		}
		return val, nil
	})
}

// Delete deletes the key from the redis instance.
func (r *Redis) Delete(ctx context.Context, key string) error {
	return retryOnBrokenPipe(BrokenPipeRetries, func() error {
		conn, err := r.redisPool.GetContext(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()

		_, err = conn.Do("DEL", key)
		if err != nil {
			return err
		}
		return nil
	})
}

// FlushAll removes all key-value pairs from the redis instance.
func (r *Redis) FlushAll(ctx context.Context) error {
	return retryOnBrokenPipe(BrokenPipeRetries, func() error {
		conn, err := r.redisPool.GetContext(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()

		_, err = conn.Do("FLUSHALL")
		if err != nil {
			return err
		}
		return nil
	})
}

// ListKeys lists all the keys in the redis instance.
func (r *Redis) ListKeys(ctx context.Context) ([]string, error) {
	return retryOnBrokenPipeStrings(BrokenPipeRetries, func() ([]string, error) {
		conn, err := r.redisPool.GetContext(ctx)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		keys, err := redis.Strings(conn.Do("KEYS", "*"))
		if err != nil {
			return nil, err
		}
		return keys, nil
	})
}

func retryOnBrokenPipe(attempts int, fn func() error) error {
	if err := fn(); err != nil {
		if errors.Is(err, syscall.EPIPE) {
			if attempts--; attempts > 0 {
				return retryOnBrokenPipe(attempts, fn)
			}
		}
		return err
	}
	return nil
}

func retryOnBrokenPipeBytes(attempts int, fn func() ([]byte, error)) ([]byte, error) {
	b, err := fn()
	if err != nil {
		if errors.Is(err, syscall.EPIPE) {
			if attempts--; attempts > 0 {
				return retryOnBrokenPipeBytes(attempts, fn)
			}
		}
		return nil, err
	}
	return b, nil
}

func retryOnBrokenPipeStrings(attempts int, fn func() ([]string, error)) ([]string, error) {
	s, err := fn()
	if err != nil {
		if errors.Is(err, syscall.EPIPE) {
			if attempts--; attempts > 0 {
				return retryOnBrokenPipeStrings(attempts, fn)
			}
		}
		return nil, err
	}
	return s, nil
}
