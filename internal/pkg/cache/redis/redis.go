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

	"github.com/gomodule/redigo/redis"
	"github.com/googleforgames/open-saves/internal/pkg/cache"
)

// Redis is an implementation of the cache.Cache interface.
type Redis struct {
	redisPool *redis.Pool
}

var _ cache.Driver = new(Redis)

// NewRedis creates a new Redis instance.
func NewRedis(address string, opts ...redis.DialOption) *Redis {
	rp := &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", address, opts...)
		},
		MaxIdle:   500,
		MaxActive: 10000,
	}
	return &Redis{
		redisPool: rp,
	}
}

// Set adds a key-value pair to the redis instance.
func (r *Redis) Set(ctx context.Context, key string, value []byte) error {
	conn := r.redisPool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, value)
	if err != nil {
		return err
	}
	return nil
}

// Get retrieves the value for a given key.
func (r *Redis) Get(ctx context.Context, key string) ([]byte, error) {
	conn := r.redisPool.Get()
	defer conn.Close()

	val, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return nil, err
	}
	return val, nil
}

// Delete deletes the key from the redis instance.
func (r *Redis) Delete(ctx context.Context, key string) error {
	conn := r.redisPool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	if err != nil {
		return err
	}
	return nil
}

// FlushAll removes all key-value pairs from the redis instance.
func (r *Redis) FlushAll(ctx context.Context) error {
	conn := r.redisPool.Get()
	defer conn.Close()

	_, err := conn.Do("FLUSHALL")
	if err != nil {
		return err
	}
	return nil
}

// ListKeys lists all the keys in the redis instance.
func (r *Redis) ListKeys(ctx context.Context) ([]string, error) {
	conn := r.redisPool.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", "*"))
	if err != nil {
		return nil, err
	}
	return keys, nil
}
