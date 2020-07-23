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

	"github.com/gomodule/redigo/redis"
)

type Redis struct {
	redisPool *redis.Pool
}

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

func (r *Redis) Set(ctx context.Context, key, value string) error {
	conn := r.redisPool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, value)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) Get(ctx context.Context, key string) (string, error) {
	conn := r.redisPool.Get()
	defer conn.Close()

	val, err := redis.String(conn.Do("GET", key))
	if err != nil {
		return "", err
	}
	return val, nil
}

func (r *Redis) Delete(ctx context.Context, key string) error {
	conn := r.redisPool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", key)
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) ListKeys(ctx context.Context) ([]string, error) {
	conn := r.redisPool.Get()
	defer conn.Close()

	keys, err := redis.Strings(conn.Do("KEYS", "*"))
	if err != nil {
		return nil, err
	}
	return keys, nil
}
