// Copyright 2022 Google LLC
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

package config

import "time"

const (
	OpenSavesPort    = "open_saves_port"
	OpenSavesCloud   = "open_saves_cloud"
	OpenSavesBucket  = "open_saves_bucket"
	OpenSavesProject = "open_saves_project"
	LogLevel         = "log_level"
	HealthCheckPort  = "health_check_port"

	CacheDefaultTTL = "cache_default_ttl"

	RedisAddress         = "redis_address"
	RedisMinIdleConns    = "redis_min_idle_conns"
	RedisPoolSize        = "redis_pool_size"
	RedisIdleTimeout     = "redis_idle_timeout"
	RedisMaxRetries      = "redis_max_retries"
	RedisMinRetryBackoff = "redis_min_retry_backoff"
	RedisMaxRetryBackoff = "redis_max_retry_backoff"

	BlobMaxInlineSize = "blob_max_inline_size"
)

type ServiceConfig struct {
	ServerConfig
	HealthCheckConfig
	CacheConfig
	RedisConfig
	BlobConfig
}

// ServerConfig defines common fields needed to start Open Saves.
type ServerConfig struct {
	Address string
	Cloud   string
	Bucket  string
	Project string
}

type HealthCheckConfig struct {
	Address string
}

// CacheConfig has configurations for caching control (not Redis specific).
type CacheConfig struct {
	// DefaultTTL is the default TTL for cached data.
	DefaultTTL time.Duration
}

// RedisConfig as defined in https://pkg.go.dev/github.com/go-redis/redis/v8#Options
type RedisConfig struct {
	Address string

	MaxRetries      int
	MinRetyBackoff  time.Duration
	MaxRetryBackoff time.Duration

	MinIdleConns int
	PoolSize     int
	IdleTimeout  time.Duration
}

// BlobConfig has Open Saves blob related configurations.
type BlobConfig struct {
	MaxInlineSize int
}
