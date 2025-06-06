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
	OpenSavesPort       = "open_saves_port"
	OpenSavesCloud      = "open_saves_cloud"
	OpenSavesBucket     = "open_saves_bucket"
	OpenSavesProject    = "open_saves_project"
	LogLevel            = "log_level"
	LogFormat           = "log_format"
	ShutdownGracePeriod = "shutdown_grace_period"

	CacheDefaultTTL = "cache_default_ttl"

	RedisAddress         = "redis_address"
	RedisMode            = "redis_mode"
	RedisMinIdleConns    = "redis_min_idle_conns"
	RedisPoolSize        = "redis_pool_size"
	RedisIdleTimeout     = "redis_idle_timeout"
	RedisMaxConnAge      = "redis_max_conn_age"
	RedisMaxRetries      = "redis_max_retries"
	RedisMinRetryBackoff = "redis_min_retry_backoff"
	RedisMaxRetryBackoff = "redis_max_retry_backoff"

	DefaultGarbageCollectionTTL = "blob_default_garbage_collection_ttl"
	BlobMaxInlineSize           = "blob_max_inline_size"

	GRPCKeepAliveMaxConnectionIdle     = "grpc_keepalive_max_connection_idle"
	GRPCKeepAliveMaxConnectionAge      = "grpc_keepalive_max_connection_age"
	GRPCKeepAliveMaxConnectionAgeGrace = "grpc_keepalive_max_connection_age_grace"
	GRPCKeepAliveTime                  = "grpc_keepalive_time"
	GRPCKeepAliveTimeout               = "grpc_keepalive_timeout"

	EnableMetrics              = "enable_metrics"
	MetricsEnableGRPCCollector = "metrics_enable_grpc_collector"
	MetricsEnableHTTPCollector = "metrics_enable_http_collector"

	EnableTrace              = "enable_trace"
	TraceSampleRate          = "trace_sample_rate"
	TraceServiceName         = "trace_service_name"
	TraceEnableGRPCCollector = "trace_enable_grpc_collector"
	TraceEnableHTTPCollector = "trace_enable_http_collector"

	DatastoreTXMaxAttempts = "datastore_tx_max_attempts"
)

type ServiceConfig struct {
	ServerConfig
	CacheConfig
	RedisConfig
	BlobConfig
	GRPCServerConfig
	DatastoreConfig
}

// ServerConfig defines common fields needed to start Open Saves.
type ServerConfig struct {
	Address             string
	Cloud               string
	Bucket              string
	Project             string
	ShutdownGracePeriod time.Duration

	// The following enables OpenTelemetry Metrics
	// See https://github.com/open-telemetry/opentelemetry-go/tree/main/exporters/otlp/otlpmetric for how to configure the exporters with env variables
	EnableMetrics              bool
	MetricsEnableGRPCCollector bool
	MetricsEnableHTTPCollector bool

	// The following enables OpenTelemetry Tracing
	// It is EXPERIMENTAL and subject to change or removal without notice.
	// See https://github.com/open-telemetry/opentelemetry-go/tree/main/exporters/otlp/otlptrace for how to configure the exporters with env variables
	EnableTrace              bool
	TraceSampleRate          float64
	TraceServiceName         string
	TraceEnableGRPCCollector bool
	TraceEnableHTTPCollector bool
}

// CacheConfig has configurations for caching control (not Redis specific).
type CacheConfig struct {
	// DefaultTTL is the default TTL for cached data.
	DefaultTTL time.Duration
}

// RedisConfig as defined in https://pkg.go.dev/github.com/redis/go-redis/v9#Options
type RedisConfig struct {
	Address   string
	RedisMode string

	MaxRetries      int
	MinRetyBackoff  time.Duration
	MaxRetryBackoff time.Duration

	MinIdleConns int
	PoolSize     int
	IdleTimeout  time.Duration
	MaxConnAge   time.Duration
}

// BlobConfig has Open Saves blob related configurations.
type BlobConfig struct {
	DefaultGarbageCollectionTTL time.Duration
	MaxInlineSize               int
}

// GRPCServerConfig has the configurations for grpc server, for now keepAlive parameters
// all the parameters are handled in time.Duration
type GRPCServerConfig struct {
	MaxConnectionIdle     time.Duration
	MaxConnectionAge      time.Duration
	MaxConnectionAgeGrace time.Duration
	Time                  time.Duration
	Timeout               time.Duration
}

type DatastoreConfig struct {
	TXMaxAttempts int
}
