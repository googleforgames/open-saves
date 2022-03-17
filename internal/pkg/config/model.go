package config

import "time"

const (
	OpenSavesPort    = "open_saves_port"
	OpenSavesCloud   = "open_saves_cloud"
	OpenSavesBucket  = "open_saves_bucket"
	OpenSavesProject = "open_saves_project"
	LogLevel         = "log_level"

	RedisAddress         = "redis_address"
	RedisMinIdleConns    = "redis_min_idle_conns"
	RedisPoolSize        = "redis_pool_size"
	RedisIdleTimeout     = "redis_idle_timeout"
	RedisMaxRetries      = "redis_max_retries"
	RedisMinRetryBackoff = "redis_min_retry_backoff"
	RedisMaxRetryBackoff = "redis_max_retry_backoff"
)

type ServiceConfig struct {
	ServerConfig
	RedisConfig
}

// ServerConfig defines common fields needed to start Open Saves.
type ServerConfig struct {
	Address string
	Cloud   string
	Bucket  string
	Cache   string
	Project string
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
