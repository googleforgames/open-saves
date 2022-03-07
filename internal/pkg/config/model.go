package config

import "time"

const (
	OpenSavesPort    = "open_saves_port"
	OpenSavesCloud   = "open_saves_cloud"
	OpenSavesBucket  = "open_saves_bucket"
	OpenSavesProject = "open_saves_project"
	LogLevel         = "log_level"

	RedisAddress         = "redis_address"
	RedisPoolMaxIdle     = "redis_pool_maxIdle"
	RedisPoolMaxActive   = "redis_pool_maxActive"
	RedisPoolIdleTimeout = "redis_pool_idleTimeout"
	RedisPoolWait        = "redis_pool_wait"
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

type RedisConfig struct {
	Address string
	Pool    RedisPool
}

// RedisPool as defined in redis.Pool
type RedisPool struct {
	MaxIdle     int
	MaxActive   int
	IdleTimeout time.Duration
	Wait        bool
}
