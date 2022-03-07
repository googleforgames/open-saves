package config

import "time"

const (
	OPEN_SAVES_PORT    = "server.port"
	OPEN_SAVES_CLOUD   = "server.cloud"
	OPEN_SAVES_BUCKET  = "server.bucket"
	OPEN_SAVES_PROJECT = "server.project"
	OPEN_SAVES_CACHE   = "redis.address"
	LOG_LEVEL          = "server.logLevel"

	REDIS_ADDRESS           = "redis.address"
	REDIS_POOL_MAX_IDLE     = "redis.pool.MaxIdle"
	REDIS_POOL_MAX_ACTIVE   = "redis.pool.MaxActive"
	REDIS_POOL_IDLE_TIMEOUT = "redis.pool.IdleTimeout"
	REDIS_POOL_WAIT         = "redis.pool.Wait"
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

type RedisPool struct {
	MaxIdle     int
	MaxActive   int
	IdleTimeout time.Duration
	Wait        bool
}
