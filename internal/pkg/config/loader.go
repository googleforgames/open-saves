package config

import (
	"flag"
	"fmt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"strconv"
	"time"
)

func Load(path string) (*ServiceConfig, error) {
	// Load default config from disk
	viper.SetConfigName("service")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("/configs/")
	if path != "" {
		viper.AddConfigPath(path)
	}

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Error("cannot find config file, aborting")
		} else {
			log.Error("error reading config file, aborting")
		}
		return nil, err
	}

	// Environment variable overrides
	if err := viper.BindEnv(OPEN_SAVES_PORT, "OPEN_SAVES_PORT"); err != nil {
		log.Warning("cannot bind env var with %s", OPEN_SAVES_PORT)
	}
	if err := viper.BindEnv(OPEN_SAVES_CLOUD, "OPEN_SAVES_CLOUD"); err != nil {
		log.Warning("cannot bind env var with %s", OPEN_SAVES_CLOUD)
	}
	if err := viper.BindEnv(OPEN_SAVES_BUCKET, "OPEN_SAVES_BUCKET"); err != nil {
		log.Warning("cannot bind env var with %s", OPEN_SAVES_BUCKET)
	}
	if err := viper.BindEnv(OPEN_SAVES_PROJECT, "OPEN_SAVES_PROJECT"); err != nil {
		log.Warning("cannot bind env var with %s", OPEN_SAVES_PROJECT)
	}
	if err := viper.BindEnv(OPEN_SAVES_CACHE, "OPEN_SAVES_CACHE"); err != nil {
		log.Warning("cannot bind env var with %s", OPEN_SAVES_CACHE)
	}
	if err := viper.BindEnv(LOG_LEVEL, "LOG_LEVEL"); err != nil {
		log.Warning("cannot bind env var with %s", LOG_LEVEL)
	}

	// Reads command line arguments, for backward compatibility
	var (
		port     = flag.Uint("port", viper.GetUint(OPEN_SAVES_PORT), "The port number to run Open Saves on")
		cloud    = flag.String("cloud", viper.GetString(OPEN_SAVES_CLOUD), "The public cloud provider you wish to run Open Saves on")
		bucket   = flag.String("bucket", viper.GetString(OPEN_SAVES_BUCKET), "The bucket which will hold Open Saves blobs")
		project  = flag.String("project", viper.GetString(OPEN_SAVES_PROJECT), "The GCP project ID to use for Datastore")
		cache    = flag.String("cache", viper.GetString(OPEN_SAVES_CACHE), "The address of the cache store instance")
		logLevel = flag.String("log", viper.GetString(LOG_LEVEL), "The level to log messages at")
	)
	flag.Parse()

	if *port != viper.GetUint(OPEN_SAVES_PORT) {
		viper.Set(OPEN_SAVES_PORT, port)
	}
	if *cloud != viper.GetString(OPEN_SAVES_CLOUD) {
		viper.Set(OPEN_SAVES_CLOUD, *cloud)
	}
	if *bucket != viper.GetString(OPEN_SAVES_BUCKET) {
		viper.Set(OPEN_SAVES_BUCKET, *bucket)
	}
	if *project != viper.GetString(OPEN_SAVES_PROJECT) {
		viper.Set(OPEN_SAVES_PROJECT, *project)
	}
	if *cache != viper.GetString(OPEN_SAVES_CACHE) {
		viper.Set(OPEN_SAVES_CACHE, *cache)
	}
	if *logLevel != viper.GetString(LOG_LEVEL) {
		viper.Set(LOG_LEVEL, *logLevel)
	}

	if *cloud == "" {
		log.Fatal("missing -cloud argument for cloud provider")
	}
	if *bucket == "" {
		log.Fatal("missing -bucket argument for storing blobs")
	}
	if *project == "" {
		log.Fatal("missing -project argument")
	}
	if *cache == "" {
		log.Fatal("missing -cache argument for cache store")
	}

	serverConfig := ServerConfig{
		Address: fmt.Sprintf(":%d", viper.GetUint(OPEN_SAVES_PORT)),
		Cloud:   viper.GetString(OPEN_SAVES_CLOUD),
		Bucket:  viper.GetString(OPEN_SAVES_BUCKET),
		Project: viper.GetString(OPEN_SAVES_PROJECT),
		Cache:   viper.GetString(OPEN_SAVES_CACHE),
	}

	// Cloud Run environment populates the PORT env var, so check for it here.
	if p := os.Getenv("PORT"); p != "" {
		p, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			log.Fatal("failed to parse PORT env variable, make sure it is of type uint")
		}
		serverConfig.Address = fmt.Sprintf(":%d", p)
	}

	// Redis configuration
	redisPool := RedisPool{
		MaxIdle:     viper.GetInt(REDIS_POOL_MAX_IDLE),
		MaxActive:   viper.GetInt(REDIS_POOL_MAX_ACTIVE),
		IdleTimeout: time.Duration(viper.GetUint(REDIS_POOL_IDLE_TIMEOUT)) * time.Second,
		Wait:        viper.GetBool(REDIS_POOL_WAIT),
	}
	redisConfig := RedisConfig{
		Address: viper.GetString(REDIS_ADDRESS),
		Pool:    redisPool,
	}

	return &ServiceConfig{
		ServerConfig: serverConfig,
		RedisConfig:  redisConfig,
	}, nil
}
