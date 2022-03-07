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
	viper.AutomaticEnv()
	if err := viper.BindEnv(RedisAddress, "OPEN_SAVES_CACHE", "REDIS_ADDRESS"); err != nil {
		log.Warning("cannot bind env var %s with %s", "OPEN_SAVES_CACHE", RedisAddress)
	}

	// Reads command line arguments, for backward compatibility
	var (
		port     = flag.Uint("port", viper.GetUint(OpenSavesPort), "The port number to run Open Saves on")
		cloud    = flag.String("cloud", viper.GetString(OpenSavesCloud), "The public cloud provider you wish to run Open Saves on")
		bucket   = flag.String("bucket", viper.GetString(OpenSavesBucket), "The bucket which will hold Open Saves blobs")
		project  = flag.String("project", viper.GetString(OpenSavesProject), "The GCP project ID to use for Datastore")
		cache    = flag.String("cache", viper.GetString(RedisAddress), "The address of the cache store instance")
		logLevel = flag.String("log", viper.GetString(LogLevel), "The level to log messages at")
	)
	flag.Parse()

	if *port != viper.GetUint(OpenSavesPort) {
		viper.Set(OpenSavesPort, port)
	}
	if *cloud != viper.GetString(OpenSavesCloud) {
		viper.Set(OpenSavesCloud, *cloud)
	}
	if *bucket != viper.GetString(OpenSavesBucket) {
		viper.Set(OpenSavesBucket, *bucket)
	}
	if *project != viper.GetString(OpenSavesProject) {
		viper.Set(OpenSavesProject, *project)
	}
	if *cache != viper.GetString(RedisAddress) {
		viper.Set(RedisAddress, *cache)
	}
	if *logLevel != viper.GetString(LogLevel) {
		viper.Set(LogLevel, *logLevel)
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
		Address: fmt.Sprintf(":%d", viper.GetUint(OpenSavesPort)),
		Cloud:   viper.GetString(OpenSavesCloud),
		Bucket:  viper.GetString(OpenSavesBucket),
		Project: viper.GetString(OpenSavesProject),
		Cache:   viper.GetString(RedisAddress),
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
		MaxIdle:     viper.GetInt(RedisPoolMaxIdle),
		MaxActive:   viper.GetInt(RedisPoolMaxActive),
		IdleTimeout: time.Duration(viper.GetUint(RedisPoolIdleTimeout)) * time.Second,
		Wait:        viper.GetBool(RedisPoolWait),
	}
	redisConfig := RedisConfig{
		Address: viper.GetString(RedisAddress),
		Pool:    redisPool,
	}

	return &ServiceConfig{
		ServerConfig: serverConfig,
		RedisConfig:  redisConfig,
	}, nil
}
