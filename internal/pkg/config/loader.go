package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
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
		log.Warningf("cannot bind env var %s to %s", "OPEN_SAVES_CACHE", RedisAddress)
	}

	// Reads command line arguments, for backward compatibility
	pflag.Uint("port", 6000, "The port number to run Open Saves on")
	pflag.String("cloud", "gcp", "The public cloud provider you wish to run Open Saves on")
	pflag.String("bucket", "", "The bucket which will hold Open Saves blobs")
	pflag.String("project", "", "The GCP project ID to use for Datastore")
	pflag.String("cache", "", "The address of the cache store instance")
	pflag.String("log", "", "The level to log messages at")

	if err := viper.BindPFlag(OpenSavesPort, pflag.Lookup("port")); err != nil {
		log.Warningf("cannot bind flag %s to %s", "port", OpenSavesPort)
	}
	if err := viper.BindPFlag(OpenSavesCloud, pflag.Lookup("cloud")); err != nil {
		log.Warningf("cannot bind flag %s to %s", "cloud", OpenSavesCloud)
	}
	if err := viper.BindPFlag(OpenSavesBucket, pflag.Lookup("bucket")); err != nil {
		log.Warningf("cannot bind flag %s to %s", "bucket", OpenSavesBucket)
	}
	if err := viper.BindPFlag(OpenSavesProject, pflag.Lookup("project")); err != nil {
		log.Warningf("cannot bind flag %s to %s", "project", OpenSavesProject)
	}
	if err := viper.BindPFlag(RedisAddress, pflag.Lookup("cache")); err != nil {
		log.Warningf("cannot bind flag %s to %s", "cache", RedisAddress)
	}
	if err := viper.BindPFlag(LogLevel, pflag.Lookup("log")); err != nil {
		log.Warningf("cannot bind flag %s to %s", "log", LogLevel)
	}
	pflag.Parse()

	// Log fatal error when required config is missing
	if viper.GetString(OpenSavesCloud) == "" {
		log.Fatal("missing -cloud argument for cloud provider")
	}
	if viper.GetString(OpenSavesBucket) == "" {
		log.Fatal("missing -bucket argument for storing blobs")
	}
	if viper.GetString(OpenSavesProject) == "" {
		log.Fatal("missing -project argument")
	}
	if viper.GetString(RedisAddress) == "" {
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
	redisConfig := RedisConfig{
		Address:         viper.GetString(RedisAddress),
		MaxRetries:      viper.GetInt(RedisMaxRetries),
		MinRetyBackoff:  viper.GetDuration(RedisMinRetryBackoff),
		MaxRetryBackoff: viper.GetDuration(RedisMaxRetryBackoff),
		MinIdleConns:    viper.GetInt(RedisMinIdleConns),
		PoolSize:        viper.GetInt(RedisPoolSize),
		IdleTimeout:     time.Duration(viper.GetUint(RedisIdleTimeout)) * time.Second,
	}

	return &ServiceConfig{
		ServerConfig: serverConfig,
		RedisConfig:  redisConfig,
	}, nil
}
