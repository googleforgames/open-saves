package main

import (
	"context"
	"flag"
	"os"
	"strconv"

	"github.com/googleforgames/open-saves/internal/app/collector"
	log "github.com/sirupsen/logrus"
)

func main() {
	defaultCloud := getEnvVarString("OPEN_SAVES_CLOUD", "gcp")
	defaultBucket := getEnvVarString("OPEN_SAVES_BUCKET", "gs://triton-dev-store")
	defaultProject := getEnvVarString("OPEN_SAVES_PROJECT", "triton-for-games-dev")
	defaultCache := getEnvVarString("OPEN_SAVES_CACHE", "localhost:6379")

	var (
		cloud   = flag.String("cloud", defaultCloud, "The public cloud provider you wish to run Open Saves on")
		bucket  = flag.String("bucket", defaultBucket, "The bucket which will hold Open Saves blobs")
		project = flag.String("project", defaultProject, "The GCP project ID to use for Datastore")
		cache   = flag.String("cache", defaultCache, "The address of the cache store instance")
	)

	flag.Parse()
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

	cfg := &collector.Config{
		Cloud:   *cloud,
		Bucket:  *bucket,
		Project: *project,
		Cache:   *cache,
	}

	ctx := context.Background()
	collector.Run(ctx, cfg)
}

func getEnvVarUInt(name string, defValue uint64) uint64 {
	if value := os.Getenv(name); value != "" {
		uval, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			log.Warningf("failed to parse %s env variable, default to %v", name, defValue)
			uval = defValue
		}
		return uval
	}
	return defValue
}

func getEnvVarString(name string, defValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defValue
}
