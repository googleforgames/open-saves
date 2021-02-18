package main

import (
	"context"
	"flag"
	"time"

	"github.com/googleforgames/open-saves/internal/app/collector"
	"github.com/googleforgames/open-saves/internal/pkg/cmd"
	log "github.com/sirupsen/logrus"
)

func main() {
	defaultCloud := cmd.GetEnvVarString("OPEN_SAVES_CLOUD", "gcp")
	defaultBucket := cmd.GetEnvVarString("OPEN_SAVES_BUCKET", "gs://triton-dev-store")
	defaultProject := cmd.GetEnvVarString("OPEN_SAVES_PROJECT", "triton-for-games-dev")
	defaultCache := cmd.GetEnvVarString("OPEN_SAVES_CACHE", "localhost:6379")
	defaultBefore := cmd.GetEnvVarDuration("OPEN_SAVES_COLLECT_BEFORE", 24*time.Hour)

	var (
		cloud   = flag.String("cloud", defaultCloud, "The public cloud provider you wish to run Open Saves on")
		bucket  = flag.String("bucket", defaultBucket, "The bucket which will hold Open Saves blobs")
		project = flag.String("project", defaultProject, "The GCP project ID to use for Datastore")
		cache   = flag.String("cache", defaultCache, "The address of the cache store instance")
		before  = flag.Duration("collect-before", defaultBefore, "Collector deletes entries older than this time.Duration value (e.g. \"24h\")")
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
		Before:  time.Now().Add(-*before),
	}

	ctx := context.Background()
	collector.Run(ctx, cfg)
}
