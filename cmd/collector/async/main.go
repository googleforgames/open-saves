package main

import (
	"context"
	"flag"

	asynccollector "github.com/googleforgames/open-saves/internal/app/collector/async"
	"github.com/googleforgames/open-saves/internal/pkg/cmd"
)

func main() {
	defaultPort := cmd.GetEnvVarString("OPEN_SAVES_ASYNC_COLLECTOR_PORT", "8080")
	defaultCloud := cmd.GetEnvVarString("OPEN_SAVES_CLOUD", "gcp")
	defaultBucket := cmd.GetEnvVarString("OPEN_SAVES_BUCKET", "gs://triton-dev-store")
	defaultProject := cmd.GetEnvVarString("OPEN_SAVES_PROJECT", "triton-for-games-dev")

	var (
		port    = flag.String("port", defaultPort, "Port where new events will be listened for")
		cloud   = flag.String("cloud", defaultCloud, "The public cloud provider you wish to run Open Saves on")
		bucket  = flag.String("bucket", defaultBucket, "The bucket which will hold Open Saves blobs")
		project = flag.String("project", defaultProject, "The GCP project ID to use for Datastore")
	)

	flag.Parse()

	cfg := &asynccollector.Config{
		Port:    *port,
		Cloud:   *cloud,
		Bucket:  *bucket,
		Project: *project,
	}

	ctx := context.Background()
	asynccollector.Run(ctx, cfg)
}
