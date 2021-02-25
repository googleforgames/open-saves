// Copyright 2021 Google LLC
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
	defaultExpiration := cmd.GetEnvVarDuration("OPEN_SAVES_GARBAGE_EXPIRATION", 24*time.Hour)

	var (
		cloud      = flag.String("cloud", defaultCloud, "The public cloud provider you wish to run Open Saves on")
		bucket     = flag.String("bucket", defaultBucket, "The bucket which will hold Open Saves blobs")
		project    = flag.String("project", defaultProject, "The GCP project ID to use for Datastore")
		cache      = flag.String("cache", defaultCache, "The address of the cache store instance")
		expiration = flag.Duration("garbage-expiration", defaultExpiration, "Collector deletes entries older than this time.Duration value (e.g. \"24h\")")
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
		Before:  time.Now().Add(-*expiration),
	}

	ctx := context.Background()
	collector.Run(ctx, cfg)
}
