// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"

	"github.com/googleforgames/triton/internal/app/server"
)

func main() {
	defaultPort := getEnvVarUInt("TRITON_PORT", 6000)
	defaultCloud := getEnvVarString("TRITON_CLOUD", "gcp")
	defaultBucket := getEnvVarString("TRITON_BUCKET", "gs://triton-dev-store")
	defaultProject := getEnvVarString("TRITON_PROJECT", "triton-for-games-dev")
	defaultCache := getEnvVarString("TRITON_CACHE", "localhost:6379")

	var (
		port    = flag.Uint("port", uint(defaultPort), "The port number to run Triton on")
		cloud   = flag.String("cloud", defaultCloud, "The public cloud provider you wish to run Triton on")
		bucket  = flag.String("bucket", defaultBucket, "The bucket which will hold Triton blobs")
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

	cfg := &server.Config{
		Address: fmt.Sprintf(":%d", *port),
		Cloud:   *cloud,
		Bucket:  *bucket,
		Project: *project,
		Cache:   *cache,
	}

	// Cloud Run environment populates the PORT env var, so check for it here.
	if p := os.Getenv("PORT"); p != "" {
		p, err := strconv.ParseUint(p, 10, 64)
		if err != nil {
			log.Fatal("failed to parse PORT env variable, make sure it is of type uint")
		}
		cfg.Address = fmt.Sprintf(":%d", p)
	}

	ctx := context.Background()
	if err := server.Run(ctx, "tcp", cfg); err != nil {
		log.Fatalf("got error starting server: %v", err)
	}
}

func getEnvVarString(name string, defValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defValue
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
