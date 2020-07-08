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
	"log"

	"github.com/googleforgames/triton/internal/app/server"
)

var (
	port    = flag.Uint("port", 6000, "The port number to run Triton on")
	cloud   = flag.String("cloud", "gcp", "The public cloud provider you wish to run Triton on")
	bucket  = flag.String("bucket", "gs://triton-dev-store", "The bucket which will hold Triton blobs")
	project = flag.String("project", "triton-for-games-dev", "The GCP project ID to use for Datastore")
)

func main() {
	flag.Parse()
	if *cloud == "" {
		log.Fatal("missing -cloud argument for cloud provider")
	}
	if *bucket == "" {
		log.Fatal("missing -bucket argument for storing blobs")
	}

	cfg := &server.Config{
		Address: fmt.Sprintf(":%d", *port),
		Cloud:   *cloud,
		Bucket:  *bucket,
		Project: *project,
	}

	ctx := context.Background()
	if err := server.Run(ctx, "tcp", cfg); err != nil {
		log.Fatalf("got error starting server: %v", err)
	}
}
