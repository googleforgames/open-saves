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

	"github.com/googleforgames/open-saves/examples/load_tester/load"
	"github.com/googleforgames/open-saves/examples/load_tester/load/getrecord"
	"github.com/googleforgames/open-saves/examples/load_tester/load/ping"
)

var (
	address        = flag.String("address", "localhost:6000", "Address of Open Saves server")
	insecure       = flag.Bool("insecure", false, "Dial grpc server insecurely")
	concurrency    = flag.Int("concurrency", 1, "Number of concurrent requests per connection")
	numRequests    = flag.Int("requests", 100, "Number of benchmark requests")
	numConnections = flag.Int("connections", 1, "Number of gRPC connections")
	testerName     = flag.String("tester", "ping", "Tester name (ping, getrecord)")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	var tester load.Tester
	switch *testerName {
	case "ping":
		tester = new(ping.Tester)
	case "getrecord":
		tester = new(getrecord.Tester)
	default:
		flag.PrintDefaults()
		return
	}

	co := &load.ConnOptions{
		Address:  *address,
		Insecure: *insecure,
	}
	config := &load.TestOptions{
		Concurrency: *concurrency,
		Requests:    *numRequests,
		Connections: *numConnections,
	}

	load.Run(ctx, tester, co, config)
}
