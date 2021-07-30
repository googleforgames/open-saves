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

package ping

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/examples/load_tester/load"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// Tester is an implementation of load.Tester for Ping.
type Tester struct {
	elapsed   time.Duration
	succeeded int
}

// Run runs the load test according to config using conns.
// Call PrintResults to print results.
func (b *Tester) Run(ctx context.Context, conns []*grpc.ClientConn, config *load.TestOptions) error {
	log.Infof("Starting Ping calls, connections = %v, concurrency = %v, requests = %v",
		len(conns), config.Concurrency, config.Requests)

	start := time.Now()
	succChan := make(chan int, len(conns))
	var wg sync.WaitGroup
	wg.Add(len(conns))
	for _, conn := range conns {
		go func(conn *grpc.ClientConn) {
			succeeded := 0
			defer func() {
				wg.Done()
				succChan <- succeeded
			}()
			succeeded, _ = b.runWithConn(ctx, conn, config.Requests/len(conns), config.Concurrency)
		}(conn)
	}
	wg.Wait()
	b.elapsed = time.Since(start)

	b.succeeded = 0
	for i := 0; i < len(conns); i++ {
		b.succeeded += <-succChan
	}
	return nil
}

func (b *Tester) runWithConn(ctx context.Context, conn *grpc.ClientConn, numRequests, concurrency int) (int, error) {
	var wg sync.WaitGroup
	wg.Add(concurrency)

	succChan := make(chan int, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(numIterations int) {
			succeeded := 0
			defer func() {
				wg.Done()
				succChan <- succeeded
			}()
			client := pb.NewOpenSavesClient(conn)

			req := &pb.PingRequest{}
			for i := 0; i < numIterations; i++ {
				if _, err := client.Ping(ctx, req); err != nil {
					log.Errorf("Ping failed: %v", err)
					return
				} else {
					succeeded++
				}
			}
		}(numRequests / concurrency)
	}
	wg.Wait()

	succeeded := 0
	for i := 0; i < concurrency; i++ {
		succeeded += <-succChan
	}

	return succeeded, nil
}

// PrintResults prints load test results to stdout.
func (b *Tester) PrintResults() {
	fmt.Printf("%v pings/second (%v calls in %v seconds)\n",
		float64(b.succeeded)/b.elapsed.Seconds(),
		b.succeeded, b.elapsed.Seconds(),
	)
}
