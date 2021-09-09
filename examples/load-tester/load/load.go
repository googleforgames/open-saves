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

package load

import (
	"context"
	"crypto/x509"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Tester is a load tester interface to implement.
// The Run function calls Tester.Run first and PrintResults to print results.
type Tester interface {
	Run(ctx context.Context, conns []*grpc.ClientConn, config *TestOptions) error
	PrintResults()
}

// ConnOptions is a set of connection options
type ConnOptions struct {
	// Address is the address of the server to run against
	Address string
	// Insecure is whether to use an insecure connection
	Insecure bool
}

// TestOptions is a set of load testing options
type TestOptions struct {
	// Requests is the number of total requests
	Requests int
	// Connections is the number of connections
	Connections int
	// Concurrency is the number of concurrent requests per connection
	Concurrency int
}

// Run runs a load testing session with the specified tester, ConnOptions, and TestOptions.
func Run(ctx context.Context, tester Tester, co *ConnOptions, to *TestOptions) {
	conns := newConnections(ctx, co, to.Connections)

	if err := tester.Run(ctx, conns, to); err != nil {
		log.Errorf("Error during benchmark: %v", err)
	} else {
		tester.PrintResults()
	}
}

func newClientConn(ctx context.Context, co *ConnOptions) *grpc.ClientConn {
	opts := []grpc.DialOption{}
	if co.Insecure {
		opts = append(opts, grpc.WithInsecure())
	} else {
		x509, _ := x509.SystemCertPool()
		creds := credentials.NewClientTLSFromCert(x509, "")
		opts = append(opts, grpc.WithTransportCredentials(creds))
	}

	conn, err := grpc.Dial(co.Address, opts...)
	if err != nil {
		log.Fatalf("got err dialing conn pool: %v", err)
	}

	return conn
}

func newConnections(ctx context.Context, co *ConnOptions, numConnections int) []*grpc.ClientConn {
	conns := make([]*grpc.ClientConn, numConnections)
	for i := range conns {
		conns[i] = newClientConn(ctx, co)
	}
	return conns
}
