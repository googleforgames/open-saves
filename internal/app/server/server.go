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

package server

import (
	"context"
	"net"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	tritonpb "github.com/googleforgames/triton/api"
)

// Config defines common fields needed to start Triton.
type Config struct {
	Address string
	Cloud   string
	Bucket  string
	Cache   string
	Project string
}

// Run starts the triton gRPC service.
func Run(ctx context.Context, network string, cfg *Config) error {
	lis, err := net.Listen(network, cfg.Address)
	if err != nil {
		return err
	}
	defer func() {
		if err := lis.Close(); err != nil {
			log.Errorf("Failed to close %s %s: %v", network, cfg.Address, err)
		}
	}()

	s := grpc.NewServer()
	tritonServer, err := newTritonServer(ctx, cfg.Cloud, cfg.Project, cfg.Bucket)
	if err != nil {
		return err
	}
	tritonpb.RegisterTritonServer(s, tritonServer)

	log.Infof("starting server on %s %s", network, cfg.Address)
	return s.Serve(lis)
}
