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

// Run starts the triton gRPC service.
func Run(ctx context.Context, network, address string) error {
	lis, err := net.Listen(network, address)
	if err != nil {
		return err
	}
	defer func() {
		if err := lis.Close(); err != nil {
			log.Errorf("Failed to close %s %s: %v", network, address, err)
		}
	}()

	s := grpc.NewServer()
	tritonpb.RegisterTritonServer(s, newTritonServer())

	log.Infof("starting server on %s %s", network, address)
	return s.Serve(lis)
}
