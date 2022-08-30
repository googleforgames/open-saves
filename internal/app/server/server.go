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
	"github.com/googleforgames/open-saves/internal/pkg/config"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/googleforgames/open-saves/api"
)

var serviceName = "grpc.health.v1.opensaves"

// Run starts the Open Saves and health check gRPC servers.
func Run(ctx context.Context, network string, cfg *config.ServiceConfig) error {
	// Handle servers lifecycle
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	g, ctx := errgroup.WithContext(ctx)

	// gRPC Health Server
	grpcHealthServer := grpc.NewServer()
	healthServer := health.NewServer()
	g.Go(func() error {
		healthpb.RegisterHealthServer(grpcHealthServer, healthServer)
		healthServer.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)

		addr := cfg.HealthCheckConfig.Address
		listener, err := net.Listen(network, addr)
		if err != nil {
			log.Errorf("gRPC Health server: failed to listen %s %s: %v", network, addr, err)
			return err
		}

		log.Infof("gRPC health server serving at %s", addr)
		return grpcHealthServer.Serve(listener)
	})

	// gRPC server
	server, err := newOpenSavesServer(ctx, cfg)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()

	g.Go(func() error {
		pb.RegisterOpenSavesServer(grpcServer, server)
		reflection.Register(grpcServer)

		addr := cfg.ServerConfig.Address
		listener, err := net.Listen(network, addr)
		if err != nil {
			log.Errorf("gRPC server: failed to listen %s %s: %v", network, addr, err)
			return err
		}

		log.Infof("starting server on %s %s", network, cfg.ServerConfig.Address)
		healthServer.SetServingStatus(serviceName, healthpb.HealthCheckResponse_SERVING)
		return grpcServer.Serve(listener)
	})

	select {
	case <-sigs:
		break
	case <-ctx.Done():
		break
	}
	log.Debug("received shutdown signal")
	healthServer.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)

	if grpcHealthServer != nil {
		log.Debug("grpc health server stop")
		grpcHealthServer.Stop()
	}
	if grpcServer != nil {
		log.Debug("grpc server graceful stop")
		grpcServer.GracefulStop()
	}

	log.Debug("waiting for all servers to complete")
	err = g.Wait()
	if err != nil {
		log.Errorf("server returning an error: %v", err)
		return err
	}

	log.Debug("existing without errors")
	return nil
}
