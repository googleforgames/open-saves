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
	"google.golang.org/grpc/keepalive"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/googleforgames/open-saves/internal/pkg/config"

	pb "github.com/googleforgames/open-saves/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

const (
	serviceName = "grpc.health.v1.opensaves"
)

// Run starts the Open Saves gRPC service.
func Run(ctx context.Context, network string, cfg *config.ServiceConfig) error {
	log.Infof("starting server on %s %s", network, cfg.ServerConfig.Address)

	lis, err := net.Listen(network, cfg.ServerConfig.Address)
	if err != nil {
		return err
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	svOptions := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     cfg.GRPCServerConfig.MaxConnectionIdle,
			MaxConnectionAge:      cfg.GRPCServerConfig.MaxConnectionAge,
			MaxConnectionAgeGrace: cfg.GRPCServerConfig.MaxConnectionAgeGrace,
			Time:                  cfg.GRPCServerConfig.Time,
			Timeout:               cfg.GRPCServerConfig.Timeout,
		}),
	}

	s := grpc.NewServer(svOptions...)

	healthcheck := health.NewServer()
	healthcheck.SetServingStatus(serviceName, healthgrpc.HealthCheckResponse_SERVING)
	healthgrpc.RegisterHealthServer(s, healthcheck)
	server, err := newOpenSavesServer(ctx, cfg)
	if err != nil {
		return err
	}
	pb.RegisterOpenSavesServer(s, server)
	reflection.Register(s)

	go func() {
		select {
		case s := <-sigs:
			log.Infof("received signal: %v\n", s)
		case <-ctx.Done():
			log.Infoln("context cancelled")
		}
		log.Infoln("set health check to not serving")
		healthcheck.SetServingStatus(serviceName, healthgrpc.HealthCheckResponse_NOT_SERVING)

		log.Infoln("starting server shutdown grace period")
		time.Sleep(cfg.ShutdownGracePeriod)

		log.Infoln("stopping open saves server gracefully")
		s.GracefulStop()
		log.Infoln("stopped open saves server gracefully")
	}()

	return s.Serve(lis)
}
