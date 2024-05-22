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
	"github.com/googleforgames/open-saves/internal/pkg/metrics"
	"github.com/googleforgames/open-saves/internal/pkg/tracing"
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc/keepalive"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/googleforgames/open-saves/api"
	"github.com/googleforgames/open-saves/internal/pkg/config"
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

	unaryChain := []grpc.UnaryServerInterceptor{}
	streamChain := []grpc.StreamServerInterceptor{}

	grpcOptions := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     cfg.GRPCServerConfig.MaxConnectionIdle,
			MaxConnectionAge:      cfg.GRPCServerConfig.MaxConnectionAge,
			MaxConnectionAgeGrace: cfg.GRPCServerConfig.MaxConnectionAgeGrace,
			Time:                  cfg.GRPCServerConfig.Time,
			Timeout:               cfg.GRPCServerConfig.Timeout,
		}),
	}

	// Tracing
	var tracer *trace.TracerProvider
	if cfg.EnableTrace {
		log.Printf("Enabling CloudTrace exporter with sample rate: %f\n", cfg.ServerConfig.TraceSampleRate)

		unaryChain = append(unaryChain, otelgrpc.UnaryServerInterceptor())
		streamChain = append(streamChain, otelgrpc.StreamServerInterceptor())

		tracer, err = tracing.InitTracer(cfg.ServerConfig.TraceSampleRate, cfg.ServerConfig.EnableGRPCCollector, cfg.ServerConfig.EnableHTTPCollector, cfg.TraceServiceName)
		if err != nil {
			return err
		}
	}

	defer func() {
		if tracer != nil {
			if err := tracer.Shutdown(context.Background()); err != nil {
				log.Fatalf("Error shutting down tracer provider: %v", err)
			}
		}
	}()

	//Metrics
	var serverMetrics *grpcprom.ServerMetrics
	if cfg.EnableMetrics {
		log.Info("metrics is enabled, enabling Prometheus metrics")

		reg := prometheus.NewRegistry()

		serverMetrics = metrics.Initialize(reg, cfg.MetricsPort)

		unaryChain = append(unaryChain, serverMetrics.UnaryServerInterceptor())
		streamChain = append(streamChain, serverMetrics.StreamServerInterceptor())

	}

	grpcOptions = append(grpcOptions, grpc.ChainUnaryInterceptor(unaryChain...))
	grpcOptions = append(grpcOptions, grpc.ChainStreamInterceptor(streamChain...))

	s := grpc.NewServer(grpcOptions...)

	healthcheck := health.NewServer()
	healthcheck.SetServingStatus(serviceName, healthgrpc.HealthCheckResponse_SERVING)
	healthgrpc.RegisterHealthServer(s, healthcheck)
	server, err := newOpenSavesServer(ctx, cfg)
	if err != nil {
		return err
	}
	pb.RegisterOpenSavesServer(s, server)
	reflection.Register(s)

	if cfg.EnableMetrics {
		serverMetrics.InitializeMetrics(s)
	}

	go func() {
		select {
		case s := <-sigs:
			log.Infof("received signal: %v\n", s)
		case <-ctx.Done():
			log.Infoln("server: context cancelled")
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
