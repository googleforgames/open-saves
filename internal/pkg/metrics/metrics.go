package metrics

import (
	"errors"
	"fmt"
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func Initialize(reg *prometheus.Registry, metricsPort int32) *grpcprom.ServerMetrics {
	srvMetrics := grpcprom.NewServerMetrics(
		grpcprom.WithServerHandlingTimeHistogram(
			grpcprom.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
		),
	)

	reg.MustRegister(srvMetrics)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {

		httpSrv := &http.Server{Addr: fmt.Sprintf(":%d", metricsPort)}
		m := http.NewServeMux()
		// Create HTTP handler for Prometheus metrics.
		m.Handle("/metrics", promhttp.HandlerFor(
			reg,
			promhttp.HandlerOpts{
				// Opt out of OpenMetrics not supporting exemplars.
				EnableOpenMetrics: false,
			},
		))

		httpSrv.Handler = m

		err := httpSrv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			log.Infof("http metrics server terminated: %s", err)
		} else {
			log.Errorf("http metrics server terminated unexpectedly: %s", err)
		}
		sigs <- syscall.SIGQUIT
	}()

	return srvMetrics
}
