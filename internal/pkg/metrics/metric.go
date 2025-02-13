package metrics

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/metric"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
)

func InitMetrics(enableGRPCCollector bool, enableHTTPCollector bool) (*metric.MeterProvider, error) {
	extraResources, _ := sdkresource.New(
		context.Background(),
		sdkresource.WithOS(),
		sdkresource.WithProcess(),
		sdkresource.WithContainer(),
		sdkresource.WithHost(),
		sdkresource.WithAttributes(),
	)
	resource, _ := sdkresource.Merge(
		sdkresource.Default(),
		extraResources,
	)

	options := []metric.Option{metric.WithResource(resource)}

	if enableGRPCCollector {
		exporter, err := otlpmetricgrpc.New(context.Background())
		if err != nil {
			return nil, err
		}
		options = append(options, metric.WithReader(metric.NewPeriodicReader(exporter)))
	}

	if enableHTTPCollector {
		exporter, err := otlpmetrichttp.New(context.Background())
		if err != nil {
			return nil, err
		}
		options = append(options, metric.WithReader(metric.NewPeriodicReader(exporter)))
	}

	meterProvider := metric.NewMeterProvider(options...)

	// Register as global meter provider so that it can be used via otel.Meter
	// and accessed using otel.GetMeterProvider.
	// Most instrumentation libraries use the global meter provider as default.
	// If the global meter provider is not set then a no-op implementation
	// is used, which fails to generate data.
	otel.SetMeterProvider(meterProvider)

	return meterProvider, nil
}
