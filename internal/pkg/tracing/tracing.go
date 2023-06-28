package tracing

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
)

var ServiceName = "open-saves"

func InitTracer(rate float64, enableGRPCCollector bool, enableHTTPCollector bool, serviceName string) (*sdktrace.TracerProvider, error) {
	if len(serviceName) > 0 {
		ServiceName = serviceName
	}

	extraResources, _ := sdkresource.New(
		context.Background(),
		sdkresource.WithOS(),
		sdkresource.WithProcess(),
		sdkresource.WithContainer(),
		sdkresource.WithHost(),
		sdkresource.WithAttributes(semconv.ServiceName(ServiceName)),
	)
	resource, _ := sdkresource.Merge(
		sdkresource.Default(),
		extraResources,
	)

	options := []sdktrace.TracerProviderOption{sdktrace.WithResource(resource),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(rate)))}

	if enableGRPCCollector {
		grpcExporter, err := otlptracegrpc.New(context.Background())
		if err != nil {
			return nil, err
		}
		options = append(options, sdktrace.WithBatcher(grpcExporter))
	}

	if enableHTTPCollector {
		httpExporter, err := otlptracehttp.New(context.Background())
		if err != nil {
			return nil, err
		}
		options = append(options, sdktrace.WithBatcher(httpExporter))
	}

	tp := sdktrace.NewTracerProvider(options...)
	otel.SetTracerProvider(tp)

	return tp, nil
}
