package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func initTelemetry(ctx context.Context, endpoint string) (*sdktrace.TracerProvider, *sdkmetric.MeterProvider, error) {
	res, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName("test-client")))
	if err != nil {
		return nil, nil, err
	}

	traceExp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	metricExp, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpoint(endpoint), otlpmetricgrpc.WithInsecure())
	if err != nil {
		return nil, nil, err
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(1*time.Second))), sdkmetric.WithResource(res))
	otel.SetMeterProvider(mp)

	return tp, mp, nil
}

func main() {
	slog.Info("Starting NATS OpenTelemetry Integration Test Suite...")

	natsURL := getEnv("NATS_URL", "nats://nats:4222")
	otlpEndpoint := getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	outputDir := getEnv("OTEL_OUTPUT_DIR", "/tmp/otel-output")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Wait for NATS to become ready
	var nc *nats.Conn
	var err error
	for i := 0; i < 30; i++ {
		nc, err = nats.Connect(natsURL)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		slog.Error("Could not connect to NATS", "err", err)
		os.Exit(1)
	}
	defer nc.Close()
	slog.Info("Connected to NATS server successfully", "url", natsURL)

	tp, mp, err := initTelemetry(ctx, otlpEndpoint)
	if err != nil {
		slog.Error("Failed to init telemetry", "err", err)
		os.Exit(1)
	}

	// Wait for receiver to be ready
	time.Sleep(3 * time.Second)

	if err := runScenarios(ctx, nc, natsURL); err != nil {
		slog.Error("Integration scenarios failed", "err", err)
		os.Exit(1)
	}

	// Flush telemetry providers
	slog.Info("Flushing OTel traces and metrics...")
	_ = tp.ForceFlush(ctx)
	_ = mp.ForceFlush(ctx)
	_ = tp.Shutdown(ctx)
	_ = mp.Shutdown(ctx)

	// Wait for collector to write files to disk
	slog.Info("Waiting for OTel collector output to flush to disk...")
	time.Sleep(5 * time.Second)

	// Verify Output Files
	slog.Info("Verifying OpenTelemetry Collector Output against Semantic Conventions...")
	verifyTelemetryOutputs(outputDir)
}
