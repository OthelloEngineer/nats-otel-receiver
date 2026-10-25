package receiver

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Config configures the NATS OpenTelemetry Receiver
type Config struct {
	NatsURL        string
	NatsMonitorURL string
	OTLPEndpoint   string
	OTLPInsecure   bool
	ScrapeInterval time.Duration
	TappedSubjects []string
	ServiceName    string
}

// Server coordinates metrics, logs, and traces receivers
type Server struct {
	cfg     Config
	nc      *nats.Conn
	metrics *MetricsReceiver
	logs    *LogsReceiver
	traces  *TracesReceiver

	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	loggerProvider *sdklog.LoggerProvider

	shutdownFuncs []func(context.Context) error
}

// NewServer initializes the OpenTelemetry pipeline and receivers
func NewServer(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "nats-otel-receiver"
	}
	if cfg.ScrapeInterval == 0 {
		cfg.ScrapeInterval = 5 * time.Second
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	s := &Server{cfg: cfg}

	// 1. Setup OTLP Traces Exporter & Provider
	var traceOpts []otlptracegrpc.Option
	traceOpts = append(traceOpts, otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint))
	if cfg.OTLPInsecure {
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
	}
	traceExp, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}
	s.tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(s.tracerProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	s.shutdownFuncs = append(s.shutdownFuncs, s.tracerProvider.Shutdown)

	// 2. Setup OTLP Metrics Exporter & Provider
	var metricOpts []otlpmetricgrpc.Option
	metricOpts = append(metricOpts, otlpmetricgrpc.WithEndpoint(cfg.OTLPEndpoint))
	if cfg.OTLPInsecure {
		metricOpts = append(metricOpts, otlpmetricgrpc.WithInsecure())
	}
	metricExp, err := otlpmetricgrpc.New(ctx, metricOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}
	s.meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(cfg.ScrapeInterval))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(s.meterProvider)
	s.shutdownFuncs = append(s.shutdownFuncs, s.meterProvider.Shutdown)

	// 3. Setup OTLP Logs Exporter & Provider
	var logOpts []otlploggrpc.Option
	logOpts = append(logOpts, otlploggrpc.WithEndpoint(cfg.OTLPEndpoint))
	if cfg.OTLPInsecure {
		logOpts = append(logOpts, otlploggrpc.WithInsecure())
	}
	logExp, err := otlploggrpc.New(ctx, logOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create log exporter: %w", err)
	}
	s.loggerProvider = sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
		sdklog.WithResource(res),
	)
	s.shutdownFuncs = append(s.shutdownFuncs, s.loggerProvider.Shutdown)

	// 4. Connect to NATS
	nc, err := nats.Connect(cfg.NatsURL, nats.Name("nats-otel-receiver"))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats at %s: %w", cfg.NatsURL, err)
	}
	s.nc = nc

	// 5. Initialize Sub-Receivers
	meter := s.meterProvider.Meter("nats-otel-receiver")
	mr, err := NewMetricsReceiver(cfg.NatsMonitorURL, meter)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics receiver: %w", err)
	}
	s.metrics = mr

	otelLogger := s.loggerProvider.Logger("nats-otel-receiver")
	s.logs = NewLogsReceiver(nc, otelLogger)

	tracer := s.tracerProvider.Tracer("nats-otel-receiver")
	s.traces = NewTracesReceiver(nc, tracer, otelLogger, cfg.NatsURL)

	return s, nil
}

// Start runs all receivers
func (s *Server) Start(ctx context.Context) error {
	slog.Info("Starting NATS OpenTelemetry Receiver",
		"nats", s.cfg.NatsURL,
		"monitor", s.cfg.NatsMonitorURL,
		"otlp", s.cfg.OTLPEndpoint,
	)

	// Start Logs Receiver
	if err := s.logs.Start(ctx); err != nil {
		return fmt.Errorf("failed to start logs receiver: %w", err)
	}

	// Start Traces Receiver
	if err := s.traces.Start(s.cfg.TappedSubjects); err != nil {
		return fmt.Errorf("failed to start traces receiver: %w", err)
	}

	// Start Metrics Receiver polling in background
	go s.metrics.Start(ctx, s.cfg.ScrapeInterval)

	return nil
}

// Shutdown gracefully shuts down all receivers and flushes OTLP exporters
func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("Shutting down NATS OpenTelemetry Receiver...")
	if s.logs != nil {
		s.logs.Stop()
	}
	if s.traces != nil {
		s.traces.Stop()
	}
	if s.nc != nil {
		s.nc.Close()
	}

	for _, fn := range s.shutdownFuncs {
		if err := fn(ctx); err != nil {
			slog.Warn("Error shutting down provider", "err", err)
		}
	}
	return nil
}
