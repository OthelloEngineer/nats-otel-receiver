package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/carrier"
	"github.com/nats-otel/nats-otel/pkg/natsotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type PingPayload struct {
	Seq       int64  `json:"seq"`
	Sender    string `json:"sender"`
	Timestamp int64  `json:"timestamp_ns"`
}

type PongPayload struct {
	Seq          int64  `json:"seq"`
	WorkerID     string `json:"worker_id"`
	OriginalTime int64  `json:"original_timestamp_ns"`
	PongTime     int64  `json:"pong_timestamp_ns"`
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func initTelemetry(ctx context.Context, serviceName, otlpEndpoint string) (log.Logger, func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceInstanceID("ping-client-1"),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create resource: %w", err)
	}

	traceExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(otlpEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(2*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	var lp *sdklog.LoggerProvider
	logExp, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint(otlpEndpoint),
		otlploggrpc.WithInsecure(),
	)
	if err == nil {
		lp = sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
			sdklog.WithResource(res),
		)
	}

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	var otelLogger log.Logger
	if lp != nil {
		otelLogger = lp.Logger(serviceName)
	}

	shutdown := func(ctx context.Context) error {
		_ = tp.Shutdown(ctx)
		_ = mp.Shutdown(ctx)
		if lp != nil {
			_ = lp.Shutdown(ctx)
		}
		return nil
	}
	return otelLogger, shutdown, nil
}

func main() {
	var (
		natsURL      = flag.String("nats", getEnv("NATS_URL", "nats://127.0.0.1:4222"), "NATS server URL")
		otlpEndpoint = flag.String("otlp-endpoint", getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:4317"), "OTLP gRPC endpoint")
		interval     = flag.Duration("interval", 1*time.Second, "Ping broadcast interval")
		serviceName  = flag.String("service", getEnv("SERVICE_NAME", "ping-service"), "Service name for telemetry")
	)
	flag.Parse()

	slog.Info("Starting Ping Service", "nats", *natsURL, "otlp", *otlpEndpoint, "interval", *interval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	otelLogger, shutdownTelemetry, err := initTelemetry(ctx, *serviceName, *otlpEndpoint)
	if err != nil {
		slog.Error("Failed to initialize telemetry", "err", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, sCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer sCancel()
		_ = shutdownTelemetry(shutdownCtx)
	}()

	tracer := otel.GetTracerProvider().Tracer(*serviceName)
	meter := otel.GetMeterProvider().Meter(*serviceName)

	roundtripHistogram, err := meter.Float64Histogram("ping.roundtrip.duration",
		metric.WithDescription("Roundtrip duration of ping-pong requests in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		slog.Error("Failed to create roundtrip histogram", "err", err)
		os.Exit(1)
	}

	// Connect to NATS with retries
	var nc *nats.Conn
	for i := 0; i < 30; i++ {
		nc, err = nats.Connect(*natsURL, nats.Name(*serviceName))
		if err == nil {
			break
		}
		slog.Warn("Waiting for NATS server...", "attempt", i+1, "err", err)
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		slog.Error("Failed to connect to NATS", "err", err)
		os.Exit(1)
	}
	defer nc.Close()

	client, err := natsotel.NewClient(nc)
	if err != nil {
		slog.Error("Failed to wrap NATS client with natsotel", "err", err)
		os.Exit(1)
	}

	// Initialize JetStream for demo persistence metrics
	js, err := nc.JetStream()
	if err == nil {
		_, _ = js.AddStream(&nats.StreamConfig{
			Name:     "ORDERS",
			Subjects: []string{"orders.>"},
			Storage:  nats.FileStorage,
			MaxMsgs:  5000,
		})
		_, _ = js.AddConsumer("ORDERS", &nats.ConsumerConfig{
			Durable:   "order-audit",
			AckPolicy: nats.AckExplicitPolicy,
		})
		slog.Info("JetStream ORDERS stream initialized successfully")
	}

	// Subscribe to responses
	sub, err := client.Subscribe("ping.replies", func(ctx context.Context, msg *nats.Msg) error {
		var pong PongPayload
		if err := json.Unmarshal(msg.Data, &pong); err != nil {
			slog.Warn("Failed to decode pong payload", "err", err)
			return err
		}

		now := time.Now().UnixNano()
		roundtripSec := float64(now-pong.OriginalTime) / 1e9

		roundtripHistogram.Record(ctx, roundtripSec, metric.WithAttributes(
			attribute.String("pong.worker.id", pong.WorkerID),
		))

		if otelLogger != nil {
			span := trace.SpanFromContext(ctx)
			rec := log.Record{}
			rec.SetTimestamp(time.Now())
			rec.SetObservedTimestamp(time.Now())
			rec.SetSeverity(log.SeverityInfo)
			rec.SetSeverityText("INFO")
			rec.SetBody(attribute.StringValue(fmt.Sprintf("Received pong reply seq=%d from %s (rtt=%.2fms) [trace_id=%s span_id=%s]", pong.Seq, pong.WorkerID, roundtripSec*1000, span.SpanContext().TraceID().String(), span.SpanContext().SpanID().String())))
			rec.AddAttributes(
				attribute.Int64("ping.seq", pong.Seq),
				attribute.String("pong.worker.id", pong.WorkerID),
				attribute.Float64("roundtrip.duration.ms", roundtripSec*1000),
				attribute.String("messaging.system", "nats"),
				attribute.String("messaging.destination.name", "ping.replies"),
			)
			otelLogger.Emit(ctx, rec)
		}

		slog.Info("Pong received",
			"seq", pong.Seq,
			"worker", pong.WorkerID,
			"roundtrip_ms", fmt.Sprintf("%.2fms", roundtripSec*1000),
		)
		return nil
	})
	if err != nil {
		slog.Error("Failed to subscribe to ping.replies", "err", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	var seq int64
	slog.Info("Ping service ready. Broadcasting pings...")

	for {
		select {
		case <-sigChan:
			slog.Info("Shutting down Ping Service...")
			return
		case <-ticker.C:
			currentSeq := atomic.AddInt64(&seq, 1)
			sendPing(ctx, client, tracer, otelLogger, js, currentSeq, *serviceName)
		}
	}
}

func sendPing(parentCtx context.Context, client *natsotel.Client, tracer trace.Tracer, otelLogger log.Logger, js nats.JetStreamContext, seq int64, serviceName string) {
	ctx, span := tracer.Start(parentCtx, "Ping Transaction",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.Int64("ping.seq", seq),
			attribute.String("app.role", "sender"),
		),
	)
	defer span.End()

	payload := PingPayload{
		Seq:       seq,
		Sender:    serviceName,
		Timestamp: time.Now().UnixNano(),
	}
	data, _ := json.Marshal(payload)

	msg := nats.NewMsg("ping.requests")
	msg.Data = data
	msg.Header = make(nats.Header)
	msg.Header.Set("Nats-Msg-Id", fmt.Sprintf("PING-%d", seq))
	msg.Reply = "ping.replies"

	// Inject W3C TraceContext into the NATS message header
	otel.GetTextMapPropagator().Inject(ctx, carrier.HeaderCarrier(msg.Header))

	// Publish via instrumented natsotel client
	if err := client.PublishMsg(ctx, msg); err != nil {
		span.RecordError(err)
		slog.Error("Failed to publish ping", "seq", seq, "err", err)
		return
	}

	// Publish JetStream message to populate JetStream stream, consumer, and storage metrics
	if js != nil {
		orderPayload := fmt.Sprintf(`{"order_id":%d,"item":"nats-pro-license","timestamp":%d}`, seq, time.Now().UnixNano())
		orderMsg := nats.NewMsg("orders.create")
		orderMsg.Data = []byte(orderPayload)
		orderMsg.Header = make(nats.Header)
		orderMsg.Header.Set("Nats-Msg-Id", fmt.Sprintf("ORDER-%d", seq))
		otel.GetTextMapPropagator().Inject(ctx, carrier.HeaderCarrier(orderMsg.Header))
		_, _ = js.PublishMsg(orderMsg)
	}

	if otelLogger != nil {
		rec := log.Record{}
		rec.SetTimestamp(time.Now())
		rec.SetObservedTimestamp(time.Now())
		rec.SetSeverity(log.SeverityInfo)
		rec.SetSeverityText("INFO")
		rec.SetBody(attribute.StringValue(fmt.Sprintf("Dispatched ping request seq=%d to %s [trace_id=%s span_id=%s]", seq, msg.Subject, span.SpanContext().TraceID().String(), span.SpanContext().SpanID().String())))
		rec.AddAttributes(
			attribute.Int64("ping.seq", seq),
			attribute.String("messaging.destination.name", msg.Subject),
			attribute.String("messaging.system", "nats"),
		)
		otelLogger.Emit(ctx, rec)
	}

	slog.Info("Ping sent", "seq", seq, "subject", msg.Subject)
}
