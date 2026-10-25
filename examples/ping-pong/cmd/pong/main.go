package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
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

func initTelemetry(ctx context.Context, serviceName, instanceID, otlpEndpoint string) (log.Logger, func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceInstanceID(instanceID),
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
		serviceName  = flag.String("service", getEnv("SERVICE_NAME", "pong-service"), "Service name for telemetry")
		workerID     = flag.String("worker", getEnv("WORKER_ID", "pong-worker-1"), "Worker identifier for queue group")
	)
	flag.Parse()

	slog.Info("Starting Pong Service", "nats", *natsURL, "otlp", *otlpEndpoint, "worker", *workerID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	otelLogger, shutdownTelemetry, err := initTelemetry(ctx, *serviceName, *workerID, *otlpEndpoint)
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

	pongsCounter, err := meter.Int64Counter("pong.replies.total",
		metric.WithDescription("Total number of pongs replied"),
		metric.WithUnit("{reply}"),
	)
	if err != nil {
		slog.Error("Failed to create pongs counter", "err", err)
		os.Exit(1)
	}

	// Connect to NATS with retries
	var nc *nats.Conn
	for i := 0; i < 30; i++ {
		nc, err = nats.Connect(*natsURL, nats.Name(*serviceName+"-"+*workerID))
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

	// QueueSubscribe to load balance across multiple pong worker replicas
	sub, err := client.QueueSubscribe("ping.requests", "pong-workers", func(ctx context.Context, msg *nats.Msg) error {
		var ping PingPayload
		if err := json.Unmarshal(msg.Data, &ping); err != nil {
			slog.Warn("Failed to decode ping payload", "err", err)
			return err
		}

		// Execute child span for processing work
		ctx, span := tracer.Start(ctx, "process_pong",
			trace.WithSpanKind(trace.SpanKindInternal),
			trace.WithAttributes(
				attribute.Int64("ping.seq", ping.Seq),
				attribute.String("pong.worker.id", *workerID),
			),
		)
		defer span.End()

		// Simulate business logic / database lookup
		workDelay := time.Duration(15+rand.Intn(25)) * time.Millisecond
		time.Sleep(workDelay)

		replySubj := msg.Reply
		if replySubj == "" {
			replySubj = "ping.replies"
		}

		pong := PongPayload{
			Seq:          ping.Seq,
			WorkerID:     *workerID,
			OriginalTime: ping.Timestamp,
			PongTime:     time.Now().UnixNano(),
		}
		pongBytes, _ := json.Marshal(pong)

		replyMsg := nats.NewMsg(replySubj)
		replyMsg.Data = pongBytes
		replyMsg.Header = make(nats.Header)
		replyMsg.Header.Set("Nats-Msg-Id", fmt.Sprintf("PONG-%d", ping.Seq))

		// Propagate active trace context back to sender
		otel.GetTextMapPropagator().Inject(ctx, carrier.HeaderCarrier(replyMsg.Header))

		if err := client.PublishMsg(ctx, replyMsg); err != nil {
			span.RecordError(err)
			slog.Error("Failed to send pong reply", "seq", ping.Seq, "err", err)
			return err
		}

		pongsCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("pong.worker.id", *workerID),
		))

		if otelLogger != nil {
			rec := log.Record{}
			rec.SetTimestamp(time.Now())
			rec.SetObservedTimestamp(time.Now())
			rec.SetSeverity(log.SeverityInfo)
			rec.SetSeverityText("INFO")
			rec.SetBody(attribute.StringValue(fmt.Sprintf("Worker %s processed ping seq=%d (delay=%v) and replied [trace_id=%s span_id=%s]", *workerID, ping.Seq, workDelay, span.SpanContext().TraceID().String(), span.SpanContext().SpanID().String())))
			rec.AddAttributes(
				attribute.Int64("ping.seq", ping.Seq),
				attribute.String("pong.worker.id", *workerID),
				attribute.Float64("work_delay.ms", float64(workDelay.Milliseconds())),
				attribute.String("messaging.system", "nats"),
				attribute.String("messaging.destination.name", replySubj),
			)
			otelLogger.Emit(ctx, rec)
		}

		slog.Info("Pong reply sent",
			"seq", ping.Seq,
			"worker", *workerID,
			"work_delay", workDelay.String(),
			"dest", replySubj,
		)
		return nil
	})
	if err != nil {
		slog.Error("Failed to subscribe with queue group", "err", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	slog.Info("Pong service worker ready and listening on ping.requests (queue: pong-workers)...")

	// Wait for shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	slog.Info("Shutting down Pong Service...")
}
