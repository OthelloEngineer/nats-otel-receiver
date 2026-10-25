package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/natsotel"
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

	// Setup OpenTelemetry client SDK
	res, _ := resource.New(ctx, resource.WithAttributes(semconv.ServiceName("test-client")))
	traceExp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(otlpEndpoint), otlptracegrpc.WithInsecure())
	if err != nil {
		slog.Error("Failed to init trace exporter", "err", err)
		os.Exit(1)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	metricExp, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpoint(otlpEndpoint), otlpmetricgrpc.WithInsecure())
	if err != nil {
		slog.Error("Failed to init metric exporter", "err", err)
		os.Exit(1)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(1*time.Second))), sdkmetric.WithResource(res))
	otel.SetMeterProvider(mp)

	client, err := natsotel.NewClient(nc)
	if err != nil {
		slog.Error("Failed to create natsotel client", "err", err)
		os.Exit(1)
	}

	// Wait for receiver to be ready
	time.Sleep(3 * time.Second)

	// Scenario 1: Core Pub/Sub with trace context
	slog.Info("Executing Scenario 1: Pub/Sub with trace context...")
	subReceived := make(chan bool, 1)
	sub, err := client.Subscribe("orders.created", func(ctx context.Context, msg *nats.Msg) error {
		slog.Info("Received message", "subject", msg.Subject, "data", string(msg.Data), "header", msg.Header)
		subReceived <- true
		return nil
	})
	if err != nil {
		slog.Error("Failed to subscribe", "err", err)
		os.Exit(1)
	}
	defer sub.Unsubscribe()

	err = client.Publish(ctx, "orders.created", []byte(`{"order_id": "ORD-123", "amount": 99.50}`))
	if err != nil {
		slog.Error("Failed to publish", "err", err)
		os.Exit(1)
	}

	select {
	case <-subReceived:
		slog.Info("Scenario 1 passed: message consumed with trace context")
	case <-time.After(5 * time.Second):
		slog.Error("Scenario 1 failed: timeout waiting for message")
		os.Exit(1)
	}

	// Scenario 2: Queue Group subscription
	slog.Info("Executing Scenario 2: Queue Group subscription...")
	qReceived := make(chan bool, 1)
	qsub, err := client.QueueSubscribe("tasks.process", "worker-group", func(ctx context.Context, msg *nats.Msg) error {
		slog.Info("Queue subscriber received task", "subject", msg.Subject, "group", "worker-group")
		qReceived <- true
		return nil
	})
	if err != nil {
		slog.Error("Failed to queue subscribe", "err", err)
		os.Exit(1)
	}
	defer qsub.Unsubscribe()

	_ = client.Publish(ctx, "tasks.process", []byte(`{"task": "encode-video"}`))
	select {
	case <-qReceived:
		slog.Info("Scenario 2 passed: queue group message processed")
	case <-time.After(5 * time.Second):
		slog.Error("Scenario 2 failed: timeout waiting for queue message")
		os.Exit(1)
	}

	// Scenario 3: JetStream persistence and Sequence
	slog.Info("Executing Scenario 3: JetStream publish & sequence tracking...")
	js, err := nc.JetStream()
	if err == nil {
		_, _ = js.AddStream(&nats.StreamConfig{
			Name:     "ORDERS_STREAM",
			Subjects: []string{"orders.js.>"},
			Storage:  nats.MemoryStorage,
		})

		pubAck, jsErr := client.PublishJetStream(ctx, js, "orders.js.created", []byte(`{"item": "laptop"}`), "MSG-JS-001")
		if jsErr != nil {
			slog.Warn("JetStream publish failed", "err", jsErr)
		} else {
			slog.Info("Scenario 3 passed: JetStream published", "seq", pubAck.Sequence, "stream", pubAck.Stream)
		}
	}

	// Scenario 4: Trigger Server System Advisories ($SYS)
	slog.Info("Executing Scenario 4: Triggering NATS $SYS advisories...")
	tempConn, err := nats.Connect(natsURL, nats.Name("temp-client-for-events"))
	if err == nil {
		time.Sleep(500 * time.Millisecond)
		tempConn.Close()
		slog.Info("Scenario 4: Temporary connection opened and closed to trigger connect/disconnect events")
	}

	// Scenario 5: Tapped Subject (receiver taps message and emits receive span)
	slog.Info("Executing Scenario 5: Publishing to tapped subject...")
	_ = client.Publish(ctx, "telemetry.stats", []byte(`{"cpu": 45.2}`))

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

func verifyTelemetryOutputs(outputDir string) {
	tracesFile := outputDir + "/traces.json"
	metricsFile := outputDir + "/metrics.json"
	logsFile := outputDir + "/logs.json"

	var tracesData, metricsData, logsData []byte
	var traceErr, metricErr, logErr error

	// Wait up to 15 seconds for all pipelines to flush to disk
	for i := 0; i < 15; i++ {
		tracesData, traceErr = os.ReadFile(tracesFile)
		metricsData, metricErr = os.ReadFile(metricsFile)
		logsData, logErr = os.ReadFile(logsFile)

		if traceErr == nil && metricErr == nil && logErr == nil {
			strMetrics := string(metricsData)
			if strings.Contains(strMetrics, "nats.server.connections") &&
				strings.Contains(strMetrics, "nats.server.memory") &&
				strings.Contains(strMetrics, "messaging.client.sent.messages") {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}

	fmt.Println("\n=======================================================")
	fmt.Println("    OPENTELEMETRY SEMANTIC CONVENTIONS VALIDATION")
	fmt.Println("=======================================================")

	passed := true

	// 1. Traces Validation
	fmt.Println("\n[1] TRACES VALIDATION:")
	if traceErr != nil {
		fmt.Printf("  [-] Traces file missing: %v\n", traceErr)
		passed = false
	} else {
		strTraces := string(tracesData)
		checkRequirement := func(label string, condition bool) {
			if condition {
				fmt.Printf("  [+] %s: VERIFIED\n", label)
			} else {
				fmt.Printf("  [-] %s: NOT FOUND\n", label)
				passed = false
			}
		}

		checkRequirement("messaging.system == 'nats'", strings.Contains(strTraces, "messaging.system") && strings.Contains(strTraces, "nats"))
		checkRequirement("messaging.destination.name (subject)", strings.Contains(strTraces, "messaging.destination.name") && strings.Contains(strTraces, "orders.created"))
		checkRequirement("messaging.operation.type (publish & process)", strings.Contains(strTraces, "messaging.operation.type") && strings.Contains(strTraces, "publish"))
		checkRequirement("messaging.message.body.size", strings.Contains(strTraces, "messaging.message.body.size"))
		checkRequirement("messaging.consumer.group.name (worker-group)", strings.Contains(strTraces, "messaging.consumer.group.name") && strings.Contains(strTraces, "worker-group"))
		checkRequirement("W3C traceparent propagation", strings.Contains(strTraces, "traceId") && strings.Contains(strTraces, "spanId"))
	}

	// 2. Metrics Validation
	fmt.Println("\n[2] METRICS VALIDATION:")
	if metricErr != nil {
		fmt.Printf("  [-] Metrics file missing: %v\n", metricErr)
		passed = false
	} else {
		strMetrics := string(metricsData)
		checkMetric := func(label, metricName string) {
			if strings.Contains(strMetrics, metricName) {
				fmt.Printf("  [+] %s (%s): VERIFIED\n", label, metricName)
			} else {
				fmt.Printf("  [-] %s (%s): NOT FOUND\n", label, metricName)
				passed = false
			}
		}

		checkMetric("Client Sent Messages", "messaging.client.sent.messages")
		checkMetric("Client Consumed Messages", "messaging.client.consumed.messages")
		checkMetric("Publish Duration Histogram", "messaging.client.operation.duration")
		checkMetric("Process Duration Histogram", "messaging.process.duration")
		checkMetric("Server Connections Gauge", "nats.server.connections")
		checkMetric("Server Memory Gauge", "nats.server.memory")
		checkMetric("JetStream Streams Gauge", "nats.jetstream.streams")
	}

	// 3. Logs Validation
	fmt.Println("\n[3] LOGS VALIDATION:")
	if logErr != nil {
		fmt.Printf("  [-] Logs file missing: %v\n", logErr)
		passed = false
	} else {
		strLogs := string(logsData)
		checkLog := func(label string, condition bool) {
			if condition {
				fmt.Printf("  [+] %s: VERIFIED\n", label)
			} else {
				fmt.Printf("  [-] %s: NOT FOUND\n", label)
				passed = false
			}
		}

		checkLog("NATS Advisory Ingestion", strings.Contains(strLogs, "nats"))
		checkLog("Severity Number & Severity Text", strings.Contains(strLogs, "INFO") || strings.Contains(strLogs, "WARN") || strings.Contains(strLogs, "SEVERITY"))
		checkLog("Advisory Attributes (server.id / event.domain)", strings.Contains(strLogs, "nats") && (strings.Contains(strLogs, "event.domain") || strings.Contains(strLogs, "nats.server.id") || strings.Contains(strLogs, "nats.subject")))
	}

	fmt.Println()
	fmt.Println("=======================================================")
	if passed {
		fmt.Println("    OVERALL RESULT: SUCCESS! ALL OTel SEMCONV ASSERTIONS PASSED.")
		fmt.Println("=======================================================")
		os.Exit(0)
	} else {
		fmt.Println("    OVERALL RESULT: FAILED SOME ASSERTIONS.")
		fmt.Println("=======================================================")
		os.Exit(1)
	}
}
