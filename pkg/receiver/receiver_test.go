package receiver_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/receiver"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type recordingLogger struct {
	noop.Logger
	records []log.Record
}

func (r *recordingLogger) Emit(ctx context.Context, record log.Record) {
	r.records = append(r.records, record)
}

func TestMetricsReceiverScrape(t *testing.T) {
	// 1. Mock NATS HTTP monitoring server
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/varz":
			v := receiver.VarzResponse{
				ServerID:    "TEST_SRV_1",
				ServerName:  "nats-node-1",
				Version:     "2.10.22",
				Host:        "127.0.0.1",
				Port:        4222,
				Connections: 12,
				Mem:         104857600,
				InMsgs:      500,
				OutMsgs:     450,
			}
			_ = json.NewEncoder(w).Encode(v)
		case "/jsz":
			j := receiver.JszResponse{
				Streams:   3,
				Consumers: 8,
				Messages:  1000,
				Bytes:     204800,
			}
			_ = json.NewEncoder(w).Encode(j)
		default:
			http.NotFound(w, r)
		}
	}))
	defer mockServer.Close()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("test-meter")

	mr, err := receiver.NewMetricsReceiver(mockServer.URL, meter)
	if err != nil {
		t.Fatalf("failed to create metrics receiver: %v", err)
	}

	ctx := context.Background()
	if err := mr.ScrapeOnce(ctx); err != nil {
		t.Fatalf("ScrapeOnce failed: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	foundConn := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "nats.server.connections" {
				foundConn = true
			}
		}
	}
	if !foundConn {
		t.Errorf("expected metric nats.server.connections to be recorded")
	}
}

func TestLogsReceiverSeverities(t *testing.T) {
	rec := &recordingLogger{}
	lr := receiver.NewLogsReceiver(nil, rec)

	// Test connect event -> INFO
	connectMsg := &nats.Msg{
		Subject: "$SYS.ACCOUNT.TEST.CONNECT",
		Data:    []byte(`{"type":"CLIENT_CONNECT","server":{"id":"S1","name":"srv1"}}`),
	}
	lr.HandleEvent(context.Background(), connectMsg)

	// Test disconnect event -> WARN
	disconnectMsg := &nats.Msg{
		Subject: "$SYS.ACCOUNT.TEST.DISCONNECT",
		Data:    []byte(`{"type":"CLIENT_DISCONNECT","reason":"slow consumer","server":{"id":"S1"}}`),
	}
	lr.HandleEvent(context.Background(), disconnectMsg)

	// Test advisory error -> ERROR
	errorMsg := &nats.Msg{
		Subject: "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES",
		Data:    []byte(`{"type":"io.nats.jetstream.advisory.v1.max_deliver","stream":"S","consumer":"C"}`),
	}
	lr.HandleEvent(context.Background(), errorMsg)

	if len(rec.records) != 3 {
		t.Fatalf("expected 3 log records, got %d", len(rec.records))
	}

	if rec.records[0].Severity() != log.SeverityInfo {
		t.Errorf("expected INFO for connect event, got %v", rec.records[0].Severity())
	}
	if rec.records[1].Severity() != log.SeverityWarn {
		t.Errorf("expected WARN for disconnect event, got %v", rec.records[1].Severity())
	}
	if rec.records[2].Severity() != log.SeverityError {
		t.Errorf("expected ERROR for max deliveries event, got %v", rec.records[2].Severity())
	}
}

func TestTracesReceiverTraceContextExtraction(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	tracer := tp.Tracer("test-tracer")
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tr := receiver.NewTracesReceiver(nil, tracer, nil, "nats://127.0.0.1:4222")

	msg := &nats.Msg{
		Subject: "orders.placed",
		Data:    []byte(`{"amount": 42.0}`),
		Header:  make(nats.Header),
	}
	msg.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	msg.Header.Set("Nats-Msg-Id", "MSG-TRACE-1")

	tr.HandleMessage(msg)

	spans := sr.Ended()
	if len(spans) == 0 {
		t.Fatal("expected ended span")
	}
	span := spans[0]

	if span.Name() != "orders.placed receive" {
		t.Errorf("expected span name 'orders.placed receive', got %q", span.Name())
	}
	if span.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("expected consumer span kind, got: %v", span.SpanKind())
	}

	expectedTraceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if span.Parent().TraceID() != expectedTraceID {
		t.Errorf("expected parent trace ID %s, got %s", expectedTraceID, span.Parent().TraceID())
	}
}
