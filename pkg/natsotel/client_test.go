package natsotel_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/natsemconv"
	"github.com/nats-otel/nats-otel/pkg/natsotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// startMockNATSServer starts an in-process TCP listener performing a minimal NATS handshake.
func startMockNATSServer(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock NATS listener: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				// Send NATS server INFO
				info := "INFO {\"server_id\":\"test-srv\",\"server_name\":\"test\",\"version\":\"2.10.0\",\"proto\":1,\"headers\":true,\"max_payload\":1048576}\r\n"
				if _, err := c.Write([]byte(info)); err != nil {
					return
				}

				buf := make([]byte, 2048)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					data := string(buf[:n])
					if strings.Contains(data, "PING") {
						if _, err := c.Write([]byte("PONG\r\n")); err != nil {
							return
						}
					}
				}
			}(conn)
		}
	}()

	url := "nats://" + ln.Addr().String()
	cleanup := func() {
		_ = ln.Close()
		<-done
	}
	return url, cleanup
}

func setupTestOTel(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return sr
}

func TestNewClient(t *testing.T) {
	client, err := natsotel.NewClient(nil)
	if err != nil {
		t.Fatalf("unexpected error creating client with nil conn: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.Conn() != nil {
		t.Fatalf("expected nil conn, got: %v", client.Conn())
	}
}

func TestPublishMsgTraceContextInjection(t *testing.T) {
	sr := setupTestOTel(t)

	serverURL, cleanup := startMockNATSServer(t)
	defer cleanup()

	nc, err := nats.Connect(serverURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("failed to connect to mock NATS: %v", err)
	}
	defer nc.Close()

	client, err := natsotel.NewClient(nc)
	if err != nil {
		t.Fatalf("failed to create natsotel client: %v", err)
	}

	ctx := context.Background()
	msg := nats.NewMsg("orders.created")
	msg.Data = []byte(`{"order_id": "ORD-123"}`)
	msg.Header.Set("Nats-Msg-Id", "MSG-100")

	err = client.PublishMsg(ctx, msg)
	if err != nil {
		t.Fatalf("PublishMsg failed: %v", err)
	}

	// Verify W3C traceparent was injected into NATS header
	tp := msg.Header.Get("traceparent")
	if tp == "" {
		t.Errorf("expected traceparent header to be injected, got empty")
	}

	// Verify Span attributes
	spans := sr.Ended()
	if len(spans) == 0 {
		t.Fatal("expected at least one ended span")
	}
	span := spans[0]
	if span.Name() != "orders.created publish" {
		t.Errorf("expected span name 'orders.created publish', got %q", span.Name())
	}
	if span.SpanKind() != trace.SpanKindProducer {
		t.Errorf("expected producer span kind, got: %v", span.SpanKind())
	}

	attrs := make(map[string]string)
	for _, a := range span.Attributes() {
		attrs[string(a.Key)] = a.Value.Emit()
	}

	if attrs[natsemconv.MessagingSystemKey] != "nats" {
		t.Errorf("expected messaging.system=nats, got %q", attrs[natsemconv.MessagingSystemKey])
	}
	if attrs[natsemconv.MessagingOperationTypeKey] != "publish" {
		t.Errorf("expected messaging.operation.type=publish, got %q", attrs[natsemconv.MessagingOperationTypeKey])
	}
	if attrs[natsemconv.MessagingDestinationNameKey] != "orders.created" {
		t.Errorf("expected messaging.destination.name=orders.created, got %q", attrs[natsemconv.MessagingDestinationNameKey])
	}
	if attrs[natsemconv.MessagingMessageIDKey] != "MSG-100" {
		t.Errorf("expected messaging.message.id=MSG-100, got %q", attrs[natsemconv.MessagingMessageIDKey])
	}
}

func TestPublishAnonymousDestination(t *testing.T) {
	sr := setupTestOTel(t)

	serverURL, cleanup := startMockNATSServer(t)
	defer cleanup()

	nc, err := nats.Connect(serverURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("failed to connect to mock NATS: %v", err)
	}
	defer nc.Close()

	client, err := natsotel.NewClient(nc)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Publish(context.Background(), "_INBOX.abc.123", []byte("ping"))
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	spans := sr.Ended()
	if len(spans) == 0 {
		t.Fatal("expected ended span")
	}
	span := spans[len(spans)-1]

	attrs := make(map[string]interface{})
	for _, a := range span.Attributes() {
		attrs[string(a.Key)] = a.Value.AsInterface()
	}

	if anon, ok := attrs[natsemconv.MessagingDestinationAnonymousKey].(bool); !ok || !anon {
		t.Errorf("expected messaging.destination.anonymous=true for _INBOX")
	}
	if _, exists := attrs[natsemconv.MessagingDestinationNameKey]; exists {
		t.Errorf("messaging.destination.name should be omitted for anonymous destinations")
	}
}

func TestPublishDefensiveGuards(t *testing.T) {
	client, err := natsotel.NewClient(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Nil message error
	err = client.PublishMsg(context.Background(), nil)
	if err == nil {
		t.Error("expected error publishing nil message")
	}

	// Nil connection error
	msg := nats.NewMsg("foo")
	err = client.PublishMsg(context.Background(), msg)
	if err == nil {
		t.Error("expected error publishing with nil nats connection")
	}
}

func TestHandleConsumedMessage(t *testing.T) {
	sr := setupTestOTel(t)

	client, err := natsotel.NewClient(nil)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	incomingTraceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	incomingSpanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")

	msg := nats.NewMsg("orders.received")
	msg.Data = []byte(`{"order_id": "ORD-456"}`)
	msg.Header = make(nats.Header)
	msg.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	msg.Header.Set("Nats-Msg-Id", "MSG-200")

	handlerInvoked := false
	handler := func(ctx context.Context, m *nats.Msg) error {
		handlerInvoked = true
		// Verify span context in handler context matches incoming traceparent
		sc := trace.SpanContextFromContext(ctx)
		if sc.TraceID() != incomingTraceID {
			t.Errorf("expected trace ID %s, got %s", incomingTraceID, sc.TraceID())
		}
		return nil
	}

	client.HandleConsumedMessage(context.Background(), msg, "billing-workers", handler)

	if !handlerInvoked {
		t.Fatal("handler was not invoked")
	}

	spans := sr.Ended()
	if len(spans) == 0 {
		t.Fatal("expected ended consumer span")
	}
	span := spans[len(spans)-1]

	if span.Name() != "orders.received process" {
		t.Errorf("expected span name 'orders.received process', got %q", span.Name())
	}
	if span.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("expected consumer span kind, got: %v", span.SpanKind())
	}
	if span.Parent().TraceID() != incomingTraceID {
		t.Errorf("expected parent trace ID %s, got %s", incomingTraceID, span.Parent().TraceID())
	}
	if span.Parent().SpanID() != incomingSpanID {
		t.Errorf("expected parent span ID %s, got %s", incomingSpanID, span.Parent().SpanID())
	}

	attrs := make(map[string]string)
	for _, a := range span.Attributes() {
		attrs[string(a.Key)] = a.Value.Emit()
	}

	if attrs[natsemconv.MessagingConsumerGroupNameKey] != "billing-workers" {
		t.Errorf("expected consumer group 'billing-workers', got %q", attrs[natsemconv.MessagingConsumerGroupNameKey])
	}
	if attrs[natsemconv.MessagingMessageIDKey] != "MSG-200" {
		t.Errorf("expected message id 'MSG-200', got %q", attrs[natsemconv.MessagingMessageIDKey])
	}
}
