package natsotel_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/natsemconv"
	"github.com/nats-otel/nats-otel/pkg/natsotel"
	"go.opentelemetry.io/otel/trace"
)

func TestPublishMsgTraceContextInjection(t *testing.T) {
	sr := setupTestOTel(t)

	serverURL, cleanup := startMockNATSServer(t)
	defer cleanup()

	nc, err := nats.Connect(serverURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("failed to connect to mock NATS: %v", err)
	}
	defer nc.Close()

	ctx := context.Background()
	msg := nats.NewMsg("orders.created")
	msg.Data = []byte(`{"order_id": "ORD-123"}`)
	msg.Header.Set("Nats-Msg-Id", "MSG-100")

	err = natsotel.PublishMsg(ctx, nc, msg)
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

	err = natsotel.Publish(context.Background(), nc, "_INBOX.abc.123", []byte("ping"))
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
	// Nil message error
	err := natsotel.PublishMsg(context.Background(), nil, nil)
	if err == nil {
		t.Error("expected error publishing nil message")
	}

	// Nil connection error
	msg := nats.NewMsg("foo")
	err = natsotel.PublishMsg(context.Background(), nil, msg)
	if err == nil {
		t.Error("expected error publishing with nil nats connection")
	}
}

func TestFunctionalPublishAndPublishMsg(t *testing.T) {
	sr := setupTestOTel(t)

	serverURL, cleanup := startMockNATSServer(t)
	defer cleanup()

	nc, err := nats.Connect(serverURL, nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("failed to connect to mock NATS: %v", err)
	}
	defer nc.Close()

	ctx := context.Background()

	// Test natsotel.Publish
	err = natsotel.Publish(ctx, nc, "payments.processed", []byte(`{"id": 1}`))
	if err != nil {
		t.Fatalf("natsotel.Publish failed: %v", err)
	}

	// Test natsotel.PublishMsg
	msg := nats.NewMsg("payments.refunded")
	msg.Data = []byte(`{"id": 2}`)
	err = natsotel.PublishMsg(ctx, nc, msg)
	if err != nil {
		t.Fatalf("natsotel.PublishMsg failed: %v", err)
	}

	if msg.Header.Get("traceparent") == "" {
		t.Errorf("expected traceparent header to be injected by PublishMsg")
	}

	spans := sr.Ended()
	if len(spans) < 2 {
		t.Fatalf("expected at least 2 spans, got %d", len(spans))
	}
}
