package natsotel_test

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/natsemconv"
	"github.com/nats-otel/nats-otel/pkg/natsotel"
	"go.opentelemetry.io/otel/trace"
)

func TestWrapQueueHandlerConsumedMessage(t *testing.T) {
	sr := setupTestOTel(t)

	incomingTraceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	incomingSpanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")

	msg := nats.NewMsg("orders.received")
	msg.Data = []byte(`{"order_id": "ORD-456"}`)
	msg.Header = make(nats.Header)
	msg.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	msg.Header.Set("Nats-Msg-Id", "MSG-200")

	handlerInvoked := false
	handler := natsotel.WrapQueueHandler("billing-workers", func(ctx context.Context, m *nats.Msg) error {
		handlerInvoked = true
		sc := trace.SpanContextFromContext(ctx)
		if sc.TraceID() != incomingTraceID {
			t.Errorf("expected trace ID %s, got %s", incomingTraceID, sc.TraceID())
		}
		return nil
	})

	handler(msg)

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

func TestWrapHandlerAndWrapQueueHandler(t *testing.T) {
	sr := setupTestOTel(t)

	incomingTraceID, _ := trace.TraceIDFromHex("5cf92f3577b34da6a3ce929d0e0e4736")
	incomingSpanID, _ := trace.SpanIDFromHex("00a067aa0ba902b7")

	msg := nats.NewMsg("tasks.execute")
	msg.Data = []byte(`{"task": "send_email"}`)
	msg.Header = make(nats.Header)
	msg.Header.Set("traceparent", "00-5cf92f3577b34da6a3ce929d0e0e4736-00a067aa0ba902b7-01")

	// 1. Test WrapHandler
	handlerInvoked := false
	handler := natsotel.WrapHandler(func(ctx context.Context, m *nats.Msg) {
		handlerInvoked = true
		sc := trace.SpanContextFromContext(ctx)
		if sc.TraceID() != incomingTraceID {
			t.Errorf("expected trace ID %s, got %s", incomingTraceID, sc.TraceID())
		}
	})
	handler(msg)

	if !handlerInvoked {
		t.Fatal("WrapHandler callback was not invoked")
	}

	spans := sr.Ended()
	if len(spans) == 0 {
		t.Fatal("expected span from WrapHandler")
	}
	span := spans[len(spans)-1]
	if span.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("expected SpanKindConsumer, got %v", span.SpanKind())
	}
	if span.Parent().TraceID() != incomingTraceID || span.Parent().SpanID() != incomingSpanID {
		t.Errorf("expected parent %s-%s, got %s-%s", incomingTraceID, incomingSpanID, span.Parent().TraceID(), span.Parent().SpanID())
	}

	// 2. Test WrapQueueHandler
	queueHandlerInvoked := false
	queueHandler := natsotel.WrapQueueHandler("email-workers", func(ctx context.Context, m *nats.Msg) error {
		queueHandlerInvoked = true
		return nil
	})
	queueHandler(msg)

	if !queueHandlerInvoked {
		t.Fatal("WrapQueueHandler callback was not invoked")
	}

	spans = sr.Ended()
	span = spans[len(spans)-1]
	foundQueueGroup := false
	for _, a := range span.Attributes() {
		if string(a.Key) == natsemconv.MessagingConsumerGroupNameKey && a.Value.AsString() == "email-workers" {
			foundQueueGroup = true
		}
	}
	if !foundQueueGroup {
		t.Errorf("expected messaging.consumer.group.name attribute to be email-workers")
	}
}

func TestInjectAndExtract(t *testing.T) {
	setupTestOTel(t)

	expectedTraceID, _ := trace.TraceIDFromHex("12345678901234567890123456789012")
	expectedSpanID, _ := trace.SpanIDFromHex("1234567890123456")

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    expectedTraceID,
		SpanID:     expectedSpanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	header := make(nats.Header)
	natsotel.Inject(ctx, header)

	if header.Get("traceparent") == "" {
		t.Fatal("expected traceparent header to be injected")
	}

	extractedCtx := natsotel.Extract(context.Background(), header)
	extractedSc := trace.SpanContextFromContext(extractedCtx)

	if extractedSc.TraceID() != expectedTraceID {
		t.Errorf("expected trace ID %s, got %s", expectedTraceID, extractedSc.TraceID())
	}
	if extractedSc.SpanID() != expectedSpanID {
		t.Errorf("expected span ID %s, got %s", expectedSpanID, extractedSc.SpanID())
	}
}
