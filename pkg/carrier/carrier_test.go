package carrier_test

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/carrier"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContextPropagation(t *testing.T) {
	propagator := propagation.TraceContext{}

	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	if err != nil {
		t.Fatal(err)
	}
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	if err != nil {
		t.Fatal(err)
	}

	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})

	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)

	header := make(nats.Header)
	propagator.Inject(ctx, carrier.HeaderCarrier(header))

	tp := header.Get("traceparent")
	if tp != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Errorf("unexpected traceparent header: %s", tp)
	}

	// Extract
	extractedCtx := propagator.Extract(context.Background(), carrier.HeaderCarrier(header))
	extractedSpanContext := trace.SpanContextFromContext(extractedCtx)

	if extractedSpanContext.TraceID() != traceID {
		t.Errorf("extracted trace ID mismatch: expected %s, got %s", traceID, extractedSpanContext.TraceID())
	}
	if extractedSpanContext.SpanID() != spanID {
		t.Errorf("extracted span ID mismatch: expected %s, got %s", spanID, extractedSpanContext.SpanID())
	}
}

func TestHeaderCarrierMethods(t *testing.T) {
	header := make(nats.Header)
	c := carrier.HeaderCarrier(header)

	// Set and Get
	c.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	c.Set("tracestate", "congo=t61rcWkgMzE")

	if val := c.Get("traceparent"); val != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Errorf("unexpected Get value for traceparent: %s", val)
	}
	if val := c.Get("tracestate"); val != "congo=t61rcWkgMzE" {
		t.Errorf("unexpected Get value for tracestate: %s", val)
	}
	if val := c.Get("nonexistent"); val != "" {
		t.Errorf("expected empty string for non-existent key, got: %s", val)
	}

	// Keys
	keys := c.Keys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}

	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	// Note: NATS canonicalizes header keys (MIME header casing)
	if !keySet["Traceparent"] && !keySet["traceparent"] {
		t.Errorf("expected traceparent key in keys: %v", keys)
	}
	if !keySet["Tracestate"] && !keySet["tracestate"] {
		t.Errorf("expected tracestate key in keys: %v", keys)
	}
}

func TestHeaderCarrierNil(t *testing.T) {
	var c carrier.HeaderCarrier = nil

	if val := c.Get("traceparent"); val != "" {
		t.Errorf("expected empty string from nil carrier Get, got: %s", val)
	}

	// Set on nil carrier should safely no-op without panic
	c.Set("test", "val")

	if keys := c.Keys(); keys != nil {
		t.Errorf("expected nil keys from nil carrier, got: %v", keys)
	}
}
