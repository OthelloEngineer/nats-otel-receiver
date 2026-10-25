package natsemconv_test

import (
	"testing"

	"github.com/nats-otel/nats-otel/pkg/natsemconv"
	"go.opentelemetry.io/otel/attribute"
)

func TestSpanName(t *testing.T) {
	if got := natsemconv.SpanName("orders.new", natsemconv.OpNamePublish); got != "orders.new publish" {
		t.Errorf("expected 'orders.new publish', got %q", got)
	}
	if got := natsemconv.SpanName("", natsemconv.OpNamePublish); got != "publish" {
		t.Errorf("expected 'publish', got %q", got)
	}
}

func TestIsAnonymousDestination(t *testing.T) {
	if !natsemconv.IsAnonymousDestination("_INBOX.123456.abc") {
		t.Errorf("expected _INBOX to be anonymous")
	}
	if natsemconv.IsAnonymousDestination("orders.placed") {
		t.Errorf("expected orders.placed not to be anonymous")
	}
}

func TestDestinationAttributes(t *testing.T) {
	attrs := natsemconv.DestinationAttributes("orders.checkout")
	m := make(map[attribute.Key]attribute.Value)
	for _, a := range attrs {
		m[a.Key] = a.Value
	}

	if m[attribute.Key(natsemconv.MessagingSystemKey)].AsString() != "nats" {
		t.Errorf("expected messaging.system to be nats")
	}
	if m[attribute.Key(natsemconv.MessagingDestinationNameKey)].AsString() != "orders.checkout" {
		t.Errorf("expected messaging.destination.name to be orders.checkout")
	}
}

func TestCalculateHeaderBytes(t *testing.T) {
	headers := map[string][]string{
		"traceparent": {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
	}
	bytes := natsemconv.CalculateHeaderBytes(headers)
	if bytes <= 0 {
		t.Errorf("expected header bytes > 0, got %d", bytes)
	}
}

func TestServerPeerAttributes(t *testing.T) {
	tests := []struct {
		url          string
		expectedAddr string
		expectedPort int64
	}{
		{"nats://127.0.0.1:4222", "127.0.0.1", 4222},
		{"tls://demo.nats.io:4443", "demo.nats.io", 4443},
		{"nats://user:pass@nats.internal:4222", "nats.internal", 4222},
		{"localhost:4222", "localhost", 4222},
		{"", "", 0},
	}

	for _, tt := range tests {
		attrs := natsemconv.ServerPeerAttributes(tt.url)
		if tt.url == "" {
			if attrs != nil {
				t.Errorf("expected nil for empty url, got %v", attrs)
			}
			continue
		}

		m := make(map[string]attribute.Value)
		for _, a := range attrs {
			m[string(a.Key)] = a.Value
		}

		if m[natsemconv.ServerAddressKey].AsString() != tt.expectedAddr {
			t.Errorf("for %s, expected addr %s, got %s", tt.url, tt.expectedAddr, m[natsemconv.ServerAddressKey].AsString())
		}
		if tt.expectedPort != 0 {
			if m[natsemconv.ServerPortKey].AsInt64() != tt.expectedPort {
				t.Errorf("for %s, expected port %d, got %d", tt.url, tt.expectedPort, m[natsemconv.ServerPortKey].AsInt64())
			}
		}
	}
}
