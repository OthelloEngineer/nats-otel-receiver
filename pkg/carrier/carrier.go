package carrier

import (
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/propagation"
)

// HeaderCarrier adapts nats.Header to OpenTelemetry propagation.TextMapCarrier.
// This enables standard W3C TraceContext and Baggage propagators to inject and extract
// trace state across NATS message boundaries without manual header slicing.
type HeaderCarrier nats.Header

var _ propagation.TextMapCarrier = HeaderCarrier{}

// Get returns the first value associated with the passed key.
func (c HeaderCarrier) Get(key string) string {
	if c == nil {
		return ""
	}
	return nats.Header(c).Get(key)
}

// Set stores the key-value pair in the carrier.
func (c HeaderCarrier) Set(key, val string) {
	if c == nil {
		return
	}
	nats.Header(c).Set(key, val)
}

// Keys lists all keys stored in this carrier.
func (c HeaderCarrier) Keys() []string {
	if c == nil {
		return nil
	}
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
