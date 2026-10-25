package natsotel

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/carrier"
)

// Inject injects the OpenTelemetry trace context from ctx into the NATS message headers.
func Inject(ctx context.Context, header nats.Header, opts ...Option) {
	if header == nil {
		return
	}
	cfg := newConfig(opts...)
	cfg.Propagator().Inject(ctx, carrier.HeaderCarrier(header))
}

// Extract extracts the OpenTelemetry trace context from the NATS message headers into ctx.
func Extract(ctx context.Context, header nats.Header, opts ...Option) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if header == nil {
		return ctx
	}
	cfg := newConfig(opts...)
	return cfg.Propagator().Extract(ctx, carrier.HeaderCarrier(header))
}
