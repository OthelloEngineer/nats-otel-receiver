package natsotel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/nats-otel/nats-otel/pkg/natsotel"

// Option configures OpenTelemetry NATS instrumentation.
type Option func(*config)

type config struct {
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	propagator     propagation.TextMapPropagator
	queueGroup     string
}

func newConfig(opts ...Option) *config {
	cfg := &config{}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	return cfg
}

// WithTracerProvider specifies a tracer provider to use for creating a tracer.
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *config) {
		if tp != nil {
			c.tracerProvider = tp
		}
	}
}

// WithMeterProvider specifies a meter provider to use for creating a meter.
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) {
		if mp != nil {
			c.meterProvider = mp
		}
	}
}

// WithPropagator specifies a custom text map propagator.
func WithPropagator(p propagation.TextMapPropagator) Option {
	return func(c *config) {
		if p != nil {
			c.propagator = p
		}
	}
}

// WithQueueGroup specifies the queue group name for semantic conventions.
func WithQueueGroup(queue string) Option {
	return func(c *config) {
		c.queueGroup = queue
	}
}

// Tracer returns the configured tracer or defaults to otel.GetTracerProvider().
func (c *config) Tracer() trace.Tracer {
	if c.tracerProvider != nil {
		return c.tracerProvider.Tracer(instrumentationName)
	}
	return otel.GetTracerProvider().Tracer(instrumentationName)
}

// Propagator returns the configured propagator or defaults to otel.GetTextMapPropagator().
func (c *config) Propagator() propagation.TextMapPropagator {
	if c.propagator != nil {
		return c.propagator
	}
	return otel.GetTextMapPropagator()
}

// Instruments returns metric instruments for the configured or default meter provider.
func (c *config) Instruments() *instruments {
	if c.meterProvider != nil {
		return newInstruments(c.meterProvider)
	}
	return getDefaultInstruments()
}
