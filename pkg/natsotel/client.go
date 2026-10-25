package natsotel

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/carrier"
	"github.com/nats-otel/nats-otel/pkg/natsemconv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Client wraps a NATS connection with OpenTelemetry messaging instrumentation
type Client struct {
	nc         *nats.Conn
	tracer     trace.Tracer
	meter      metric.Meter
	propagator propagation.TextMapPropagator

	sentMsgsCounter     metric.Int64Counter
	consumedMsgsCounter metric.Int64Counter
	publishDuration     metric.Float64Histogram
	processDuration     metric.Float64Histogram
}

// MsgHandler is the OpenTelemetry-aware message handler
type MsgHandler func(ctx context.Context, msg *nats.Msg) error

// NewClient creates an instrumented NATS client
func NewClient(nc *nats.Conn) (*Client, error) {
	tracer := otel.GetTracerProvider().Tracer("github.com/nats-otel/nats-otel/client")
	meter := otel.GetMeterProvider().Meter("github.com/nats-otel/nats-otel/client")

	sentCounter, err := meter.Int64Counter(natsemconv.MetricMessagingClientSentMessages,
		metric.WithDescription("Number of messages sent by the client"),
		metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}

	consumedCounter, err := meter.Int64Counter(natsemconv.MetricMessagingClientConsumedMessages,
		metric.WithDescription("Number of messages consumed by the client"),
		metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}

	pubDuration, err := meter.Float64Histogram(natsemconv.MetricMessagingClientOpDuration,
		metric.WithDescription("Duration of messaging publish operations"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	procDuration, err := meter.Float64Histogram(natsemconv.MetricMessagingProcessDuration,
		metric.WithDescription("Duration of processing consumed messages"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	return &Client{
		nc:                  nc,
		tracer:              tracer,
		meter:               meter,
		propagator:          otel.GetTextMapPropagator(),
		sentMsgsCounter:     sentCounter,
		consumedMsgsCounter: consumedCounter,
		publishDuration:     pubDuration,
		processDuration:     procDuration,
	}, nil
}

// Conn returns the underlying NATS connection
func (c *Client) Conn() *nats.Conn {
	return c.nc
}

// Publish publishes a message with OpenTelemetry tracing and metrics
func (c *Client) Publish(ctx context.Context, subject string, data []byte) error {
	msg := nats.NewMsg(subject)
	msg.Data = data
	return c.PublishMsg(ctx, msg)
}

// PublishMsg publishes a nats.Msg, injecting W3C tracecontext and recording metrics
func (c *Client) PublishMsg(ctx context.Context, msg *nats.Msg) error {
	if msg == nil {
		return fmt.Errorf("cannot publish nil message")
	}
	if c.nc == nil {
		return fmt.Errorf("nats connection is nil")
	}

	startTime := time.Now()

	spanName := natsemconv.SpanName(msg.Subject, natsemconv.OpNamePublish)
	ctx, span := c.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String(natsemconv.MessagingSystemKey, natsemconv.MessagingSystemNATS),
			attribute.String(natsemconv.MessagingOperationTypeKey, natsemconv.OpTypePublish),
			attribute.String(natsemconv.MessagingOperationNameKey, natsemconv.OpNamePublish),
			attribute.Int(natsemconv.MessagingMessageBodySizeKey, len(msg.Data)),
		),
	)
	defer span.End()

	if natsemconv.IsAnonymousDestination(msg.Subject) {
		span.SetAttributes(attribute.Bool(natsemconv.MessagingDestinationAnonymousKey, true))
	} else {
		span.SetAttributes(attribute.String(natsemconv.MessagingDestinationNameKey, msg.Subject))
	}

	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}

	// Inject W3C TraceContext into headers
	c.propagator.Inject(ctx, carrier.HeaderCarrier(msg.Header))

	// Envelope size calculation
	span.SetAttributes(attribute.Int(
		natsemconv.MessagingMessageEnvelopeSizeKey,
		len(msg.Data)+natsemconv.CalculateHeaderBytes(msg.Header),
	))

	if msgID := msg.Header.Get("Nats-Msg-Id"); msgID != "" {
		span.SetAttributes(attribute.String(natsemconv.MessagingMessageIDKey, msgID))
	}

	if c.nc != nil {
		span.SetAttributes(natsemconv.ServerPeerAttributes(c.nc.ConnectedUrl())...)
	}

	err := c.nc.PublishMsg(msg)
	duration := time.Since(startTime).Seconds()

	attrs := []attribute.KeyValue{
		attribute.String(natsemconv.MessagingSystemKey, natsemconv.MessagingSystemNATS),
		attribute.String(natsemconv.MessagingDestinationNameKey, msg.Subject),
		attribute.String(natsemconv.MessagingOperationTypeKey, natsemconv.OpTypePublish),
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", err)))
	} else {
		c.sentMsgsCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
		c.publishDuration.Record(ctx, duration, metric.WithAttributes(attrs...))
	}

	return err
}

// Subscribe subscribes to a subject with consumer tracing and message duration metrics
func (c *Client) Subscribe(subject string, handler MsgHandler) (*nats.Subscription, error) {
	return c.nc.Subscribe(subject, func(msg *nats.Msg) {
		c.HandleConsumedMessage(context.Background(), msg, "", handler)
	})
}

// QueueSubscribe subscribes to a subject with a queue group (messaging.consumer.group.name)
func (c *Client) QueueSubscribe(subject, queue string, handler MsgHandler) (*nats.Subscription, error) {
	return c.nc.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		c.HandleConsumedMessage(context.Background(), msg, queue, handler)
	})
}

// HandleConsumedMessage processes an incoming message with OpenTelemetry tracing and metrics.
// It extracts W3C tracecontext, creates a CONSUMER span conforming to messaging semantic conventions,
// tracks processing latency, and records messaging metrics.
func (c *Client) HandleConsumedMessage(ctx context.Context, msg *nats.Msg, queueGroup string, handler MsgHandler) {
	if msg == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	startTime := time.Now()

	// Extract W3C TraceContext
	if msg.Header != nil {
		ctx = c.propagator.Extract(ctx, carrier.HeaderCarrier(msg.Header))
	}

	spanName := natsemconv.SpanName(msg.Subject, natsemconv.OpNameProcess)
	ctx, span := c.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String(natsemconv.MessagingSystemKey, natsemconv.MessagingSystemNATS),
			attribute.String(natsemconv.MessagingOperationTypeKey, natsemconv.OpTypeProcess),
			attribute.String(natsemconv.MessagingOperationNameKey, natsemconv.OpNameProcess),
			attribute.Int(natsemconv.MessagingMessageBodySizeKey, len(msg.Data)),
			attribute.Int(natsemconv.MessagingMessageEnvelopeSizeKey, len(msg.Data)+natsemconv.CalculateHeaderBytes(msg.Header)),
		),
	)
	defer span.End()

	if natsemconv.IsAnonymousDestination(msg.Subject) {
		span.SetAttributes(attribute.Bool(natsemconv.MessagingDestinationAnonymousKey, true))
	} else {
		span.SetAttributes(attribute.String(natsemconv.MessagingDestinationNameKey, msg.Subject))
	}

	if queueGroup != "" {
		span.SetAttributes(attribute.String(natsemconv.MessagingConsumerGroupNameKey, queueGroup))
	}

	if msg.Header != nil {
		if msgID := msg.Header.Get("Nats-Msg-Id"); msgID != "" {
			span.SetAttributes(attribute.String(natsemconv.MessagingMessageIDKey, msgID))
		}
	}

	if c.nc != nil {
		span.SetAttributes(natsemconv.ServerPeerAttributes(c.nc.ConnectedUrl())...)
	}

	err := handler(ctx, msg)
	duration := time.Since(startTime).Seconds()

	metricAttrs := []attribute.KeyValue{
		attribute.String(natsemconv.MessagingSystemKey, natsemconv.MessagingSystemNATS),
		attribute.String(natsemconv.MessagingDestinationNameKey, msg.Subject),
		attribute.String(natsemconv.MessagingOperationTypeKey, natsemconv.OpTypeProcess),
	}
	if queueGroup != "" {
		metricAttrs = append(metricAttrs, attribute.String(natsemconv.MessagingConsumerGroupNameKey, queueGroup))
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", err)))
	} else {
		c.consumedMsgsCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		c.processDuration.Record(ctx, duration, metric.WithAttributes(metricAttrs...))
	}
}

// PublishJetStream publishes to JetStream and captures sequence / message ID
func (c *Client) PublishJetStream(ctx context.Context, js nats.JetStreamContext, subject string, data []byte, msgID string) (*nats.PubAck, error) {
	msg := nats.NewMsg(subject)
	msg.Data = data
	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}
	if msgID != "" {
		msg.Header.Set("Nats-Msg-Id", msgID)
	}

	spanName := natsemconv.SpanName(subject, natsemconv.OpNamePublish)
	ctx, span := c.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String(natsemconv.MessagingSystemKey, natsemconv.MessagingSystemNATS),
			attribute.String(natsemconv.MessagingDestinationNameKey, subject),
			attribute.String(natsemconv.MessagingOperationTypeKey, natsemconv.OpTypePublish),
			attribute.String(natsemconv.MessagingOperationNameKey, natsemconv.OpNamePublish),
			attribute.Int(natsemconv.MessagingMessageBodySizeKey, len(data)),
		),
	)
	defer span.End()

	c.propagator.Inject(ctx, carrier.HeaderCarrier(msg.Header))

	pubAck, err := js.PublishMsg(msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	span.SetAttributes(
		attribute.String(natsemconv.MessagingMessageIDKey, fmt.Sprintf("%d", pubAck.Sequence)),
		attribute.String("messaging.jetstream.stream", pubAck.Stream),
	)

	return pubAck, nil
}
