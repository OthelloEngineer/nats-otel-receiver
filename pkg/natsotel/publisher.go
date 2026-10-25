package natsotel

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/carrier"
	"github.com/nats-otel/nats-otel/pkg/natsemconv"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// PublishMsg publishes msg to NATS, injecting W3C TraceContext and recording SemConv metrics and spans.
func PublishMsg(ctx context.Context, nc *nats.Conn, msg *nats.Msg, opts ...Option) error {
	if msg == nil {
		return fmt.Errorf("cannot publish nil message")
	}
	if nc == nil {
		return fmt.Errorf("nats connection is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cfg := newConfig(opts...)
	tracer := cfg.Tracer()
	propagator := cfg.Propagator()
	inst := cfg.Instruments()

	startTime := time.Now()
	spanName := natsemconv.SpanName(msg.Subject, natsemconv.OpNamePublish)
	ctx, span := tracer.Start(ctx, spanName,
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

	propagator.Inject(ctx, carrier.HeaderCarrier(msg.Header))

	span.SetAttributes(attribute.Int(
		natsemconv.MessagingMessageEnvelopeSizeKey,
		len(msg.Data)+natsemconv.CalculateHeaderBytes(msg.Header),
	))

	if msgID := msg.Header.Get("Nats-Msg-Id"); msgID != "" {
		span.SetAttributes(attribute.String(natsemconv.MessagingMessageIDKey, msgID))
	}

	span.SetAttributes(natsemconv.ServerPeerAttributes(nc.ConnectedUrl())...)

	err := nc.PublishMsg(msg)
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
	} else if inst.sentMsgsCounter != nil {
		inst.sentMsgsCounter.Add(ctx, 1, metric.WithAttributes(attrs...))
		if inst.publishDuration != nil {
			inst.publishDuration.Record(ctx, duration, metric.WithAttributes(attrs...))
		}
	}

	return err
}

// Publish is a convenience wrapper around PublishMsg for subject and raw byte data.
func Publish(ctx context.Context, nc *nats.Conn, subject string, data []byte, opts ...Option) error {
	msg := nats.NewMsg(subject)
	msg.Data = data
	return PublishMsg(ctx, nc, msg, opts...)
}

// PublishJetStream publishes to JetStream with OpenTelemetry context injection and captures stream/sequence metadata.
func PublishJetStream(ctx context.Context, js nats.JetStreamContext, msg *nats.Msg, opts ...Option) (*nats.PubAck, error) {
	if msg == nil {
		return nil, fmt.Errorf("cannot publish nil message")
	}
	if js == nil {
		return nil, fmt.Errorf("jetstream context is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cfg := newConfig(opts...)
	tracer := cfg.Tracer()
	propagator := cfg.Propagator()

	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}

	spanName := natsemconv.SpanName(msg.Subject, natsemconv.OpNamePublish)
	ctx, span := tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String(natsemconv.MessagingSystemKey, natsemconv.MessagingSystemNATS),
			attribute.String(natsemconv.MessagingDestinationNameKey, msg.Subject),
			attribute.String(natsemconv.MessagingOperationTypeKey, natsemconv.OpTypePublish),
			attribute.String(natsemconv.MessagingOperationNameKey, natsemconv.OpNamePublish),
			attribute.Int(natsemconv.MessagingMessageBodySizeKey, len(msg.Data)),
		),
	)
	defer span.End()

	propagator.Inject(ctx, carrier.HeaderCarrier(msg.Header))

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
