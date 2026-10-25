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

// RequestMsg sends a request msg to NATS and waits for the response, using ctx for timeout and cancellation.
func RequestMsg(ctx context.Context, nc *nats.Conn, msg *nats.Msg, opts ...Option) (*nats.Msg, error) {
	if msg == nil {
		return nil, fmt.Errorf("cannot request nil message")
	}
	if nc == nil {
		return nil, fmt.Errorf("nats connection is nil")
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
		trace.WithSpanKind(trace.SpanKindClient),
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

	reply, err := nc.RequestMsgWithContext(ctx, msg)
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

	return reply, err
}

// Request is a convenience wrapper around RequestMsg for subject and raw byte data.
func Request(ctx context.Context, nc *nats.Conn, subject string, data []byte, opts ...Option) (*nats.Msg, error) {
	msg := nats.NewMsg(subject)
	msg.Data = data
	return RequestMsg(ctx, nc, msg, opts...)
}
