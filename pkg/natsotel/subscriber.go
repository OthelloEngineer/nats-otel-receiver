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

// HandlerFunc is an OpenTelemetry-aware message handler that receives the extracted context.
type HandlerFunc func(ctx context.Context, msg *nats.Msg)

// ErrorHandlerFunc is an OpenTelemetry-aware message handler that returns an error.
type ErrorHandlerFunc func(ctx context.Context, msg *nats.Msg) error

// WrapHandler wraps a HandlerFunc into a standard nats.MsgHandler.
func WrapHandler(handler HandlerFunc, opts ...Option) nats.MsgHandler {
	return WrapErrorHandler(func(ctx context.Context, msg *nats.Msg) error {
		handler(ctx, msg)
		return nil
	}, opts...)
}

// WrapQueueHandler wraps an ErrorHandlerFunc for a specific queue group into a standard nats.MsgHandler.
func WrapQueueHandler(queue string, handler ErrorHandlerFunc, opts ...Option) nats.MsgHandler {
	allOpts := append([]Option{WithQueueGroup(queue)}, opts...)
	return WrapErrorHandler(handler, allOpts...)
}

// WrapErrorHandler wraps an ErrorHandlerFunc into a standard nats.MsgHandler.
func WrapErrorHandler(handler ErrorHandlerFunc, opts ...Option) nats.MsgHandler {
	cfg := newConfig(opts...)
	tracer := cfg.Tracer()
	propagator := cfg.Propagator()
	inst := cfg.Instruments()

	return func(msg *nats.Msg) {
		if msg == nil {
			return
		}
		startTime := time.Now()
		ctx := context.Background()

		// Extract W3C TraceContext
		if msg.Header != nil {
			ctx = propagator.Extract(ctx, carrier.HeaderCarrier(msg.Header))
		}

		spanName := natsemconv.SpanName(msg.Subject, natsemconv.OpNameProcess)
		ctx, span := tracer.Start(ctx, spanName,
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

		if cfg.queueGroup != "" {
			span.SetAttributes(attribute.String(natsemconv.MessagingConsumerGroupNameKey, cfg.queueGroup))
		}

		if msg.Header != nil {
			if msgID := msg.Header.Get("Nats-Msg-Id"); msgID != "" {
				span.SetAttributes(attribute.String(natsemconv.MessagingMessageIDKey, msgID))
			}
		}

		err := handler(ctx, msg)
		duration := time.Since(startTime).Seconds()

		metricAttrs := []attribute.KeyValue{
			attribute.String(natsemconv.MessagingSystemKey, natsemconv.MessagingSystemNATS),
			attribute.String(natsemconv.MessagingDestinationNameKey, msg.Subject),
			attribute.String(natsemconv.MessagingOperationTypeKey, natsemconv.OpTypeProcess),
		}
		if cfg.queueGroup != "" {
			metricAttrs = append(metricAttrs, attribute.String(natsemconv.MessagingConsumerGroupNameKey, cfg.queueGroup))
		}

		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.SetAttributes(attribute.String("error.type", fmt.Sprintf("%T", err)))
		} else if inst.consumedMsgsCounter != nil {
			inst.consumedMsgsCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
			if inst.processDuration != nil {
				inst.processDuration.Record(ctx, duration, metric.WithAttributes(metricAttrs...))
			}
		}
	}
}
