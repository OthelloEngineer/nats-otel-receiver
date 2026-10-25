package receiver

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/carrier"
	"github.com/nats-otel/nats-otel/pkg/natsemconv"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TracesReceiver monitors subjects and creates OpenTelemetry messaging spans
type TracesReceiver struct {
	nc         *nats.Conn
	tracer     trace.Tracer
	logger     log.Logger
	propagator propagation.TextMapPropagator
	serverURL  string
	subs       []*nats.Subscription
}

// NewTracesReceiver creates a new TracesReceiver
func NewTracesReceiver(nc *nats.Conn, tracer trace.Tracer, logger log.Logger, serverURL string) *TracesReceiver {
	return &TracesReceiver{
		nc:         nc,
		tracer:     tracer,
		logger:     logger,
		propagator: otel.GetTextMapPropagator(),
		serverURL:  serverURL,
	}
}

// Start begins tapping configured subjects
func (tr *TracesReceiver) Start(subjects []string) error {
	if len(subjects) == 0 {
		subjects = []string{"telemetry.>", "events.>", "orders.>"}
	}

	for _, subj := range subjects {
		sub, err := tr.nc.Subscribe(subj, func(msg *nats.Msg) {
			tr.HandleMessage(msg)
		})
		if err != nil {
			slog.Warn("Failed to subscribe to trace tap subject", "subject", subj, "err", err)
			continue
		}
		tr.subs = append(tr.subs, sub)
	}
	return nil
}

// Stop stops tapping
func (tr *TracesReceiver) Stop() {
	for _, sub := range tr.subs {
		_ = sub.Unsubscribe()
	}
}

// HandleMessage inspects a NATS message, extracts trace context, and emits a consumer span
func (tr *TracesReceiver) HandleMessage(msg *nats.Msg) {
	ctx := context.Background()
	if msg.Header != nil {
		ctx = tr.propagator.Extract(ctx, carrier.HeaderCarrier(msg.Header))
	}

	spanName := natsemconv.SpanName(msg.Subject, natsemconv.OpNameReceive)
	ctx, span := tr.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String(natsemconv.MessagingSystemKey, natsemconv.MessagingSystemNATS),
			attribute.String(natsemconv.MessagingOperationTypeKey, natsemconv.OpTypeReceive),
			attribute.String(natsemconv.MessagingOperationNameKey, natsemconv.OpNameReceive),
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

	if msg.Header != nil {
		if msgID := msg.Header.Get("Nats-Msg-Id"); msgID != "" {
			span.SetAttributes(attribute.String(natsemconv.MessagingMessageIDKey, msgID))
		}
	}

	// Add server peer attributes
	span.SetAttributes(natsemconv.ServerPeerAttributes(tr.serverURL)...)

	if tr.logger != nil {
		rec := log.Record{}
		rec.SetTimestamp(time.Now())
		rec.SetObservedTimestamp(time.Now())
		rec.SetSeverity(log.SeverityInfo)
		rec.SetSeverityText("INFO")
		rec.SetBody(attribute.StringValue(fmt.Sprintf("Wiretapped %s message (%d bytes) [trace_id=%s span_id=%s]", msg.Subject, len(msg.Data), span.SpanContext().TraceID().String(), span.SpanContext().SpanID().String())))
		rec.AddAttributes(
			attribute.String(natsemconv.MessagingSystemKey, natsemconv.MessagingSystemNATS),
			attribute.String(natsemconv.MessagingDestinationNameKey, msg.Subject),
			attribute.Int(natsemconv.MessagingMessageBodySizeKey, len(msg.Data)),
			attribute.String("wiretap", "true"),
			attribute.String("event.domain", "nats"),
			attribute.String("event.name", "wiretap.message"),
		)
		if msg.Header != nil {
			if msgID := msg.Header.Get("Nats-Msg-Id"); msgID != "" {
				rec.AddAttributes(attribute.String(natsemconv.MessagingMessageIDKey, msgID))
			}
		}
		tr.logger.Emit(ctx, rec)
	}
}
