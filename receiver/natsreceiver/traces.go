package natsreceiver

import (
	"context"
	"crypto/rand"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/carrier"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

type tracesReceiver struct {
	cfg      *Config
	set      receiver.Settings
	consumer consumer.Traces
	logger   *zap.Logger
	obsrecv  *receiverhelper.ObsReport
	nc       *nats.Conn
	subs     []*nats.Subscription
}

func newTracesReceiver(cfg *Config, set receiver.Settings, next consumer.Traces) (*tracesReceiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             set.ID,
		Transport:              "nats",
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, err
	}

	return &tracesReceiver{
		cfg:      cfg,
		set:      set,
		consumer: next,
		logger:   set.Logger,
		obsrecv:  obsrecv,
	}, nil
}

func (tr *tracesReceiver) Start(ctx context.Context, host component.Host) error {
	tr.logger.Info("Starting NATS traces receiver",
		zap.String("endpoint", tr.cfg.Endpoint),
		zap.String("queue_group", tr.cfg.QueueGroup))

	nc, err := newNATSConn(ctx, tr.cfg, "otelcol-nats-traces-receiver", tr.logger)
	if err != nil {
		return err
	}
	tr.nc = nc

	subjects := tr.cfg.TappedSubjects
	if len(subjects) == 0 {
		subjects = []string{"telemetry.>", "events.>", "orders.>"}
	}

	for _, subj := range subjects {
		var sub *nats.Subscription
		var subErr error
		if tr.cfg.QueueGroup != "" {
			sub, subErr = nc.QueueSubscribe(subj, tr.cfg.QueueGroup, func(msg *nats.Msg) {
				tr.handleMessage(msg)
			})
		} else {
			sub, subErr = nc.Subscribe(subj, func(msg *nats.Msg) {
				tr.handleMessage(msg)
			})
		}
		if subErr != nil {
			tr.logger.Warn("Failed to subscribe to trace subject", zap.String("subject", subj), zap.Error(subErr))
			continue
		}
		tr.subs = append(tr.subs, sub)
	}

	return nil
}

func (tr *tracesReceiver) Shutdown(ctx context.Context) error {
	for _, sub := range tr.subs {
		_ = sub.Unsubscribe()
	}
	if tr.nc != nil {
		tr.nc.Close()
	}
	return nil
}

func (tr *tracesReceiver) handleMessage(msg *nats.Msg) {
	now := time.Now()
	nowTS := pcommon.NewTimestampFromTime(now)

	var parentSpanCtx trace.SpanContext
	if msg.Header != nil {
		ctx := propagation.TraceContext{}.Extract(context.Background(), carrier.HeaderCarrier(msg.Header))
		parentSpanCtx = trace.SpanContextFromContext(ctx)
	}

	var traceID [16]byte
	var parentSpanID [8]byte
	hasParent := parentSpanCtx.IsValid()

	if hasParent {
		traceID = parentSpanCtx.TraceID()
		parentSpanID = parentSpanCtx.SpanID()
	} else {
		_, _ = rand.Read(traceID[:])
	}

	var newSpanID [8]byte
	_, _ = rand.Read(newSpanID[:])

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "nats")
	rs.Resource().Attributes().PutStr("messaging.system", "nats")

	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("github.com/nats-otel/nats-otel/receiver/natsreceiver")
	ss.Scope().SetVersion("1.0.0")

	span := ss.Spans().AppendEmpty()
	span.SetName(msg.Subject + " receive")
	span.SetKind(ptrace.SpanKindConsumer)
	span.SetStartTimestamp(nowTS)
	span.SetEndTimestamp(nowTS)
	span.SetTraceID(pcommon.TraceID(traceID))
	span.SetSpanID(pcommon.SpanID(newSpanID))
	if hasParent {
		span.SetParentSpanID(pcommon.SpanID(parentSpanID))
		span.SetFlags(uint32(parentSpanCtx.TraceFlags()))
		if parentSpanCtx.TraceState().Len() > 0 {
			span.TraceState().FromRaw(parentSpanCtx.TraceState().String())
		}
	}

	// Messaging Semantic Convention Attributes
	span.Attributes().PutStr("messaging.system", "nats")
	span.Attributes().PutStr("messaging.destination.name", msg.Subject)
	span.Attributes().PutStr("messaging.operation.type", "receive")
	span.Attributes().PutStr("messaging.operation.name", "receive")
	span.Attributes().PutInt("messaging.message.body.size", int64(len(msg.Data)))

	if strings.HasPrefix(msg.Subject, "_INBOX.") {
		span.Attributes().PutBool("messaging.destination.anonymous", true)
	}

	if msg.Header != nil {
		if msgID := msg.Header.Get("Nats-Msg-Id"); msgID != "" {
			span.Attributes().PutStr("messaging.message.id", msgID)
		}
	}

	opCtx := tr.obsrecv.StartTracesOp(context.Background())
	err := tr.consumer.ConsumeTraces(opCtx, td)
	tr.obsrecv.EndTracesOp(opCtx, "nats", td.SpanCount(), err)
	if err != nil {
		tr.logger.Error("Failed to deliver traces to next consumer", zap.Error(err))
	}
}
