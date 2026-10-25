package natsreceiver

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/carrier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestTracesReceiverHandleMessageWithCarrier(t *testing.T) {
	sink := new(consumertest.TracesSink)
	set := receivertest.NewNopSettings(componentType)
	tr, err := newTracesReceiver(&Config{Endpoint: "nats://localhost:4222"}, set, sink)
	require.NoError(t, err)

	// Prepare parent trace context
	parentTraceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	require.NoError(t, err)
	parentSpanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	require.NoError(t, err)

	parentSC := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    parentTraceID,
		SpanID:     parentSpanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), parentSC)

	// Inject context using carrier.HeaderCarrier
	msg := nats.NewMsg("orders.created")
	msg.Data = []byte(`{"order_id": 1001}`)
	msg.Header = make(nats.Header)
	msg.Header.Set("Nats-Msg-Id", "msg-uuid-1001")
	propagation.TraceContext{}.Inject(ctx, carrier.HeaderCarrier(msg.Header))

	// Handle message
	tr.handleMessage(msg)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	rs := traces[0].ResourceSpans().At(0)
	assert.Equal(t, "nats", rs.Resource().Attributes().AsRaw()["messaging.system"])

	span := rs.ScopeSpans().At(0).Spans().At(0)
	assert.Equal(t, "orders.created receive", span.Name())
	assert.Equal(t, ptrace.SpanKindConsumer, span.Kind())

	// Verify trace context linkage via carrier
	assert.Equal(t, pcommon.TraceID(parentTraceID), span.TraceID())
	assert.Equal(t, pcommon.SpanID(parentSpanID), span.ParentSpanID())
	assert.NotEqual(t, pcommon.SpanID(parentSpanID), span.SpanID()) // new span ID for receiver span

	attrs := span.Attributes().AsRaw()
	assert.Equal(t, "nats", attrs["messaging.system"])
	assert.Equal(t, "orders.created", attrs["messaging.destination.name"])
	assert.Equal(t, "receive", attrs["messaging.operation.type"])
	assert.Equal(t, "receive", attrs["messaging.operation.name"])
	assert.Equal(t, int64(len(msg.Data)), attrs["messaging.message.body.size"])
	assert.Equal(t, "msg-uuid-1001", attrs["messaging.message.id"])
}

func TestTracesReceiverHandleMessageWithoutHeader(t *testing.T) {
	sink := new(consumertest.TracesSink)
	set := receivertest.NewNopSettings(componentType)
	tr, err := newTracesReceiver(&Config{Endpoint: "nats://localhost:4222"}, set, sink)
	require.NoError(t, err)

	msg := nats.NewMsg("_INBOX.123456")
	msg.Data = []byte(`ping`)

	tr.handleMessage(msg)

	traces := sink.AllTraces()
	require.Len(t, traces, 1)

	span := traces[0].ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	assert.False(t, span.TraceID().IsEmpty())
	assert.False(t, span.SpanID().IsEmpty())
	assert.True(t, span.ParentSpanID().IsEmpty())

	attrs := span.Attributes().AsRaw()
	assert.Equal(t, true, attrs["messaging.destination.anonymous"])
}
