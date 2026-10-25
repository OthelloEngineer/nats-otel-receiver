package natsreceiver

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestLogsReceiverHandleAdvisoryFixture(t *testing.T) {
	sink := new(consumertest.LogsSink)
	set := receivertest.NewNopSettings(componentType)
	lr, err := newLogsReceiver(&Config{Endpoint: "nats://localhost:4222"}, set, sink)
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join("testdata", "advisory.json"))
	require.NoError(t, err)

	msg := &nats.Msg{
		Subject: "$JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS",
		Data:    data,
	}

	lr.handleMessage(msg)

	logs := sink.AllLogs()
	require.Len(t, logs, 1)

	rl := logs[0].ResourceLogs().At(0)
	assert.Equal(t, "nats", rl.Resource().Attributes().AsRaw()["messaging.system"])
	assert.Equal(t, "NC2YTR3U73IUG5X4X7726G74F7X672AOPD4H2W575773B3J7EUS2E6Y2", rl.Resource().Attributes().AsRaw()["nats.server.id"])

	record := rl.ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, plog.SeverityNumberInfo, record.SeverityNumber())
	assert.Equal(t, "INFO", record.SeverityText())
	assert.Equal(t, string(data), record.Body().Str())

	attrs := record.Attributes().AsRaw()
	assert.Equal(t, "nats", attrs["messaging.system"])
	assert.Equal(t, "nats", attrs["event.domain"])
	assert.Equal(t, "$JS.EVENT.ADVISORY.STREAM.CREATED.ORDERS", attrs["nats.subject"])
	assert.Equal(t, "io.nats.jetstream.advisory.v1.stream_action", attrs["nats.event.type"])
	assert.Equal(t, "io.nats.jetstream.advisory.v1.stream_action", attrs["event.name"])
	assert.Equal(t, "ORDERS", attrs["messaging.destination.name"])
	assert.Equal(t, int64(1042), attrs["nats.client.id"])
	assert.Equal(t, "order-service-producer", attrs["client.name"])
}

func TestLogsReceiverSeverityMapping(t *testing.T) {
	sink := new(consumertest.LogsSink)
	set := receivertest.NewNopSettings(componentType)
	lr, err := newLogsReceiver(&Config{Endpoint: "nats://localhost:4222"}, set, sink)
	require.NoError(t, err)

	// Disconnect event -> WARN
	lr.handleMessage(&nats.Msg{
		Subject: "$SYS.SERVER.TEST.CLIENT.DISCONNECT",
		Data:    []byte(`{"type":"io.nats.server.v1.client_disconnect","reason":"slow consumer"}`),
	})

	// Error event -> ERROR
	lr.handleMessage(&nats.Msg{
		Subject: "$JS.EVENT.ADVISORY.MAX_DELIVER.ORDERS",
		Data:    []byte(`{"type":"io.nats.jetstream.advisory.v1.max_deliver","stream":"ORDERS"}`),
	})

	logs := sink.AllLogs()
	require.Len(t, logs, 2)

	rec1 := logs[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, plog.SeverityNumberWarn, rec1.SeverityNumber())
	assert.Equal(t, "WARN", rec1.SeverityText())

	rec2 := logs[1].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	assert.Equal(t, plog.SeverityNumberError, rec2.SeverityNumber())
	assert.Equal(t, "ERROR", rec2.SeverityText())
}
