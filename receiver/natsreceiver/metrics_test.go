package natsreceiver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestBuildMetricsFromFixtures(t *testing.T) {
	varzBytes, err := os.ReadFile(filepath.Join("testdata", "varz.json"))
	require.NoError(t, err)

	var varz varzResponse
	err = json.Unmarshal(varzBytes, &varz)
	require.NoError(t, err)

	jszBytes, err := os.ReadFile(filepath.Join("testdata", "jsz.json"))
	require.NoError(t, err)

	var jsz jszResponse
	err = json.Unmarshal(jszBytes, &jsz)
	require.NoError(t, err)

	set := receivertest.NewNopSettings(componentType)
	mr, err := newMetricsReceiver(&Config{Endpoint: "nats://localhost:4222"}, set, consumertest.NewNop())
	require.NoError(t, err)
	metrics := mr.buildMetrics(&varz, &jsz)

	require.Equal(t, 1, metrics.ResourceMetrics().Len())
	rm := metrics.ResourceMetrics().At(0)
	attrs := rm.Resource().Attributes()

	svcName, ok := attrs.Get("service.name")
	assert.True(t, ok)
	assert.Equal(t, "nats", svcName.Str())

	sys, ok := attrs.Get("messaging.system")
	assert.True(t, ok)
	assert.Equal(t, "nats", sys.Str())

	serverID, ok := attrs.Get("nats.server.id")
	assert.True(t, ok)
	assert.Equal(t, "NC2YTR3U73IUG5X4X7726G74F7X672AOPD4H2W575773B3J7EUS2E6Y2", serverID.Str())

	require.Equal(t, 1, rm.ScopeMetrics().Len())
	sm := rm.ScopeMetrics().At(0)
	assert.Equal(t, "github.com/nats-otel/nats-otel/receiver/natsreceiver", sm.Scope().Name())

	metricMap := make(map[string]int64)
	for i := 0; i < sm.Metrics().Len(); i++ {
		m := sm.Metrics().At(i)
		switch m.Type() {
		case 1: // Gauge
			dp := m.Gauge().DataPoints().At(0)
			metricMap[m.Name()] = dp.IntValue()
		case 2: // Sum
			dp := m.Sum().DataPoints().At(0)
			metricMap[m.Name()] = dp.IntValue()
		}
	}

	assert.Equal(t, int64(42), metricMap["nats.server.connections"])
	assert.Equal(t, int64(3), metricMap["nats.server.slow_consumers"])
	assert.Equal(t, int64(31457280), metricMap["nats.server.memory"])
	assert.Equal(t, int64(1250000), metricMap["messaging.client.consumed.messages"])
	assert.Equal(t, int64(2480000), metricMap["messaging.client.sent.messages"])
	assert.Equal(t, int64(104857600), metricMap["nats.server.in_bytes"])
	assert.Equal(t, int64(209715200), metricMap["nats.server.out_bytes"])
	assert.Equal(t, int64(12), metricMap["nats.jetstream.streams"])
	assert.Equal(t, int64(34), metricMap["nats.jetstream.consumers"])
	assert.Equal(t, int64(500000), metricMap["nats.jetstream.messages"])
	assert.Equal(t, int64(524288000), metricMap["nats.jetstream.bytes"])
}
