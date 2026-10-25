package natsreceiver_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-otel/nats-otel/receiver/natsreceiver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	factory := natsreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(component.MustNewType("nats"), "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	rCfg, ok := cfg.(*natsreceiver.Config)
	require.True(t, ok)
	assert.Equal(t, "nats://localhost:4222", rCfg.Endpoint)
	assert.NoError(t, rCfg.Validate())
}

func TestLoadConfigFull(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config_full.yaml"))
	require.NoError(t, err)

	factory := natsreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(component.MustNewType("nats"), "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	rCfg, ok := cfg.(*natsreceiver.Config)
	require.True(t, ok)
	assert.Equal(t, "nats://nats.production:4222", rCfg.Endpoint)
	assert.Equal(t, "otelcol-nats-cluster", rCfg.QueueGroup)
	assert.Equal(t, "otel-collector", rCfg.Auth.Username)
	assert.Equal(t, "secret-password", string(rCfg.Auth.Password))
	assert.Equal(t, "auth-token-123", string(rCfg.Auth.Token))
	assert.True(t, rCfg.TLS.InsecureSkipVerify)
	assert.Equal(t, "nats.production", rCfg.TLS.ServerName)
	assert.Equal(t, "http://nats.production:8222", rCfg.Monitor.Endpoint)
	assert.Equal(t, 10*time.Second, rCfg.Monitor.Timeout)
	assert.Equal(t, "http://nats.production:8222", rCfg.MonitorURL)
	assert.Equal(t, 10*time.Second, rCfg.ScrapeInterval)
	assert.Equal(t, []string{"telemetry.>", "events.>", "orders.>"}, rCfg.TappedSubjects)
	assert.NoError(t, rCfg.Validate())
}

func TestLoadConfigInvalidEndpoint(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config_invalid_endpoint.yaml"))
	require.NoError(t, err)

	factory := natsreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(component.MustNewType("nats"), "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	rCfg, ok := cfg.(*natsreceiver.Config)
	require.True(t, ok)
	assert.ErrorContains(t, rCfg.Validate(), "endpoint must not be empty")
}

func TestLoadConfigInvalidInterval(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config_invalid_interval.yaml"))
	require.NoError(t, err)

	factory := natsreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig()

	sub, err := cm.Sub(component.NewIDWithName(component.MustNewType("nats"), "").String())
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	rCfg, ok := cfg.(*natsreceiver.Config)
	require.True(t, ok)
	assert.ErrorContains(t, rCfg.Validate(), "scrape_interval must be non-negative")
}
