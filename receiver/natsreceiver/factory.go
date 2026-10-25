package natsreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

var (
	componentType = component.MustNewType("nats")
	stability     = component.StabilityLevelAlpha
)

// NewFactory creates a factory for NATS receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		componentType,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, stability),
		receiver.WithLogs(createLogsReceiver, stability),
		receiver.WithTraces(createTracesReceiver, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint:       "nats://localhost:4222",
		MonitorURL:     "http://localhost:8222",
		Monitor:        confighttp.NewDefaultClientConfig(),
		TLS:            configtls.NewDefaultClientConfig(),
		ScrapeInterval: 5 * time.Second,
		TappedSubjects: []string{"telemetry.>", "events.>", "orders.>"},
	}
}

func createMetricsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	rCfg := cfg.(*Config)
	return newMetricsReceiver(rCfg, set, consumer)
}

func createLogsReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	rCfg := cfg.(*Config)
	return newLogsReceiver(rCfg, set, consumer)
}

func createTracesReceiver(
	_ context.Context,
	set receiver.Settings,
	cfg component.Config,
	consumer consumer.Traces,
) (receiver.Traces, error) {
	rCfg := cfg.(*Config)
	return newTracesReceiver(rCfg, set, consumer)
}
