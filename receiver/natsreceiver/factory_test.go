package natsreceiver_test

import (
	"context"
	"testing"

	"github.com/nats-otel/nats-otel/receiver/natsreceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestFactoryType(t *testing.T) {
	factory := natsreceiver.NewFactory()
	if factory.Type() != component.MustNewType("nats") {
		t.Errorf("unexpected factory type: %v", factory.Type())
	}
}

func TestFactoryCreateDefaultConfig(t *testing.T) {
	factory := natsreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig()

	rCfg, ok := cfg.(*natsreceiver.Config)
	if !ok {
		t.Fatalf("unexpected config type: %T", cfg)
	}

	if rCfg.Endpoint != "nats://localhost:4222" {
		t.Errorf("unexpected default endpoint: %s", rCfg.Endpoint)
	}

	if err := rCfg.Validate(); err != nil {
		t.Errorf("default config validation failed: %v", err)
	}
}

func TestFactoryCreateReceivers(t *testing.T) {
	factory := natsreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig()
	set := receivertest.NewNopSettings(component.MustNewType("nats"))

	ctx := context.Background()

	// Metrics
	mr, err := factory.CreateMetrics(ctx, set, cfg, consumertest.NewNop())
	if err != nil {
		t.Fatalf("failed to create metrics receiver: %v", err)
	}
	if mr == nil {
		t.Fatal("expected non-nil metrics receiver")
	}

	// Logs
	lr, err := factory.CreateLogs(ctx, set, cfg, consumertest.NewNop())
	if err != nil {
		t.Fatalf("failed to create logs receiver: %v", err)
	}
	if lr == nil {
		t.Fatal("expected non-nil logs receiver")
	}

	// Traces
	tr, err := factory.CreateTraces(ctx, set, cfg, consumertest.NewNop())
	if err != nil {
		t.Fatalf("failed to create traces receiver: %v", err)
	}
	if tr == nil {
		t.Fatal("expected non-nil traces receiver")
	}
}
