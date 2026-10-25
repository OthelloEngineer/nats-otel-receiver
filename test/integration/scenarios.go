package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/natsotel"
)

// runScenarios executes integration test scenarios covering Pub/Sub, Queue Groups, JetStream, Advisories, and Wiretap.
func runScenarios(ctx context.Context, nc *nats.Conn, natsURL string) error {
	// Scenario 1: Core Pub/Sub with trace context
	slog.Info("Executing Scenario 1: Pub/Sub with trace context...")
	subReceived := make(chan bool, 1)
	sub, err := nc.Subscribe("orders.created", natsotel.WrapErrorHandler(func(ctx context.Context, msg *nats.Msg) error {
		slog.Info("Received message", "subject", msg.Subject, "data", string(msg.Data), "header", msg.Header)
		subReceived <- true
		return nil
	}))
	if err != nil {
		return fmt.Errorf("scenario 1 subscribe failed: %w", err)
	}
	defer sub.Unsubscribe()

	err = natsotel.Publish(ctx, nc, "orders.created", []byte(`{"order_id": "ORD-123", "amount": 99.50}`))
	if err != nil {
		return fmt.Errorf("scenario 1 publish failed: %w", err)
	}

	select {
	case <-subReceived:
		slog.Info("Scenario 1 passed: message consumed with trace context")
	case <-time.After(5 * time.Second):
		return fmt.Errorf("scenario 1 timeout waiting for message")
	}

	// Scenario 2: Queue Group subscription
	slog.Info("Executing Scenario 2: Queue Group subscription...")
	qReceived := make(chan bool, 1)
	qsub, err := nc.QueueSubscribe("tasks.process", "worker-group", natsotel.WrapQueueHandler("worker-group", func(ctx context.Context, msg *nats.Msg) error {
		slog.Info("Queue subscriber received task", "subject", msg.Subject, "group", "worker-group")
		qReceived <- true
		return nil
	}))
	if err != nil {
		return fmt.Errorf("scenario 2 queue subscribe failed: %w", err)
	}
	defer qsub.Unsubscribe()

	_ = natsotel.Publish(ctx, nc, "tasks.process", []byte(`{"task": "encode-video"}`))
	select {
	case <-qReceived:
		slog.Info("Scenario 2 passed: queue group message processed")
	case <-time.After(5 * time.Second):
		return fmt.Errorf("scenario 2 timeout waiting for queue message")
	}

	// Scenario 3: JetStream persistence and Sequence
	slog.Info("Executing Scenario 3: JetStream publish & sequence tracking...")
	js, err := nc.JetStream()
	if err == nil {
		_, _ = js.AddStream(&nats.StreamConfig{
			Name:     "ORDERS_STREAM",
			Subjects: []string{"orders.js.>"},
			Storage:  nats.MemoryStorage,
		})

		jsMsg := nats.NewMsg("orders.js.created")
		jsMsg.Data = []byte(`{"item": "laptop"}`)
		jsMsg.Header = make(nats.Header)
		jsMsg.Header.Set("Nats-Msg-Id", "MSG-JS-001")

		pubAck, jsErr := natsotel.PublishJetStream(ctx, js, jsMsg)
		if jsErr != nil {
			slog.Warn("JetStream publish failed", "err", jsErr)
		} else {
			slog.Info("Scenario 3 passed: JetStream published", "seq", pubAck.Sequence, "stream", pubAck.Stream)
		}
	}

	// Scenario 4: Trigger Server System Advisories ($SYS)
	slog.Info("Executing Scenario 4: Triggering NATS $SYS advisories...")
	tempConn, err := nats.Connect(natsURL, nats.Name("temp-client-for-events"))
	if err == nil {
		time.Sleep(500 * time.Millisecond)
		tempConn.Close()
		slog.Info("Scenario 4: Temporary connection opened and closed to trigger connect/disconnect events")
	}

	// Scenario 5: Tapped Subject (receiver taps message and emits receive span)
	slog.Info("Executing Scenario 5: Publishing to tapped subject...")
	_ = natsotel.Publish(ctx, nc, "telemetry.stats", []byte(`{"cpu": 45.2}`))

	return nil
}
