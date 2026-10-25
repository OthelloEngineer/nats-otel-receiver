package receiver

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/natsemconv"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
)

// NatsServerEvent represents the standard NATS advisory envelope
type NatsServerEvent struct {
	Type      string          `json:"type"`
	ID        string          `json:"id"`
	Timestamp time.Time       `json:"timestamp"`
	Server    NatsServerInfo  `json:"server"`
	Client    *NatsClientInfo `json:"client,omitempty"`
	Reason    string          `json:"reason,omitempty"`
	Stream    string          `json:"stream,omitempty"`
	Consumer  string          `json:"consumer,omitempty"`
	StreamSeq uint64          `json:"stream_seq,omitempty"`
}

type NatsServerInfo struct {
	Name    string `json:"name"`
	Host    string `json:"host"`
	ID      string `json:"id"`
	Cluster string `json:"cluster"`
	Version string `json:"version"`
}

type NatsClientInfo struct {
	ID   uint64 `json:"id"`
	User string `json:"user"`
	Host string `json:"host"`
	Name string `json:"name"`
	Lang string `json:"lang"`
}

// LogsReceiver subscribes to NATS $SYS and advisory subjects and exports them as OTel logs
type LogsReceiver struct {
	nc     *nats.Conn
	logger log.Logger
	subs   []*nats.Subscription
}

// NewLogsReceiver creates a new LogsReceiver instance
func NewLogsReceiver(nc *nats.Conn, logger log.Logger) *LogsReceiver {
	return &LogsReceiver{
		nc:     nc,
		logger: logger,
	}
}

// Start subscribes to system and advisory subjects
func (lr *LogsReceiver) Start(ctx context.Context) error {
	subjects := []string{
		"$SYS.ACCOUNT.*.CONNECT",
		"$SYS.ACCOUNT.*.DISCONNECT",
		"$SYS.ACCOUNT.*.SERVER.CONNS",
		"$SYS.SERVER.*.CLIENT.CONNECT",
		"$SYS.SERVER.*.CLIENT.DISCONNECT",
		"$JS.EVENT.ADVISORY.>",
	}

	for _, subj := range subjects {
		sub, err := lr.nc.Subscribe(subj, func(msg *nats.Msg) {
			lr.HandleEvent(context.Background(), msg)
		})
		if err != nil {
			slog.Warn("Failed to subscribe to advisory subject", "subject", subj, "err", err)
			continue
		}
		lr.subs = append(lr.subs, sub)
	}

	return nil
}

// Stop unsubscribes from all subjects
func (lr *LogsReceiver) Stop() {
	for _, sub := range lr.subs {
		_ = sub.Unsubscribe()
	}
}

// HandleEvent processes an advisory or system event message and emits an OTel log record
func (lr *LogsReceiver) HandleEvent(ctx context.Context, msg *nats.Msg) {
	var ev NatsServerEvent
	_ = json.Unmarshal(msg.Data, &ev)

	now := time.Now()
	eventTime := ev.Timestamp
	if eventTime.IsZero() {
		eventTime = now
	}

	// Determine severity based on event type
	sev := log.SeverityInfo
	sevText := "INFO"
	lowerType := strings.ToLower(ev.Type)
	lowerSubj := strings.ToLower(msg.Subject)

	if strings.Contains(lowerType, "disconnect") || strings.Contains(lowerSubj, "disconnect") {
		sev = log.SeverityWarn
		sevText = "WARN"
	} else if strings.Contains(lowerType, "max_deliver") || strings.Contains(lowerType, "error") || strings.Contains(lowerSubj, "error") {
		sev = log.SeverityError
		sevText = "ERROR"
	}

	record := log.Record{}
	record.SetTimestamp(eventTime)
	record.SetObservedTimestamp(now)
	record.SetSeverity(sev)
	record.SetSeverityText(sevText)
	record.SetBody(attribute.StringValue(string(msg.Data)))

	// Populate Semantic Convention attributes
	attrs := []attribute.KeyValue{
		attribute.String(natsemconv.MessagingSystemKey, natsemconv.MessagingSystemNATS),
		attribute.String("event.domain", "nats"),
		attribute.String("nats.subject", msg.Subject),
	}

	if ev.Type != "" {
		attrs = append(attrs, attribute.String(natsemconv.NatsEventTypeKey, ev.Type))
		attrs = append(attrs, attribute.String("event.name", ev.Type))
	} else {
		attrs = append(attrs, attribute.String("event.name", msg.Subject))
	}

	if ev.Server.ID != "" {
		attrs = append(attrs, attribute.String(natsemconv.NatsServerIDKey, ev.Server.ID))
	}
	if ev.Server.Name != "" {
		attrs = append(attrs, attribute.String("server.name", ev.Server.Name))
	}
	if ev.Client != nil {
		attrs = append(attrs, attribute.Int64(natsemconv.NatsClientIDKey, int64(ev.Client.ID)))
		if ev.Client.Name != "" {
			attrs = append(attrs, attribute.String("client.name", ev.Client.Name))
		}
	}
	if ev.Stream != "" {
		attrs = append(attrs, attribute.String(natsemconv.MessagingDestinationNameKey, ev.Stream))
	}
	if ev.Consumer != "" {
		attrs = append(attrs, attribute.String(natsemconv.MessagingConsumerGroupNameKey, ev.Consumer))
	}

	record.AddAttributes(attrs...)
	lr.logger.Emit(ctx, record)
}
