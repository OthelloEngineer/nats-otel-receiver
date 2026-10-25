package natsreceiver

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type natsAdvisoryEvent struct {
	Type      string         `json:"type"`
	ID        string         `json:"id"`
	Timestamp time.Time      `json:"timestamp"`
	Server    natsServerInfo `json:"server"`
	Client    *struct {
		ID   uint64 `json:"id"`
		Name string `json:"name"`
	} `json:"client,omitempty"`
	Stream   string `json:"stream,omitempty"`
	Consumer string `json:"consumer,omitempty"`
}

type natsServerInfo struct {
	Name    string `json:"name"`
	Host    string `json:"host"`
	ID      string `json:"id"`
	Cluster string `json:"cluster"`
}

type logsReceiver struct {
	cfg      *Config
	set      receiver.Settings
	consumer consumer.Logs
	logger   *zap.Logger
	obsrecv  *receiverhelper.ObsReport
	nc       *nats.Conn
	subs     []*nats.Subscription
}

func newLogsReceiver(cfg *Config, set receiver.Settings, next consumer.Logs) (*logsReceiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             set.ID,
		Transport:              "nats",
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, err
	}

	return &logsReceiver{
		cfg:      cfg,
		set:      set,
		consumer: next,
		logger:   set.Logger,
		obsrecv:  obsrecv,
	}, nil
}

func (lr *logsReceiver) Start(ctx context.Context, host component.Host) error {
	lr.logger.Info("Starting NATS logs receiver",
		zap.String("endpoint", lr.cfg.Endpoint),
		zap.String("queue_group", lr.cfg.QueueGroup))

	nc, err := newNATSConn(ctx, lr.cfg, "otelcol-nats-logs-receiver", lr.logger)
	if err != nil {
		return err
	}
	lr.nc = nc

	subjects := []string{
		"$SYS.ACCOUNT.*.CONNECT",
		"$SYS.ACCOUNT.*.DISCONNECT",
		"$SYS.ACCOUNT.*.SERVER.CONNS",
		"$SYS.SERVER.*.CLIENT.CONNECT",
		"$SYS.SERVER.*.CLIENT.DISCONNECT",
		"$JS.EVENT.ADVISORY.>",
	}

	for _, subj := range subjects {
		var sub *nats.Subscription
		var subErr error
		if lr.cfg.QueueGroup != "" {
			sub, subErr = nc.QueueSubscribe(subj, lr.cfg.QueueGroup, func(msg *nats.Msg) {
				lr.handleMessage(msg)
			})
		} else {
			sub, subErr = nc.Subscribe(subj, func(msg *nats.Msg) {
				lr.handleMessage(msg)
			})
		}
		if subErr != nil {
			lr.logger.Warn("Failed to subscribe to advisory subject", zap.String("subject", subj), zap.Error(subErr))
			continue
		}
		lr.subs = append(lr.subs, sub)
	}

	return nil
}

func (lr *logsReceiver) Shutdown(ctx context.Context) error {
	for _, sub := range lr.subs {
		_ = sub.Unsubscribe()
	}
	if lr.nc != nil {
		lr.nc.Close()
	}
	return nil
}

func (lr *logsReceiver) handleMessage(msg *nats.Msg) {
	var ev natsAdvisoryEvent
	_ = json.Unmarshal(msg.Data, &ev)

	now := time.Now()
	eventTime := ev.Timestamp
	if eventTime.IsZero() {
		eventTime = now
	}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "nats")
	rl.Resource().Attributes().PutStr("messaging.system", "nats")
	if ev.Server.ID != "" {
		rl.Resource().Attributes().PutStr("nats.server.id", ev.Server.ID)
	}

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("github.com/nats-otel/nats-otel/receiver/natsreceiver")
	sl.Scope().SetVersion("1.0.0")

	lrRecord := sl.LogRecords().AppendEmpty()
	lrRecord.SetTimestamp(pcommon.NewTimestampFromTime(eventTime))
	lrRecord.SetObservedTimestamp(pcommon.NewTimestampFromTime(now))

	// Severity
	lowerType := strings.ToLower(ev.Type)
	lowerSubj := strings.ToLower(msg.Subject)
	if strings.Contains(lowerType, "disconnect") || strings.Contains(lowerSubj, "disconnect") {
		lrRecord.SetSeverityNumber(plog.SeverityNumberWarn)
		lrRecord.SetSeverityText("WARN")
	} else if strings.Contains(lowerType, "max_deliver") || strings.Contains(lowerSubj, "error") {
		lrRecord.SetSeverityNumber(plog.SeverityNumberError)
		lrRecord.SetSeverityText("ERROR")
	} else {
		lrRecord.SetSeverityNumber(plog.SeverityNumberInfo)
		lrRecord.SetSeverityText("INFO")
	}

	lrRecord.Body().SetStr(string(msg.Data))

	// Semantic Attributes
	lrRecord.Attributes().PutStr("messaging.system", "nats")
	lrRecord.Attributes().PutStr("event.domain", "nats")
	lrRecord.Attributes().PutStr("nats.subject", msg.Subject)
	if ev.Type != "" {
		lrRecord.Attributes().PutStr("nats.event.type", ev.Type)
		lrRecord.Attributes().PutStr("event.name", ev.Type)
	} else {
		lrRecord.Attributes().PutStr("event.name", msg.Subject)
	}

	if ev.Client != nil {
		lrRecord.Attributes().PutInt("nats.client.id", int64(ev.Client.ID))
		if ev.Client.Name != "" {
			lrRecord.Attributes().PutStr("client.name", ev.Client.Name)
		}
	}
	if ev.Stream != "" {
		lrRecord.Attributes().PutStr("messaging.destination.name", ev.Stream)
	}
	if ev.Consumer != "" {
		lrRecord.Attributes().PutStr("messaging.consumer.group.name", ev.Consumer)
	}

	opCtx := lr.obsrecv.StartLogsOp(context.Background())
	err := lr.consumer.ConsumeLogs(opCtx, ld)
	lr.obsrecv.EndLogsOp(opCtx, "nats", ld.LogRecordCount(), err)
	if err != nil {
		lr.logger.Error("Failed to deliver logs to next consumer", zap.Error(err))
	}
}
