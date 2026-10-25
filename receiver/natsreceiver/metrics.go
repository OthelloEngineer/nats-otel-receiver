package natsreceiver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"
)

type varzResponse struct {
	ServerID      string    `json:"server_id"`
	ServerName    string    `json:"server_name"`
	Version       string    `json:"version"`
	Host          string    `json:"host"`
	Port          int       `json:"port"`
	Start         time.Time `json:"start"`
	Mem           int64     `json:"mem"`
	Cores         int       `json:"cores"`
	CPU           float64   `json:"cpu"`
	Connections   int       `json:"connections"`
	InMsgs        int64     `json:"in_msgs"`
	OutMsgs       int64     `json:"out_msgs"`
	InBytes       int64     `json:"in_bytes"`
	OutBytes      int64     `json:"out_bytes"`
	SlowConsumers int64     `json:"slow_consumers"`
}

type jszResponse struct {
	Streams   int   `json:"streams"`
	Consumers int   `json:"consumers"`
	Messages  int64 `json:"messages"`
	Bytes     int64 `json:"bytes"`
}

type metricsReceiver struct {
	cfg        *Config
	set        receiver.Settings
	consumer   consumer.Metrics
	logger     *zap.Logger
	obsrecv    *receiverhelper.ObsReport
	httpClient *http.Client
	baseURL    string
	cancel     context.CancelFunc
}

func newMetricsReceiver(cfg *Config, set receiver.Settings, next consumer.Metrics) (*metricsReceiver, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             set.ID,
		Transport:              "nats",
		ReceiverCreateSettings: set,
	})
	if err != nil {
		return nil, err
	}

	return &metricsReceiver{
		cfg:      cfg,
		set:      set,
		consumer: next,
		logger:   set.Logger,
		obsrecv:  obsrecv,
	}, nil
}

func (mr *metricsReceiver) Start(ctx context.Context, host component.Host) error {
	baseURL := mr.cfg.Monitor.Endpoint
	if baseURL == "" {
		baseURL = mr.cfg.MonitorURL
	}
	mr.baseURL = baseURL

	mr.logger.Info("Starting NATS metrics receiver", zap.String("monitor_url", mr.baseURL))

	if mr.baseURL != "" {
		if host != nil {
			client, err := mr.cfg.Monitor.ToClient(ctx, host, mr.set.TelemetrySettings)
			if err != nil {
				return fmt.Errorf("failed to create monitor HTTP client: %w", err)
			}
			mr.httpClient = client
		} else if mr.httpClient == nil {
			mr.httpClient = &http.Client{Timeout: 5 * time.Second}
		}
	}

	pollCtx, cancel := context.WithCancel(context.Background())
	mr.cancel = cancel

	interval := mr.cfg.ScrapeInterval
	if interval == 0 {
		interval = 5 * time.Second
	}

	go mr.pollLoop(pollCtx, interval)
	return nil
}

func (mr *metricsReceiver) Shutdown(ctx context.Context) error {
	if mr.cancel != nil {
		mr.cancel()
	}
	return nil
}

func (mr *metricsReceiver) pollLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial scrape
	mr.scrapeAndSend(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mr.scrapeAndSend(ctx)
		}
	}
}

func (mr *metricsReceiver) scrapeAndSend(ctx context.Context) {
	if mr.baseURL == "" {
		return
	}

	varz, err := mr.fetchVarz(ctx)
	if err != nil {
		mr.logger.Warn("Failed to scrape /varz", zap.Error(err))
		return
	}

	jsz, _ := mr.fetchJsz(ctx)

	md := mr.buildMetrics(varz, jsz)
	numPoints := md.DataPointCount()

	opCtx := mr.obsrecv.StartMetricsOp(ctx)
	err = mr.consumer.ConsumeMetrics(opCtx, md)
	mr.obsrecv.EndMetricsOp(opCtx, "nats", numPoints, err)

	if err != nil {
		mr.logger.Error("Failed to deliver metrics to next consumer", zap.Error(err))
	}
}

func (mr *metricsReceiver) fetchVarz(ctx context.Context) (*varzResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mr.baseURL+"/varz", nil)
	if err != nil {
		return nil, err
	}
	if mr.httpClient == nil {
		mr.httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := mr.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code from /varz: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var varz varzResponse
	if err := json.Unmarshal(body, &varz); err != nil {
		return nil, err
	}
	return &varz, nil
}

func (mr *metricsReceiver) fetchJsz(ctx context.Context) (*jszResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mr.baseURL+"/jsz", nil)
	if err != nil {
		return nil, err
	}
	if mr.httpClient == nil {
		mr.httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := mr.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code from /jsz: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var jsz jszResponse
	if err := json.Unmarshal(body, &jsz); err != nil {
		return nil, err
	}
	return &jsz, nil
}

func (mr *metricsReceiver) buildMetrics(varz *varzResponse, jsz *jszResponse) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "nats")
	rm.Resource().Attributes().PutStr("messaging.system", "nats")
	if varz.ServerID != "" {
		rm.Resource().Attributes().PutStr("nats.server.id", varz.ServerID)
	}

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/nats-otel/nats-otel/receiver/natsreceiver")
	sm.Scope().SetVersion("1.0.0")

	now := pcommon.NewTimestampFromTime(time.Now())

	addGaugeInt := func(name, desc, unit string, val int64) {
		m := sm.Metrics().AppendEmpty()
		m.SetName(name)
		m.SetDescription(desc)
		m.SetUnit(unit)
		m.SetEmptyGauge()
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.SetIntValue(val)
		dp.Attributes().PutStr("messaging.system", "nats")
		if varz.ServerID != "" {
			dp.Attributes().PutStr("nats.server.id", varz.ServerID)
		}
	}

	addSumInt := func(name, desc, unit string, val int64) {
		m := sm.Metrics().AppendEmpty()
		m.SetName(name)
		m.SetDescription(desc)
		m.SetUnit(unit)
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := sum.DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.SetIntValue(val)
		dp.Attributes().PutStr("messaging.system", "nats")
		if varz.ServerID != "" {
			dp.Attributes().PutStr("nats.server.id", varz.ServerID)
		}
	}

	// Messaging Semantic Conventions & Broker Metrics
	addGaugeInt("nats.server.connections", "Current active client connections", "{connection}", int64(varz.Connections))
	addGaugeInt("nats.server.slow_consumers", "Total slow consumers detected by NATS server", "{consumer}", varz.SlowConsumers)
	addGaugeInt("nats.server.memory", "NATS server memory usage in bytes", "By", varz.Mem)

	// OTel SemConv messaging client equivalents from broker perspective
	addSumInt("messaging.client.consumed.messages", "Total messages received by NATS server", "{message}", varz.InMsgs)
	addSumInt("messaging.client.sent.messages", "Total messages sent by NATS server", "{message}", varz.OutMsgs)
	addSumInt("nats.server.in_bytes", "Total bytes received by NATS server", "By", varz.InBytes)
	addSumInt("nats.server.out_bytes", "Total bytes sent by NATS server", "By", varz.OutBytes)

	if jsz != nil {
		addGaugeInt("nats.jetstream.streams", "Total JetStream streams configured", "{stream}", int64(jsz.Streams))
		addGaugeInt("nats.jetstream.consumers", "Total JetStream consumers configured", "{consumer}", int64(jsz.Consumers))
		addGaugeInt("nats.jetstream.messages", "Total JetStream stored messages", "{message}", jsz.Messages)
		addGaugeInt("nats.jetstream.bytes", "Total JetStream storage bytes used", "By", jsz.Bytes)
	}

	return md
}
