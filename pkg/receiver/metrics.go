package receiver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/nats-otel/nats-otel/pkg/natsemconv"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// VarzResponse represents NATS /varz monitoring endpoint
type VarzResponse struct {
	ServerID      string    `json:"server_id"`
	ServerName    string    `json:"server_name"`
	Version       string    `json:"version"`
	Host          string    `json:"host"`
	Port          int       `json:"port"`
	Start         time.Time `json:"start"`
	Uptime        string    `json:"uptime"`
	Mem           int64     `json:"mem"`
	Cores         int       `json:"cores"`
	CPU           float64   `json:"cpu"`
	Connections   int       `json:"connections"`
	TotalConns    int64     `json:"total_connections"`
	InMsgs        int64     `json:"in_msgs"`
	OutMsgs       int64     `json:"out_msgs"`
	InBytes       int64     `json:"in_bytes"`
	OutBytes      int64     `json:"out_bytes"`
	SlowConsumers int64     `json:"slow_consumers"`
	Subscriptions uint32    `json:"subscriptions"`
}

// JszResponse represents NATS /jsz monitoring endpoint
type JszResponse struct {
	Streams   int   `json:"streams"`
	Consumers int   `json:"consumers"`
	Messages  int64 `json:"messages"`
	Bytes     int64 `json:"bytes"`
}

// MetricsReceiver scrapes NATS monitoring HTTP endpoints and publishes OTel metrics
type MetricsReceiver struct {
	monitorURL string
	client     *http.Client
	meter      metric.Meter

	// Instruments
	serverConns    metric.Int64Gauge
	serverInMsgs   metric.Int64Counter
	serverOutMsgs  metric.Int64Counter
	serverInBytes  metric.Int64Counter
	serverOutBytes metric.Int64Counter
	slowConsumers  metric.Int64Gauge
	serverCPU      metric.Float64Gauge
	serverMemory   metric.Int64Gauge

	jsStreams   metric.Int64Gauge
	jsConsumers metric.Int64Gauge
	jsMessages  metric.Int64Gauge
	jsBytes     metric.Int64Gauge

	lastInMsgs   int64
	lastOutMsgs  int64
	lastInBytes  int64
	lastOutBytes int64
}

// NewMetricsReceiver initializes the metrics receiver instruments
func NewMetricsReceiver(monitorURL string, meter metric.Meter) (*MetricsReceiver, error) {
	mr := &MetricsReceiver{
		monitorURL: monitorURL,
		client:     &http.Client{Timeout: 5 * time.Second},
		meter:      meter,
	}

	var err error
	mr.serverConns, err = meter.Int64Gauge(natsemconv.MetricNatsServerConnections,
		metric.WithDescription("Current active client connections to NATS server"))
	if err != nil {
		return nil, err
	}

	mr.serverInMsgs, err = meter.Int64Counter(natsemconv.MetricMessagingClientConsumedMessages,
		metric.WithDescription("Total messages received by NATS server (conforming to messaging.client.consumed.messages)"),
		metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}

	mr.serverOutMsgs, err = meter.Int64Counter(natsemconv.MetricMessagingClientSentMessages,
		metric.WithDescription("Total messages sent by NATS server (conforming to messaging.client.sent.messages)"),
		metric.WithUnit("{message}"))
	if err != nil {
		return nil, err
	}

	mr.serverInBytes, err = meter.Int64Counter(natsemconv.MetricNatsServerInBytes,
		metric.WithDescription("Total bytes received by NATS server"),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}

	mr.serverOutBytes, err = meter.Int64Counter(natsemconv.MetricNatsServerOutBytes,
		metric.WithDescription("Total bytes sent by NATS server"),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}

	mr.slowConsumers, err = meter.Int64Gauge(natsemconv.MetricNatsServerSlowConsumers,
		metric.WithDescription("Total slow consumers detected by NATS server"))
	if err != nil {
		return nil, err
	}

	mr.serverCPU, err = meter.Float64Gauge("nats.server.cpu",
		metric.WithDescription("NATS server CPU utilization percentage"))
	if err != nil {
		return nil, err
	}

	mr.serverMemory, err = meter.Int64Gauge("nats.server.memory",
		metric.WithDescription("NATS server memory usage in bytes"),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}

	mr.jsStreams, err = meter.Int64Gauge(natsemconv.MetricNatsJetStreamStreams,
		metric.WithDescription("Total JetStream streams configured"))
	if err != nil {
		return nil, err
	}

	mr.jsConsumers, err = meter.Int64Gauge(natsemconv.MetricNatsJetStreamConsumers,
		metric.WithDescription("Total JetStream consumers configured"))
	if err != nil {
		return nil, err
	}

	mr.jsMessages, err = meter.Int64Gauge(natsemconv.MetricNatsJetStreamMessages,
		metric.WithDescription("Total JetStream stored messages"))
	if err != nil {
		return nil, err
	}

	mr.jsBytes, err = meter.Int64Gauge(natsemconv.MetricNatsJetStreamBytes,
		metric.WithDescription("Total JetStream storage bytes used"),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}

	return mr, nil
}

// ScrapeOnce performs one scrape cycle of /varz and /jsz
func (mr *MetricsReceiver) ScrapeOnce(ctx context.Context) error {
	varz, err := mr.fetchVarz(ctx)
	if err != nil {
		return fmt.Errorf("error fetching /varz: %w", err)
	}

	baseAttrs := []attribute.KeyValue{
		attribute.String(natsemconv.MessagingSystemKey, natsemconv.MessagingSystemNATS),
		attribute.String(natsemconv.NatsServerIDKey, varz.ServerID),
		attribute.String("server.name", varz.ServerName),
		attribute.String("server.version", varz.Version),
		attribute.String(natsemconv.ServerAddressKey, varz.Host),
		attribute.Int(natsemconv.ServerPortKey, varz.Port),
	}

	mr.serverConns.Record(ctx, int64(varz.Connections), metric.WithAttributes(baseAttrs...))
	mr.slowConsumers.Record(ctx, varz.SlowConsumers, metric.WithAttributes(baseAttrs...))
	mr.serverCPU.Record(ctx, varz.CPU, metric.WithAttributes(baseAttrs...))
	mr.serverMemory.Record(ctx, varz.Mem, metric.WithAttributes(baseAttrs...))

	// Record delta for counters
	if varz.InMsgs > mr.lastInMsgs {
		delta := varz.InMsgs - mr.lastInMsgs
		mr.serverInMsgs.Add(ctx, delta, metric.WithAttributes(baseAttrs...))
		mr.lastInMsgs = varz.InMsgs
	}
	if varz.OutMsgs > mr.lastOutMsgs {
		delta := varz.OutMsgs - mr.lastOutMsgs
		mr.serverOutMsgs.Add(ctx, delta, metric.WithAttributes(baseAttrs...))
		mr.lastOutMsgs = varz.OutMsgs
	}
	if varz.InBytes > mr.lastInBytes {
		delta := varz.InBytes - mr.lastInBytes
		mr.serverInBytes.Add(ctx, delta, metric.WithAttributes(baseAttrs...))
		mr.lastInBytes = varz.InBytes
	}
	if varz.OutBytes > mr.lastOutBytes {
		delta := varz.OutBytes - mr.lastOutBytes
		mr.serverOutBytes.Add(ctx, delta, metric.WithAttributes(baseAttrs...))
		mr.lastOutBytes = varz.OutBytes
	}

	// Fetch /jsz (if JetStream enabled)
	jsz, err := mr.fetchJsz(ctx)
	if err == nil && jsz != nil {
		mr.jsStreams.Record(ctx, int64(jsz.Streams), metric.WithAttributes(baseAttrs...))
		mr.jsConsumers.Record(ctx, int64(jsz.Consumers), metric.WithAttributes(baseAttrs...))
		mr.jsMessages.Record(ctx, jsz.Messages, metric.WithAttributes(baseAttrs...))
		mr.jsBytes.Record(ctx, jsz.Bytes, metric.WithAttributes(baseAttrs...))
	}

	return nil
}

func (mr *MetricsReceiver) fetchVarz(ctx context.Context) (*VarzResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mr.monitorURL+"/varz", nil)
	if err != nil {
		return nil, err
	}
	resp, err := mr.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var varz VarzResponse
	if err := json.Unmarshal(body, &varz); err != nil {
		return nil, err
	}
	return &varz, nil
}

func (mr *MetricsReceiver) fetchJsz(ctx context.Context) (*JszResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mr.monitorURL+"/jsz", nil)
	if err != nil {
		return nil, err
	}
	resp, err := mr.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var jsz JszResponse
	if err := json.Unmarshal(body, &jsz); err != nil {
		return nil, err
	}
	return &jsz, nil
}

// Start begins periodic polling loop
func (mr *MetricsReceiver) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial scrape
	_ = mr.ScrapeOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = mr.ScrapeOnce(ctx)
		}
	}
}
