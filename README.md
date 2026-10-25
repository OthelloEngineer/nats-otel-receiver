# NATS OpenTelemetry Integration

[![CI](https://github.com/nats-otel/nats-otel/actions/workflows/ci.yml/badge.svg)](https://github.com/nats-otel/nats-otel/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/nats-otel/nats-otel)](https://goreportcard.com/report/github.com/nats-otel/nats-otel)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![OpenTelemetry](https://img.shields.io/badge/OTel-v1.26+_SemConv-orange.svg)](https://opentelemetry.io/docs/specs/semconv/messaging/)

A production-ready OpenTelemetry integration for **NATS (Core & JetStream)** engineered for full alignment with the [OpenTelemetry Semantic Conventions for Messaging Systems](https://opentelemetry.io/docs/specs/semconv/messaging/).

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                               OpenTelemetry Collector Pipeline                          │
│                                                                                         │
│   receivers:                                                                            │
│     nats:                                                                               │
│       endpoint: nats://nats:4222                                                        │
│       queue_group: "otelcol-nats-cluster"                                               │
│       auth: { credentials_file: "/etc/nats/user.creds" }                                │
│       tls:  { ca_file: "/etc/ssl/certs/ca.pem" }                                        │
│       monitor: { endpoint: "http://nats:8222" }                                         │
│       tapped_subjects: ["orders.>", "events.>"]                                         │
│                                                                                         │
│       ├── Metrics (pmetric) ──► [batch] ──► [debug / otlp / prometheus]                 │
│       ├── Logs (plog)       ──► [batch] ──► [debug / otlp / file]                       │
│       └── Traces (ptrace)   ──► [batch] ──► [debug / otlp / jaeger]                     │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

| Component | Path | Description |
| :--- | :--- | :--- |
| **Collector Receiver** | [`receiver/natsreceiver`](receiver/natsreceiver) | Native OpenTelemetry Collector Contrib receiver (metrics, logs, traces). See [Receiver README](receiver/natsreceiver/README.md) and [Metrics Documentation](receiver/natsreceiver/documentation.md). |
| **Custom Collector** | [`cmd/otelcol-nats`](cmd/otelcol-nats) | OpenTelemetry Collector binary embedding `natsreceiver`. Configured for building via OCB ([`builder-config.yaml`](builder-config.yaml)). |
| **Client Instrumentation** | [`pkg/natsotel`](pkg/natsotel) | Go SDK wrapper for NATS clients, providing W3C trace context injection/extraction and operation histograms. |
| **TraceContext Carrier** | [`pkg/carrier`](pkg/carrier) | Zero-overhead `propagation.TextMapCarrier` adapter for `nats.Header`. |
| **Semantic Conventions** | [`pkg/natsemconv`](pkg/natsemconv) | OTel Semantic Conventions v1.26+ mappings and attribute helpers for NATS. |
| **Standalone Sidecar** | [`cmd/nats-otel-receiver`](cmd/nats-otel-receiver) | External daemon / sidecar alternative to `prometheus-nats-exporter` exporting directly via OTLP. |
| **LGTM Demo & Dashboards**| [`examples/lgtm`](examples/lgtm) | Complete local Loki, Grafana, Tempo, and Prometheus stack with Ping-Pong demo and Grafana dashboards. |

---

## Quickstart: Client Instrumentation (`pkg/natsotel`)

Wrap your standard NATS Go connection to automatically inject W3C `traceparent` headers on publish and extract them on consume:

```go
package main

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/nats-otel/nats-otel/pkg/natsotel"
)

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	client, _ := natsotel.NewClient(nc)

	// Publish with automatic traceparent injection & latency histogram recording
	_ = client.Publish(context.Background(), "orders.created", []byte(`{"id": 42}`))

	// Consume with automatic traceparent extraction and consumer span linking
	_, _ = client.QueueSubscribe("orders.created", "billing-group", func(ctx context.Context, msg *nats.Msg) error {
		// ctx contains the active consumer span linked to the publisher's trace
		return nil
	})
}
```

---

## Collector Configuration

The receiver runs inside any OpenTelemetry Collector distribution. See [receiver/natsreceiver/README.md](receiver/natsreceiver/README.md) for full configuration details.

```yaml
receivers:
  nats:
    endpoint: nats://nats:4222
    queue_group: "otelcol-nats-cluster"
    monitor:
      endpoint: http://nats:8222
    scrape_interval: 5s
    tapped_subjects:
      - "orders.>"
      - "events.>"
      - "telemetry.>"
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

processors:
  batch:
    timeout: 1s

exporters:
  debug:
    verbosity: detailed

service:
  pipelines:
    metrics:
      receivers: [nats, otlp]
      processors: [batch]
      exporters: [debug]
    logs:
      receivers: [nats]
      processors: [batch]
      exporters: [debug]
    traces:
      receivers: [nats, otlp]
      processors: [batch]
      exporters: [debug]
```

---

## Semantic Conventions Scorecard (100% Verified)

| Signal | OTel Semantic Convention | NATS Mapping |
| :--- | :--- | :--- |
| **Trace** | `messaging.system` | Exact string `"nats"` |
| **Trace** | `messaging.destination.name` | NATS Subject (e.g. `orders.created`) or JetStream Stream |
| **Trace** | `messaging.destination.anonymous` | `true` for NATS ephemeral inboxes (`_INBOX.>`), `false` otherwise |
| **Trace** | `messaging.operation.type` | `publish`, `receive`, or `process` |
| **Trace** | `messaging.consumer.group.name` | NATS Queue Group name or JetStream Durable Consumer |
| **Trace** | `messaging.message.id` | `Nats-Msg-Id` header or JetStream sequence ID |
| **Trace** | Context Propagation | Standard W3C `traceparent` and `tracestate` in message headers |
| **Metric** | `messaging.client.sent.messages` | Counter `{message}` tracking total sent messages |
| **Metric** | `messaging.client.consumed.messages`| Counter `{message}` tracking total received messages |
| **Metric** | `messaging.client.operation.duration`| Histogram `s` measuring publish execution latency |
| **Metric** | `messaging.process.duration` | Histogram `s` measuring subscriber message processing latency |
| **Metric** | `nats.server.*` | Active connections, memory, CPU, bytes in/out, slow consumers |
| **Metric** | `nats.jetstream.*` | JetStream streams, consumers, messages, and storage bytes |
| **Log** | Log Record Model | SeverityNumber, SeverityText, Timestamp, ObservedTimestamp |
| **Log** | Event Ingestion | Server connect/disconnect events and JetStream advisories (`$SYS.>`, `$JS.EVENT.ADVISORY.>`) |

---

## Building & Verification

### Build Binaries
```bash
# Standard Go build
make build

# Build via official OpenTelemetry Collector Builder (OCB) container
make build-ocb
```

### Run Unit Tests
```bash
make test
# Or with race detector:
make test-race
```

### Run Multi-Container Integration Tests
Validates the entire pipeline against live NATS and JetStream in Docker:
```bash
make test-containers
```

---

## LGTM Observability Stack & Ping-Pong Demo

Launch the full **Loki, Grafana, Tempo, and Prometheus** local demo environment paired with live Ping-Pong microservices with a single command:

```bash
make lgtm-up
```

- **Grafana UI**: [http://localhost:3000](http://localhost:3000) (Login: `admin` / `admin`)
- **Dashboards Included**:
  1. **NATS OpenTelemetry: Broker & Messaging Overview**: Live connections, CPU/memory, JetStream capacity, Semantic Conventions message rates, and NATS `$SYS` advisory logs.
  2. **Ping-Pong: Distributed Tracing & Flow**: Queue group horizontal load balancing, roundtrip latency heatmaps, and correlated Tempo distributed traces.
- **Stop Stack**:
  ```bash
  make lgtm-down
  ```

See [examples/lgtm/README.md](examples/lgtm/README.md) for full details, architecture, and Grafana Scenes React SDK code.

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.

