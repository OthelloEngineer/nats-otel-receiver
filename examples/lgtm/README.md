# NATS OpenTelemetry LGTM Demo Stack & Ping-Pong Sample Apps

This directory contains a complete local **LGTM observability stack** (Loki, Grafana, Tempo, Prometheus) paired with a live **Ping-Pong distributed microservices application** to demonstrate end-to-end NATS OpenTelemetry instrumentation.

---

## Architecture Overview

```
                               ┌────────────────────────────────────────────────────────┐
                               │                 Ping-Pong Sample App                   │
                               │                                                        │
                               │   ┌───────────────┐               ┌────────────────┐   │
                               │   │  Ping Client  │               │  Pong Workers  │   │
                               │   │ (Producer +   │               │ (Queue Group:  │   │
                               │   │  Consumer)    │               │  pong-workers) │   │
                               │   └───┬───────▲───┘               └───▲────────┬───┘   │
                               └───────┼───────┼───────────────────────┼────────┼───────┘
                     W3C TraceContext  │       │   W3C TraceContext    │        │
                   (ping.requests)     │       │     (ping.replies)    │        │
                                       ▼       │                       │        ▼
┌──────────────────────────────────────────────┴───────────────────────┴────────────────┐
│                                NATS Message Broker (Core + JetStream)                 │
│   • Subjects: ping.requests, ping.replies                                             │
│   • Advisories & Events: $SYS.>, $JS.EVENT.ADVISORY.>                                 │
│   • HTTP Monitoring: /varz, /jsz                                                      │
└──────────────────────────────────────┬────────────────────────────────────────────────┘
                                       │
                                       ▼
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                        NATS OpenTelemetry Receiver / Bridge                           │
│   • Scrapes broker metrics (/varz, /jsz)                                              │
│   • Ingests system advisory logs ($SYS)                                               │
│   • Taps messaging subjects (ping.>, orders.>)                                        │
│   • Exports all telemetry via standard OTLP gRPC                                      │
└──────────────────────────────────────┬────────────────────────────────────────────────┘
                                       │ OTLP gRPC (Traces, Metrics, Logs)
                                       ▼
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                           OpenTelemetry Collector (Routing)                           │
│         ├── Traces (OTLP)     ──►  Tempo      (Port 3200)                             │
│         ├── Metrics (Scrape)  ──►  Prometheus (Port 9090)                             │
│         └── Logs (OTLP HTTP)  ──►  Loki       (Port 3100)                             │
└──────────────────────────────────────┬────────────────────────────────────────────────┘
                                       │
                                       ▼
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                               Grafana (Port 3005)                                     │
│   • Pre-configured Data Sources: Prometheus, Tempo, Loki                              │
│   • 1-Click Trace-to-Logs & Trace-to-Metrics Correlation                              │
│   • Pre-provisioned Dashboards:                                                       │
│       1. NATS OpenTelemetry: Broker & Messaging Overview                             │
│       2. Ping-Pong: Distributed Tracing & Flow                                        │
│   • Modern React Dashboard code using @grafana/scenes SDK                             │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Quickstart: Running the LGTM Stack

### 1. Start the Stack

From the root of the repository, execute:

```bash
make lgtm-up
```

This compiles the Linux binaries for the Ping-Pong apps and NATS receiver, builds the Docker containers, and launches all 10 services.

### 2. Access the UIs

| Component | URL | Credentials / Notes |
| :--- | :--- | :--- |
| **Grafana** | [http://localhost:3000](http://localhost:3000) | User: `admin`, Password: `admin` |
| **Prometheus** | [http://localhost:9090](http://localhost:9090) | PromQL metrics explorer |
| **Tempo API** | [http://localhost:3200](http://localhost:3200) | Distributed trace search API |
| **Loki API** | [http://localhost:3100](http://localhost:3100) | Log aggregation API |
| **NATS Broker** | `nats://localhost:4222` | Core & JetStream messaging |
| **NATS Monitor**| [http://localhost:8222](http://localhost:8222) | Native `/varz` and `/jsz` JSON endpoints |

### 3. Stop the Stack

To stop all services and remove volumes:

```bash
make lgtm-down
```

---

## Pre-Provisioned Dashboards

Navigate to **Dashboards** -> **NATS & Messaging** in Grafana:

### Dashboard 1: NATS OpenTelemetry: Broker & Messaging Overview
- **Broker Health**: Active client connections, server memory, CPU utilization, slow consumers count.
- **OTel Semantic Conventions**: Sent vs. consumed message rates broken down by destination subject (`messaging.destination.name`).
- **Operation Durations**: 95th percentile latency graphs for publish execution and subscriber processing durations.
- **JetStream Storage**: Active streams, durable consumers, stored message counts, and storage bytes.
- **Live Advisories**: Streaming Loki log panel capturing NATS server connect/disconnect advisories and JetStream events in real time.

### Dashboard 2: Ping-Pong: Distributed Tracing & Flow
- **Traffic Overview**: Ping request rate and Pong reply rate.
- **Horizontal Load Balancing**: Live stacked bar charts showing requests distributed across `pong-worker-1` and `pong-worker-2` via NATS Queue Groups.
- **End-to-End Latency**: Median (p50), p95, and p99 round-trip latency quantiles.
- **Live Application Logs**: Correlated application logs with 1-click links into Tempo distributed traces.

---

## Distributed Tracing with Tempo

In Grafana, go to **Explore** -> Select **Tempo**:

1. Click **Search** to see recent traces for `ping-service` or `pong-service`.
2. Inspect any trace to view the 8-span distributed transaction:
   - `Ping Transaction` (Client root span)
   - `ping.requests publish` (Producer span with W3C header injection)
   - `ping.requests receive` (Tapped broker span recorded by `natsreceiver`)
   - `ping.requests process` (Consumer span on queue group `pong-workers`)
   - `process_pong` (Internal child span in worker)
   - `ping.replies publish` (Producer span for reply)
   - `ping.replies receive` (Tapped broker span)
   - `ping.replies process` (Consumer span on reply client)

---

## Grafana Scenes React SDK (`scenes/NatsScenesDashboard.tsx`)

For modern frontend applications and Grafana App Plugins, [`scenes/NatsScenesDashboard.tsx`](scenes/NatsScenesDashboard.tsx) demonstrates how to define the dashboard as code using Grafana's `@grafana/scenes` SDK:

```tsx
import {
  EmbeddedScene,
  SceneFlexLayout,
  PanelBuilders,
  SceneQueryRunner,
} from '@grafana/scenes';

export function buildNatsScenesDashboard(): EmbeddedScene {
  return new EmbeddedScene({
    body: new SceneFlexLayout({
      children: [
        PanelBuilders.timeseries()
          .setTitle('Messaging Traffic')
          .setData(new SceneQueryRunner({
            datasource: { uid: 'prometheus' },
            queries: [{ expr: 'sum by (messaging_destination_name) (rate(nats_messaging_client_sent_messages_total[1m]))' }],
          }))
          .build(),
      ],
    }),
  });
}
```
