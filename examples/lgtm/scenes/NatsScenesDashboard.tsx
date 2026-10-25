import React from 'react';
import {
  EmbeddedScene,
  SceneFlexLayout,
  SceneFlexItem,
  PanelBuilders,
  SceneTimePicker,
  SceneTimeRange,
  SceneVariableSet,
  TextBoxVariable,
  SceneRefreshPicker,
  SceneQueryRunner,
} from '@grafana/scenes';

/**
 * NatsScenesDashboard is a modern Grafana Scenes React component demonstrating
 * how to build an interactive, dynamic dashboard for NATS OpenTelemetry using the
 * latest Grafana Scenes SDK (@grafana/scenes).
 *
 * This component can be embedded into:
 * 1. A custom Grafana App Plugin (pages/NatsDashboardPage.tsx)
 * 2. A standalone React portal or internal developer platform using Grafana Scenes.
 */
export function buildNatsScenesDashboard(): EmbeddedScene {
  // Define dynamic variables
  const variables = new SceneVariableSet({
    variables: [
      new TextBoxVariable({
        name: 'subjectFilter',
        label: 'Subject Filter',
        value: '.*',
      }),
    ],
  });

  // Time range with 5s auto-refresh
  const timeRange = new SceneTimeRange({
    from: 'now-15m',
    to: 'now',
  });

  return new EmbeddedScene({
    $timeRange: timeRange,
    $variables: variables,
    body: new SceneFlexLayout({
      direction: 'column',
      children: [
        // Top Toolbar: Time range & Refresh controls
        new SceneFlexLayout({
          direction: 'row',
          children: [
            new SceneFlexItem({
              body: new SceneTimePicker({}),
            }),
            new SceneFlexItem({
              body: new SceneRefreshPicker({
                intervals: ['2s', '5s', '10s', '30s'],
                autoEnabled: true,
              }),
            }),
          ],
        }),

        // Row 1: Executive KPI Stats
        new SceneFlexLayout({
          direction: 'row',
          children: [
            new SceneFlexItem({
              width: '25%',
              height: 120,
              body: PanelBuilders.stat()
                .setTitle('Active Client Connections')
                .setData(
                  new SceneQueryRunner({
                    datasource: { uid: 'prometheus' },
                    queries: [
                      {
                        refId: 'A',
                        expr: 'nats_server_connections or vector(0)',
                      },
                    ],
                  })
                )
                .build(),
            }),
            new SceneFlexItem({
              width: '25%',
              height: 120,
              body: PanelBuilders.stat()
                .setTitle('Messages Sent / sec')
                .setData(
                  new SceneQueryRunner({
                    datasource: { uid: 'prometheus' },
                    queries: [
                      {
                        refId: 'A',
                        expr: 'sum(rate(nats_messaging_client_sent_messages_total[1m])) or vector(0)',
                      },
                    ],
                  })
                )
                .build(),
            }),
            new SceneFlexItem({
              width: '25%',
              height: 120,
              body: PanelBuilders.stat()
                .setTitle('Messages Consumed / sec')
                .setData(
                  new SceneQueryRunner({
                    datasource: { uid: 'prometheus' },
                    queries: [
                      {
                        refId: 'A',
                        expr: 'sum(rate(nats_messaging_client_consumed_messages_total[1m])) or vector(0)',
                      },
                    ],
                  })
                )
                .build(),
            }),
            new SceneFlexItem({
              width: '25%',
              height: 120,
              body: PanelBuilders.stat()
                .setTitle('JetStream Messages')
                .setData(
                  new SceneQueryRunner({
                    datasource: { uid: 'prometheus' },
                    queries: [
                      {
                        refId: 'A',
                        expr: 'nats_jetstream_messages or vector(0)',
                      },
                    ],
                  })
                )
                .build(),
            }),
          ],
        }),

        // Row 2: Throughput & Latency Time Series
        new SceneFlexLayout({
          direction: 'row',
          children: [
            new SceneFlexItem({
              width: '50%',
              height: 350,
              body: PanelBuilders.timeseries()
                .setTitle('Messaging Traffic (by Destination Subject)')
                .setData(
                  new SceneQueryRunner({
                    datasource: { uid: 'prometheus' },
                    queries: [
                      {
                        refId: 'A',
                        expr: 'sum by (messaging_destination_name) (rate(nats_messaging_client_sent_messages_total[1m]))',
                        legendFormat: 'Sent: {{messaging_destination_name}}',
                      },
                      {
                        refId: 'B',
                        expr: 'sum by (messaging_destination_name) (rate(nats_messaging_client_consumed_messages_total[1m]))',
                        legendFormat: 'Consumed: {{messaging_destination_name}}',
                      },
                    ],
                  })
                )
                .build(),
            }),
            new SceneFlexItem({
              width: '50%',
              height: 350,
              body: PanelBuilders.timeseries()
                .setTitle('Ping-Pong Roundtrip Latency (p95 & p99)')
                .setUnit('s')
                .setData(
                  new SceneQueryRunner({
                    datasource: { uid: 'prometheus' },
                    queries: [
                      {
                        refId: 'A',
                        expr: 'histogram_quantile(0.95, sum by (le) (rate(nats_ping_roundtrip_duration_seconds_bucket[1m])))',
                        legendFormat: 'p95 Latency',
                      },
                      {
                        refId: 'B',
                        expr: 'histogram_quantile(0.99, sum by (le) (rate(nats_ping_roundtrip_duration_seconds_bucket[1m])))',
                        legendFormat: 'p99 Latency',
                      },
                    ],
                  })
                )
                .build(),
            }),
          ],
        }),

        // Row 3: Real-time System Logs & Advisories (Loki)
        new SceneFlexLayout({
          direction: 'row',
          children: [
            new SceneFlexItem({
              width: '100%',
              height: 320,
              body: PanelBuilders.logs()
                .setTitle('Live NATS Advisory Logs (Loki)')
                .setData(
                  new SceneQueryRunner({
                    datasource: { uid: 'loki' },
                    queries: [
                      {
                        refId: 'A',
                        expr: '{job=~".+"}',
                      },
                    ],
                  })
                )
                .build(),
            }),
          ],
        }),
      ],
    }),
  });
}

/**
 * React Component rendering the NATS Scenes dashboard.
 */
export const NatsScenesDashboardComponent: React.FC = () => {
  const scene = React.useMemo(() => buildNatsScenesDashboard(), []);
  return <scene.Component model={scene} />;
};

export default NatsScenesDashboardComponent;
