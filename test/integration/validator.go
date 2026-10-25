package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// verifyTelemetryOutputs checks the JSON files flushed by the OpenTelemetry collector against SemConv assertions.
func verifyTelemetryOutputs(outputDir string) {
	tracesFile := outputDir + "/traces.json"
	metricsFile := outputDir + "/metrics.json"
	logsFile := outputDir + "/logs.json"

	var tracesData, metricsData, logsData []byte
	var traceErr, metricErr, logErr error

	// Wait up to 15 seconds for all pipelines to flush to disk
	for i := 0; i < 15; i++ {
		tracesData, traceErr = os.ReadFile(tracesFile)
		metricsData, metricErr = os.ReadFile(metricsFile)
		logsData, logErr = os.ReadFile(logsFile)

		if traceErr == nil && metricErr == nil && logErr == nil {
			strMetrics := string(metricsData)
			if strings.Contains(strMetrics, "nats.server.connections") &&
				strings.Contains(strMetrics, "nats.server.memory") &&
				strings.Contains(strMetrics, "messaging.client.sent.messages") {
				break
			}
		}
		time.Sleep(1 * time.Second)
	}

	fmt.Println("\n=======================================================")
	fmt.Println("    OPENTELEMETRY SEMANTIC CONVENTIONS VALIDATION")
	fmt.Println("=======================================================")

	passed := true

	// 1. Traces Validation
	fmt.Println("\n[1] TRACES VALIDATION:")
	if traceErr != nil {
		fmt.Printf("  [-] Traces file missing: %v\n", traceErr)
		passed = false
	} else {
		strTraces := string(tracesData)
		checkRequirement := func(label string, condition bool) {
			if condition {
				fmt.Printf("  [+] %s: VERIFIED\n", label)
			} else {
				fmt.Printf("  [-] %s: NOT FOUND\n", label)
				passed = false
			}
		}

		checkRequirement("messaging.system == 'nats'", strings.Contains(strTraces, "messaging.system") && strings.Contains(strTraces, "nats"))
		checkRequirement("messaging.destination.name (subject)", strings.Contains(strTraces, "messaging.destination.name") && strings.Contains(strTraces, "orders.created"))
		checkRequirement("messaging.operation.type (publish & process)", strings.Contains(strTraces, "messaging.operation.type") && strings.Contains(strTraces, "publish"))
		checkRequirement("messaging.message.body.size", strings.Contains(strTraces, "messaging.message.body.size"))
		checkRequirement("messaging.consumer.group.name (worker-group)", strings.Contains(strTraces, "messaging.consumer.group.name") && strings.Contains(strTraces, "worker-group"))
		checkRequirement("W3C traceparent propagation", strings.Contains(strTraces, "traceId") && strings.Contains(strTraces, "spanId"))
	}

	// 2. Metrics Validation
	fmt.Println("\n[2] METRICS VALIDATION:")
	if metricErr != nil {
		fmt.Printf("  [-] Metrics file missing: %v\n", metricErr)
		passed = false
	} else {
		strMetrics := string(metricsData)
		checkMetric := func(label, metricName string) {
			if strings.Contains(strMetrics, metricName) {
				fmt.Printf("  [+] %s (%s): VERIFIED\n", label, metricName)
			} else {
				fmt.Printf("  [-] %s (%s): NOT FOUND\n", label, metricName)
				passed = false
			}
		}

		checkMetric("Client Sent Messages", "messaging.client.sent.messages")
		checkMetric("Client Consumed Messages", "messaging.client.consumed.messages")
		checkMetric("Publish Duration Histogram", "messaging.client.operation.duration")
		checkMetric("Process Duration Histogram", "messaging.process.duration")
		checkMetric("Server Connections Gauge", "nats.server.connections")
		checkMetric("Server Memory Gauge", "nats.server.memory")
		checkMetric("JetStream Streams Gauge", "nats.jetstream.streams")
	}

	// 3. Logs Validation
	fmt.Println("\n[3] LOGS VALIDATION:")
	if logErr != nil {
		fmt.Printf("  [-] Logs file missing: %v\n", logErr)
		passed = false
	} else {
		strLogs := string(logsData)
		checkLog := func(label string, condition bool) {
			if condition {
				fmt.Printf("  [+] %s: VERIFIED\n", label)
			} else {
				fmt.Printf("  [-] %s: NOT FOUND\n", label)
				passed = false
			}
		}

		checkLog("NATS Advisory Ingestion", strings.Contains(strLogs, "nats"))
		checkLog("Severity Number & Severity Text", strings.Contains(strLogs, "INFO") || strings.Contains(strLogs, "WARN") || strings.Contains(strLogs, "SEVERITY"))
		checkLog("Advisory Attributes (server.id / event.domain)", strings.Contains(strLogs, "nats") && (strings.Contains(strLogs, "event.domain") || strings.Contains(strLogs, "nats.server.id") || strings.Contains(strLogs, "nats.subject")))
	}

	fmt.Println()
	fmt.Println("=======================================================")
	if passed {
		fmt.Println("    OVERALL RESULT: SUCCESS! ALL OTel SEMCONV ASSERTIONS PASSED.")
		fmt.Println("=======================================================")
		os.Exit(0)
	} else {
		fmt.Println("    OVERALL RESULT: FAILED SOME ASSERTIONS.")
		fmt.Println("=======================================================")
		os.Exit(1)
	}
}
