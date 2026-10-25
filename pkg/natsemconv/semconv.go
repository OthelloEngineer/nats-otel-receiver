package natsemconv

import (
	"net"
	"net/url"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// OpenTelemetry Semantic Conventions for Messaging Systems (v1.26+)
const (
	// Messaging System name
	MessagingSystemKey  = "messaging.system"
	MessagingSystemNATS = "nats"

	// Operation attributes
	MessagingOperationTypeKey = "messaging.operation.type"
	MessagingOperationNameKey = "messaging.operation.name"

	// Standard operation types
	OpTypePublish = "publish"
	OpTypeReceive = "receive"
	OpTypeProcess = "process"
	OpTypeSettle  = "settle"

	// Standard operation names
	OpNamePublish = "publish"
	OpNameReceive = "receive"
	OpNameProcess = "process"
	OpNameDeliver = "deliver"
	OpNameAck     = "ack"
	OpNameNack    = "nack"

	// Destination attributes
	MessagingDestinationNameKey      = "messaging.destination.name"
	MessagingDestinationAnonymousKey = "messaging.destination.anonymous"
	MessagingDestinationTemplateKey  = "messaging.destination.template"

	// Message attributes
	MessagingMessageIDKey           = "messaging.message.id"
	MessagingMessageBodySizeKey     = "messaging.message.body.size"
	MessagingMessageEnvelopeSizeKey = "messaging.message.envelope.size"
	MessagingConsumerGroupNameKey   = "messaging.consumer.group.name"

	// Network / Server attributes
	ServerAddressKey = "server.address"
	ServerPortKey    = "server.port"

	// Standard Metric Names
	MetricMessagingClientSentMessages     = "messaging.client.sent.messages"
	MetricMessagingClientConsumedMessages = "messaging.client.consumed.messages"
	MetricMessagingClientOpDuration       = "messaging.client.operation.duration"
	MetricMessagingProcessDuration        = "messaging.process.duration"

	// NATS Server Broker Metrics
	MetricNatsServerConnections   = "nats.server.connections"
	MetricNatsServerInMsgs        = "nats.server.in_msgs"
	MetricNatsServerOutMsgs       = "nats.server.out_msgs"
	MetricNatsServerInBytes       = "nats.server.in_bytes"
	MetricNatsServerOutBytes      = "nats.server.out_bytes"
	MetricNatsServerSlowConsumers = "nats.server.slow_consumers"
	MetricNatsJetStreamStreams    = "nats.jetstream.streams"
	MetricNatsJetStreamConsumers  = "nats.jetstream.consumers"
	MetricNatsJetStreamMessages   = "nats.jetstream.messages"
	MetricNatsJetStreamBytes      = "nats.jetstream.bytes"

	// NATS Log / Advisory Event Attributes
	NatsEventTypeKey = "nats.event.type"
	NatsServerIDKey  = "nats.server.id"
	NatsClientIDKey  = "nats.client.id"
)

// SpanName formats standard span name: "{destination} {operation}"
func SpanName(destination, operation string) string {
	if destination == "" {
		return operation
	}
	return destination + " " + operation
}

// IsAnonymousDestination checks if a NATS subject is an ephemeral inbox
func IsAnonymousDestination(subject string) bool {
	return strings.HasPrefix(subject, "_INBOX.")
}

// DestinationAttributes returns standard destination attributes
func DestinationAttributes(subject string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(MessagingSystemKey, MessagingSystemNATS),
	}
	if IsAnonymousDestination(subject) {
		attrs = append(attrs, attribute.Bool(MessagingDestinationAnonymousKey, true))
	} else if subject != "" {
		attrs = append(attrs, attribute.String(MessagingDestinationNameKey, subject))
	}
	return attrs
}

// BaseMessagingAttributes creates base attributes common to all NATS messaging spans
func BaseMessagingAttributes(system, opType, opName, destination string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String(MessagingSystemKey, system),
		attribute.String(MessagingOperationTypeKey, opType),
		attribute.String(MessagingOperationNameKey, opName),
	}
	if IsAnonymousDestination(destination) {
		attrs = append(attrs, attribute.Bool(MessagingDestinationAnonymousKey, true))
	} else if destination != "" {
		attrs = append(attrs, attribute.String(MessagingDestinationNameKey, destination))
	}
	return attrs
}

// CalculateHeaderBytes calculates size of NATS headers in bytes
func CalculateHeaderBytes(headers map[string][]string) int {
	if headers == nil {
		return 0
	}
	size := 0
	for k, vals := range headers {
		for _, v := range vals {
			size += len(k) + len(v) + 4 // ": " and "\r\n"
		}
	}
	return size
}

// ServerPeerAttributes returns server address and port attributes conforming to OpenTelemetry conventions.
func ServerPeerAttributes(serverURL string) []attribute.KeyValue {
	if serverURL == "" {
		return nil
	}

	raw := serverURL
	if !strings.Contains(raw, "://") {
		raw = "nats://" + raw
	}

	u, err := url.Parse(raw)
	if err == nil && u.Host != "" {
		host, portStr, err := net.SplitHostPort(u.Host)
		if err == nil {
			if port, err := strconv.Atoi(portStr); err == nil {
				return []attribute.KeyValue{
					attribute.String(ServerAddressKey, host),
					semconv.ServerPort(port),
				}
			}
			return []attribute.KeyValue{
				attribute.String(ServerAddressKey, host),
			}
		}
		return []attribute.KeyValue{
			attribute.String(ServerAddressKey, u.Host),
		}
	}

	return []attribute.KeyValue{
		attribute.String(ServerAddressKey, serverURL),
	}
}
