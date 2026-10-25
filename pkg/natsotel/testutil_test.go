package natsotel_test

import (
	"net"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// startMockNATSServer starts an in-process TCP listener performing a minimal NATS handshake.
func startMockNATSServer(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock NATS listener: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				// Send NATS server INFO
				info := "INFO {\"server_id\":\"test-srv\",\"server_name\":\"test\",\"version\":\"2.10.0\",\"proto\":1,\"headers\":true,\"max_payload\":1048576}\r\n"
				if _, err := c.Write([]byte(info)); err != nil {
					return
				}

				buf := make([]byte, 2048)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					data := string(buf[:n])
					if strings.Contains(data, "PING") {
						if _, err := c.Write([]byte("PONG\r\n")); err != nil {
							return
						}
					}
				}
			}(conn)
		}
	}()

	url := "nats://" + ln.Addr().String()
	cleanup := func() {
		_ = ln.Close()
		<-done
	}
	return url, cleanup
}

func setupTestOTel(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return sr
}
