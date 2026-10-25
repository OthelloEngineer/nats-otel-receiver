package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-otel/nats-otel/pkg/receiver"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	var (
		natsURL        = flag.String("nats", getEnv("NATS_URL", "nats://127.0.0.1:4222"), "NATS server URL")
		natsMonitorURL = flag.String("monitor", getEnv("NATS_MONITOR_URL", "http://127.0.0.1:8222"), "NATS HTTP monitoring endpoint")
		otlpEndpoint   = flag.String("otlp-endpoint", getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:4317"), "OTLP gRPC endpoint")
		otlpInsecure   = flag.Bool("otlp-insecure", getEnv("OTEL_EXPORTER_OTLP_INSECURE", "true") == "true", "Use insecure gRPC connection for OTLP")
		scrapeInterval = flag.Duration("interval", 5*time.Second, "Scrape interval for NATS monitoring endpoints")
		tappedSubjects = flag.String("tapped-subjects", getEnv("TAPPED_SUBJECTS", "telemetry.>,events.>,orders.>"), "Comma-separated list of subjects to tap for traces")
	)
	flag.Parse()

	slog.Info("Initializing NATS OpenTelemetry Receiver/Bridge...")

	subjects := strings.Split(*tappedSubjects, ",")
	for i := range subjects {
		subjects[i] = strings.TrimSpace(subjects[i])
	}

	cfg := receiver.Config{
		NatsURL:        *natsURL,
		NatsMonitorURL: *natsMonitorURL,
		OTLPEndpoint:   *otlpEndpoint,
		OTLPInsecure:   *otlpInsecure,
		ScrapeInterval: *scrapeInterval,
		TappedSubjects: subjects,
		ServiceName:    "nats-otel-receiver",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := receiver.NewServer(ctx, cfg)
	if err != nil {
		slog.Error("Failed to initialize server", "err", err)
		os.Exit(1)
	}

	if err := srv.Start(ctx); err != nil {
		slog.Error("Failed to start server", "err", err)
		os.Exit(1)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	slog.Info("Shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("Error during shutdown", "err", err)
	}
	slog.Info("Shutdown completed.")
}
