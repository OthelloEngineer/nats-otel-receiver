package natsreceiver

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// newNATSConn initializes a robust, production-grade NATS client connection
// handling TLS, enterprise authentication, and connection lifecycle event callbacks.
func newNATSConn(ctx context.Context, cfg *Config, clientName string, logger *zap.Logger) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(clientName),
		nats.DisconnectErrHandler(func(c *nats.Conn, err error) {
			logger.Warn("NATS connection disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			logger.Info("NATS connection reconnected", zap.String("url", c.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(c *nats.Conn) {
			logger.Info("NATS connection closed")
		}),
		nats.ErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
			subj := ""
			if s != nil {
				subj = s.Subject
			}
			logger.Error("NATS async subscription error", zap.String("subject", subj), zap.Error(err))
		}),
	}

	// Authentication options
	if cfg.Auth.Credentials != "" {
		opts = append(opts, nats.UserCredentials(cfg.Auth.Credentials))
	} else if cfg.Auth.NKey != "" {
		opts = append(opts, nats.Nkey(string(cfg.Auth.NKey), nil))
	} else if cfg.Auth.Token != "" {
		opts = append(opts, nats.Token(string(cfg.Auth.Token)))
	} else if cfg.Auth.Username != "" {
		opts = append(opts, nats.UserInfo(cfg.Auth.Username, string(cfg.Auth.Password)))
	}

	// TLS configuration (only activate when TLS endpoint or TLS settings are specified)
	if isTLSEnabled(cfg) {
		tlsConfig, err := cfg.TLS.LoadTLSConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS configuration: %w", err)
		}
		if tlsConfig != nil {
			opts = append(opts, nats.Secure(tlsConfig))
		}
	}

	return nats.Connect(cfg.Endpoint, opts...)
}

func isTLSEnabled(cfg *Config) bool {
	if strings.HasPrefix(cfg.Endpoint, "tls://") {
		return true
	}
	if cfg.TLS.InsecureSkipVerify || cfg.TLS.ServerName != "" {
		return true
	}
	if cfg.TLS.CAFile != "" || cfg.TLS.CAPem != "" ||
		cfg.TLS.CertFile != "" || cfg.TLS.CertPem != "" ||
		cfg.TLS.KeyFile != "" || cfg.TLS.KeyPem != "" {
		return true
	}
	return false
}
