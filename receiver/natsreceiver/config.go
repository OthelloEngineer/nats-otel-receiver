package natsreceiver

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
)

// AuthConfig defines authentication parameters for NATS connections.
type AuthConfig struct {
	Username    string              `mapstructure:"username"`
	Password    configopaque.String `mapstructure:"password"`
	Token       configopaque.String `mapstructure:"token"`
	NKey        configopaque.String `mapstructure:"nkey"`
	Credentials string              `mapstructure:"credentials_file"`
}

// Config defines configuration for the NATS receiver.
type Config struct {
	Endpoint       string                  `mapstructure:"endpoint"`
	Auth           AuthConfig              `mapstructure:"auth"`
	TLS            configtls.ClientConfig  `mapstructure:"tls"`
	QueueGroup     string                  `mapstructure:"queue_group"`
	MonitorURL     string                  `mapstructure:"monitor_url"`
	Monitor        confighttp.ClientConfig `mapstructure:"monitor"`
	ScrapeInterval time.Duration           `mapstructure:"scrape_interval"`
	TappedSubjects []string                `mapstructure:"tapped_subjects"`
}

// Validate checks if the receiver configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.Endpoint == "" {
		return errors.New("endpoint must not be empty")
	}
	if cfg.ScrapeInterval < 0 {
		return errors.New("scrape_interval must be non-negative")
	}
	if err := cfg.TLS.Config.Validate(); err != nil {
		return err
	}
	if cfg.Monitor.Endpoint != "" {
		if err := cfg.Monitor.Validate(); err != nil {
			return err
		}
	}
	return nil
}
