package config

import (
    "fmt"
    "strings"
    "time"

    "github.com/sirupsen/logrus"
    "github.com/spf13/viper"
)

// Config holds top-level application configuration groups.
type Config struct {
    Service   ServiceConfig   `mapstructure:"service"`
    Kafka     KafkaConfig     `mapstructure:"kafka"`
    Couchbase CouchbaseConfig `mapstructure:"couchbase"`
    Logging   LoggingConfig   `mapstructure:"logging"`
    Metrics   MetricsConfig   `mapstructure:"metrics"`
}

// ServiceConfig defines basic runtime context of the service.
type ServiceConfig struct {
    Name        string        `mapstructure:"name"`
    Version     string        `mapstructure:"version"`
    Environment string        `mapstructure:"environment"`
    Port        string        `mapstructure:"port"`
    Timeout     time.Duration `mapstructure:"timeout"`
}

// KafkaConfig groups settings necessary to connect to Kafka.
type KafkaConfig struct {
    Brokers        []string      `mapstructure:"brokers"`
    RawTopic       string        `mapstructure:"raw_topic"`
    ProcessedTopic string        `mapstructure:"processed_topic"`
    ErrorTopic     string        `mapstructure:"error_topic"`
    ConsumerGroup  string        `mapstructure:"consumer_group"`
    BatchSize      int           `mapstructure:"batch_size"`
    FlushTimeout   time.Duration `mapstructure:"flush_timeout"`
    RetryAttempts  int           `mapstructure:"retry_attempts"`
}

// CouchbaseConfig groups settings necessary to connect to Couchbase.
type CouchbaseConfig struct {
    ConnectionString string        `mapstructure:"connection_string"`
    Username         string        `mapstructure:"username"`
    Password         string        `mapstructure:"password"`
    Bucket           string        `mapstructure:"bucket"`
    Scope            string        `mapstructure:"scope"`
    Collection       string        `mapstructure:"collection"`
    Timeout          time.Duration `mapstructure:"timeout"`
}

// LoggingConfig controls application logging behavior.
type LoggingConfig struct {
    Level  string `mapstructure:"level"`  // "info", "debug", etc.
    Format string `mapstructure:"format"` // "json" or "text"
}

// MetricsConfig defines settings for metrics exposure.
type MetricsConfig struct {
    Enabled bool   `mapstructure:"enabled"`
    Port    string `mapstructure:"port"`
    Path    string `mapstructure:"path"`
}

// Load reads configuration from files and environment variables.
func Load() (*Config, error) {
    setDefaults()

    viper.SetEnvPrefix("DOC_INDEXER")
    viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
    viper.AutomaticEnv()

    viper.SetConfigName("config")
    viper.SetConfigType("yaml")
    viper.AddConfigPath(".")
    viper.AddConfigPath("./configs")
    viper.AddConfigPath("/etc/doc-indexer")

    if err := viper.ReadInConfig(); err != nil {
        if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
            return nil, fmt.Errorf("error reading config file: %w", err)
        }
        logrus.Info("No config file found, using defaults and environment variables")
    }

    var cfg Config
    if err := viper.Unmarshal(&cfg); err != nil {
        return nil, fmt.Errorf("unable to decode config: %w", err)
    }

    if err := cfg.Validate(); err != nil {
        return nil, fmt.Errorf("invalid configuration: %w", err)
    }

    return &cfg, nil
}

// setDefaults establishes default values for configuration.
func setDefaults() {
    viper.SetDefault("service.name", "doc-indexer")
    viper.SetDefault("service.version", "1.0.0")
    viper.SetDefault("service.environment", "development")
    viper.SetDefault("service.port", "8080")
    viper.SetDefault("service.timeout", "30s")

    viper.SetDefault("kafka.brokers", []string{"localhost:9092"})
    viper.SetDefault("kafka.raw_topic", "documents.raw")
    viper.SetDefault("kafka.processed_topic", "documents.processed")
    viper.SetDefault("kafka.error_topic", "documents.errors")
    viper.SetDefault("kafka.consumer_group", "doc-enricher")
    viper.SetDefault("kafka.batch_size", 100)
    viper.SetDefault("kafka.flush_timeout", "5s")
    viper.SetDefault("kafka.retry_attempts", 3)

    viper.SetDefault("couchbase.connection_string", "couchbase://localhost")
    viper.SetDefault("couchbase.username", "Administrator")
    viper.SetDefault("couchbase.password", "password")
    viper.SetDefault("couchbase.bucket", "indexed_docs")
    viper.SetDefault("couchbase.scope", "_default")
    viper.SetDefault("couchbase.collection", "_default")
    viper.SetDefault("couchbase.timeout", "10s")

    viper.SetDefault("logging.level", "info")
    viper.SetDefault("logging.format", "json")

    viper.SetDefault("metrics.enabled", true)
    viper.SetDefault("metrics.port", "9090")
    viper.SetDefault("metrics.path", "/metrics")
}

// Validate ensures critical configuration values are present.
func (c *Config) Validate() error {
    if len(c.Kafka.Brokers) == 0 {
        return fmt.Errorf("kafka brokers cannot be empty")
    }
    if c.Kafka.RawTopic == "" {
        return fmt.Errorf("kafka raw topic cannot be empty")
    }
    if c.Couchbase.ConnectionString == "" {
        return fmt.Errorf("couchbase connection string cannot be empty")
    }
    return nil
}

// IsDevelopment returns true when running in development mode.
func (c *Config) IsDevelopment() bool {
    return strings.EqualFold(c.Service.Environment, "development")
}

// IsProduction returns true when running in production mode.
func (c *Config) IsProduction() bool {
    return strings.EqualFold(c.Service.Environment, "production")
}
