package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"distributed-document-ingest-enrich-system/internal/config"
	"distributed-document-ingest-enrich-system/internal/couchbase"
	"distributed-document-ingest-enrich-system/internal/kafka"
	"distributed-document-ingest-enrich-system/internal/models"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

// IndexerService consumes enriched docs from Kafka and writes them to Couchbase.
type IndexerService struct {
	config     *config.Config
	consumer   kafka.Consumer
	repository *couchbase.DocumentRepository
	logger     *logrus.Logger
	metrics    *IndexerMetrics

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// IndexerMetrics tracks throughput and latency (EMA) for observability.
type IndexerMetrics struct {
	IndexedCount     int64     `json:"indexed_count"`
	ErrorCount       int64     `json:"error_count"`
	StartTime        time.Time `json:"start_time"`
	LastIndexedAt    time.Time `json:"last_indexed_at"`
	AverageIndexTime float64   `json:"average_index_time_ms"`
	mu               sync.RWMutex
}

func main() {
	// (No CLI flags currently, but keep flag.Parse for future options.)
	flag.Parse()

	// Load configuration (defaults + env + optional file).
	cfg, err := config.Load()
	if err != nil {
		logrus.Fatalf("Failed to load config: %v", err)
	}

	// Structured logging.
	logger := setupLogging(cfg)
	logger.Info("Starting Document Indexer Service")

	// Build the service.
	service, err := NewIndexerService(cfg, logger)
	if err != nil {
		logger.Fatalf("Failed to create indexer service: %v", err)
	}

	// Root context + graceful shutdown wiring.
	ctx, cancel := context.WithCancel(context.Background())
	service.ctx = ctx
	service.cancel = cancel

	// OS signals → cancel context → graceful stop.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("Shutdown signal received, starting graceful shutdown...")
		cancel()
	}()

	// Run until the consumer stops (ctx cancelled or fatal error).
	if err := service.Start(); err != nil {
		logger.Fatalf("Service failed: %v", err)
	}

	logger.Info("Document Indexer Service stopped")
}

// NewIndexerService wires Couchbase + Kafka consumer + repository.
func NewIndexerService(cfg *config.Config, logger *logrus.Logger) (*IndexerService, error) {
	// Couchbase client (waits until cluster/bucket ready).
	cbConfig := couchbase.Config{
		ConnectionString: cfg.Couchbase.ConnectionString,
		Username:         cfg.Couchbase.Username,
		Password:         cfg.Couchbase.Password,
		BucketName:       cfg.Couchbase.Bucket,
		ScopeName:        cfg.Couchbase.Scope,
		CollectionName:   cfg.Couchbase.Collection,
		ConnectTimeout:   cfg.Couchbase.Timeout,
		OperationTimeout: cfg.Couchbase.Timeout,
	}
	cbClient, err := couchbase.NewClient(cbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Couchbase client: %w", err)
	}

	// Best-effort index creation (safe to ignore if already present).
	if err := cbClient.CreateIndexes(); err != nil {
		logger.WithError(err).Warn("Failed to create some indexes")
	}

	repo := couchbase.NewDocumentRepository(cbClient)

	svc := &IndexerService{
		config:     cfg,
		repository: repo,
		logger:     logger,
		metrics: &IndexerMetrics{
			StartTime: time.Now(),
		},
	}

	// Message handler closure.
	handler := svc.createMessageHandler()

	// Kafka consumer (own group for indexer).
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:       cfg.Kafka.Brokers,
		GroupID:       cfg.Kafka.ConsumerGroup + "-indexer",
		RetryAttempts: cfg.Kafka.RetryAttempts,
	}, handler)
	if err != nil {
		_ = cbClient.Close()
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	svc.consumer = consumer
	return svc, nil
}

// Start runs the consume loop and blocks until shutdown, then cleans up.
func (s *IndexerService) Start() error {
	s.logger.WithFields(logrus.Fields{
		"consumer_group":  s.config.Kafka.ConsumerGroup + "-indexer",
		"processed_topic": s.config.Kafka.ProcessedTopic,
		"bucket":          s.config.Couchbase.Bucket,
	}).Info("Starting indexer service")

	// Periodic metrics log.
	s.startMetricsReporting()

	// Consume until context cancellation.
	topics := []string{s.config.Kafka.ProcessedTopic}
	if err := s.consumer.Start(s.ctx, topics); err != nil {
		s.logger.WithError(err).Error("Consumer failed")
		return err
	}

	// Wait for background goroutines to exit, then cleanup.
	s.wg.Wait()
	s.cleanup()
	return nil
}

// createMessageHandler parses, validates, and stores messages to Couchbase.
func (s *IndexerService) createMessageHandler() kafka.MessageHandler {
	return func(ctx context.Context, message *sarama.ConsumerMessage) error {
		start := time.Now()

		s.logger.WithFields(logrus.Fields{
			"topic":     message.Topic,
			"partition": message.Partition,
			"offset":    message.Offset,
		}).Debug("Processing message for indexing")

		// Decode enriched document.
		var doc models.EnrichedDocument
		if err := json.Unmarshal(message.Value, &doc); err != nil {
			s.logger.WithError(err).Error("Failed to unmarshal enriched document")
			s.incrementErrorCount()
			return err
		}

		// Store in Couchbase (keyed by doc.ID).
		if err := s.repository.Store(ctx, &doc); err != nil {
			s.logger.WithError(err).WithField("document_id", doc.ID).
				Error("Failed to store document")
			s.incrementErrorCount()
			return err
		}

		// Metrics.
		s.updateMetrics(time.Since(start))

		s.logger.WithFields(logrus.Fields{
			"document_id": doc.ID,
			"type":        doc.Type,
			"index_time":  time.Since(start),
		}).Info("Document indexed successfully")

		return nil
	}
}

// --- metrics & lifecycle ---

func (s *IndexerService) updateMetrics(indexTime time.Duration) {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	s.metrics.IndexedCount++
	s.metrics.LastIndexedAt = time.Now()

	// Exponential moving average (alpha = 0.1).
	ms := float64(indexTime.Nanoseconds()) / 1e6
	if s.metrics.AverageIndexTime == 0 {
		s.metrics.AverageIndexTime = ms
	} else {
		s.metrics.AverageIndexTime = 0.9*s.metrics.AverageIndexTime + 0.1*ms
	}
}

func (s *IndexerService) incrementErrorCount() {
	s.metrics.mu.Lock()
	s.metrics.ErrorCount++
	s.metrics.mu.Unlock()
}

func (s *IndexerService) startMetricsReporting() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.logMetrics()
			case <-s.ctx.Done():
				return
			}
		}
	}()
}

func (s *IndexerService) logMetrics() {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()

	uptime := time.Since(s.metrics.StartTime)

	s.logger.WithFields(logrus.Fields{
		"indexed_count":     s.metrics.IndexedCount,
		"error_count":       s.metrics.ErrorCount,
		"uptime_seconds":    uptime.Seconds(),
		"avg_index_time_ms": s.metrics.AverageIndexTime,
		"last_indexed_at":   s.metrics.LastIndexedAt,
	}).Info("Indexer metrics")
}

func (s *IndexerService) cleanup() {
	s.logger.Info("Cleaning up indexer resources...")
	if s.consumer != nil {
		if err := s.consumer.Stop(); err != nil {
			s.logger.WithError(err).Warn("Error stopping consumer")
		}
	}
	s.logger.Info("Indexer cleanup completed")
}

// setupLogging configures Logrus according to config.
func setupLogging(cfg *config.Config) *logrus.Logger {
	logger := logrus.New()

	level, err := logrus.ParseLevel(cfg.Logging.Level)
	if err != nil {
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)

	if cfg.Logging.Format == "json" {
		logger.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: time.RFC3339,
		})
	} else {
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: time.RFC3339,
		})
	}
	return logger
}
