package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"

	"distributed-document-ingest-enrich-system/internal/config"
	"distributed-document-ingest-enrich-system/internal/enrich"
	"distributed-document-ingest-enrich-system/internal/kafka"
	"distributed-document-ingest-enrich-system/internal/models"
)

// EnricherService wires together the Kafka consumer/producer and the NLP Enricher.
type EnricherService struct {
	config   *config.Config
	enricher *enrich.Enricher
	consumer kafka.Consumer
	producer kafka.Producer
	logger   *logrus.Logger
	metrics  *ServiceMetrics

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// ServiceMetrics holds rolling counters/aggregates for observability.
type ServiceMetrics struct {
	ProcessedCount        int64            `json:"processed_count"`
	ErrorCount            int64            `json:"error_count"`
	StartTime             time.Time        `json:"start_time"`
	LastProcessedAt       time.Time        `json:"last_processed_at"`
	AverageProcessingTime float64          `json:"average_processing_time_ms"`
	TotalProcessingTime   time.Duration    `json:"total_processing_time"`
	DocumentsByType       map[string]int64 `json:"documents_by_type"`
	mu                    sync.RWMutex
}

func main() {
	var (
		logLevel   = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		healthPort = flag.String("health-port", "9090", "Health check port")
	)
	flag.Parse()

	// Load configuration (env/config file/defaults).
	cfg, err := config.Load()
	if err != nil {
		logrus.Fatalf("Failed to load config: %v", err)
	}

	// Optional override via CLI.
	if *logLevel != "" {
		cfg.Logging.Level = *logLevel
	}

	// Structured logging.
	logger := setupLogging(cfg)
	logger.WithFields(logrus.Fields{
		"service": "enricher",
		"version": "1.0.0",
	}).Info("Starting Document Enricher Service")

	// Build the service.
	service, err := NewEnricherService(cfg, logger)
	if err != nil {
		logger.Fatalf("Failed to create enricher service: %v", err)
	}

	// Root context + graceful shutdown wiring.
	ctx, cancel := context.WithCancel(context.Background())
	service.ctx = ctx
	service.cancel = cancel

	// Health endpoints.
	healthChecker := NewHealthChecker(service, logger)
	go healthChecker.StartHealthServer(ctx, *healthPort)

	// OS signals → cancel context → graceful stop.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.WithField("signal", sig.String()).Info("Shutdown signal received, starting graceful shutdown...")
		cancel()
	}()

	// Run until the consumer stops (ctx cancelled or fatal error).
	if err := service.Start(); err != nil {
		logger.Fatalf("Service failed: %v", err)
	}
	logger.Info("Document Enricher Service stopped gracefully")
}

// NewEnricherService constructs the enricher, producer, and consumer.
func NewEnricherService(cfg *config.Config, logger *logrus.Logger) (*EnricherService, error) {
	// Enricher configuration (could be externalized later).
	enricher := enrich.NewEnricher(enrich.Config{
		MaxKeywords:     10,
		MaxSummaryLines: 3,
		EnableSummary:   true,
		EnableSentiment: true,
		EnableEntities:  true,
	})

	// Kafka producer (processed + errors topics).
	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:       cfg.Kafka.Brokers,
		RetryAttempts: cfg.Kafka.RetryAttempts,
		FlushTimeout:  cfg.Kafka.FlushTimeout,
		BatchSize:     cfg.Kafka.BatchSize,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	svc := &EnricherService{
		config:   cfg,
		enricher: enricher,
		producer: producer,
		logger:   logger,
		metrics: &ServiceMetrics{
			StartTime:       time.Now(),
			DocumentsByType: make(map[string]int64),
		},
	}

	// Message handler closure.
	handler := svc.createMessageHandler()

	// Kafka consumer (group + retries).
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:       cfg.Kafka.Brokers,
		GroupID:       cfg.Kafka.ConsumerGroup,
		RetryAttempts: cfg.Kafka.RetryAttempts,
	}, handler)
	if err != nil {
		_ = producer.Close()
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	svc.consumer = consumer
	return svc, nil
}

// Start runs the consume loop and blocks until shutdown, then cleans up.
func (s *EnricherService) Start() error {
	s.logger.WithFields(logrus.Fields{
		"consumer_group":  s.config.Kafka.ConsumerGroup,
		"raw_topic":       s.config.Kafka.RawTopic,
		"processed_topic": s.config.Kafka.ProcessedTopic,
		"error_topic":     s.config.Kafka.ErrorTopic,
	}).Info("Starting enricher service")

	// Periodic metrics log.
	s.startMetricsReporting()

	// Consume until context cancellation.
	topics := []string{s.config.Kafka.RawTopic}
	if err := s.consumer.Start(s.ctx, topics); err != nil {
		s.logger.WithError(err).Error("Consumer failed")
		return err
	}

	// Wait for background goroutines to exit, then cleanup.
	s.wg.Wait()
	s.cleanup()
	return nil
}

// createMessageHandler parses, enriches, and routes messages.
func (s *EnricherService) createMessageHandler() kafka.MessageHandler {
	return func(ctx context.Context, message *sarama.ConsumerMessage) error {
		start := time.Now()

		s.logger.WithFields(logrus.Fields{
			"topic":     message.Topic,
			"partition": message.Partition,
			"offset":    message.Offset,
			"key":       string(message.Key),
		}).Debug("Processing message")

		// Decode raw document.
		var raw models.RawDocument
		if err := json.Unmarshal(message.Value, &raw); err != nil {
			s.logger.WithError(err).WithField("message_size", len(message.Value)).
				Error("Failed to unmarshal raw document")
			s.incrementErrorCount()
			return s.sendToErrorTopic(message.Value, err, "unmarshal_error", "")
		}

		s.logger.WithFields(logrus.Fields{
			"document_id": raw.ID,
			"filename":    raw.Filename,
			"file_type":   raw.FileType,
			"source":      raw.Source,
		}).Info("Processing document")

		// Enrichment pipeline.
		enriched, err := s.enricher.EnrichDocument(&raw)
		if err != nil {
			s.logger.WithError(err).WithFields(logrus.Fields{
				"document_id": raw.ID, "filename": raw.Filename, "file_type": raw.FileType,
			}).Error("Failed to enrich document")
			s.incrementErrorCount()
			return s.sendToErrorTopic(message.Value, err, "enrichment_error", raw.ID)
		}

		// Send to processed topic.
		if err := s.producer.SendMessage(ctx, s.config.Kafka.ProcessedTopic, enriched.ID, enriched); err != nil {
			s.logger.WithError(err).WithField("document_id", enriched.ID).
				Error("Failed to send processed document")
			s.incrementErrorCount()
			// Commit offset anyway; mirror payload to error topic for manual inspection.
			_ = s.sendToErrorTopic(message.Value, err, "send_error", enriched.ID)
			return nil
		}

		// Metrics.
		s.updateMetrics(time.Since(start), enriched.Type)

		s.logger.WithFields(logrus.Fields{
			"document_id":     enriched.ID,
			"document_type":   enriched.Type,
			"keyword_count":   len(enriched.Keywords),
			"word_count":      enriched.WordCount,
			"language":        enriched.Language,
			"processing_time": time.Since(start),
		}).Info("Document processed successfully")

		return nil
	}
}

// sendToErrorTopic mirrors failures to the error topic with context for triage.
func (s *EnricherService) sendToErrorTopic(originalMessage []byte, err error, errorType, documentID string) error {
	if documentID == "" {
		documentID = "unknown"
	}

	errDoc := models.ProcessingError{
		DocumentID: documentID,
		Error:      err.Error(),
		Stage:      errorType,
		Timestamp:  time.Now(),
		Retryable:  s.isRetryableError(err),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if sendErr := s.producer.SendMessage(ctx, s.config.Kafka.ErrorTopic, errDoc.DocumentID, errDoc); sendErr != nil {
		s.logger.WithError(sendErr).Error("Failed to send error to error topic")
		return sendErr
	}

	s.logger.WithFields(logrus.Fields{
		"document_id": documentID,
		"error_type":  errorType,
		"retryable":   errDoc.Retryable,
	}).Warn("Sent error to error topic")

	return nil
}

// isRetryableError does a very rough, string-based classification.
func (s *EnricherService) isRetryableError(err error) bool {
	needle := strings.ToLower(err.Error())
	for _, sub := range []string{"connection refused", "timeout", "network", "temporary", "unavailable"} {
		if strings.Contains(needle, sub) {
			return true
		}
	}
	return false
}

// updateMetrics updates rolling counters and EMA of processing time.
func (s *EnricherService) updateMetrics(processingTime time.Duration, docType models.DocumentType) {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	s.metrics.ProcessedCount++
	s.metrics.LastProcessedAt = time.Now()
	s.metrics.TotalProcessingTime += processingTime

	// Per-type counter.
	s.metrics.DocumentsByType[string(docType)]++

	// Exponential moving average (alpha = 0.1).
	ms := float64(processingTime.Nanoseconds()) / 1e6
	if s.metrics.AverageProcessingTime == 0 {
		s.metrics.AverageProcessingTime = ms
	} else {
		s.metrics.AverageProcessingTime = 0.9*s.metrics.AverageProcessingTime + 0.1*ms
	}
}

func (s *EnricherService) incrementErrorCount() {
	s.metrics.mu.Lock()
	s.metrics.ErrorCount++
	s.metrics.mu.Unlock()
}

// startMetricsReporting logs a heartbeat snapshot on an interval.
func (s *EnricherService) startMetricsReporting() {
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
				s.logger.Info("Stopping metrics reporting")
				return
			}
		}
	}()
}

func (s *EnricherService) logMetrics() {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()

	uptime := time.Since(s.metrics.StartTime)
	var throughput float64
	if sec := uptime.Seconds(); sec > 0 {
		throughput = float64(s.metrics.ProcessedCount) / sec
	}

	s.logger.WithFields(logrus.Fields{
		"processed_count":         s.metrics.ProcessedCount,
		"error_count":             s.metrics.ErrorCount,
		"uptime_seconds":          uptime.Seconds(),
		"avg_processing_time_ms":  s.metrics.AverageProcessingTime,
		"throughput_docs_per_sec": throughput,
		"last_processed_at":       s.metrics.LastProcessedAt,
		"documents_by_type":       s.metrics.DocumentsByType,
	}).Info("Service metrics")
}

// cleanup closes client resources.
func (s *EnricherService) cleanup() {
	s.logger.Info("Cleaning up enricher resources...")

	if s.consumer != nil {
		if err := s.consumer.Stop(); err != nil {
			s.logger.WithError(err).Error("Error stopping consumer")
		}
	}
	if s.producer != nil {
		if err := s.producer.Close(); err != nil {
			s.logger.WithError(err).Error("Error closing producer")
		}
	}
	s.logger.Info("Enricher cleanup completed")
}

// setupLogging configures Logrus according to config.
func setupLogging(cfg *config.Config) *logrus.Logger {
	logger := logrus.New()

	level, err := logrus.ParseLevel(cfg.Logging.Level)
	if err != nil {
		level = logrus.InfoLevel
		logger.WithError(err).Warn("Invalid log level, using info")
	}
	logger.SetLevel(level)

	if cfg.Logging.Format == "json" {
		logger.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: time.RFC3339,
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  "timestamp",
				logrus.FieldKeyLevel: "level",
				logrus.FieldKeyMsg:   "message",
			},
		})
	} else {
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: time.RFC3339,
		})
	}
	return logger
}

// getMetrics provides a snapshot map (safe for JSON) of service metrics.
func (s *EnricherService) getMetrics() map[string]interface{} {
	s.metrics.mu.RLock()
	defer s.metrics.mu.RUnlock()

	uptime := time.Since(s.metrics.StartTime)
	var throughput float64
	if sec := uptime.Seconds(); sec > 0 {
		throughput = float64(s.metrics.ProcessedCount) / sec
	}
	var errRate float64
	total := s.metrics.ProcessedCount + s.metrics.ErrorCount
	if total > 0 {
		errRate = float64(s.metrics.ErrorCount) / float64(total)
	}

	return map[string]interface{}{
		"processed_count":         s.metrics.ProcessedCount,
		"error_count":             s.metrics.ErrorCount,
		"start_time":              s.metrics.StartTime,
		"last_processed_at":       s.metrics.LastProcessedAt,
		"avg_processing_time_ms":  s.metrics.AverageProcessingTime,
		"total_processing_time":   s.metrics.TotalProcessingTime.String(),
		"uptime_seconds":          uptime.Seconds(),
		"throughput_per_second":   throughput,
		"error_rate":              errRate,
		"documents_by_type":       s.metrics.DocumentsByType,
	}
}
