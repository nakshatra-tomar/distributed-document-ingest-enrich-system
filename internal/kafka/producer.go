package kafka

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/IBM/sarama"
    "github.com/sirupsen/logrus"
)

// Producer exposes sync and async send methods and a Close hook.
type Producer interface {
    // SendMessage publishes a record synchronously and returns when the broker acks it.
    SendMessage(ctx context.Context, topic, key string, value interface{}) error
    // SendMessageAsync enqueues a record for async publishing; returns once queued or ctx cancels.
    SendMessageAsync(ctx context.Context, topic, key string, value interface{}) error
    // Close flushes and closes underlying producers.
    Close() error
}

type producer struct {
    syncProducer  sarama.SyncProducer
    asyncProducer sarama.AsyncProducer
    logger        *logrus.Logger
    config        ProducerConfig
}

// ProducerConfig holds client‑side settings for throughput/reliability.
type ProducerConfig struct {
    Brokers       []string
    RetryAttempts int
    FlushTimeout  time.Duration
    BatchSize     int
}

// NewProducer constructs both sync and async Sarama producers with safe defaults.
func NewProducer(cfg ProducerConfig) (Producer, error) {
    sc := sarama.NewConfig()

    // Reliability / idempotence
    sc.Producer.RequiredAcks = sarama.WaitForAll // acks=all is required for idempotence
    sc.Producer.Retry.Max = cfg.RetryAttempts    // >0 required for idempotence
    sc.Producer.Return.Successes = true
    sc.Producer.Return.Errors = true
    sc.Producer.Idempotent = true
    sc.Net.MaxOpenRequests = 1 // keeps in-flight per connection to 1 for strict ordering

    // Throughput / batching
    sc.Producer.Flush.Frequency = cfg.FlushTimeout
    sc.Producer.Flush.Messages = cfg.BatchSize
    sc.Producer.Compression = sarama.CompressionSnappy

    // TIP: If your cluster requires a specific Kafka version, set sc.Version accordingly.

    // Sync producer
    sp, err := sarama.NewSyncProducer(cfg.Brokers, sc)
    if err != nil {
        return nil, fmt.Errorf("failed to create sync producer: %w", err)
    }

    // Async producer
    ap, err := sarama.NewAsyncProducer(cfg.Brokers, sc)
    if err != nil {
        _ = sp.Close()
        return nil, fmt.Errorf("failed to create async producer: %w", err)
    }

    p := &producer{
        syncProducer:  sp,
        asyncProducer: ap,
        logger:        logrus.New(),
        config:        cfg,
    }

    // Drain async error channel so it doesn't block.
    go p.handleAsyncErrors()

    return p, nil
}

// SendMessage publishes a message synchronously and logs the assigned partition/offset.
func (p *producer) SendMessage(ctx context.Context, topic, key string, value interface{}) error {
    msg, err := p.createMessage(topic, key, value)
    if err != nil {
        return err
    }

    partition, offset, err := p.syncProducer.SendMessage(msg)
    if err != nil {
        p.logger.WithError(err).WithFields(logrus.Fields{
            "topic": topic, "key": key,
        }).Error("failed to send message")
        return fmt.Errorf("failed to send message: %w", err)
    }

    p.logger.WithFields(logrus.Fields{
        "topic": topic, "partition": partition, "offset": offset, "key": key,
    }).Debug("message sent successfully")
    return nil
}

// SendMessageAsync enqueues a message for asynchronous publish or returns on context cancel.
func (p *producer) SendMessageAsync(ctx context.Context, topic, key string, value interface{}) error {
    msg, err := p.createMessage(topic, key, value)
    if err != nil {
        return err
    }
    select {
    case p.asyncProducer.Input() <- msg:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

// createMessage normalizes value into bytes and builds a ProducerMessage.
func (p *producer) createMessage(topic, key string, value interface{}) (*sarama.ProducerMessage, error) {
    var (
        payload []byte
        err     error
    )

    switch v := value.(type) {
    case []byte:
        payload = v
    case string:
        payload = []byte(v)
    default:
        payload, err = json.Marshal(value)
        if err != nil {
            return nil, fmt.Errorf("failed to marshal message: %w", err)
        }
    }

    return &sarama.ProducerMessage{
        Topic:     topic,
        Key:       sarama.StringEncoder(key),
        Value:     sarama.ByteEncoder(payload),
        Timestamp: time.Now(),
    }, nil
}

// handleAsyncErrors continuously drains and logs async send errors.
func (p *producer) handleAsyncErrors() {
    for err := range p.asyncProducer.Errors() {
        p.logger.WithError(err.Err).WithFields(logrus.Fields{
            "topic": err.Msg.Topic, "partition": err.Msg.Partition,
        }).Error("async producer error")
    }
}

// Close flushes and closes both sync and async producers, returning a combined error if any.
func (p *producer) Close() error {
    var errs []error
    if err := p.syncProducer.Close(); err != nil {
        errs = append(errs, err)
    }
    if err := p.asyncProducer.Close(); err != nil {
        errs = append(errs, err)
    }
    if len(errs) > 0 {
        return fmt.Errorf("errors closing producer: %v", errs)
    }
    return nil
}
