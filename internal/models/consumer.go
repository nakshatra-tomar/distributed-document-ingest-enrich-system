package kafka

import (
    "context"
    "fmt"
    "sync"
    "time"

    "github.com/IBM/sarama"
    "github.com/sirupsen/logrus"
)

// MessageHandler processes a single Kafka message. Return an error to trigger retry logic.
type MessageHandler func(ctx context.Context, message *sarama.ConsumerMessage) error

// Consumer represents a Kafka consumer group runner.
type Consumer interface {
    // Start begins consuming the provided topics until the context is cancelled or a fatal error occurs.
    Start(ctx context.Context, topics []string) error
    // Stop closes the underlying consumer group.
    Stop() error
}

type consumer struct {
    consumerGroup sarama.ConsumerGroup
    handler       MessageHandler
    logger        *logrus.Logger
    config        ConsumerConfig

    ready chan struct{} // closed once the first Setup runs
    wg    sync.WaitGroup
    once  sync.Once
}

// ConsumerConfig holds essential consumer settings.
type ConsumerConfig struct {
    Brokers       []string
    GroupID       string
    Topics        []string
    RetryAttempts int
}

// NewConsumer creates a new Kafka consumer group with sensible defaults and error handling.
func NewConsumer(cfg ConsumerConfig, handler MessageHandler) (Consumer, error) {
    sc := sarama.NewConfig()

    // Group & session behavior (tune as needed)
    sc.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
    sc.Consumer.Offsets.Initial = sarama.OffsetOldest
    sc.Consumer.Group.Session.Timeout = 10 * time.Second
    sc.Consumer.Group.Heartbeat.Interval = 3 * time.Second
    sc.Consumer.MaxProcessingTime = 2 * time.Minute

    // Commit behavior
    sc.Consumer.Return.Errors = true
    sc.Consumer.Offsets.AutoCommit.Enable = true
    sc.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

    // TIP: if your broker requires a specific version, set sc.Version explicitly.

    cg, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, sc)
    if err != nil {
        return nil, fmt.Errorf("failed to create consumer group: %w", err)
    }

    log := logrus.New()
    log.SetFormatter(&logrus.JSONFormatter{})

    return &consumer{
        consumerGroup: cg,
        handler:       handler,
        logger:        log,
        config:        cfg,
        ready:         make(chan struct{}),
    }, nil
}

// Start begins the consume loop and blocks until the context is cancelled or an unrecoverable error occurs.
func (c *consumer) Start(ctx context.Context, topics []string) error {
    c.logger.WithFields(logrus.Fields{
        "group_id": c.config.GroupID,
        "topics":   topics,
    }).Info("starting Kafka consumer")

    // Main consume loop must be called repeatedly; it returns on rebalance.
    c.wg.Add(1)
    go func() {
        defer c.wg.Done()
        for {
            if err := c.consumerGroup.Consume(ctx, topics, c); err != nil {
                c.logger.WithError(err).Error("error from consumer group")
                return
            }
            // Exit if context cancelled; otherwise loop to rejoin after a rebalance.
            if ctx.Err() != nil {
                c.logger.Info("consumer context cancelled")
                return
            }
        }
    }()

    // Drain async consumer-group errors.
    c.wg.Add(1)
    go func() {
        defer c.wg.Done()
        for {
            select {
            case err := <-c.consumerGroup.Errors():
                if err != nil {
                    c.logger.WithError(err).Error("consumer group error")
                }
            case <-ctx.Done():
                c.logger.Info("stopping error handler")
                return
            }
        }
    }()

    // Wait until Setup marks the consumer ready (first assignment).
    <-c.ready
    c.logger.Info("consumer started and ready")

    // Block here until goroutines finish due to ctx cancellation or fatal error.
    c.wg.Wait()
    return nil
}

// Stop closes the consumer group (use in addition to cancelling the Start() context).
func (c *consumer) Stop() error {
    c.logger.Info("stopping consumer")
    return c.consumerGroup.Close()
}

// Setup is called once per new session; mark readiness here.
// NOTE: sarama may call Setup multiple times across rebalances—protect with once.Do.
func (c *consumer) Setup(sarama.ConsumerGroupSession) error {
    c.once.Do(func() { close(c.ready) })
    return nil
}

// Cleanup is called at the end of a session; nothing to clean up here.
func (c *consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim runs in its own goroutine per partition. Do not start new goroutines in here.
func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for {
        select {
        case msg := <-claim.Messages():
            if msg == nil {
                return nil
            }

            if err := c.processMessageWithRetryAndTimeout(session.Context(), msg); err != nil {
                c.logger.WithError(err).WithFields(logrus.Fields{
                    "topic":     msg.Topic,
                    "partition": msg.Partition,
                    "offset":    msg.Offset,
                    "key":       string(msg.Key),
                }).Error("failed to process message after retries")

                // In production, consider a dead-letter topic instead of marking success.
                session.MarkMessage(msg, "")
                continue
            }

            // Mark message as processed (offset commit is handled by auto-commit).
            session.MarkMessage(msg, "")

        case <-session.Context().Done():
            return nil
        }
    }
}

// processMessageWithRetryAndTimeout wraps the user handler with retries, per-attempt timeouts, and backoff.
func (c *consumer) processMessageWithRetryAndTimeout(ctx context.Context, msg *sarama.ConsumerMessage) error {
    var lastErr error

    for attempt := 1; attempt <= c.config.RetryAttempts; attempt++ {
        attemptCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
        err := c.handler(attemptCtx, msg)
        cancel()

        if err == nil {
            if attempt > 1 {
                c.logger.WithFields(logrus.Fields{
                    "topic":     msg.Topic,
                    "partition": msg.Partition,
                    "offset":    msg.Offset,
                    "attempt":   attempt,
                }).Info("message processed successfully after retry")
            }
            return nil
        }

        lastErr = err

        // Context cancelled? bail out.
        if ctx.Err() != nil {
            return ctx.Err()
        }

        c.logger.WithError(err).WithFields(logrus.Fields{
            "attempt":      attempt,
            "max_attempts": c.config.RetryAttempts,
            "topic":        msg.Topic,
            "partition":    msg.Partition,
            "offset":       msg.Offset,
        }).Warn("message processing failed; retrying")

        // Exponential backoff with cap (simple quadratic here).
        backoff := time.Duration(attempt*attempt) * time.Second
        if backoff > 30*time.Second {
            backoff = 30 * time.Second
        }

        select {
        case <-time.After(backoff):
        case <-ctx.Done():
            return ctx.Err()
        }
    }

    return lastErr
}
