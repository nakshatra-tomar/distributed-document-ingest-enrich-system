package main

import (
    "context"
    "encoding/base64"
    "flag"
    "fmt"
    "io/ioutil"
    "log"
    "path/filepath"
    "time"

    "distributed-document-ingest-enrich-system/internal/config"
    "distributed-document-ingest-enrich-system/internal/kafka"
    "distributed-document-ingest-enrich-system/internal/models"

    "github.com/google/uuid"
    "github.com/sirupsen/logrus"
)

// main acts as a CLI producer to send one or more documents into the Kafka raw topic.
func main() {
    var (
        filePath = flag.String("file", "", "Path to file to process")
        fileType = flag.String("type", "", "File type (pdf, txt, eml)")
        source   = flag.String("source", "cli", "Source of the document")
        count    = flag.Int("count", 1, "Number of times to send the document")
    )
    flag.Parse()

    if *filePath == "" {
        log.Fatal("file path is required")
    }

    // Load configuration (from env, config file, or defaults)
    cfg, err := config.Load()
    if err != nil {
        log.Fatalf("failed to load config: %v", err)
    }

    // Set up structured logging
    logger := logrus.New()
    if cfg.IsDevelopment() {
        logger.SetLevel(logrus.DebugLevel)
        logger.SetFormatter(&logrus.TextFormatter{})
    } else {
        logger.SetFormatter(&logrus.JSONFormatter{})
    }

    // Prepare Kafka producer
    producerCfg := kafka.ProducerConfig{
        Brokers:       cfg.Kafka.Brokers,
        RetryAttempts: cfg.Kafka.RetryAttempts,
        FlushTimeout:  cfg.Kafka.FlushTimeout,
        BatchSize:     cfg.Kafka.BatchSize,
    }

    producer, err := kafka.NewProducer(producerCfg)
    if err != nil {
        log.Fatalf("failed to create Kafka producer: %v", err)
    }
    defer producer.Close()

    // Read file into memory
    content, err := ioutil.ReadFile(*filePath)
    if err != nil {
        log.Fatalf("failed to read file: %v", err)
    }

    // Detect file type if not provided
    if *fileType == "" {
        *fileType = detectFileType(*filePath)
    }

    // Prepare the base RawDocument
    doc := &models.RawDocument{
        ID:         uuid.New().String(),
        Filename:   filepath.Base(*filePath),
        FileType:   *fileType,
        ContentB64: base64.StdEncoding.EncodeToString(content),
        Source:     *source,
        Timestamp:  time.Now(),
        Metadata: map[string]string{
            "file_size": fmt.Sprintf("%d", len(content)),
            "producer":  "cli",
        },
    }

    // Validate mandatory fields before sending
    if err := doc.Validate(); err != nil {
        log.Fatalf("invalid document: %v", err)
    }

    ctx := context.Background()

    // Send N copies of the document into Kafka
    for i := 0; i < *count; i++ {
        if i > 0 {
            // Each iteration gets a new ID/timestamp so messages are unique
            doc.ID = uuid.New().String()
            doc.Timestamp = time.Now()
        }

        if err := producer.SendMessage(ctx, cfg.Kafka.RawTopic, doc.ID, doc); err != nil {
            log.Fatalf("failed to send message: %v", err)
        }

        logger.WithFields(logrus.Fields{
            "document_id": doc.ID,
            "filename":    doc.Filename,
            "file_type":   doc.FileType,
            "size":        len(content),
            "attempt":     i + 1,
        }).Info("document sent successfully")

        if i < *count-1 {
            time.Sleep(100 * time.Millisecond) // small delay between sends
        }
    }

    logger.Infof("successfully sent %d document(s)", *count)
}

// detectFileType infers a type label from a filename extension.
func detectFileType(filename string) string {
    switch ext := filepath.Ext(filename); ext {
    case ".pdf":
        return "pdf"
    case ".txt", ".log":
        return "txt"
    case ".eml":
        return "eml"
    default:
        return "txt" // default to text
    }
}
