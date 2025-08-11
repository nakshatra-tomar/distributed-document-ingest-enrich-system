package enrich

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"distributed-document-ingest-enrich-system/internal/models"
	"distributed-document-ingest-enrich-system/internal/parser"

	"github.com/sirupsen/logrus"
)

// Enricher orchestrates parsing + NLP to produce an EnrichedDocument.
type Enricher struct {
	parserRegistry *parser.Registry
	nlpProcessor   *NLPProcessor
	logger         *logrus.Logger
	config         Config
}

// Config controls which NLP features run and rough limits.
type Config struct {
	MaxKeywords     int  `json:"max_keywords"`
	MaxSummaryLines int  `json:"max_summary_lines"`
	EnableSummary   bool `json:"enable_summary"`
	EnableSentiment bool `json:"enable_sentiment"`
	EnableEntities  bool `json:"enable_entities"`
}

// DefaultConfig returns safe defaults for local/dev usage.
func DefaultConfig() Config {
	return Config{
		MaxKeywords:     10,
		MaxSummaryLines: 3,
		EnableSummary:   true,
		EnableSentiment: true,
		EnableEntities:  true,
	}
}

// NewEnricher builds an Enricher with a fresh registry and NLP processor.
func NewEnricher(cfg Config) *Enricher {
	if cfg.MaxKeywords <= 0 {
		cfg.MaxKeywords = 10
	}
	if cfg.MaxSummaryLines <= 0 {
		cfg.MaxSummaryLines = 3
	}

	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{})

	return &Enricher{
		parserRegistry: parser.NewRegistry(),
		nlpProcessor:   NewNLPProcessor(),
		logger:         log,
		config:         cfg,
	}
}

// EnrichDocument parses, analyzes and returns an EnrichedDocument.
func (e *Enricher) EnrichDocument(raw *models.RawDocument) (*models.EnrichedDocument, error) {
	start := time.Now()

	e.logger.WithFields(logrus.Fields{
		"document_id": raw.ID,
		"filename":    raw.Filename,
		"file_type":   raw.FileType,
		"source":      raw.Source,
		"size_b64":    len(raw.ContentB64),
	}).Info("starting document enrichment")

	// Basic validation of incoming payload.
	if err := raw.Validate(); err != nil {
		e.logger.WithError(err).Error("invalid raw document")
		return nil, fmt.Errorf("invalid raw document: %w", err)
	}

	// Decode base64 → raw bytes.
	content, err := base64.StdEncoding.DecodeString(raw.ContentB64)
	if err != nil {
		e.logger.WithError(err).Error("failed to decode base64 content")
		return nil, fmt.Errorf("failed to decode base64 content: %w", err)
	}

	// Choose parser by file type; fall back to text if unknown.
	p, perr := e.parserRegistry.GetParser(raw.FileType)
	if perr != nil {
		e.logger.WithError(perr).WithField("file_type", raw.FileType).
			Warn("no parser found for type; falling back to text")
		if p, _ = e.parserRegistry.GetParser("txt"); p == nil {
			return nil, fmt.Errorf("no suitable parser available for type %q", raw.FileType)
		}
	}

	text, err := p.Parse(content)
	if err != nil {
		e.logger.WithError(err).Error("failed to parse document content")
		return nil, fmt.Errorf("failed to parse document content: %w", err)
	}
	if strings.TrimSpace(text) == "" {
		e.logger.Warn("parsed content is empty")
		return nil, fmt.Errorf("parsed content is empty")
	}

	// Run NLP pipeline.
	data := e.performNLPEnrichment(text)

	// Compute simple metrics.
	wordCount := len(strings.Fields(text))

	// Build enriched doc.
	enriched := &models.EnrichedDocument{
		ID:          raw.ID,
		Filename:    raw.Filename,
		Keywords:    data.Keywords,
		Type:        data.DocumentType,
		Language:    data.Language,
		Content:     text,
		Summary:     data.Summary,
		Metadata:    make(map[string]string),
		Timestamp:   raw.Timestamp,
		ProcessedAt: time.Now(),
		WordCount:   wordCount,
		Size:        int64(len(content)),
	}

	// Carry forward original metadata (if any).
	for k, v := range raw.Metadata {
		enriched.Metadata[k] = v
	}

	// Add enrichment/processing metadata.
	e.addEnrichmentMetadata(enriched, data, time.Since(start))

	e.logger.WithFields(logrus.Fields{
		"document_id":     enriched.ID,
		"word_count":      enriched.WordCount,
		"keyword_count":   len(enriched.Keywords),
		"document_type":   enriched.Type,
		"language":        enriched.Language,
		"processing_time": time.Since(start),
	}).Info("document enrichment completed successfully")

	return enriched, nil
}

// EnrichmentData aggregates NLP results.
type EnrichmentData struct {
	Keywords     []string
	DocumentType models.DocumentType
	Language     string
	Summary      string
	Sentiment    string
	Entities     map[string][]string
}

// performNLPEnrichment executes all enabled NLP features.
func (e *Enricher) performNLPEnrichment(text string) EnrichmentData {
	data := EnrichmentData{}

	data.Keywords = e.nlpProcessor.ExtractKeywords(text, e.config.MaxKeywords)
	data.Language = e.nlpProcessor.DetectLanguage(text)
	data.DocumentType = e.nlpProcessor.ClassifyDocument("", text) // filename not used here

	if e.config.EnableSummary {
		data.Summary = e.nlpProcessor.GenerateSummary(text, e.config.MaxSummaryLines)
	}
	if e.config.EnableSentiment {
		data.Sentiment = e.nlpProcessor.AnalyzeSentiment(text)
	}
	if e.config.EnableEntities {
		data.Entities = e.nlpProcessor.ExtractEntities(text)
	}
	return data
}

// addEnrichmentMetadata stamps metadata fields about processing and features.
func (e *Enricher) addEnrichmentMetadata(enriched *models.EnrichedDocument, data EnrichmentData, d time.Duration) {
	// Preserve any existing keys; only set if absent to avoid clobbering upstream metadata.
	if _, ok := enriched.Metadata["processing_time_ms"]; !ok {
		enriched.Metadata["processing_time_ms"] = fmt.Sprintf("%.2f", float64(d.Nanoseconds())/1e6)
	}
	if _, ok := enriched.Metadata["keyword_count"]; !ok {
		enriched.Metadata["keyword_count"] = fmt.Sprintf("%d", len(data.Keywords))
	}
	enriched.Metadata["enricher_version"] = "1.0.0"
	enriched.Metadata["nlp_features"] = e.getNLPFeatures()

	if data.Sentiment != "" {
		enriched.Metadata["sentiment"] = data.Sentiment
	}
	if len(data.Entities) > 0 {
		if b, err := json.Marshal(data.Entities); err == nil {
			enriched.Metadata["entities"] = string(b)
		}
	}
}

// getNLPFeatures returns a CSV list of enabled NLP features.
func (e *Enricher) getNLPFeatures() string {
	features := []string{"keywords", "language", "classification"}
	if e.config.EnableSummary {
		features = append(features, "summary")
	}
	if e.config.EnableSentiment {
		features = append(features, "sentiment")
	}
	if e.config.EnableEntities {
		features = append(features, "entities")
	}
	return strings.Join(features, ",")
}

// GetSupportedTypes exposes the registry's supported file types.
func (e *Enricher) GetSupportedTypes() []string {
	return e.parserRegistry.SupportedTypes()
}

// GetStats returns simple runtime/feature info for diagnostics.
func (e *Enricher) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"supported_types": e.GetSupportedTypes(),
		"config":          e.config,
		"version":         "1.0.0",
	}
}
