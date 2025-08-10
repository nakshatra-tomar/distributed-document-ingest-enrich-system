package models

import (
    "encoding/json"
    "fmt"
    "time"
)

// RawDocument represents the incoming document from Kafka.
type RawDocument struct {
    ID         string            `json:"id"`
    Filename   string            `json:"filename"`
    FileType   string            `json:"file_type"`
    ContentB64 string            `json:"content_base64"`
    Source     string            `json:"source"`
    Timestamp  time.Time         `json:"timestamp"`
    Metadata   map[string]string `json:"metadata,omitempty"`
}

// Validate ensures that required fields are present in RawDocument.
func (rd *RawDocument) Validate() error {
    if rd.ID == "" {
        return fmt.Errorf("document ID is required")
    }
    if rd.Filename == "" {
        return fmt.Errorf("filename is required")
    }
    if rd.ContentB64 == "" {
        return fmt.Errorf("content is required")
    }
    return nil
}

// DocumentType classifies the type of document.
type DocumentType string

// Supported document types.
const (
    DocTypeReport  DocumentType = "report"
    DocTypeEmail   DocumentType = "email"
    DocTypeLog     DocumentType = "log"
    DocTypeInvoice DocumentType = "invoice"
    DocTypeLegal   DocumentType = "legal"
    DocTypeUnknown DocumentType = "unknown"
)

// IsValid checks if the DocumentType is recognized.
func (dt DocumentType) IsValid() bool {
    switch dt {
    case DocTypeReport, DocTypeEmail, DocTypeLog, DocTypeInvoice, DocTypeLegal, DocTypeUnknown:
        return true
    }
    return false
}

// EnrichedDocument represents the processed document ready for indexing.
type EnrichedDocument struct {
    ID          string            `json:"id"`
    Filename    string            `json:"filename"`
    Keywords    []string          `json:"keywords"`
    Type        DocumentType      `json:"type"`
    Language    string            `json:"language"`
    Content     string            `json:"content"`
    Summary     string            `json:"summary,omitempty"`
    Metadata    map[string]string `json:"metadata"`
    Timestamp   time.Time         `json:"timestamp"`
    ProcessedAt time.Time         `json:"processed_at"`
    WordCount   int               `json:"word_count"`
    Size        int64             `json:"size"`
}

// ToJSON serializes EnrichedDocument to JSON bytes.
func (ed *EnrichedDocument) ToJSON() ([]byte, error) {
    return json.Marshal(ed)
}

// FromJSON deserializes JSON bytes into an EnrichedDocument.
func FromJSON(data []byte) (*EnrichedDocument, error) {
    var doc EnrichedDocument
    if err := json.Unmarshal(data, &doc); err != nil {
        return nil, err
    }
    return &doc, nil
}

// ProcessingError represents failures at various pipeline stages.
type ProcessingError struct {
    DocumentID string    `json:"document_id"`
    Error      string    `json:"error"`    // A brief error message
    Stage      string    `json:"stage"`    // E.g., "parsing", "enriching", "indexing"
    Timestamp  time.Time `json:"timestamp"`
    Retryable  bool      `json:"retryable"` // Whether retry is safe
}
