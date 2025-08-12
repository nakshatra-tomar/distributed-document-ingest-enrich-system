package couchbase

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"distributed-document-ingest-enrich-system/internal/models"

	"github.com/couchbase/gocb/v2"
	"github.com/sirupsen/logrus"
)

// DocumentRepository provides CRUD + search over EnrichedDocument in Couchbase.
type DocumentRepository struct {
	client *Client
	logger *logrus.Logger
}

// SearchOptions controls server-side filtering/pagination for Search().
type SearchOptions struct {
	Keywords []string            `json:"keywords,omitempty"` // matches ANY k IN doc.keywords SATISFIES k LIKE ...
	Type     models.DocumentType `json:"type,omitempty"`
	Language string              `json:"language,omitempty"`
	Source   string              `json:"source,omitempty"`
	DateFrom *time.Time          `json:"date_from,omitempty"`
	DateTo   *time.Time          `json:"date_to,omitempty"`
	Limit    int                 `json:"limit,omitempty"`
	Offset   int                 `json:"offset,omitempty"`
}

// SearchResult returns a page of documents plus simple paging info.
type SearchResult struct {
	Documents []models.EnrichedDocument `json:"documents"`
	Total     int                       `json:"total"`  // -1 means “there might be more” (we only fetched a page)
	Limit     int                       `json:"limit"`
	Offset    int                       `json:"offset"`
}

// NewDocumentRepository wires the repository with a client + JSON logger.
func NewDocumentRepository(client *Client) *DocumentRepository {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	return &DocumentRepository{client: client, logger: logger}
}

// Store upserts an EnrichedDocument keyed by its ID.
func (r *DocumentRepository) Store(ctx context.Context, doc *models.EnrichedDocument) error {
	key := r.generateDocumentKey(doc.ID)

	r.logger.WithFields(logrus.Fields{
		"document_id": doc.ID,
		"key":         key,
		"type":        doc.Type,
	}).Debug("storing document")

	_, err := r.client.collection.Upsert(key, doc, &gocb.UpsertOptions{
		Timeout: r.client.config.OperationTimeout,
	})
	if err != nil {
		r.logger.WithError(err).WithField("document_id", doc.ID).Error("failed to store document")
		return fmt.Errorf("store %s: %w", doc.ID, err)
	}

	r.logger.WithFields(logrus.Fields{
		"document_id": doc.ID,
		"type":        doc.Type,
		"size":        doc.Size,
	}).Info("document stored")
	return nil
}

// Get fetches a document by ID (returns gocb.ErrDocumentNotFound wrapped if missing).
func (r *DocumentRepository) Get(ctx context.Context, documentID string) (*models.EnrichedDocument, error) {
	key := r.generateDocumentKey(documentID)

	res, err := r.client.collection.Get(key, &gocb.GetOptions{
		Timeout: r.client.config.OperationTimeout,
	})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return nil, fmt.Errorf("document not found: %s", documentID)
		}
		return nil, fmt.Errorf("get %s: %w", documentID, err)
	}

	var doc models.EnrichedDocument
	if err := res.Content(&doc); err != nil {
		return nil, fmt.Errorf("decode %s: %w", documentID, err)
	}
	return &doc, nil
}

// Delete removes a document by ID (no-op if already missing).
func (r *DocumentRepository) Delete(ctx context.Context, documentID string) error {
	key := r.generateDocumentKey(documentID)
	_, err := r.client.collection.Remove(key, &gocb.RemoveOptions{
		Timeout: r.client.config.OperationTimeout,
	})
	if err != nil && !errors.Is(err, gocb.ErrDocumentNotFound) {
		return fmt.Errorf("delete %s: %w", documentID, err)
	}
	r.logger.WithField("document_id", documentID).Info("document deleted")
	return nil
}

// Search runs a parameterized SQL++ (N1QL) query over the collection using the provided options.
func (r *DocumentRepository) Search(ctx context.Context, options SearchOptions) (*SearchResult, error) {
	// sane paging defaults
	if options.Limit <= 0 {
		options.Limit = 50
	}
	if options.Limit > 1000 {
		options.Limit = 1000
	}
	if options.Offset < 0 {
		options.Offset = 0
	}

	query, params := r.buildSearchQuery(options)

	r.logger.WithFields(logrus.Fields{
		"query":  query,
		"params": params,
	}).Debug("executing search")

	result, err := r.client.cluster.Query(query, &gocb.QueryOptions{
		NamedParameters: params,
		Timeout:         r.client.config.OperationTimeout,
	})
	if err != nil {
		r.logger.WithError(err).Error("search query failed")
		return nil, fmt.Errorf("search query: %w", err)
	}
	defer result.Close()

	docs := make([]models.EnrichedDocument, 0, options.Limit)
	for result.Next() {
		var row struct {
			Document models.EnrichedDocument `json:"doc"`
		}
		if err := result.Row(&row); err != nil {
			r.logger.WithError(err).Warn("decode search row")
			continue
		}
		docs = append(docs, row.Document)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("iterate search: %w", err)
	}

	// Cheap “is there probably more?” signal without a second COUNT() query.
	total := len(docs)
	if len(docs) == options.Limit {
		total = -1
	}

	r.logger.WithFields(logrus.Fields{
		"result_count": len(docs),
		"limit":        options.Limit,
		"offset":       options.Offset,
	}).Info("search completed")

	return &SearchResult{
		Documents: docs,
		Total:     total,
		Limit:     options.Limit,
		Offset:    options.Offset,
	}, nil
}

// GetByType convenience wrapper for Search() by document type.
func (r *DocumentRepository) GetByType(ctx context.Context, docType models.DocumentType, limit int) ([]models.EnrichedDocument, error) {
	opts := SearchOptions{Type: docType, Limit: limit}
	res, err := r.Search(ctx, opts)
	if err != nil {
		return nil, err
	}
	return res.Documents, nil
}

// GetRecent returns the most recently processed documents (ordered DESC).
func (r *DocumentRepository) GetRecent(ctx context.Context, limit int) ([]models.EnrichedDocument, error) {
	if limit <= 0 {
		limit = 20
	}
	keyspace := r.qualifiedKeyspace()

	q := fmt.Sprintf(`
		SELECT doc.* FROM %s AS doc
		WHERE doc.type IS NOT MISSING
		ORDER BY doc.processed_at DESC
		LIMIT $limit
	`, keyspace)

	result, err := r.client.cluster.Query(q, &gocb.QueryOptions{
		NamedParameters: map[string]interface{}{"limit": limit},
		Timeout:         r.client.config.OperationTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("recent query: %w", err)
	}
	defer result.Close()

	var docs []models.EnrichedDocument
	for result.Next() {
		var doc models.EnrichedDocument
		if err := result.Row(&doc); err != nil {
			r.logger.WithError(err).Warn("decode recent row")
			continue
		}
		docs = append(docs, doc)
	}
	return docs, result.Err()
}

// GetStats returns simple aggregated stats across the collection.
func (r *DocumentRepository) GetStats(ctx context.Context) (map[string]interface{}, error) {
	keyspace := r.qualifiedKeyspace()

	q := fmt.Sprintf(`
		SELECT 
			COUNT(*) AS total_documents,
			COUNT(CASE WHEN type = 'report'  THEN 1 END) AS reports,
			COUNT(CASE WHEN type = 'email'   THEN 1 END) AS emails,
			COUNT(CASE WHEN type = 'log'     THEN 1 END) AS logs,
			COUNT(CASE WHEN type = 'invoice' THEN 1 END) AS invoices,
			COUNT(CASE WHEN type = 'legal'   THEN 1 END) AS legal,
			COUNT(CASE WHEN language = 'en'  THEN 1 END) AS english_docs,
			AVG(word_count) AS avg_word_count
		FROM %s
		WHERE type IS NOT MISSING
	`, keyspace)

	result, err := r.client.cluster.Query(q, &gocb.QueryOptions{
		Timeout: r.client.config.OperationTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("stats query: %w", err)
	}
	defer result.Close()

	var stats map[string]interface{}
	if result.Next() {
		if err := result.Row(&stats); err != nil {
			return nil, fmt.Errorf("decode stats: %w", err)
		}
	}
	return stats, result.Err()
}

// --- helpers ---

func (r *DocumentRepository) generateDocumentKey(documentID string) string {
	return fmt.Sprintf("doc::%s", documentID)
}

// qualifiedKeyspace returns `bucket`.`scope`.`collection` for Server 7+ collections.
func (r *DocumentRepository) qualifiedKeyspace() string {
	return fmt.Sprintf("`%s`.`%s`.`%s`",
		r.client.config.BucketName,
		r.client.config.ScopeName,
		r.client.config.CollectionName,
	)
}

// buildSearchQuery composes a parameterized SQL++ query and its named params.
func (r *DocumentRepository) buildSearchQuery(options SearchOptions) (string, map[string]interface{}) {
	keyspace := r.qualifiedKeyspace()
	conds := []string{"doc.type IS NOT MISSING"} // baseline condition
	params := make(map[string]interface{})

	// Type filter
	if options.Type != "" && options.Type != models.DocTypeUnknown {
		conds = append(conds, "doc.type = $type")
		params["type"] = string(options.Type)
	}

	// Language filter
	if options.Language != "" {
		conds = append(conds, "doc.language = $language")
		params["language"] = options.Language
	}

	// Source filter
	if options.Source != "" {
		conds = append(conds, "doc.metadata.source = $source")
		params["source"] = options.Source
	}

	// Keywords filter (ANY ... SATISFIES ...)
	if len(options.Keywords) > 0 {
		like := make([]string, len(options.Keywords))
		for i, kw := range options.Keywords {
			// For case-insensitive matching, we normalize to LOWER() on field and parameter.
			// e.g. ANY k IN doc.keywords SATISFIES LOWER(k) LIKE $keyword0 END
			p := fmt.Sprintf("keyword%d", i)
			like[i] = fmt.Sprintf("ANY k IN doc.keywords SATISFIES LOWER(k) LIKE $%s END", p)
			params[p] = "%"+strings.ToLower(kw)+"%"
		}
		conds = append(conds, "("+strings.Join(like, " OR ")+")")
	}

	// Date range (inclusive)
	if options.DateFrom != nil {
		conds = append(conds, "doc.timestamp >= $date_from")
		params["date_from"] = options.DateFrom.Format(time.RFC3339)
	}
	if options.DateTo != nil {
		conds = append(conds, "doc.timestamp <= $date_to")
		params["date_to"] = options.DateTo.Format(time.RFC3339)
	}

	where := strings.Join(conds, " AND ")

	q := fmt.Sprintf(`
		SELECT doc FROM %s AS doc
		WHERE %s
		ORDER BY doc.processed_at DESC
		LIMIT $limit OFFSET $offset
	`, keyspace, where)

	params["limit"] = options.Limit
	params["offset"] = options.Offset
	return q, params
}
