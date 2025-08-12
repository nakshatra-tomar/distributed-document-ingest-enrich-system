package couchbase

import (
	"fmt"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/sirupsen/logrus"
)

// Client wraps Cluster/Bucket/Collection handles plus config+logger.
type Client struct {
	cluster    *gocb.Cluster
	bucket     *gocb.Bucket
	collection *gocb.Collection
	config     Config
	logger     *logrus.Logger
}

// Config holds connection and keyspace details.
type Config struct {
	ConnectionString string        `json:"connection_string"`
	Username         string        `json:"username"`
	Password         string        `json:"password"`
	BucketName       string        `json:"bucket_name"`
	ScopeName        string        `json:"scope_name"`
	CollectionName   string        `json:"collection_name"`
	ConnectTimeout   time.Duration `json:"connect_timeout"`
	OperationTimeout time.Duration `json:"operation_timeout"`
}

// NewClient connects to Couchbase, opens the bucket/scope/collection, and waits for readiness.
func NewClient(config Config) (*Client, error) {
	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{})

	// sensible defaults
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 10 * time.Second
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 5 * time.Second
	}
	if config.ScopeName == "" {
		config.ScopeName = "_default"
	}
	if config.CollectionName == "" {
		config.CollectionName = "_default"
	}

	opts := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		},
		TimeoutsConfig: gocb.TimeoutsConfig{
			ConnectTimeout: config.ConnectTimeout,
			KVTimeout:      config.OperationTimeout,
		},
	}

	cluster, err := gocb.Connect(config.ConnectionString, opts)
	if err != nil {
		return nil, fmt.Errorf("connect cluster: %w", err)
	}

	// Wait for cluster to be ready before proceeding. :contentReference[oaicite:1]{index=1}
	if err := cluster.WaitUntilReady(config.ConnectTimeout, nil); err != nil {
		_ = cluster.Close(nil)
		return nil, fmt.Errorf("cluster not ready: %w", err)
	}

	bucket := cluster.Bucket(config.BucketName)
	if err := bucket.WaitUntilReady(config.ConnectTimeout, nil); err != nil {
		_ = cluster.Close(nil)
		return nil, fmt.Errorf("bucket not ready: %w", err)
	}

	scope := bucket.Scope(config.ScopeName)
	collection := scope.Collection(config.CollectionName)

	c := &Client{
		cluster:    cluster,
		bucket:     bucket,
		collection: collection,
		config:     config,
		logger:     log,
	}

	log.WithFields(logrus.Fields{
		"bucket":     config.BucketName,
		"scope":      config.ScopeName,
		"collection": config.CollectionName,
	}).Info("connected to Couchbase")

	return c, nil
}

// Close shuts down the cluster connection.
func (c *Client) Close() error {
	if c.cluster != nil {
		return c.cluster.Close(nil)
	}
	return nil
}

func (c *Client) GetCollection() *gocb.Collection { return c.collection }
func (c *Client) GetBucket() *gocb.Bucket         { return c.bucket }
func (c *Client) GetCluster() *gocb.Cluster       { return c.cluster }

// Ping performs a health check against the cluster (KV service). :contentReference[oaicite:2]{index=2}
func (c *Client) Ping() error {
	_, err := c.cluster.Ping(&gocb.PingOptions{
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeKeyValue},
		// (optional) ReportID can be set for tracing/debug.
	})
	return err
}

// CreateIndexes creates simple secondary indexes helpful for your queries.
// Uses the Query Index Manager; targeting scope/collection on Server 7+. :contentReference[oaicite:3]{index=3}
func (c *Client) CreateIndexes() error {
	mgr := c.cluster.QueryIndexes()

	type idx struct {
		name   string
		fields []string
	}
	indexes := []idx{
		{"idx_document_type", []string{"type"}},
		{"idx_document_keywords", []string{"keywords"}},
		{"idx_document_timestamp", []string{"timestamp"}},
		{"idx_document_language", []string{"language"}},
		{"idx_document_source", []string{"metadata.source"}},
	}

	for _, ix := range indexes {
		err := mgr.CreateIndex(
			c.config.BucketName,
			ix.name,
			ix.fields,
			&gocb.CreateQueryIndexOptions{
				IgnoreIfExists: true,
				// make sure the index is created on the right keyspace (Server 7+ scopes/collections)
				ScopeName:      c.config.ScopeName,
				CollectionName: c.config.CollectionName,
				// (optional) NumReplicas, Deferred, etc., can be set here.
			},
		)
		if err != nil {
			c.logger.WithError(err).WithField("index", ix.name).
				Warn("failed to create index")
		} else {
			c.logger.WithField("index", ix.name).
				Info("index created (or already exists)")
		}
	}
	return nil
}
