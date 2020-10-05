package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"os"
	"strconv"
	"sync"
)

const (
	EnvReadIndex = "READ_INDEX"
	EnvPrimaryIndex = "PRIMARY_INDEX"
	EnvSecondaryIndex = "SECONDARY_INDEX"
)

var (
	esClient *elasticsearch.Client
	once     sync.Once
)

type Document struct {
	ID int
	Message string
}

func NewESClient() (*elasticsearch.Client, error) {
	return elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{"http://localhost:9200"},
	})
}

func Ingest(ctx context.Context, doc Document) error {
	var c *elasticsearch.Client
	var err error
	if c, err = client(); err != nil {
		return err
	}

	primaryIndex := os.Getenv(EnvPrimaryIndex)
	if err := saveToIndex(ctx, c, primaryIndex, doc); err != nil {
		return err
	}

	secondaryIndex := os.Getenv(EnvSecondaryIndex)
	if secondaryIndex == "" {
		return nil
	}

	if err := saveToIndex(ctx, c, secondaryIndex, doc); err != nil {
		return err
	}

	return nil
}

func client() (*elasticsearch.Client, error) {
	var err error
	once.Do(func() {
		esClient, err = NewESClient()
	})
	if err != nil {
		return nil, err
	}
	return esClient, nil
}

func saveToIndex(ctx context.Context, client *elasticsearch.Client, index string, doc Document) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshall document: %w", err)
	}
	_, err = client.Index(
		index,
		bytes.NewReader(b),
		client.Index.WithDocumentID(strconv.Itoa(doc.ID)),
		client.Index.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to index document %d: %w", doc.ID, err)
	}
	return nil
}
