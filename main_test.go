package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/janakerman/elastic-reindex/ingest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

const (
	indexA = "a"
	indexB = "b"
)

func TestReindex(t *testing.T) {
	// 1. Set primary index to index A.
	fmt.Println("Set the primary index to index 'a'")
	setReadIndex(indexA)
	setPrimaryIndexTo(indexA)

	// 2. Start sending documents.
	fmt.Println("Start indexing documents into index 'a'")
	stop := make(chan struct{}, 1)
	numSent := make(chan int)
	go func() {
		sent, err :=  startSendingDocuments(stop)
		assert.Nil(t, err, "failed sending documented")
		numSent <- sent
	}()
	wait()

	// 2. Add secondary index to start duplicating writes to the new index.
	fmt.Println("Add secondary index to start duplicating writes to index 'b'")
	setSecondaryIndex(indexB)
	wait()



	// 3. Reindex primary into secondary
	fmt.Println("Reindex 'a' into 'b'")
	if err := reindex(indexA, indexB); err != nil {
		t.Errorf("reindex operation failed: %v", err)
	}
	wait()

	// 4. Switch read index
	// 5. Switch the primary index
	// 6. Delete the old index


	// 7. Stop ingesting documents
	fmt.Println("Stop indexing documents")
	stop <- struct{}{}
	sent := <- numSent

	// 9. Assert correctness
	require.Greater(t, sent, 0, fmt.Sprintf("no documents sent"))
}

func esClient(t *testing.T) *elasticsearch.Client {
	c, err := ingest.NewESClient()
	if err != nil {
		t.Errorf("failed to create elastic client")
	}
	return c
}

func setReadIndex(index string) {
	_ = os.Setenv(ingest.EnvReadIndex, index)
}

func setPrimaryIndexTo(index string) {
	_ = os.Setenv(ingest.EnvPrimaryIndex, index)
}

func setSecondaryIndex(index string) {
	_ = os.Setenv(ingest.EnvSecondaryIndex, index)
}

func wait() {
	<- time.After(1 * time.Second)
}

func startSendingDocuments(stop <- chan struct{}) (int, error) {
	workers := 10
	sendDoc := make(chan ingest.Document, 2 * workers)

	go func() {
		n := 0
		for {
			select {
			case _ = <-stop:
				close(sendDoc)
				return
			default:
				sendDoc<-ingest.Document{ID: n, Message: fmt.Sprintf("document %d", n)}
			}
			n++
		}
	}()

	return sendDocuments(context.Background(), workers, sendDoc)
}

func sendDocuments(ctx context.Context, workers int, documents <- chan ingest.Document) (int, error) {
	g, gCtx := errgroup.WithContext(ctx)
	var numDocs uint64

	for x:=0; x<workers; x++ {
		g.Go(func() error {
			for d := range documents {
				iCtx, _ :=  context.WithDeadline(gCtx, time.Now().Add(5 * time.Second))
				if err := ingest.Ingest(iCtx, d); err != nil {
					return err
				}
				atomic.AddUint64(&numDocs, 1)
			}
			return nil
		})
	}

	err := g.Wait()
	return int(numDocs), err
}

func reindex(from, to string) error {
	c, err := ingest.NewESClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %v" ,err)
	}

	type index struct {
		Index string `json:"index"`
	}
	req := struct {
		Source index `json:"source"`
		Dest index `json:"dest"`
	}{
		Source: index{Index: from},
		Dest: index{Index: to},
	}

	b, err := json.Marshal(req)
	if err != nil {
		return nil
	}

	_, err = c.Reindex(
		bytes.NewBuffer(b),
	)
	if err != nil {
		return err
	}
	return nil
}