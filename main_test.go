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
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

const (
	indexA = "a"
	indexB = "b"
)
var (
	docID = 0
)

func TestReindex(t *testing.T) {
	// Delete indexes
	_ = deleteIndex(indexA)
	_ = deleteIndex(indexB)

	// Set primary index to index A.
	fmt.Println("Set the read index and primary index to index 'a'")
	setReadIndex(indexA)
	setPrimaryIndexTo(indexA)

	// Start sending documents.
	fmt.Println("Start indexing documents into index 'a'")
	stop := make(chan struct{}, 1)
	numSent := make(chan int)
	go func() {
		sent, err :=  startSendingDocuments(stop)
		assert.Nil(t, err, "failed sending documented")
		numSent <- sent
	}()

	// Start reading documents to demonstrate progress
	stopLogging := make(chan struct{}, 1)
	go func() {
		logDocuments(stopLogging)
	}()
	wait()

	// Add secondary index to start duplicating writes to the new index.
	fmt.Println("Add secondary index to start duplicating writes to index 'b'")
	setSecondaryIndex(indexB)
	wait()

	// Reindex primary into secondary
	fmt.Println("Reindex 'a' into 'b'")
	if err := reindex(indexA, indexB); err != nil {
		t.Errorf("reindex operation failed: %v", err)
	}
	wait()

	// Stop index documents to allow indexes to refresh and settle to demonstrate that indexes are in sync
	fmt.Println("Stop indexing documents so we can see indexes settle")
	stop <- struct{}{}
	fmt.Println("Index 'a' and 'b' should be in sync")
	wait()

	// Back to indexing documents
	fmt.Println("Start indexing documents again")
	stop = make(chan struct{}, 1)
	go func() {
		sent, err :=  startSendingDocuments(stop)
		assert.Nil(t, err, "failed sending documented")
		numSent <- sent
	}()
	wait()

	// Now you could switch the read index over to B as it is in sync with A
	fmt.Println("Switch read index to index 'b' (could be an alias)")
	setReadIndex(indexB)
	fmt.Println("Reads are now made to index 'b'")
	wait()

	// We can write to index B as our primary index and stop our writes to A
	fmt.Println("Switch the primary index to index 'b'")
	setPrimaryIndexTo(indexB)
	setSecondaryIndex("")
	fmt.Println("Primary index set to 'b'")
	wait()

	// Now would be a good time to delete index A
	fmt.Println("Delete index 'a'")
	_ = deleteIndex(indexA)
	wait()

	// End of the example
	fmt.Println("Stop indexing documents")
	stop <- struct{}{}
	sent := <- numSent + <- numSent
	wait()

	// TODO: Add some assertions!
	require.Greater(t, sent, 0, fmt.Sprintf("no documents sent"))
	stopLogging <- struct{}{}
	close(stop)
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
	<- time.After(6 * time.Second)
}

func deleteIndex(index string) error {
	client, err := ingest.NewESClient()
	if err != nil {
		return err
	}
	_, err = client.Indices.Delete([]string{index})
	if err != nil {
		return err
	}
	return nil
}

func startSendingDocuments(stop <- chan struct{}) (int, error) {
	workers := 1
	sendDoc := make(chan ingest.Document, 2 * workers)

	go func() {
		for {
			select {
			case _ = <-stop:
				close(sendDoc)
				return
			default:
				sendDoc<-ingest.Document{ID: docID, Message: fmt.Sprintf("document %d", docID)}
			}
			docID++
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

func logDocuments(stop <- chan struct{}) {
	client, _ := ingest.NewESClient()

	for {
		select {
		case <- stop:
			return
		default:
			idA, numA, _ := logLatestDocument(client, indexA)
			idb, numb, _ := logLatestDocument(client, indexB)
			fmt.Printf("A[num: %d - ID: %s] B[num: %d - ID: %s]\n", numA, idA, numb, idb)
			time.Sleep(2 * time.Second)
		}
	}

}

func logLatestDocument(client *elasticsearch.Client, index string) (lastID string, num int, err error) {

	var buf bytes.Buffer
	q := map[string]interface{}{
		"sort": []map[string]interface{}{
			{
				"ID": map[string]interface{}{
					"order": "desc",
				},
			},
		},
		"size": 1,
	}
	if err := json.NewEncoder(&buf).Encode(q); err != nil {
		return "", 0, fmt.Errorf("error encoding query: %w", err)
	}

	res, err := client.Search(
		client.Search.WithIndex(index),
		client.Search.WithBody(&buf),
		client.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		return "", 0, err
	}
	defer res.Body.Close()

	if res.IsError() {
		errStr, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return "",0, fmt.Errorf("error reading response error: %w", err)
		}
		return "", 0, fmt.Errorf("error searching index %s: %s", index, string(errStr))
	}

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return "", 0, fmt.Errorf("failed parsing search response body: %v", err)
	}
	numHits := int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
	hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
	if len(hits) == 0 {
		return "", 0, fmt.Errorf("no hits to log")
	}
	hit := hits[0].(map[string]interface{})
	return hit["_id"].(string), numHits, nil
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