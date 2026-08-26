package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProcessCycleInsertsAndDeletesPair(t *testing.T) {
	t.Parallel()

	body := []byte("rowbinary-payload")
	var createRequests atomic.Int32
	var insertRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query().Get("query")
		switch {
		case strings.HasPrefix(strings.TrimSpace(query), "CREATE TABLE"):
			createRequests.Add(1)
		case strings.HasPrefix(query, "INSERT INTO"):
			insertRequests.Add(1)
			actual, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read request body: %v", err)
			}
			if string(actual) != string(body) {
				t.Errorf("unexpected request body: %q", actual)
			}
		default:
			t.Errorf("unexpected query: %q", query)
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := testConfig(t, server.URL)
	dataPath, metaPath := writeTestPair(t, cfg.doneDir, body, validTestMeta(len(body)))
	processor := newReprocessor(cfg)

	result, err := processor.processCycle(context.Background(), processOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.processed != 1 || result.insertedRows != 23 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if createRequests.Load() != 1 || insertRequests.Load() != 1 {
		t.Fatalf("unexpected requests: create=%d insert=%d", createRequests.Load(), insertRequests.Load())
	}
	assertNotExists(t, dataPath)
	assertNotExists(t, metaPath)
}

func TestProcessCycleKeepsPairWhenInsertFails(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query().Get("query")
		if strings.HasPrefix(strings.TrimSpace(query), "CREATE TABLE") {
			writer.WriteHeader(http.StatusOK)
			return
		}
		http.Error(writer, "insert rejected", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	cfg := testConfig(t, server.URL)
	body := []byte("rowbinary-payload")
	dataPath, metaPath := writeTestPair(t, cfg.doneDir, body, validTestMeta(len(body)))
	processor := newReprocessor(cfg)

	result, err := processor.processCycle(context.Background(), processOptions{})
	if err == nil {
		t.Fatal("expected process cycle to fail")
	}
	if result.failed != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	assertExists(t, dataPath)
	assertExists(t, metaPath)
}

func TestProcessCycleQuarantinesInvalidPair(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, "http://127.0.0.1:1")
	body := []byte("rowbinary-payload")
	meta := validTestMeta(len(body) + 1)
	dataPath, metaPath := writeTestPair(t, cfg.doneDir, body, meta)
	processor := newReprocessor(cfg)

	result, err := processor.processCycle(context.Background(), processOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.quarantined != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	assertNotExists(t, dataPath)
	assertNotExists(t, metaPath)
	assertExists(t, filepath.Join(cfg.badDir, filepath.Base(dataPath)))
	assertExists(t, filepath.Join(cfg.badDir, filepath.Base(metaPath)))
}

func TestProcessCycleSkipsIncompletePair(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t, "http://127.0.0.1:1")
	dataPath := filepath.Join(cfg.doneDir, "failed_without_metadata.rowbinary.done")
	if err := os.WriteFile(dataPath, []byte("payload"), 0640); err != nil {
		t.Fatal(err)
	}
	processor := newReprocessor(cfg)

	result, err := processor.processCycle(context.Background(), processOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.incompletePairs != 1 || result.processed != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	assertExists(t, dataPath)
}

func TestProcessCycleArchivesSuccessfulPair(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := testConfig(t, server.URL)
	cfg.successAction = successActionArchive
	body := []byte("rowbinary-payload")
	dataPath, metaPath := writeTestPair(t, cfg.doneDir, body, validTestMeta(len(body)))
	processor := newReprocessor(cfg)

	if _, err := processor.processCycle(context.Background(), processOptions{}); err != nil {
		t.Fatal(err)
	}
	assertNotExists(t, dataPath)
	assertNotExists(t, metaPath)
	assertExists(t, filepath.Join(cfg.archiveDir, filepath.Base(dataPath)))
	assertExists(t, filepath.Join(cfg.archiveDir, filepath.Base(metaPath)))
}

func TestDryRunDoesNotInsertOrMoveFiles(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := testConfig(t, server.URL)
	body := []byte("rowbinary-payload")
	dataPath, metaPath := writeTestPair(t, cfg.doneDir, body, validTestMeta(len(body)))
	processor := newReprocessor(cfg)

	if _, err := processor.processCycle(context.Background(), processOptions{dryRun: true}); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 0 {
		t.Fatalf("dry run made %d HTTP requests", requests.Load())
	}
	assertExists(t, dataPath)
	assertExists(t, metaPath)
}

func testConfig(t *testing.T, clickHouseURL string) config {
	t.Helper()

	spoolBase := t.TempDir()
	cfg := config{
		clickHouseURL:   clickHouseURL,
		spoolBase:       spoolBase,
		doneDir:         filepath.Join(spoolBase, "done"),
		archiveDir:      filepath.Join(spoolBase, "reprocessed"),
		badDir:          filepath.Join(spoolBase, "bad"),
		workers:         1,
		pollInterval:    time.Second,
		httpTimeout:     5 * time.Second,
		maxRetries:      1,
		scanLimit:       100,
		autoCreateTable: true,
		successAction:   successActionDelete,
	}
	for _, directory := range []string{cfg.doneDir, cfg.archiveDir, cfg.badDir} {
		if err := os.MkdirAll(directory, 0750); err != nil {
			t.Fatal(err)
		}
	}
	return cfg
}

func validTestMeta(bytes int) failedInsertBatchMeta {
	return failedInsertBatchMeta{
		SchemaVersion: reducedRowBinarySchema,
		CreatedTime:   "2026-07-17 10:00:00",
		TableName:     "cgnat.huawei_cgn_nat_v2_2026_07_17",
		Rows:          23,
		Bytes:         bytes,
		Format:        "RowBinary",
		Error:         "live_insert_queue_overload queue_batch=500/512",
	}
}

func writeTestPair(t *testing.T, doneDir string, body []byte, meta failedInsertBatchMeta) (string, string) {
	t.Helper()

	dataPath := filepath.Join(doneDir, "failed_test_rows23.rowbinary.done")
	metaPath := dataPath + ".json"
	if err := os.WriteFile(dataPath, body, 0640); err != nil {
		t.Fatal(err)
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metaPath, metaJSON, 0640); err != nil {
		t.Fatal(err)
	}
	return dataPath, metaPath
}

func assertExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected %s to exist: %v", path, err)
	}
}

func assertNotExists(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected %s to be absent, got %v", path, err)
	}
}
