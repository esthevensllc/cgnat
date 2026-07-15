package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestClickHouseAlertSinkCreatesTableAndInsertsRecord(t *testing.T) {
	var mu sync.Mutex
	queries := make([]string, 0, 2)
	var inserted clickHouseAlertRow

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("database") != "cgnat" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		username, password, ok := request.BasicAuth()
		if !ok || username != "collector" || password != "secret" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		query := request.URL.Query().Get("query")
		mu.Lock()
		queries = append(queries, query)
		mu.Unlock()
		if strings.HasPrefix(strings.TrimSpace(query), "INSERT INTO") {
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read insert body: %v", err)
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			if err := json.Unmarshal(payload, &inserted); err != nil {
				t.Errorf("decode insert body: %v", err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := ClickHouseAlertConfig{
		URL:                     server.URL + "?database=cgnat",
		User:                    "collector",
		Password:                "secret",
		Table:                   "cgnat.collector_alerts",
		AutoCreate:              true,
		ConnectTimeoutSeconds:   1,
		OperationTimeoutSeconds: 2,
		RetrySeconds:            1,
		MaxRetrySeconds:         2,
	}
	sink, err := newClickHouseAlertSink(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	start := time.Date(2026, time.July, 15, 8, 30, 0, 123456000, peruTZ)
	record := AlertRecord{
		ID:        "89c94cf1-05d5-45d2-98be-10798cd77ce2",
		Hostname:  "collector-1",
		IP:        "10.96.167.132",
		StartTime: start,
		Name:      alertNoUDPTrafficName,
		Threshold: 100,
		Indicator: 100,
		State:     alertStateActive,
	}
	if err := sink.Upsert(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(queries) != 2 {
		t.Fatalf("queries = %d, want CREATE and INSERT", len(queries))
	}
	if !strings.Contains(queries[0], "ReplacingMergeTree(version)") {
		t.Fatalf("create query does not use ReplacingMergeTree: %s", queries[0])
	}
	if !strings.Contains(queries[1], "FORMAT JSONEachRow") {
		t.Fatalf("insert query format is unexpected: %s", queries[1])
	}
	if inserted.ID != record.ID || inserted.Name != alertNoUDPTrafficName || inserted.State != alertStateActive {
		t.Fatalf("inserted row = %#v", inserted)
	}
	if inserted.StartTime != "2026-07-15 08:30:00.123456" {
		t.Fatalf("start time = %s", inserted.StartTime)
	}
	if inserted.EndTime != nil {
		t.Fatalf("active alert end time = %v, want nil", *inserted.EndTime)
	}
	if inserted.Version == 0 || inserted.SentTime == "" {
		t.Fatalf("version/sent time not populated: %#v", inserted)
	}
}

func TestClickHouseAlertSinkKeepsFailedResponseAsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte("temporary failure"))
	}))
	defer server.Close()

	config := ClickHouseAlertConfig{
		URL:                     server.URL,
		Table:                   "cgnat.collector_alerts",
		AutoCreate:              false,
		ConnectTimeoutSeconds:   1,
		OperationTimeoutSeconds: 2,
		RetrySeconds:            1,
		MaxRetrySeconds:         2,
	}
	sink, err := newClickHouseAlertSink(config)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()

	err = sink.Upsert(context.Background(), AlertRecord{
		ID:        "89c94cf1-05d5-45d2-98be-10798cd77ce2",
		Hostname:  "collector-1",
		IP:        "10.96.167.132",
		StartTime: time.Now(),
		Name:      alertParseSLAName,
		Threshold: 1,
		Indicator: 2,
		State:     alertStateActive,
	})
	if err == nil || !strings.Contains(err.Error(), "clickhouse_status=503") {
		t.Fatalf("error = %v, want ClickHouse 503", err)
	}
}
