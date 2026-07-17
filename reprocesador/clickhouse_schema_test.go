package main

import (
	"net/url"
	"strings"
	"testing"
)

func TestValidateTableName(t *testing.T) {
	t.Parallel()

	valid := []string{
		"huawei_cgn_nat_v2_2026_07_17",
		"cgnat.huawei_cgn_nat_v2_2026_07_17",
	}
	for _, tableName := range valid {
		if err := validateTableName(tableName); err != nil {
			t.Fatalf("validateTableName(%q) returned %v", tableName, err)
		}
	}

	invalid := []string{"", "cgnat.", "a.b.c", "cgnat.table-name", "cgnat.table;DROP"}
	for _, tableName := range invalid {
		if err := validateTableName(tableName); err == nil {
			t.Fatalf("validateTableName(%q) unexpectedly succeeded", tableName)
		}
	}
}

func TestClickHouseInsertURL(t *testing.T) {
	t.Parallel()

	result, err := clickHouseInsertURL(
		"http://clickhouse.example:8123",
		"cgnat.huawei_cgn_nat_v2_2026_07_17",
		"reprocess_123",
	)
	if err != nil {
		t.Fatal(err)
	}

	parsed, err := url.Parse(result)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Query().Get("query_id") != "reprocess_123" {
		t.Fatalf("unexpected query_id: %q", parsed.Query().Get("query_id"))
	}
	query := parsed.Query().Get("query")
	if !strings.Contains(query, "FORMAT RowBinary") {
		t.Fatalf("query does not use RowBinary: %q", query)
	}
	if !strings.Contains(query, "event_id") || !strings.Contains(query, "packet_size") {
		t.Fatalf("query does not contain the expected columns: %q", query)
	}
}

func TestClickHouseCreateTableDDL(t *testing.T) {
	t.Parallel()

	ddl, err := clickHouseCreateTableDDL("cgnat.huawei_cgn_nat_v2_2026_07_17")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(ddl, "event_id FixedString(40)") {
		t.Fatalf("DDL does not contain event_id: %q", ddl)
	}
	if !strings.Contains(ddl, "ENGINE = MergeTree") {
		t.Fatalf("DDL does not use MergeTree: %q", ddl)
	}
}
