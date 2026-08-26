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
		legacyRowBinarySchema,
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

func TestClickHouseInsertURLForReducedSchema(t *testing.T) {
	t.Parallel()

	result, err := clickHouseInsertURL(
		"http://clickhouse.example:8123",
		"cgnat.huawei_cgn_nat_v2_2026_08_26",
		reducedRowBinarySchema,
		"reprocess_456",
	)
	if err != nil {
		t.Fatal(err)
	}
	query := mustQuery(t, result)
	for _, removedColumn := range []string{"event_time", "event_id", "event_type", "header", "protocol)"} {
		if strings.Contains(query, removedColumn) {
			t.Fatalf("reduced query unexpectedly contains %q: %q", removedColumn, query)
		}
	}
	if !strings.Contains(query, "protocol_id") || !strings.Contains(query, "packet_size") {
		t.Fatalf("reduced query does not contain expected columns: %q", query)
	}
}

func TestClickHouseCreateTableDDL(t *testing.T) {
	t.Parallel()

	ddl, err := clickHouseCreateTableDDL("cgnat.huawei_cgn_nat_v2_2026_07_17", legacyRowBinarySchema)
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

func TestClickHouseCreateTableDDLForReducedSchema(t *testing.T) {
	t.Parallel()

	ddl, err := clickHouseCreateTableDDL("cgnat.huawei_cgn_nat_v2_2026_08_26", reducedRowBinarySchema)
	if err != nil {
		t.Fatal(err)
	}
	for _, removedColumn := range []string{"event_time DateTime", "event_id FixedString", "event_type String", "header String", "protocol String"} {
		if strings.Contains(ddl, removedColumn) {
			t.Fatalf("reduced DDL unexpectedly contains %q: %q", removedColumn, ddl)
		}
	}
	if !strings.Contains(ddl, "start_time DateTime CODEC(ZSTD(3))") || !strings.Contains(ddl, "ORDER BY (end_time,") {
		t.Fatalf("reduced DDL does not contain optimized schema: %q", ddl)
	}
}

func mustQuery(t *testing.T, value string) string {
	t.Helper()
	parsed, err := url.Parse(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Query().Get("query")
}
