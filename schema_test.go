package main

import (
	"strings"
	"testing"
)

func TestReducedClickHouseSchema(t *testing.T) {
	ddl := clickHouseCreateTableDDL("cgnat.huawei_cgn_nat_v2_2026_08_26")
	for _, removedColumn := range []string{"event_time DateTime", "event_id FixedString", "event_type String", "header String", "protocol String"} {
		if strings.Contains(ddl, removedColumn) {
			t.Fatalf("DDL unexpectedly contains %q: %q", removedColumn, ddl)
		}
	}
	if !strings.Contains(ddl, "start_time DateTime CODEC(ZSTD(3))") || !strings.Contains(ddl, "ORDER BY (end_time,") {
		t.Fatalf("DDL does not contain the reduced schema: %q", ddl)
	}
}

func TestReducedRowBinaryEvent(t *testing.T) {
	entry := NATLogEntry{
		StartTime:       1,
		EndTime:         2,
		RouterIP:        3,
		RouterPort:      4,
		ProtocolID:      6,
		PrivateIP:       7,
		PrivatePort:     8,
		PublicIP:        9,
		PublicPort:      10,
		DestinationIP:   11,
		DestinationPort: 12,
		PacketSize:      13,
	}

	body, err := appendRowBinaryEvent(nil, entry)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != 35 {
		t.Fatalf("RowBinary size=%d, want 35", len(body))
	}
}
