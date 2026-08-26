package main

import (
	"fmt"
	"net/url"
	"strings"
)

const (
	legacyRowBinarySchema  = 1
	reducedRowBinarySchema = 2
)

func normalizeRowBinarySchema(version int) (int, error) {
	if version == 0 {
		return legacyRowBinarySchema, nil
	}
	if version != legacyRowBinarySchema && version != reducedRowBinarySchema {
		return 0, fmt.Errorf("unsupported_rowbinary_schema_version version=%d", version)
	}
	return version, nil
}

func insertColumnsForSchema(version int) (string, error) {
	schema, err := normalizeRowBinarySchema(version)
	if err != nil {
		return "", err
	}
	if schema == reducedRowBinarySchema {
		return "start_time, end_time, router_ip, router_port, protocol_id, private_ip, private_port, public_ip, public_port, destination_ip, destination_port, packet_size", nil
	}
	return "event_time, start_time, end_time, event_id, event_type, header, router_ip, router_port, protocol_id, protocol, private_ip, private_port, public_ip, public_port, destination_ip, destination_port, packet_size", nil
}

func validateTableName(tableName string) error {
	parts := strings.Split(tableName, ".")
	if len(parts) == 0 || len(parts) > 2 {
		return fmt.Errorf("invalid_clickhouse_table_name table=%s", tableName)
	}

	for _, part := range parts {
		if !isValidIdentifier(part) {
			return fmt.Errorf("invalid_clickhouse_identifier part=%s table=%s", part, tableName)
		}
	}

	return nil
}

func clickHouseInsertURL(baseURL, tableName string, schemaVersion int, queryID string) (string, error) {
	if err := validateTableName(tableName); err != nil {
		return "", err
	}
	insertColumns, err := insertColumnsForSchema(schemaVersion)
	if err != nil {
		return "", err
	}

	endpoint, err := url.Parse(strings.TrimRight(baseURL, "/") + "/")
	if err != nil {
		return "", fmt.Errorf("invalid_clickhouse_url: %w", err)
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return "", fmt.Errorf("invalid_clickhouse_url_scheme scheme=%s", endpoint.Scheme)
	}
	if endpoint.Host == "" {
		return "", fmt.Errorf("invalid_clickhouse_url_missing_host")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) FORMAT RowBinary", tableName, insertColumns)
	values := endpoint.Query()
	values.Set("query", query)
	if queryID != "" {
		values.Set("query_id", queryID)
	}
	endpoint.RawQuery = values.Encode()
	return endpoint.String(), nil
}

func clickHouseCreateTableDDL(tableName string, schemaVersion int) (string, error) {
	if err := validateTableName(tableName); err != nil {
		return "", err
	}
	schema, err := normalizeRowBinarySchema(schemaVersion)
	if err != nil {
		return "", err
	}
	if schema == reducedRowBinarySchema {
		return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s
(
    start_time DateTime CODEC(ZSTD(3)),
    end_time DateTime,
    router_ip IPv4,
    router_port UInt16,
    protocol_id UInt8,
    private_ip IPv4,
    private_port UInt16,
    public_ip IPv4,
    public_port UInt16,
    destination_ip IPv4,
    destination_port UInt16,
    packet_size UInt16
)
ENGINE = MergeTree
ORDER BY (end_time, router_ip, private_ip, public_ip, destination_ip, private_port, public_port)
SETTINGS index_granularity = 8192`, tableName), nil
	}

	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s
(
    event_time DateTime,
    start_time DateTime,
    end_time DateTime,
    event_id FixedString(40),
    event_type String,
    header String,
    router_ip IPv4,
    router_port UInt16,
    protocol_id UInt8,
    protocol String,
    private_ip IPv4,
    private_port UInt16,
    public_ip IPv4,
    public_port UInt16,
    destination_ip IPv4,
    destination_port UInt16,
    packet_size UInt16
)
ENGINE = MergeTree
ORDER BY (event_time, router_ip, private_ip, public_ip, destination_ip, private_port, public_port)
SETTINGS index_granularity = 8192`, tableName), nil
}

func isValidIdentifier(value string) bool {
	if value == "" {
		return false
	}

	for index, char := range value {
		isLetter := (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')
		isDigit := char >= '0' && char <= '9'
		if index == 0 {
			if !isLetter && char != '_' {
				return false
			}
			continue
		}

		if !isLetter && !isDigit && char != '_' {
			return false
		}
	}

	return true
}
