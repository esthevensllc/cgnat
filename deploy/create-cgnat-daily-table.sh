#!/usr/bin/env bash
set -euo pipefail

# Pre-create the collector table for one event date. The collector creates
# subsequent daily tables itself when its first batch for that date arrives.
config_file=${1:-/etc/huawei-cgn-go/huawei-cgn-go-test.env}
event_date=${2:-$(TZ=America/Lima date +%Y_%m_%d)}

if [[ ! -r "$config_file" ]]; then
  printf 'No se puede leer %s\n' "$config_file" >&2
  exit 1
fi
if [[ ! "$event_date" =~ ^[0-9]{4}_[0-9]{2}_[0-9]{2}$ ]]; then
  printf 'La fecha debe tener formato YYYY_MM_DD\n' >&2
  exit 1
fi

get_config() {
  awk -v key="$1" 'index($0, key "=") == 1 { value = substr($0, length(key) + 2) } END { print value }' "$config_file" | tr -d '\r'
}

table_base=$(get_config CLICKHOUSE_TABLE)
daily_tables=$(get_config CLICKHOUSE_DAILY_TABLES)
if [[ ! "$table_base" =~ ^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  printf 'CLICKHOUSE_TABLE invalida o ausente en %s\n' "$config_file" >&2
  exit 1
fi
if [[ -n "$daily_tables" && "$daily_tables" != true && "$daily_tables" != false ]]; then
  printf 'CLICKHOUSE_DAILY_TABLES debe ser true o false\n' >&2
  exit 1
fi

database=${table_base%%.*}
table_name=$table_base
if [[ "$daily_tables" != false ]]; then
  table_name="${table_base}_${event_date}"
fi

printf 'Creando %s segun %s\n' "$table_name" "$config_file"
sql=$(cat <<SQL
CREATE DATABASE IF NOT EXISTS ${database};
CREATE TABLE IF NOT EXISTS ${table_name}
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
SETTINGS index_granularity = 8192;
SHOW CREATE TABLE ${table_name};
SQL
)
clickhouse-client --user=admin --password --multiquery --query="$sql"
