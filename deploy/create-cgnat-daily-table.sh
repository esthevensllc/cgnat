#!/usr/bin/env bash
set -euo pipefail

# Pre-create the collector table for one event date and its alerts table.
# The collector creates subsequent daily event tables on the first batch.
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
alert_table=$(get_config CLICKHOUSE_ALERT_TABLE)
alert_table=${alert_table:-cgnat.collector_alerts}
if [[ ! "$table_base" =~ ^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  printf 'CLICKHOUSE_TABLE invalida o ausente en %s\n' "$config_file" >&2
  exit 1
fi
if [[ -n "$daily_tables" && "$daily_tables" != true && "$daily_tables" != false ]]; then
  printf 'CLICKHOUSE_DAILY_TABLES debe ser true o false\n' >&2
  exit 1
fi
if [[ ! "$alert_table" =~ ^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  printf 'CLICKHOUSE_ALERT_TABLE invalida en %s\n' "$config_file" >&2
  exit 1
fi

database=${table_base%%.*}
alert_database=${alert_table%%.*}
table_name=$table_base
if [[ "$daily_tables" != false ]]; then
  table_name="${table_base}_${event_date}"
fi

printf 'Preparando %s y %s segun %s\n' "$table_name" "$alert_table" "$config_file"
sql=$(cat <<SQL
CREATE DATABASE IF NOT EXISTS ${database};
CREATE DATABASE IF NOT EXISTS ${alert_database};
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
CREATE TABLE IF NOT EXISTS ${alert_table}
(
    id UUID,
    hostname LowCardinality(String),
    ip String,
    fecha_inicio DateTime64(6, 'America/Lima'),
    fecha_fin Nullable(DateTime64(6, 'America/Lima')),
    nombre LowCardinality(String),
    umbral Float64,
    porcentaje_indicador Float64,
    estado LowCardinality(String),
    fecha_envio DateTime64(6, 'America/Lima'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(fecha_inicio)
ORDER BY id
SETTINGS index_granularity = 8192;
SHOW CREATE TABLE ${alert_table};
SQL
)
clickhouse-client --user=admin --password --multiquery --query="$sql"
