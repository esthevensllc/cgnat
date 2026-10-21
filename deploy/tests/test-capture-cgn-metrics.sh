#!/bin/bash
set -euo pipefail
export LC_ALL=C
DEPLOY_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_DIR="$(mktemp -d)"
trap 'rm -f -- "$TEST_DIR"/*; rmdir -- "$TEST_DIR"' EXIT

sample() {
  local timestamp="$1" received="$2" processed="$3" parsed="$4" inserted="$5" spooled="$6" failed_live="${7:-0}"
  local parse_errors="${8:-0}" udp_drops="${9:-0}"
  echo "$timestamp host collector: metrics live_batch_mode=packet_worker_direct total_received=$received total_packet_processed=$processed total_parsed=$parsed total_inserted=$inserted total_udp_kernel_drops=$udp_drops total_packet_queue_drops=0 total_parse_errors=$parse_errors total_event_marshal_errors=0 total_insert_errors=0 total_insert_dropped_rows=$failed_live total_failed_batch_spooled=0 total_failed_rows_spooled=$spooled total_failed_spool_errors=0 total_live_insert_skipped_rows=0 total_table_create_errors=0 queue_packet=0/1000 queue_batch=0/512 udp_read_errors_10s=0"
}
sample 1000.000000 1000 990 1980 1880 0 > "$TEST_DIR/healthy.log"
sample 1900.000000 11000 10990 21980 21880 0 >> "$TEST_DIR/healthy.log"
awk -v csv_file="$TEST_DIR/healthy.csv" -f "$DEPLOY_DIR/summarize-cgn-metrics.awk" \
  "$TEST_DIR/healthy.log" > "$TEST_DIR/healthy.txt"
grep -Eq 'Paquetes UDP recibidos +10000$' "$TEST_DIR/healthy.txt"
grep -Eq 'Registros parseados/serializados +20000$' "$TEST_DIR/healthy.txt"
grep -Eq 'Registros confirmados por ClickHouse \(live\) +20000$' "$TEST_DIR/healthy.txt"
grep -q 'Saldo filas sin confirmacion live ni spool: inicio=100 fin=100 variacion=+0' "$TEST_DIR/healthy.txt"
awk -F, 'NF != 26 { exit 1 }' "$TEST_DIR/healthy.csv"

# Live failures saved to spool must not be subtracted twice.
sample 1000 1000 1000 2000 2000 0 > "$TEST_DIR/spool.log"
sample 1900 2000 2000 4000 3000 1000 1000 >> "$TEST_DIR/spool.log"
bash "$DEPLOY_DIR/capture-cgn-metrics.sh" --analyze "$TEST_DIR/spool.log" > "$TEST_DIR/spool.txt"
grep -q 'Saldo filas sin confirmacion live ni spool: inicio=0 fin=0 variacion=+0' "$TEST_DIR/spool.txt"

sample 1000 1000 1000 1980 1980 0 0 10 5 > "$TEST_DIR/errors.log"
sample 1900 11000 11000 21960 21960 0 0 20 15 >> "$TEST_DIR/errors.log"
awk -f "$DEPLOY_DIR/summarize-cgn-metrics.awk" "$TEST_DIR/errors.log" > "$TEST_DIR/errors.txt"
grep -Eq 'Paquetes parseados sin error +9990$' "$TEST_DIR/errors.txt"
grep -Eq 'Drops UDP observados por el collector +10$' "$TEST_DIR/errors.txt"

sample 1000 1000 1000 2000 1900 0 > "$TEST_DIR/drain.log"
sample 1900 2000 2000 4000 4000 0 >> "$TEST_DIR/drain.log"
awk -f "$DEPLOY_DIR/summarize-cgn-metrics.awk" "$TEST_DIR/drain.log" > "$TEST_DIR/drain.txt"
grep -q 'Saldo filas sin confirmacion live ni spool: inicio=100 fin=0 variacion=-100' "$TEST_DIR/drain.txt"
sed 's/ total_udp_kernel_drops=[0-9]*//g; s/metrics /metrics parse_only=true /g' \
  "$TEST_DIR/healthy.log" > "$TEST_DIR/missing.log"
awk -f "$DEPLOY_DIR/summarize-cgn-metrics.awk" "$TEST_DIR/missing.log" > "$TEST_DIR/missing.txt"
grep -Eq 'Drops UDP observados por el collector +NO_DISPONIBLE$' "$TEST_DIR/missing.txt"
grep -q 'PARSE_ONLY=true' "$TEST_DIR/missing.txt"

expect_invalid() {
  local code=0
  awk -f "$DEPLOY_DIR/summarize-cgn-metrics.awk" "$1" > "$TEST_DIR/invalid.txt" || code=$?
  [[ "$code" == 2 ]]
  grep -q 'CAPTURA NO VALIDA' "$TEST_DIR/invalid.txt"
}
sample 1000 1000 1000 2000 2000 0 > "$TEST_DIR/reset.log"
sample 1900 500 500 1000 1000 0 >> "$TEST_DIR/reset.log"
expect_invalid "$TEST_DIR/reset.log"
sample 1000 1000 1000 2000 2000 0 > "$TEST_DIR/restart.log"
echo '1100 collector_started' >> "$TEST_DIR/restart.log"
sample 1900 10000 10000 20000 20000 0 >> "$TEST_DIR/restart.log"
expect_invalid "$TEST_DIR/restart.log"
printf '' > "$TEST_DIR/empty.log"
expect_invalid "$TEST_DIR/empty.log"
code=0
awk -v expected_first=999 -f "$DEPLOY_DIR/summarize-cgn-metrics.awk" "$TEST_DIR/healthy.log" > "$TEST_DIR/invalid.txt" || code=$?
[[ "$code" == 2 ]]
code=0
awk -v capture_end=2000 -f "$DEPLOY_DIR/summarize-cgn-metrics.awk" "$TEST_DIR/healthy.log" > "$TEST_DIR/invalid.txt" || code=$?
[[ "$code" == 2 ]]

# Large production counters must retain their integer value (not 32-bit %d).
sample 1000 9000000000 9000000000 18000000000 18000000000 0 > "$TEST_DIR/large.log"
sample 1900 14000000000 14000000000 28000000000 28000000000 0 >> "$TEST_DIR/large.log"
awk -f "$DEPLOY_DIR/summarize-cgn-metrics.awk" "$TEST_DIR/large.log" > "$TEST_DIR/large.txt"
grep -Eq 'Paquetes UDP recibidos +5000000000$' "$TEST_DIR/large.txt"
grep -Eq 'Registros parseados/serializados +10000000000$' "$TEST_DIR/large.txt"

# Exercise the complete capture with fake system services, never a real collector.
export CGN_CAPTURE_FIXTURE="$TEST_DIR/service.log"
export CGN_BASELINE_FIXTURE="$TEST_DIR/baseline.log"
now="$(date +%s)"
sample "$((now-10)).000000" 1000 1000 2000 2000 0 > "$CGN_CAPTURE_FIXTURE"
sample "$now.000000" 2000 2000 4000 4000 0 >> "$CGN_CAPTURE_FIXTURE"
sample "$((now-50)).000000" 500 500 1000 1000 0 > "$CGN_BASELINE_FIXTURE"
sample "$((now-10)).000000" 1000 1000 2000 2000 0 >> "$CGN_BASELINE_FIXTURE"
systemctl() {
  if [[ "$1" == is-active ]]; then return 0; fi
  printf 'MainPID=123\nInvocationID=test-invocation\nActiveState=active\n'
}
journalctl() {
  if [[ "${CGN_TEST_JOURNAL_DENIED:-0}" == 1 ]]; then
    echo 'No journal files were opened due to insufficient permissions.' >&2
    return 1
  fi
  # Retain the legacy header deliberately, even if the caller requests --quiet.
  printf '%s\n' '-- Logs begin at Mon 2026-07-20 16:29:51 -05, end at Wed 2026-10-21 14:32:57 -05. --'
  if [[ "${CGN_TEST_NO_METRICS:-0}" == 1 ]]; then return 0; fi
  if [[ " $* " == *' --until '* ]]; then /usr/bin/cat "$CGN_CAPTURE_FIXTURE"
  else /usr/bin/cat "$CGN_BASELINE_FIXTURE"; fi
}
ss() { echo 'State Recv-Q Send-Q Local Address:Port Peer Address:Port'; }
hostname() { echo test-collector; }
cat() {
  case "$1" in
    /proc/sys/kernel/random/boot_id) echo test-boot ;;
    /proc/net/snmp) printf 'Udp: InDatagrams NoPorts InErrors RcvbufErrors\nUdp: 1000 0 0 0\n' ;;
    /proc/net/dev) echo 'Inter-| Receive | Transmit' ;;
    *) /usr/bin/cat "$@" ;;
  esac
}
export -f systemctl journalctl ss hostname cat
bash "$DEPLOY_DIR/capture-cgn-metrics.sh" huawei-cgn-go 1 9088 \
  "$TEST_DIR/results" "$TEST_DIR/no-spool" > "$TEST_DIR/capture.txt"
capture_dir="$(find "$TEST_DIR/results" -mindepth 1 -maxdepth 1 -type d)"
[[ -f "$capture_dir/metrics.csv" && -f "$capture_dir/summary.txt" ]]
grep -Eq 'Registros parseados/serializados +2000$' "$capture_dir/summary.txt"
grep -q 'RcvbufErrors delta=0' "$capture_dir/udp-summary.txt"
grep -q 'NO_DISPONIBLE: directorio ausente' "$capture_dir/spool-summary.txt"
grep -q "^$((now-10)).000000 " "$capture_dir/baseline.log"
if grep -q '^-- Logs begin' "$capture_dir/collector.log"; then
  echo 'El informe no debe conservar cabeceras del journal.' >&2
  exit 1
fi
rm -f -- "$capture_dir"/*
rmdir -- "$capture_dir" "$TEST_DIR/results"

code=0
CGN_TEST_JOURNAL_DENIED=1 bash "$DEPLOY_DIR/capture-cgn-metrics.sh" huawei-cgn-go 1 9088 \
  "$TEST_DIR/denied-results" > "$TEST_DIR/denied.txt" 2>&1 || code=$?
[[ "$code" == 1 && ! -e "$TEST_DIR/denied-results" ]]
grep -q 'No se pudo leer el journal' "$TEST_DIR/denied.txt"
code=0
CGN_TEST_NO_METRICS=1 bash "$DEPLOY_DIR/capture-cgn-metrics.sh" huawei-cgn-go 1 9088 \
  "$TEST_DIR/empty-results" > "$TEST_DIR/no-metrics.txt" 2>&1 || code=$?
[[ "$code" == 1 && ! -e "$TEST_DIR/empty-results" ]]
grep -q 'No hay metricas recientes' "$TEST_DIR/no-metrics.txt"
echo 'OK: captura completa, cabecera journal, muestra mas reciente, permisos, unidades, saldos, spool, reinicios, CSV.'
