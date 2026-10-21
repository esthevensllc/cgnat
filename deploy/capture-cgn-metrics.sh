#!/bin/bash
# Read existing counters; never restart or reconfigure the collector.
set -euo pipefail
export LC_ALL=C
umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ANALYZER="$SCRIPT_DIR/summarize-cgn-metrics.awk"

if [[ "${1:-}" == "--analyze" ]]; then
  [[ $# == 2 ]] || { echo "Uso: $0 --analyze collector.log" >&2; exit 1; }
  exec awk -f "$ANALYZER" "$2"
fi
if [[ "${1:-}" == "--help" || $# -gt 5 ]]; then
  echo "Uso: $0 [servicio] [segundos] [puerto] [directorio_resultados] [failed_spool]"
  echo "Defaults: huawei-cgn-go 900 9088 /var/tmp/cgn-metrics /index2/huawei-cgn-go/failed"
  exit 0
fi

SERVICE="${1:-huawei-cgn-go}"
SECONDS_TO_CAPTURE="${2:-900}"
PORT="${3:-9088}"
OUTPUT_BASE="${4:-/var/tmp/cgn-metrics}"
FAILED_DIR="${5:-/index2/huawei-cgn-go/failed}"
[[ "$SERVICE" =~ ^[a-zA-Z0-9_.@:-]+$ && "$SERVICE" != -* ]] || exit 1
[[ "$SECONDS_TO_CAPTURE" =~ ^[1-9][0-9]{0,4}$ ]] || exit 1
[[ "$PORT" =~ ^[1-9][0-9]{0,4}$ ]] && (( PORT <= 65535 )) || exit 1
for dependency in journalctl systemctl awk date sleep mkdir mktemp hostname find ss tee cmp cat; do
  command -v "$dependency" >/dev/null || { echo "Falta: $dependency" >&2; exit 1; }
done
[[ -r "$ANALYZER" ]] || { echo "Falta: $ANALYZER" >&2; exit 1; }
systemctl is-active --quiet "$SERVICE" || { echo "Servicio inactivo: $SERVICE" >&2; exit 1; }

START="$(date +%s)"
BASELINE="$(journalctl -u "$SERVICE" -b --since "@$((START - 60))" \
  -n 1 --no-pager -l -o short-unix --grep='metrics .*live_batch_mode=')"
[[ "$BASELINE" == *'total_received='* ]] || {
  echo "No hay metricas recientes. Verificar servicio y permisos de journalctl (ejecutar con sudo)." >&2
  exit 1
}
BASELINE_SECOND="${BASELINE%%.*}"
BASELINE_TIME="${BASELINE%% *}"
[[ "$BASELINE_SECOND" =~ ^[0-9]+$ ]] || exit 1
mkdir -p -- "$OUTPUT_BASE"
OUT="$(mktemp -d "$OUTPUT_BASE/$(hostname -s)_$(date -u +%Y%m%dT%H%M%SZ)_XXXXXX")"
printf '%s\n' "$BASELINE" > "$OUT/baseline.log"

snapshot() {
  local label="$1" subdir count_file
  date --iso-8601=seconds > "$OUT/time.$label.txt"
  systemctl show "$SERVICE" -p MainPID -p InvocationID -p ActiveState > "$OUT/service.$label.txt"
  cat /proc/sys/kernel/random/boot_id > "$OUT/boot.$label.txt"
  cat /proc/net/snmp > "$OUT/udp.$label.txt"
  cat /proc/net/dev > "$OUT/interfaces.$label.txt"
  ss -u -n -a -p -m "sport = :$PORT" > "$OUT/sockets.$label.txt" 2>> "$OUT/check-errors.log" || true
  for subdir in open done; do
    count_file="$OUT/spool-$subdir.$label.txt"
    if [[ -d "$FAILED_DIR/$subdir" ]]; then
      if ! find "$FAILED_DIR/$subdir" -maxdepth 1 -type f -name '*.rowbinary.*' ! -name '*.json' \
        -printf '%s\n' > "$OUT/.spool-sizes" 2>> "$OUT/check-errors.log"; then
        echo 'NO_DISPONIBLE: error al listar' > "$count_file"
      else
        awk '{ n++; bytes += $1 } END { printf "archivos=%.0f bytes=%.0f\n", n, bytes }' \
          "$OUT/.spool-sizes" > "$count_file"
      fi
    else
      echo 'NO_DISPONIBLE: directorio ausente' > "$count_file"
    fi
  done
}

{
  echo "servidor=$(hostname -f)"
  echo "servicio=$SERVICE puerto_udp=$PORT failed_spool=$FAILED_DIR"
  echo "segundos_solicitados=$SECONDS_TO_CAPTURE"
  echo "inicio=$(date --iso-8601=seconds)"
  echo "arquitectura=$(uname -m)"
} > "$OUT/context.txt"
snapshot start
echo "Capturando $SECONDS_TO_CAPTURE segundos en $(hostname -s). Resultados: $OUT"
trap 'echo "Captura interrumpida. Evidencia parcial: $OUT" >&2; exit 130' INT TERM
sleep "$SECONDS_TO_CAPTURE"
END="$(date +%s)"
snapshot end
echo "fin=$(date --iso-8601=seconds)" >> "$OUT/context.txt"

# Retain metrics and a restart marker without logging configuration or credentials.
journalctl -u "$SERVICE" --since "@$BASELINE_SECOND" --until "@$END" \
  --no-pager -l -o short-unix --grep='metrics .*live_batch_mode=|collector_started' \
  | awk '/collector_started/ { print $1, "collector_started"; next } { print }' > "$OUT/collector.log"

INVALID=0
if ! cmp -s "$OUT/service.start.txt" "$OUT/service.end.txt" || \
   ! cmp -s "$OUT/boot.start.txt" "$OUT/boot.end.txt"; then
  echo "ADVERTENCIA: cambio el proceso o el servidor durante la captura; repetirla." | tee "$OUT/restart.txt"
  INVALID=1
fi
set +e
awk -v csv_file="$OUT/metrics.csv" -v invalid_capture="$INVALID" \
  -v expected_first="$BASELINE_TIME" -v capture_end="$END" \
  -f "$ANALYZER" "$OUT/collector.log" | tee "$OUT/summary.txt"
ANALYSIS_STATUS=${PIPESTATUS[0]}
set -e

awk '
  /^Udp:/ && $2 == "InDatagrams" { for (i=2; i<=NF; i++) names[i]=$i; next }
  /^Udp:/ {
    for (i=2; i<=NF; i++) {
      if (FNR == NR) first[names[i]]=$i
      else last[names[i]]=$i
    }
  }
  END {
    print "UDP GLOBAL DEL SERVIDOR (incluye todos los puertos, ventana distinta de los logs):"
    split("InDatagrams InErrors RcvbufErrors NoPorts", keys, " ")
    for (i=1; i<=4; i++) {
      k=keys[i]
      printf "%s delta=%.0f\n", k, last[k]-first[k]
    }
    print "InErrors y RcvbufErrors se solapan: NO sumarlos."
  }
' "$OUT/udp.start.txt" "$OUT/udp.end.txt" | tee "$OUT/udp-summary.txt"
{
  echo "SPOOL ACTUAL (archivos en open/done; no incluye reprocessed):"
  for label in start end; do
    for subdir in open done; do
      printf '%s %s: ' "$label" "$subdir"
      cat "$OUT/spool-$subdir.$label.txt"
    done
  done
} | tee "$OUT/spool-summary.txt"
echo "Captura finalizada: $OUT"
exit "$ANALYSIS_STATUS"
