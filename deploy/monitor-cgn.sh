#!/bin/bash

set -u

PORT="${1:-9088}"
SERVICE="${2:-huawei-cgn-go-test}"
FAILED_DIR="${3:-/index2/huawei-cgn-go-test/failed}"

echo "=== FECHA ==="
date
echo

echo "=== SOCKETS UDP ${PORT} ==="
ss -u -n -a -p -m "sport = :${PORT}" 2>/dev/null \
  | awk 'NR == 1 || /UNCONN/ || /skmem/'
echo

echo "=== UDP GLOBAL DEL SERVIDOR ==="
awk '
/^Udp:/ && header == "" { header=$0; next }
/^Udp:/ && header != "" {
  split(header,names," "); split($0,values," ")
  for (i=2; i<=length(names); i++) metric[names[i]]=values[i]
  print "UdpInDatagrams", metric["InDatagrams"]
  print "UdpNoPorts", metric["NoPorts"]
  print "UdpInErrors", metric["InErrors"]
  print "UdpRcvbufErrors", metric["RcvbufErrors"]
}' /proc/net/snmp
echo

METRICS="$(journalctl -u "$SERVICE" -b --no-pager 2>/dev/null | grep 'metrics live_batch_mode' | tail -1)"

echo "=== COLLECTOR ==="
printf '%s\n' "$METRICS" | awk '
{
  for (i=1; i<=NF; i++) {
    split($i,pair,"=")
    if (pair[1] != "") value[pair[1]]=pair[2]
  }
  keys[1]="total_received"
  keys[2]="total_packet_processed"
  keys[3]="total_parsed"
  keys[4]="total_inserted"
  keys[5]="total_packet_queue_drops"
  keys[6]="total_insert_errors"
  keys[7]="total_failed_batch_spooled"
  keys[8]="total_failed_rows_spooled"
  keys[9]="total_failed_spool_errors"
  keys[10]="pps_received_10s"
  keys[11]="pps_packet_processed_10s"
  keys[12]="rps_parsed_10s"
  keys[13]="rps_inserted_10s"
  keys[14]="queue_packet"
  keys[15]="queue_batch"
  keys[16]="workers_packet"
  keys[17]="workers_insert"
  keys[18]="udp_read_batches_10s"
  keys[19]="udp_average_batch_10s"
  keys[20]="udp_full_batch_pct_10s"
  keys[21]="udp_read_errors_10s"
  keys[22]="udp_receiver_min_10s"
  keys[23]="udp_receiver_max_10s"
  for (i=1; i<=23; i++) printf "%-35s %s\n", keys[i], value[keys[i]]
}'
echo

echo "=== DISTRIBUCION POR SOCKET, ULTIMOS 10s ==="
printf '%s\n' "$METRICS" \
  | sed -n 's/.*udp_receiver_packets_10s=\([^ ]*\).*/\1/p' \
  | tr ',' '\n' \
  | awk -F: '{ printf "socket_%-4s %s paquetes\n", $1, $2 }'
echo

echo "=== FAILED SPOOL ==="
if [ -d "$FAILED_DIR" ]; then
  echo "open_files $(find "$FAILED_DIR/open" -type f 2>/dev/null | wc -l)"
  echo "done_files $(find "$FAILED_DIR/done" -type f -name '*.rowbinary.done' 2>/dev/null | wc -l)"
  echo "meta_files $(find "$FAILED_DIR/done" -type f -name '*.json' 2>/dev/null | wc -l)"
  du -sh "$FAILED_DIR" 2>/dev/null
else
  echo "FAILED_DIR no existe: $FAILED_DIR"
fi
