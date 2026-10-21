# Input: journalctl -o short-unix, filtered to collector metrics/restart markers.
BEGIN {
    count = split("total_received total_packet_processed total_parsed total_inserted total_udp_kernel_drops total_packet_queue_drops total_parse_errors total_event_marshal_errors total_insert_errors total_insert_dropped_rows total_failed_batch_spooled total_failed_rows_spooled total_failed_spool_errors total_live_insert_skipped_rows total_table_create_errors", keys, " ")
    if (csv_file != "") {
        printf "epoch" > csv_file
        for (k=1; k<=count; k++) printf ",%s", keys[k] > csv_file
        print ",queue_packet,queue_batch,interval_seconds,received_delta,processed_delta,parsed_delta,inserted_delta,received_pps,parsed_rps,inserted_rps" > csv_file
    }
}
/collector_started/ { if (samples) invalid_capture=1; next }
/metrics .*live_batch_mode=/ {
    delete current
    for (i=2; i<=NF; i++) {
        equals=index($i, "=")
        if (equals) current[substr($i, 1, equals-1)]=substr($i, equals+1)
    }
    timestamp=$1+0
    if (!timestamp || current["total_received"] == "") next
    for (k=1; k<=count; k++) {
        key=keys[k]
        if (!(key in current)) { missing[key]=1; continue }
        if (samples && key in previous && current[key]+0 < previous[key]+0) invalid_capture=1
    }
    if (current["parse_only"] == "true") parse_only=1
    if (!samples) {
        first_time=timestamp
        for (key in current) first[key]=current[key]
    } else {
        interval=timestamp-last_time
        if (interval <= 0) invalid_capture=1
        if (interval > 30) gaps++
    }
    split(current["queue_packet"], packet_queue, "/")
    split(current["queue_batch"], batch_queue, "/")
    if (packet_queue[1]+0 > max_packet_queue) max_packet_queue=packet_queue[1]+0
    if (batch_queue[1]+0 > max_batch_queue) max_batch_queue=batch_queue[1]+0
    if (samples && current["udp_read_errors_10s"]+0 > 0) read_errors += current["udp_read_errors_10s"]
    if (csv_file != "") {
        printf "%.6f", timestamp > csv_file
        for (k=1; k<=count; k++) printf ",%s", current[keys[k]] > csv_file
        printf ",%s,%s", current["queue_packet"], current["queue_batch"] > csv_file
        if (samples && interval > 0) {
            received=current["total_received"]-previous["total_received"]
            processed=current["total_packet_processed"]-previous["total_packet_processed"]
            parsed=current["total_parsed"]-previous["total_parsed"]
            inserted=current["total_inserted"]-previous["total_inserted"]
            printf ",%.3f,%.0f,%.0f,%.0f,%.0f,%.2f,%.2f,%.2f", interval, received, processed, parsed, inserted, received/interval, parsed/interval, inserted/interval > csv_file
        } else printf ",,,,,,,," > csv_file
        print "" > csv_file
    }
    for (key in current) { last[key]=current[key]; previous[key]=current[key] }
    last_time=timestamp
    samples++
}
function delta(key) { return last[key]-first[key] }
function show_delta(label, key) {
    if (key in missing) printf "%-44s NO_DISPONIBLE\n", label
    else printf "%-44s %.0f\n", label, delta(key)
}
function balance(values, kind) {
    if (kind == "packets") return values["total_received"]-values["total_packet_processed"]-values["total_packet_queue_drops"]
    return values["total_parsed"]-values["total_inserted"]-values["total_failed_rows_spooled"]
}
END {
    if (samples < 2) {
        print "CAPTURA NO VALIDA: se necesitan al menos dos muestras de metricas."
        exit 2
    }
    if (invalid_capture) {
        print "CAPTURA NO VALIDA: reinicio, cambio de proceso o contadores reiniciados. Repetir."
        exit 2
    }
    if (expected_first && (first_time < expected_first-0.00001 || first_time > expected_first+0.00001)) {
        print "CAPTURA NO VALIDA: falta la muestra inicial (journal rotado o incompleto)."
        exit 2
    }
    if (capture_end && capture_end-last_time > 30) {
        print "CAPTURA NO VALIDA: no hay metricas al final de la ventana (mas de 30 s)."
        exit 2
    }
    duration=last_time-first_time
    printf "Ventana de metricas: %s -> %s\n", strftime("%Y-%m-%d %H:%M:%S %z", first_time), strftime("%Y-%m-%d %H:%M:%S %z", last_time)
    printf "Duracion efectiva: %.1f segundos; muestras: %d\n\n", duration, samples
    show_delta("Paquetes UDP recibidos", "total_received")
    show_delta("Paquetes con procesamiento finalizado", "total_packet_processed")
    if (!("total_parse_errors" in missing)) printf "%-44s %.0f\n", "Paquetes parseados sin error", delta("total_packet_processed")-delta("total_parse_errors")
    show_delta("Paquetes con error de parseo", "total_parse_errors")
    if (delta("total_packet_processed") > 0 && !("total_parse_errors" in missing)) printf "Porcentaje de paquetes con error de parseo: %.6f%%\n", 100*delta("total_parse_errors")/delta("total_packet_processed")
    show_delta("Registros parseados/serializados", "total_parsed")
    show_delta("Registros confirmados por ClickHouse (live)", "total_inserted")
    printf "Tasas medias: recibidos=%.2f pps; parseados=%.2f rps; insertados=%.2f rps\n\n", delta("total_received")/duration, delta("total_parsed")/duration, delta("total_inserted")/duration
    show_delta("Drops UDP observados por el collector", "total_udp_kernel_drops")
    udp_observed=delta("total_received")+delta("total_udp_kernel_drops")
    if (udp_observed > 0 && !("total_udp_kernel_drops" in missing)) printf "Porcentaje de drops UDP observados: %.6f%%\n", 100*delta("total_udp_kernel_drops")/udp_observed
    show_delta("Drops de cola de paquetes", "total_packet_queue_drops")
    show_delta("Errores de serializacion", "total_event_marshal_errors")
    show_delta("Lotes con fallo de insert tras reintentos", "total_insert_errors")
    show_delta("Filas con fallo de insert live (*)", "total_insert_dropped_rows")
    show_delta("Filas desviadas por sobrecarga (*)", "total_live_insert_skipped_rows")
    show_delta("Lotes guardados para reproceso", "total_failed_batch_spooled")
    show_delta("Filas guardadas para reproceso (*)", "total_failed_rows_spooled")
    show_delta("Errores al guardar para reproceso", "total_failed_spool_errors")
    show_delta("Errores al preparar tabla ClickHouse", "total_table_create_errors")
    print "(*) Contadores relacionados: NO sumarlos como perdidas."
    printf "\nCola paquetes: inicio=%s fin=%s max_muestreado=%.0f\n", first["queue_packet"], last["queue_packet"], max_packet_queue
    printf "Cola lotes: inicio=%s fin=%s max_muestreado=%.0f\n", first["queue_batch"], last["queue_batch"], max_batch_queue
    if (!("total_packet_queue_drops" in missing)) printf "Saldo aproximado paquetes sin finalizar: inicio=%.0f fin=%.0f variacion=%+.0f\n", balance(first, "packets"), balance(last, "packets"), balance(last, "packets")-balance(first, "packets")
    if (!("total_failed_rows_spooled" in missing)) printf "Saldo filas sin confirmacion live ni spool: inicio=%.0f fin=%.0f variacion=%+.0f\n", balance(first, "rows"), balance(last, "rows"), balance(last, "rows")-balance(first, "rows")
    print "Los saldos incluyen buffers, lotes en formacion e inserts en curso; las muestras no son atomicas."
    print "Filas en spool NO estan confirmadas por el collector; el reprocesador lleva contadores separados."
    print "Una diferencia entre deltas recibidos/procesados o parseados/insertados NO prueba perdida: puede variar el saldo entre extremos."
    if (gaps) printf "ADVERTENCIA: %d intervalos sin muestras durante mas de 30 s.\n", gaps
    for (key in missing) printf "ADVERTENCIA: falta %s en alguna muestra.\n", key
    if (parse_only) print "ADVERTENCIA: PARSE_ONLY=true; esta captura no valida inserciones."
    if (!delta("total_received")) print "ADVERTENCIA: no hubo trafico recibido en esta ventana."
    if (read_errors) printf "ADVERTENCIA: errores de lectura UDP reportados=%.0f.\n", read_errors
    print "\nAlcance: son contadores del proceso, no una auditoria de filas unicas en ClickHouse."
    print "Cero drops observados NO demuestra cero perdida en router, red o NIC. Comparar tambien contadores del emisor."
}
