# Huawei CGN NAT Collector

Collector UDP escrito en Go para recibir eventos Huawei CGN NAT, insertar los
eventos procesados en ClickHouse y preservar en disco los lotes que no pudieron
insertarse.

El flujo de alto trafico en Linux usa varios sockets independientes
`SO_REUSEPORT` y un selector BPF por contenido
(`UDP_RECEIVE_MODE=reuseport_bpf`). El selector evita que todos los paquetes
de un router pesado queden fijados al mismo socket. Los modos `reuseport` con
hash normal del kernel y `shared_socket` se mantienen para diagnostico y
reversion.
La recepcion por lotes usa `recvmmsg` y workers adaptativos para parseo e insercion. La
insercion live hacia ClickHouse usa `RowBinary` por HTTP y los `PACKET_WORKERS`
arman los batches directamente, sin una cola central de eventos, para evitar
que `queue_event` sea el cuello de botella.

## Configuracion

Crear la configuracion local a partir de la plantilla:

```bash
cp huawei-cgn-go.example huawei-cgn-go
```

Completar la URL, el usuario y la contrasena de ClickHouse. El archivo
`huawei-cgn-go` contiene credenciales y esta excluido de Git.

`CLICKHOUSE_TABLE` es el nombre base. Con `CLICKHOUSE_DAILY_TABLES=true` el
collector inserta en tablas por dia con el formato:

```text
cgnat.huawei_cgn_nat_v2_YYYY_MM_DD
```

Antes del primer insert de cada dia ejecuta `CREATE TABLE IF NOT EXISTS` con
el esquema RowBinary esperado por el collector.

El collector ya no guarda RAW/binarios de todos los paquetes. Solo escribe
archivos RowBinary en `FAILED_SPOOL_BASE` cuando un lote no pudo insertarse en
ClickHouse despues de todos los reintentos.

## Compilacion

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
go build -trimpath -ldflags="-s -w" -o bin/huawei-cgn-go .
```

Al arrancar, el log debe mostrar:

```text
clickhouse_insert_format=RowBinary
live_batch_mode=packet_worker_direct
clickhouse_daily_tables=true
raw_spool_mode=failed_inserts_only
udp_receive_mode=reuseport_bpf
udp_reuse_port=true
udp_reuseport_hash_offsets=20,36
```

Tambien debe existir una linea anterior que confirme que el filtro se adjunto:

```text
reuseport_bpf_attached sockets=16 hash_offsets=20,36 selector=cbpf_payload_hash
```

`UDP_REUSEPORT_HASH_OFFSETS` se mide desde el primer byte del payload UDP. Los
offsets `20,36` mezclan campos variables del primer registro Huawei y no
dependen de los cuatro bytes del header. El modo requiere Linux 4.5 o superior;
si el kernel rechaza el filtro, el collector termina durante el arranque para
no ejecutar una prueba con balanceo aparente.

En esta version `BATCH_BUILDERS` y `EVENT_CHANNEL_SIZE` quedan aceptados por
compatibilidad, pero ya no controlan el camino caliente. Para validar carga,
mirar principalmente `queue_packet`, `queue_batch`, `total_failed_batch_spooled`
y `total_failed_spool_errors`. Las metricas `udp_receiver_packets_10s`,
`udp_receiver_min_10s` y `udp_receiver_max_10s` permiten comprobar que todos
los sockets reciben trafico. `pps_received_10s` ya expresa paquetes por segundo;
el campo historico `rate_received_10s` conserva el total de los ultimos diez
segundos por compatibilidad.

## Simulador UDP

Compilar el generador para ejecutarlo desde otro servidor:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
go build -trimpath -ldflags="-s -w" -o bin/udp-simulator ./cmd/udp-simulator
```

Prueba fija:

```bash
./bin/udp-simulator -target 10.96.167.132:9088 -mode legacy -pps 10000 -duration 5m -workers 8
```

Prueba incremental:

```bash
./bin/udp-simulator -target 10.96.167.132:9088 -mode multi -records 8 \
  -start-pps 10000 -max-pps 200000 -step-pps 10000 -step-every 30s \
  -duration 10m -workers 16
```
