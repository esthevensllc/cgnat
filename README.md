# Huawei CGN NAT Collector

Collector UDP escrito en Go para recibir eventos Huawei CGN NAT, conservar
paquetes RAW e insertar los eventos procesados en ClickHouse.

El flujo de alto trafico en Linux usa varios sockets UDP con `SO_REUSEPORT`,
recepcion por lotes con `recvmmsg`, colas RAW separadas por receptor, RAW
binario configurable y workers adaptativos para parseo e insercion. La
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

`CLICKHOUSE_TABLE` es el nombre base. Con `CLICKHOUSE_HOURLY_TABLES=true` el
collector inserta en tablas por hora con el formato:

```text
cgnat.huawei_cgn_nat_v2_YYYY_MM_DD_HH24
```

Antes del primer insert de cada hora ejecuta `CREATE TABLE IF NOT EXISTS` con
el esquema RowBinary esperado por el collector.

## Compilacion

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
go build -trimpath -ldflags="-s -w" -o bin/huawei-cgn-go .
```

Al arrancar, el log debe mostrar:

```text
clickhouse_insert_format=RowBinary
live_batch_mode=packet_worker_direct
clickhouse_hourly_tables=true
```

En esta version `BATCH_BUILDERS` y `EVENT_CHANNEL_SIZE` quedan aceptados por
compatibilidad, pero ya no controlan el camino caliente. Para validar carga,
mirar principalmente `queue_packet`, `queue_batch`, `total_live_insert_skipped`
y `total_live_insert_skipped_rows`. Si ClickHouse no alcanza, el modo
`LIVE_INSERT_OVERLOAD_POLICY=raw_only` conserva el RAW binario y salta la ruta
live sin frenar la recepcion UDP.

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
