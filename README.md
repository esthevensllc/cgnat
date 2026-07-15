# Huawei CGN NAT Collector

Collector UDP escrito en Go para recibir eventos Huawei CGN NAT, insertar los
registros procesados en ClickHouse y preservar en disco solamente los lotes que
no pudieron insertarse. El mismo proceso detecta problemas de recepcion,
parseo, insercion y ausencia total de trafico; las alertas se guardan en el
mismo ClickHouse usado por los eventos NAT.

## Flujo principal

En Linux, el modo recomendado para alto trafico es:

```text
UDP :9088
  -> sockets SO_REUSEPORT
  -> selector BPF por contenido
  -> recvmmsg por lotes
  -> PACKET_WORKERS
       -> parseo Huawei
       -> batches RowBinary directos
  -> INSERT_WORKERS
  -> ClickHouse
```

`UDP_RECEIVE_MODE=reuseport_bpf` evita que todos los datagramas de un router
pesado queden fijados al mismo socket. Los modos `reuseport` y `shared_socket`
se mantienen para diagnostico y reversion.

La insercion NAT usa `RowBinary` por HTTP. No existe una cola central JSON en
el camino live. Si un lote no puede insertarse despues de todos los reintentos,
se escribe en `FAILED_SPOOL_BASE` para reproceso.

## Configuracion general

Crear la configuracion local a partir de la plantilla:

```bash
cp huawei-cgn-go.example huawei-cgn-go
```

Completar como minimo:

```ini
LISTEN_ADDR=0.0.0.0:9088

CLICKHOUSE_URL=http://CLICKHOUSE_HOST:8123
CLICKHOUSE_USER=REEMPLAZAR_USUARIO
CLICKHOUSE_PASS=REEMPLAZAR_CONTRASENA
CLICKHOUSE_TABLE=cgnat.huawei_cgn_nat_v2
CLICKHOUSE_DAILY_TABLES=true

FAILED_SPOOL_BASE=/index2/huawei-cgn-go/failed
```

`CLICKHOUSE_TABLE` es el nombre base. Con tablas diarias activas, el collector
crea e inserta en:

```text
cgnat.huawei_cgn_nat_v2_YYYY_MM_DD
```

El archivo con credenciales no debe publicarse y debe mantenerse con permisos
restringidos.

## Compilacion

El proyecto ya no tiene dependencias externas ni necesita `vendor/`. Para una
compilacion offline:

```bash
GOTOOLCHAIN=local GOPROXY=off \
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
go build -trimpath -ldflags="-s -w" -o bin/huawei-cgn-go .
```

Al arrancar, el log debe incluir:

```text
clickhouse_insert_format=RowBinary
live_batch_mode=packet_worker_direct
clickhouse_daily_tables=true
raw_spool_mode=failed_inserts_only
udp_receive_mode=reuseport_bpf
udp_rxq_overflow_metrics=true
```

En modo BPF tambien debe aparecer:

```text
reuseport_bpf_attached sockets=16 hash_offsets=20,36 selector=cbpf_payload_hash
```

`UDP_REUSEPORT_HASH_OFFSETS` se mide desde el primer byte del payload UDP. Los
offsets `20,36` mezclan campos variables del primer registro Huawei. El modo
requiere Linux 4.5 o superior.

## Alertas en ClickHouse

Las alertas estan deshabilitadas por defecto y tienen un flujo independiente:

```text
cohortes por segundo
  -> evaluador
  -> estado durable
  -> alerts/outbox
  -> ClickHouse
```

No se ejecuta una consulta por paquete ni por registro NAT. Solo se inserta una
fila cuando una alerta cambia de estado o cuando debe persistirse un nuevo pico.
La entrega usa una conexion HTTP y una cola diferentes a las del flujo NAT.

Si ClickHouse no responde, el collector conserva los cambios en
`ALERT_STATE_DIR/outbox` y continua recibiendo UDP. Cuando ClickHouse se
recupera, la outbox se entrega en orden con reintentos exponenciales.

Alertas implementadas:

| Nombre | Condicion |
| --- | --- |
| `CGN_NO_UDP_TRAFFIC_60S` | Ningun paquete recibido durante una ventana completa de 60 s |
| `CGN_UDP_DROP_RATE_60S` | `SO_RXQ_OVFL / (recibidos + SO_RXQ_OVFL)` igual o mayor a `0.1%` |
| `CGN_PARSE_SLA_60S` | Paquetes que no terminaron parseo 60 s despues de recibirse, igual o mayor a `1%` |
| `CGN_INSERT_SLA_60S` | Registros que ClickHouse no confirmo 60 s despues de recibirse, igual o mayor a `1%` |

La alerta de ausencia de trafico no usa `ALERT_MIN_PACKETS`; queda habilitada
con `ALERT_NO_TRAFFIC_ENABLED=true`. Espera una ventana completa despues del
arranque antes de activarse. Si el proceso esta detenido, no puede emitir su
propia alerta y ese caso debe vigilarse externamente con systemd o Prometheus.

### Tabla de alertas

La tabla predeterminada es:

```text
cgnat.collector_alerts
```

El collector puede crearla automaticamente con
`CLICKHOUSE_ALERT_AUTO_CREATE=true`. El DDL tambien esta disponible en
[deploy/clickhouse-alerts.sql](deploy/clickhouse-alerts.sql).

Se usa `ReplacingMergeTree(version)`: una alerta conserva el mismo `id` al
pasar de `ACTIVE` a `CLEARED`. Hasta que ClickHouse complete sus merges pueden
existir varias versiones fisicas. Para consultar el estado vigente se debe usar
`FINAL`:

```sql
SELECT
    id,
    hostname,
    ip,
    fecha_inicio,
    fecha_fin,
    nombre,
    umbral,
    porcentaje_indicador,
    estado,
    fecha_envio
FROM cgnat.collector_alerts FINAL
ORDER BY fecha_envio DESC
LIMIT 100;
```

La entrega de alertas usa `JSONEachRow` porque su volumen es minimo y ya nace
de una outbox JSON. Esto no cambia el camino live NAT, que continua usando
`RowBinary`.

### Configuracion de alertas

Primero observar durante 24-48 horas sin escribir en ClickHouse:

```ini
ALERTS_ENABLED=true
ALERTS_MODE=observe
ALERT_SERVER_IP=IP_REAL_DEL_COLLECTOR
ALERT_STATE_DIR=/index2/huawei-cgn-go/alerts

ALERT_EVALUATION_INTERVAL_SECONDS=10
ALERT_WINDOW_SECONDS=60
ALERT_SLA_SECONDS=60
ALERT_MIN_PACKETS=1000
ALERT_MIN_ROWS=1000
ALERT_TRIGGER_WINDOWS=1
ALERT_CLEAR_WINDOWS=3
ALERT_ACTIVE_UPDATE_SECONDS=60
ALERT_NO_TRAFFIC_ENABLED=true

ALERT_UDP_DROP_THRESHOLD_PCT=0.1
ALERT_UDP_DROP_CLEAR_PCT=0.01
ALERT_PARSE_THRESHOLD_PCT=1
ALERT_PARSE_CLEAR_PCT=0.1
ALERT_INSERT_THRESHOLD_PCT=1
ALERT_INSERT_CLEAR_PCT=0.1
```

Despues de validar los umbrales, activar la entrega:

```ini
ALERTS_MODE=clickhouse

CLICKHOUSE_ALERT_TABLE=cgnat.collector_alerts
CLICKHOUSE_ALERT_AUTO_CREATE=true
CLICKHOUSE_ALERT_CONNECT_TIMEOUT_SECONDS=5
CLICKHOUSE_ALERT_OPERATION_TIMEOUT_SECONDS=10
CLICKHOUSE_ALERT_RETRY_SECONDS=5
CLICKHOUSE_ALERT_MAX_RETRY_SECONDS=300
```

El sink reutiliza `CLICKHOUSE_URL`, `CLICKHOUSE_USER` y `CLICKHOUSE_PASS`. No
se necesitan otras credenciales, drivers ni servicios. Las variables antiguas
`ORACLE_ALERT_*` ya no se usan.

Los JSON que ya existan en `alerts/outbox` son compatibles con el nuevo sink y
se entregaran a ClickHouse al arrancar en modo `clickhouse`.

## Despliegue completo en Linux sin Internet

Esta opcion asume el servicio de prueba y las siguientes rutas:

```text
servicio:       huawei-cgn-go-test
codigo fuente:  /opt/huawei-cgn-go/src
binario:        /opt/huawei-cgn-go/bin/huawei-cgn-go
configuracion:  /etc/huawei-cgn-go/huawei-cgn-go.env
failed spool:   /index2/huawei-cgn-go-test/failed
alertas:        /index2/huawei-cgn-go-test/alerts
```

Para produccion, sustituir el nombre del servicio y las rutas `-test`.

### 1. Copiar el proyecto

Transferir el repositorio completo a `/opt/huawei-cgn-go/src`. Los archivos
necesarios para el collector son:

```text
main.go
alert_cohorts.go
alerts.go
clickhouse_alerts.go
reuseport_hash.go
udp_receiver_linux_amd64.go
udp_receiver_fallback.go
go.mod
deploy/
```

No se necesita `vendor/`, `go.sum`, Oracle Instant Client ni acceso a Internet.
No transferir archivos `.env` con credenciales al repositorio.

Validar Go:

```bash
cd /opt/huawei-cgn-go/src
/usr/local/go/bin/go version
```

### 2. Validar el ClickHouse existente

Las alertas reutilizan la misma URL y credenciales de los eventos NAT. Desde el
collector:

```bash
curl -sS --max-time 5 http://127.0.0.1:8123/ping
```

Si `CLICKHOUSE_URL` usa otra IP o exige autenticacion, realizar la prueba con
esa URL y el usuario configurado. La respuesta esperada es:

```text
Ok.
```

El usuario necesita `CREATE TABLE`, `INSERT` y `SELECT` sobre la base `cgnat`.
Si ya crea las tablas NAT diarias, normalmente dispone de los permisos de
creacion e insercion requeridos.

### 3. Crear o validar la tabla de alertas

Con `CLICKHOUSE_ALERT_AUTO_CREATE=true`, el worker de alertas ejecuta
`CREATE TABLE IF NOT EXISTS` sin bloquear UDP. Para crearla manualmente:

```bash
cd /opt/huawei-cgn-go/src
clickhouse-client --host 127.0.0.1 --user admin --password --multiquery \
  < deploy/clickhouse-alerts.sql
```

Validar:

```bash
clickhouse-client --host 127.0.0.1 --user admin --password \
  --query "DESCRIBE TABLE cgnat.collector_alerts"
```

### 4. Preparar los directorios

```bash
install -d -o huawei-cgn -g huawei-cgn -m 0750 \
  /index2/huawei-cgn-go-test/failed \
  /index2/huawei-cgn-go-test/failed/open \
  /index2/huawei-cgn-go-test/failed/done \
  /index2/huawei-cgn-go-test/alerts \
  /index2/huawei-cgn-go-test/alerts/outbox \
  /index2/huawei-cgn-go-test/alerts/bad

namei -l /index2/huawei-cgn-go-test/alerts/outbox
```

### 5. Configurar el modo de observacion

Agregar la configuracion de alertas mostrada anteriormente a:

```text
/etc/huawei-cgn-go/huawei-cgn-go.env
```

Para el primer arranque usar:

```ini
ALERTS_ENABLED=true
ALERTS_MODE=observe
ALERT_SERVER_IP=IP_REAL_DEL_COLLECTOR
ALERT_STATE_DIR=/index2/huawei-cgn-go-test/alerts
ALERT_NO_TRAFFIC_ENABLED=true
```

Proteger el archivo:

```bash
chown root:huawei-cgn /etc/huawei-cgn-go/huawei-cgn-go.env
chmod 640 /etc/huawei-cgn-go/huawei-cgn-go.env
```

### 6. Configurar systemd

El unit `/etc/systemd/system/huawei-cgn-go-test.service` debe permitir escritura
en ambos spools:

```ini
ReadWritePaths=-/index2/huawei-cgn-go-test/failed -/index2/huawei-cgn-go-test/alerts
```

Aplicar y revisar:

```bash
systemctl daemon-reload
systemctl cat huawei-cgn-go-test
```

Una ruta incorrecta o inexistente sin el prefijo `-` puede producir
`status=226/NAMESPACE`.

### 7. Compilar offline

Se puede compilar el binario nuevo mientras la version anterior sigue activa:

```bash
cd /opt/huawei-cgn-go/src

GOTOOLCHAIN=local GOPROXY=off \
/usr/local/go/bin/go test ./...

GOTOOLCHAIN=local GOPROXY=off \
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
/usr/local/go/bin/go build \
  -trimpath \
  -ldflags="-s -w" \
  -o /opt/huawei-cgn-go/bin/huawei-cgn-go.new .
```

Validar antes de detener el servicio:

```bash
file /opt/huawei-cgn-go/bin/huawei-cgn-go.new
sha256sum /opt/huawei-cgn-go/bin/huawei-cgn-go.new
chmod 755 /opt/huawei-cgn-go/bin/huawei-cgn-go.new
```

`file` debe indicar `ELF 64-bit`, `x86-64` y un binario estatico.

### 8. Reemplazar el binario

```bash
systemctl stop huawei-cgn-go-test

cp -a /opt/huawei-cgn-go/bin/huawei-cgn-go \
  /opt/huawei-cgn-go/bin/huawei-cgn-go.bak

install -o root -g huawei-cgn -m 0755 \
  /opt/huawei-cgn-go/bin/huawei-cgn-go.new \
  /opt/huawei-cgn-go/bin/huawei-cgn-go

systemctl start huawei-cgn-go-test
systemctl status huawei-cgn-go-test --no-pager -l
```

### 9. Validar el arranque

```bash
ss -ulnp | grep ':9088'

journalctl -u huawei-cgn-go-test -b --since "-5 minutes" --no-pager -l \
  | grep -E 'collector_started|alerting_started|alert_clickhouse|metrics|error'
```

En observacion debe verse:

```text
alerts_enabled=true
alerts_mode=observe
no_traffic_enabled=true
```

Revisar transiciones durante 24-48 horas:

```bash
journalctl -u huawei-cgn-go-test -b --no-pager \
  | grep -E 'alert_transition|alert_observe' \
  | tail -100
```

### 10. Activar ClickHouse

Cambiar el archivo de entorno:

```ini
ALERTS_MODE=clickhouse
CLICKHOUSE_ALERT_TABLE=cgnat.collector_alerts
CLICKHOUSE_ALERT_AUTO_CREATE=true
CLICKHOUSE_ALERT_CONNECT_TIMEOUT_SECONDS=5
CLICKHOUSE_ALERT_OPERATION_TIMEOUT_SECONDS=10
CLICKHOUSE_ALERT_RETRY_SECONDS=5
CLICKHOUSE_ALERT_MAX_RETRY_SECONDS=300
```

Reiniciar porque el archivo de entorno solo se carga al arrancar:

```bash
systemctl restart huawei-cgn-go-test
systemctl status huawei-cgn-go-test --no-pager -l
```

Validar la entrega:

```bash
journalctl -u huawei-cgn-go-test -b --no-pager \
  | grep -E 'alerting_started|alert_transition|alert_clickhouse|alert_outbox' \
  | tail -100

find /index2/huawei-cgn-go-test/alerts/outbox \
  -maxdepth 1 -type f -name '*.json' | wc -l
```

Mensajes importantes:

```text
alert_clickhouse_ready
alert_clickhouse_delivered
alert_clickhouse_prepare_error
alert_clickhouse_delivery_error
```

Un error deja el archivo en `outbox`; no se pierde y no se detiene la captura
UDP.

### 11. Consultar las alertas

```bash
clickhouse-client --host 127.0.0.1 --user admin --password --query "
SELECT
    id, hostname, ip, fecha_inicio, fecha_fin,
    nombre, umbral, porcentaje_indicador, estado, fecha_envio
FROM cgnat.collector_alerts FINAL
ORDER BY fecha_envio DESC
LIMIT 100
FORMAT Vertical"
```

Alertas activas:

```sql
SELECT *
FROM cgnat.collector_alerts FINAL
WHERE estado = 'ACTIVE'
ORDER BY fecha_inicio DESC;
```

### 12. Rollback

```bash
systemctl stop huawei-cgn-go-test

install -o root -g huawei-cgn -m 0755 \
  /opt/huawei-cgn-go/bin/huawei-cgn-go.bak \
  /opt/huawei-cgn-go/bin/huawei-cgn-go

systemctl start huawei-cgn-go-test
systemctl status huawei-cgn-go-test --no-pager -l
```

## Monitor local

Instalar el monitor incluido:

```bash
install -o root -g root -m 0755 \
  /opt/huawei-cgn-go/src/deploy/monitor-cgn.sh \
  /usr/local/bin/monitor-cgn.sh
```

Ejecutar una captura o actualizar cada dos segundos:

```bash
/usr/local/bin/monitor-cgn.sh
watch -n 2 /usr/local/bin/monitor-cgn.sh
```

## Simulador UDP

Compilar para ejecutarlo desde otro servidor:

```bash
GOTOOLCHAIN=local GOPROXY=off \
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
go build -trimpath -ldflags="-s -w" -o bin/udp-simulator ./cmd/udp-simulator
```

Prueba fija:

```bash
./bin/udp-simulator \
  -target 10.96.167.132:9088 \
  -mode legacy \
  -pps 10000 \
  -duration 5m \
  -workers 8
```

Prueba incremental:

```bash
./bin/udp-simulator \
  -target 10.96.167.132:9088 \
  -mode multi \
  -records 8 \
  -start-pps 10000 \
  -max-pps 200000 \
  -step-pps 10000 \
  -step-every 30s \
  -duration 10m \
  -workers 16
```
