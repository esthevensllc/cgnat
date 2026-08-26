# Huawei CGN NAT Collector

Collector UDP en Go para recibir eventos Huawei CGN NAT por `9088/udp`,
parsearlos e insertarlos en tablas diarias de ClickHouse mediante `RowBinary`.
El proceso conserva en disco solamente los lotes que no pudieron insertarse y
puede registrar alertas operativas en el mismo ClickHouse.

## Que hace el proceso

```text
Routers -> UDP 9088 -> SO_REUSEPORT + BPF -> recvmmsg por lotes
        -> parseo Huawei -> lotes RowBinary -> ClickHouse
                                      |
                                      +-> failed spool si no se inserta
```

- Acepta cualquier header Huawei y paquetes con multiples registros; el header
  se usa solo para validar el paquete y no se almacena en ClickHouse.
- Distribuye trafico entre receptores UDP con `reuseport_bpf`.
- Crea automaticamente la tabla NAT correspondiente a cada dia.
- Inserta en ClickHouse sin JSON en el camino principal.
- Guarda en `FAILED_SPOOL_BASE` los lotes que agotaron sus reintentos o que se
  desviaron por sobrecarga.
- Evalua perdida UDP, atraso de parseo, atraso de insercion y ausencia de
  trafico. Las alertas pueden quedar en una outbox local si ClickHouse falla.
- No guarda una copia RAW de todos los paquetes recibidos.

## Rutas y servicios

| Elemento | Produccion | Prueba |
| --- | --- | --- |
| Proyecto | `/opt/huawei-cgn-go` | El mismo proyecto |
| Binario | `/opt/huawei-cgn-go/bin/huawei-cgn-go` | El mismo binario |
| Configuracion | `/etc/huawei-cgn-go/huawei-cgn-go.env` | `/etc/huawei-cgn-go/huawei-cgn-go-test.env` |
| Servicio | `huawei-cgn-go` | `huawei-cgn-go-test` |
| Lotes fallidos | `/index2/huawei-cgn-go/failed` | `/index2/huawei-cgn-go-test/failed` |
| Estado de alertas | `/index2/huawei-cgn-go/alerts` | `/index2/huawei-cgn-go-test/alerts` |

Produccion y prueba no deben escuchar simultaneamente en `0.0.0.0:9088`. Para
validar un servidor nuevo se arranca primero el servicio de prueba, se detiene
y luego se habilita produccion. Si deben coexistir, configure otro puerto en
`LISTEN_ADDR` para prueba y habilitelo tambien en el firewall.

## 1. Verificar el servidor

La compilacion objetivo es Linux `x86_64` y el modulo requiere Go 1.26 o una
version compatible posterior.

```bash
uname -m
uname -r

rpm -q \
  ca-certificates curl tzdata chrony \
  iproute procps-ng firewalld \
  tcpdump lsof nmap-ncat sysstat ethtool

systemctl is-active chronyd
timedatectl status
chronyc tracking
```

`curl`, `ca-certificates`, `iproute` y `systemd` son necesarios para operar y
validar el servicio. Los demas paquetes se usan para diagnostico. La hora debe
estar sincronizada antes de probar datos con timestamps.

## 2. Instalar Go sin Internet

Copiar `go1.26.3.linux-amd64.tar.gz` a `/tmp` y ejecutar como `root`:

```bash
if [ -d /usr/local/go ]; then
  mv /usr/local/go "/usr/local/go.backup.$(date +%Y%m%d%H%M%S)"
fi
tar -C /usr/local -xzf /tmp/go1.26.3.linux-amd64.tar.gz

cat >/etc/profile.d/go.sh <<'EOF'
export PATH=/usr/local/go/bin:$PATH
EOF

chmod 644 /etc/profile.d/go.sh
source /etc/profile.d/go.sh

go version
go env GOROOT GOARCH GOOS
```

La salida esperada debe indicar `linux/amd64`. No se necesita `vendor/` ni
acceso a Internet para compilar este proyecto.

## 3. Copiar el proyecto y crear directorios

Copiar el repositorio completo a `/opt/huawei-cgn-go`. Luego ejecutar:

```bash
id huawei-cgn >/dev/null 2>&1 || \
  useradd --system --no-create-home --shell /sbin/nologin huawei-cgn

id huawei-cgn
getent passwd huawei-cgn

install -d -o root -g root -m 0755 /opt/huawei-cgn-go/bin
install -d -o root -g huawei-cgn -m 0750 /etc/huawei-cgn-go

install -d -o huawei-cgn -g huawei-cgn -m 0750 \
  /index2/huawei-cgn-go/failed \
  /index2/huawei-cgn-go/failed/open \
  /index2/huawei-cgn-go/failed/done \
  /index2/huawei-cgn-go/alerts \
  /index2/huawei-cgn-go/alerts/outbox \
  /index2/huawei-cgn-go/alerts/bad \
  /index2/huawei-cgn-go-test/failed \
  /index2/huawei-cgn-go-test/failed/open \
  /index2/huawei-cgn-go-test/failed/done \
  /index2/huawei-cgn-go-test/alerts \
  /index2/huawei-cgn-go-test/alerts/outbox \
  /index2/huawei-cgn-go-test/alerts/bad
```

No se requieren `/var/log/huawei-cgn-go*`: stdout y stderr se guardan en
`journald`.

## 4. Preparar red y buffer UDP

```bash
firewall-cmd --permanent --add-port=9088/udp
firewall-cmd --reload
firewall-cmd --query-port=9088/udp

cat >/etc/sysctl.d/90-huawei-cgn.conf <<'EOF'
net.core.rmem_default = 16777216
net.core.rmem_max = 536870912
net.core.netdev_max_backlog = 250000
EOF

sysctl --system
sysctl net.core.rmem_default net.core.rmem_max net.core.netdev_max_backlog
```

El valor `UDP_READ_BUFFER_MB` de la configuracion no debe superar
`net.core.rmem_max` expresado en MiB. Para la plantilla actual ambos permiten
hasta 512 MiB por socket.

## 5. Validar ClickHouse

Desde el servidor collector, reemplazar host y credenciales:

```bash
nc -vz -w 5 CLICKHOUSE_HOST 8123
curl -sS --max-time 5 http://CLICKHOUSE_HOST:8123/ping

clickhouse-client \
  --host CLICKHOUSE_HOST \
  --user USUARIO \
  --password \
  --query "SELECT version(), now(), hostName()"
```

La respuesta HTTP esperada es `Ok.`. El usuario de la aplicacion necesita
`CREATE TABLE`, `INSERT` y `SELECT` sobre `cgnat.*` porque el collector crea
las tablas diarias y, si se habilita, la tabla de alertas.

## 6. Crear la base y las tablas

### Base de datos

Ejecutar una vez con un usuario autorizado:

```sql
CREATE DATABASE IF NOT EXISTS cgnat;
```

### Tabla diaria del proceso principal

Con `CLICKHOUSE_DAILY_TABLES=true`, el collector crea automaticamente tablas
con el patron `cgnat.huawei_cgn_nat_v2_YYYY_MM_DD`. Para precrear o validar una
fecha manualmente, reemplazar `YYYY_MM_DD` por una fecha real, por ejemplo
`2026_07_21`:

```sql
CREATE TABLE IF NOT EXISTS cgnat.huawei_cgn_nat_v2_YYYY_MM_DD
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
ORDER BY
(
    end_time,
    router_ip,
    private_ip,
    public_ip,
    destination_ip,
    private_port,
    public_port
)
SETTINGS index_granularity = 8192;
```

Para prueba se usa el mismo esquema cambiando el nombre por
`cgnat.huawei_cgn_nat_v2_test_YYYY_MM_DD`.

### Cambio al esquema reducido

El esquema reduce almacenamiento al no persistir `event_id`, `event_time`,
`event_type`, `header` ni `protocol`. La fecha de consulta se deriva de
`end_time` y el protocolo de `protocol_id`.

El collector no modifica una tabla existente. Desplegar este binario antes de
que se cree la siguiente tabla diaria, o despues del cambio de dia. No iniciar
el binario nuevo contra una tabla del dia actual con el esquema anterior:
`RowBinary` tendria columnas diferentes y ClickHouse rechazaria el INSERT.

Los lotes fallidos nuevos se marcan con `schema_version: 2`; el reprocesador
actualizado conserva compatibilidad con lotes historicos sin esa propiedad.

### Tabla de alertas

Produccion:

```sql
CREATE TABLE IF NOT EXISTS cgnat.collector_alerts
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
```

Para aislar las pruebas se recomienda crear otra tabla con el mismo esquema:

```sql
CREATE TABLE IF NOT EXISTS cgnat.collector_alerts_test
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
```

El DDL de produccion tambien esta en
[`deploy/clickhouse-alerts.sql`](deploy/clickhouse-alerts.sql).

Validar las tablas:

```sql
SHOW TABLES FROM cgnat;
DESCRIBE TABLE cgnat.collector_alerts;
```

## 7. Crear las configuraciones

```bash
install -o root -g huawei-cgn -m 0640 \
  /opt/huawei-cgn-go/huawei-cgn-go.example \
  /etc/huawei-cgn-go/huawei-cgn-go.env

install -o root -g huawei-cgn -m 0640 \
  /opt/huawei-cgn-go/huawei-cgn-go.example \
  /etc/huawei-cgn-go/huawei-cgn-go-test.env
```

Editar `/etc/huawei-cgn-go/huawei-cgn-go.env` y completar como minimo:

```ini
LISTEN_ADDR=0.0.0.0:9088
CLICKHOUSE_URL=http://CLICKHOUSE_HOST:8123
CLICKHOUSE_USER=USUARIO
CLICKHOUSE_PASS=CONTRASENA
CLICKHOUSE_TABLE=cgnat.huawei_cgn_nat_v2
CLICKHOUSE_DAILY_TABLES=true
FAILED_SPOOL_BASE=/index2/huawei-cgn-go/failed

ALERTS_ENABLED=true
ALERTS_MODE=clickhouse
ALERT_SERVER_IP=IP_REAL_DEL_COLLECTOR
ALERT_STATE_DIR=/index2/huawei-cgn-go/alerts
CLICKHOUSE_ALERT_TABLE=cgnat.collector_alerts
CLICKHOUSE_ALERT_AUTO_CREATE=true
```

En `/etc/huawei-cgn-go/huawei-cgn-go-test.env` cambiar solamente los recursos
que deben quedar aislados:

```ini
LISTEN_ADDR=0.0.0.0:9088
CLICKHOUSE_TABLE=cgnat.huawei_cgn_nat_v2_test
FAILED_SPOOL_BASE=/index2/huawei-cgn-go-test/failed
ALERT_STATE_DIR=/index2/huawei-cgn-go-test/alerts
CLICKHOUSE_ALERT_TABLE=cgnat.collector_alerts_test
```

Conservar en ambos archivos los parametros de recepcion y capacidad de la
plantilla, especialmente `UDP_RECEIVE_MODE=reuseport_bpf`. Verificar permisos:

```bash
chown root:huawei-cgn /etc/huawei-cgn-go/*.env
chmod 640 /etc/huawei-cgn-go/*.env
```

## 8. Compilar el collector

```bash
cd /opt/huawei-cgn-go

GOTOOLCHAIN=local GOPROXY=off \
/usr/local/go/bin/go test ./...

GOTOOLCHAIN=local GOPROXY=off \
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
/usr/local/go/bin/go build \
  -trimpath \
  -ldflags="-s -w" \
  -o /opt/huawei-cgn-go/bin/huawei-cgn-go.new \
  .

file /opt/huawei-cgn-go/bin/huawei-cgn-go.new
sha256sum /opt/huawei-cgn-go/bin/huawei-cgn-go.new

install -o root -g huawei-cgn -m 0755 \
  /opt/huawei-cgn-go/bin/huawei-cgn-go.new \
  /opt/huawei-cgn-go/bin/huawei-cgn-go
```

`file` debe indicar `ELF 64-bit`, `x86-64` y un ejecutable estatico. No es
necesario ejecutar `go clean -cache` en cada compilacion.

## 9. Instalar las unidades systemd

```bash
install -o root -g root -m 0644 \
  /opt/huawei-cgn-go/deploy/huawei-cgn-go.service \
  /etc/systemd/system/huawei-cgn-go.service

install -o root -g root -m 0644 \
  /opt/huawei-cgn-go/deploy/huawei-cgn-go-test.service \
  /etc/systemd/system/huawei-cgn-go-test.service

systemd-analyze verify /etc/systemd/system/huawei-cgn-go.service
systemd-analyze verify /etc/systemd/system/huawei-cgn-go-test.service
systemctl daemon-reload
```

No habilitar los dos servicios para arranque automatico. El servicio de prueba
se ejecuta manualmente; solo produccion queda habilitado al finalizar.

## 10. Ejecutar la prueba inicial

Confirmar primero que produccion no este usando el puerto:

```bash
systemctl is-active huawei-cgn-go || true
ss -ulnp | grep ':9088' || echo "Puerto 9088/udp libre"

systemctl start huawei-cgn-go-test
systemctl status huawei-cgn-go-test --no-pager -l
```

Validar arranque, socket y metricas:

```bash
ss -ulnp | grep ':9088'

journalctl -u huawei-cgn-go-test -b --since "-5 minutes" --no-pager -l \
  | grep -E 'collector_started|reuseport_bpf_attached|alerting_started|metrics|error|failed'
```

El arranque correcto debe mostrar, entre otros:

```text
clickhouse_insert_format=RowBinary
live_batch_mode=packet_worker_direct
clickhouse_daily_tables=true
raw_spool_mode=failed_inserts_only
udp_receive_mode=reuseport_bpf
```

Despues de enviar trafico con el simulador, comprobar que aumenten
`total_received`, `total_parsed` y `total_inserted`, y que permanezcan en cero
`total_udp_kernel_drops`, `total_packet_queue_drops`, `total_insert_errors` y
`total_failed_rows_spooled`.

Validar datos del dia actual, sustituyendo la fecha:

```sql
SELECT
    count() AS registros,
    min(end_time) AS primero,
    max(end_time) AS ultimo
FROM cgnat.huawei_cgn_nat_v2_test_YYYY_MM_DD;
```

## 11. Activar produccion

Cuando la prueba termine correctamente:

```bash
systemctl stop huawei-cgn-go-test
systemctl disable huawei-cgn-go-test 2>/dev/null || true

systemctl enable --now huawei-cgn-go
systemctl status huawei-cgn-go --no-pager -l
ss -ulnp | grep ':9088'
```

Revisar los primeros minutos:

```bash
journalctl -u huawei-cgn-go -b --since "-5 minutes" --no-pager -l \
  | grep -E 'collector_started|reuseport_bpf_attached|alerting_started|metrics|error|failed'
```

## Monitor local

Instalar el monitor incluido:

```bash
install -o root -g root -m 0755 \
  /opt/huawei-cgn-go/deploy/monitor-cgn.sh \
  /usr/local/bin/monitor-cgn.sh
```

Prueba:

```bash
/usr/local/bin/monitor-cgn.sh \
  9088 huawei-cgn-go-test \
  /index2/huawei-cgn-go-test/failed \
  /index2/huawei-cgn-go-test/alerts
```

Produccion:

```bash
watch -n 2 /usr/local/bin/monitor-cgn.sh \
  9088 huawei-cgn-go \
  /index2/huawei-cgn-go/failed \
  /index2/huawei-cgn-go/alerts
```

## Alertas

| Nombre | Se activa cuando |
| --- | --- |
| `CGN_NO_UDP_TRAFFIC_60S` | No se recibe ningun paquete durante 60 segundos |
| `CGN_UDP_DROP_RATE_60S` | La perdida del socket UDP llega a `0.1%` en la ventana movil de 60 segundos |
| `CGN_PARSE_SLA_60S` | Al menos `1%` de paquetes no termina el parseo dentro de 60 segundos |
| `CGN_INSERT_SLA_60S` | Al menos `1%` de registros no queda confirmado en ClickHouse dentro de 60 segundos |

Las metricas se evaluan cada 10 segundos. Una alerta se limpia despues de tres
evaluaciones consecutivas por debajo de su umbral de limpieza. Si ClickHouse no
responde, los cambios de estado quedan en `ALERT_STATE_DIR/outbox` y se reenvian
sin detener la captura UDP.

Consultar el ultimo estado de cada alerta:

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

`FINAL` es necesario porque `ReplacingMergeTree` puede conservar temporalmente
varias versiones fisicas del mismo incidente.

## Diagnostico rapido

El procedimiento para medir el consumo por columna y validar codecs sin
arriesgar tablas productivas esta en
[docs/runbooks/clickhouse-compresion.md](docs/runbooks/clickhouse-compresion.md).

```bash
systemctl status huawei-cgn-go --no-pager -l
journalctl -u huawei-cgn-go -b --since "-15 minutes" --no-pager -l
ss -u -n -a -p -m 'sport = :9088'
nstat -az UdpInDatagrams UdpInErrors UdpRcvbufErrors
find /index2/huawei-cgn-go/failed/done -maxdepth 1 -type f | wc -l
```

- `status=226/NAMESPACE`: revisar que `ReadWritePaths` exista y coincida con el
  archivo de entorno.
- `status=9/KILL` o codigo `137`: validar primero la politica de seguridad del
  servidor ejecutando un binario Go minimo; no asumir que es falta de RAM.
- `clickhouse_status=400`: revisar el SQL y el nombre de tabla que aparece en
  `journalctl`.
- `connect timed out` o `No route to host`: validar ruta, firewall y que
  ClickHouse escuche en `8123/tcp` desde la IP del collector.

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
