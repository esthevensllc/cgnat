# Huawei CGN NAT Collector

Collector UDP escrito en Go para recibir eventos Huawei CGN NAT, insertar los
eventos procesados en ClickHouse y preservar en disco los lotes que no pudieron
insertarse. Tambien puede detectar incumplimientos de recepcion, parseo e
insercion y registrar su ciclo de vida en Oracle sin bloquear el camino UDP.

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
go build -mod=vendor -trimpath -ldflags="-s -w" -o bin/huawei-cgn-go .
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

## Alertas y Oracle

Las alertas estan deshabilitadas por defecto. Su flujo es independiente:

```text
cohortes por segundo -> evaluador -> estado durable -> outbox -> Oracle
```

No se ejecuta SQL por paquete ni por registro NAT. Si Oracle no responde, el
collector sigue recibiendo UDP e insertando en ClickHouse; los cambios de
estado quedan en `ALERT_STATE_DIR/outbox` y se reintentan con backoff. El
`MERGE` usa el UUID del incidente, por lo que un reintento no duplica la fila.

Alertas implementadas:

| Nombre | Indicador |
| --- | --- |
| `CGN_UDP_DROP_RATE_60S` | `SO_RXQ_OVFL / (recibidos + SO_RXQ_OVFL)` en los ultimos 60 s |
| `CGN_PARSE_SLA_60S` | paquetes de la cohorte que no terminaron parseo 60 s despues de recibirse |
| `CGN_INSERT_SLA_60S` | registros de la cohorte que ClickHouse no confirmo 60 s despues de recibirse |

Paquetes y registros no se mezclan: la alerta de parseo usa paquetes y la de
insercion usa registros NAT. Las dos alertas SLA empiezan a evaluarse cuando
existe una ventana completa, aproximadamente 120 segundos despues del
arranque con los valores predeterminados.

Los umbrales iniciales son `0.1%` para drops UDP y `1%` para parseo e insercion.
Una alerta se limpia despues de tres evaluaciones consecutivas por debajo de
su umbral de recuperacion. El mismo `id` pasa de `ACTIVE` a `CLEARED`; no se
crea una fila en cada evaluacion.

Crear la tabla con [deploy/oracle-alerts.sql](deploy/oracle-alerts.sql). El
campo solicitado como `%indicador` se llama `PORCENTAJE_INDICADOR`, porque `%`
obligaria a usar un identificador Oracle entre comillas en todas las consultas.
`FECHA_ENVIO` se asigna en Oracle con `SYSTIMESTAMP`.

Preparar el directorio local:

```bash
mkdir -p /index2/huawei-cgn-go/alerts/{outbox,bad}
chown -R huawei-cgn:huawei-cgn /index2/huawei-cgn-go/alerts
chmod 750 /index2/huawei-cgn-go/alerts/{outbox,bad}
```

`ReadWritePaths` del unit systemd debe incluir exactamente ese directorio. La
plantilla `deploy/huawei-cgn-go.service` ya incluye la ruta productiva; para un
servicio de prueba con `/index2/huawei-cgn-go-test/alerts` hay que sustituirla y
ejecutar `systemctl daemon-reload`.

Primero ejecutar 24-48 horas sin escribir en Oracle:

```text
ALERTS_ENABLED=true
ALERTS_MODE=observe
ALERT_SERVER_IP=IP_DEL_COLLECTOR
```

Despues de validar los umbrales, completar `ORACLE_ALERT_*` y cambiar a
`ALERTS_MODE=oracle`. El driver `go-ora` es puro Go y esta incluido en
`vendor/`; no requiere Oracle Instant Client, CGO ni Internet durante la
compilacion. El archivo de entorno contiene contrasenas y debe mantenerse con
permisos `0600`.

## Opcion de despliegue completo en Linux sin Internet

Esta opcion asume las rutas usadas en el servidor de prueba:

```text
servicio:       huawei-cgn-go-test
codigo fuente:  /opt/huawei-cgn-go/src
binario:        /opt/huawei-cgn-go/bin/huawei-cgn-go
configuracion:  /etc/huawei-cgn-go/huawei-cgn-go.env
failed spool:   /index2/huawei-cgn-go-test/failed
alertas:        /index2/huawei-cgn-go-test/alerts
```

Para produccion se deben sustituir el nombre del servicio y las rutas `-test`.

### 1. Copiar el proyecto completo

El servidor sin Internet necesita el directorio `vendor/`. Se debe transferir
el repositorio completo y no solamente los archivos `.go`:

```text
main.go
alert_cohorts.go
alerts.go
oracle_alerts.go
reuseport_hash.go
udp_receiver_linux_amd64.go
udp_receiver_fallback.go
go.mod
go.sum
vendor/
```

Validar la dependencia Oracle vendorizada:

```bash
cd /opt/huawei-cgn-go/src
test -d vendor/github.com/sijms/go-ora/v2 && echo "vendor Oracle OK"
/usr/local/go/bin/go version
```

No se deben subir al repositorio archivos `.env` ni configuraciones que
contengan contrasenas reales.

### 2. Solicitar los datos y permisos Oracle

El administrador de Oracle debe entregar:

- IP o hostname de Oracle.
- Puerto TCP, normalmente `1521`.
- `SERVICE_NAME`.
- Usuario y contrasena.
- Esquema y nombre de la tabla.
- Permisos `SELECT`, `INSERT` y `UPDATE` sobre la tabla.
- Acceso de red desde el collector hasta Oracle.

Validar primero el acceso TCP:

```bash
nc -vz -w 5 IP_ORACLE 1521
```

El collector usa un driver Oracle puro Go incluido en `vendor/`. No requiere
Oracle Instant Client, SQL*Plus, CGO ni descarga de paquetes. SQL*Plus es
opcional y solo sirve para una validacion manual de la base.

Esta configuracion cubre Oracle por TCP estandar. Si la base exige `TCPS`,
wallet o certificados, se debe ampliar la configuracion antes del despliegue.

### 3. Crear la tabla Oracle

El DBA debe ejecutar `deploy/oracle-alerts.sql`, ajustando el esquema, tabla y
usuario del `GRANT`. El valor final debe coincidir con
`ORACLE_ALERT_TABLE`:

```text
CGNAT.COLLECTOR_ALERTS
```

La cuenta configurada en `ORACLE_ALERT_USER` debe estar habilitada, con la
contrasena vigente y con permisos para ejecutar el `MERGE` de apertura y cierre
de alertas.

### 4. Preparar los directorios locales

```bash
install -d -o huawei-cgn -g huawei-cgn -m 0750 \
  /index2/huawei-cgn-go-test/failed \
  /index2/huawei-cgn-go-test/failed/open \
  /index2/huawei-cgn-go-test/failed/done \
  /index2/huawei-cgn-go-test/alerts \
  /index2/huawei-cgn-go-test/alerts/outbox \
  /index2/huawei-cgn-go-test/alerts/bad
```

Confirmar permisos:

```bash
namei -l /index2/huawei-cgn-go-test/alerts/outbox
```

### 5. Configurar primero el modo de observacion

Agregar al archivo `/etc/huawei-cgn-go/huawei-cgn-go.env`:

```ini
ALERTS_ENABLED=true
ALERTS_MODE=observe
ALERT_SERVER_IP=IP_DEL_SERVIDOR_COLLECTOR
ALERT_STATE_DIR=/index2/huawei-cgn-go-test/alerts

ALERT_EVALUATION_INTERVAL_SECONDS=10
ALERT_WINDOW_SECONDS=60
ALERT_SLA_SECONDS=60
ALERT_MIN_PACKETS=1000
ALERT_MIN_ROWS=1000
ALERT_TRIGGER_WINDOWS=1
ALERT_CLEAR_WINDOWS=3
ALERT_ACTIVE_UPDATE_SECONDS=60

ALERT_UDP_DROP_THRESHOLD_PCT=0.1
ALERT_UDP_DROP_CLEAR_PCT=0.01
ALERT_PARSE_THRESHOLD_PCT=1
ALERT_PARSE_CLEAR_PCT=0.1
ALERT_INSERT_THRESHOLD_PCT=1
ALERT_INSERT_CLEAR_PCT=0.1
```

Proteger el archivo de configuracion:

```bash
chown root:huawei-cgn /etc/huawei-cgn-go/huawei-cgn-go.env
chmod 640 /etc/huawei-cgn-go/huawei-cgn-go.env
```

En modo `observe` se calculan y registran las alertas en `journald`, pero no se
abre una conexion ni se escribe en Oracle.

### 6. Configurar systemd

El unit `/etc/systemd/system/huawei-cgn-go-test.service` debe permitir escritura
en las rutas exactas configuradas:

```ini
ReadWritePaths=-/index2/huawei-cgn-go-test/failed -/index2/huawei-cgn-go-test/alerts
```

Aplicar el cambio:

```bash
systemctl daemon-reload
systemctl cat huawei-cgn-go-test
```

Una ruta incorrecta o inexistente sin el prefijo `-` puede producir el error
systemd `status=226/NAMESPACE`.

### 7. Compilar sin acceso a Internet

Se puede compilar mientras la version anterior continua ejecutandose:

```bash
cd /opt/huawei-cgn-go/src

/usr/local/go/bin/go test -mod=vendor ./...

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOPROXY=off \
/usr/local/go/bin/go build \
  -mod=vendor \
  -trimpath \
  -ldflags="-s -w" \
  -o /opt/huawei-cgn-go/bin/huawei-cgn-go.new .
```

Validar el binario antes de detener el servicio:

```bash
file /opt/huawei-cgn-go/bin/huawei-cgn-go.new
sha256sum /opt/huawei-cgn-go/bin/huawei-cgn-go.new
chmod 755 /opt/huawei-cgn-go/bin/huawei-cgn-go.new
```

`file` debe indicar `ELF 64-bit`, `x86-64`. El build usa `CGO_ENABLED=0`, por
lo que el binario no depende de librerias Oracle del sistema.

### 8. Reemplazar el binario y arrancar

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

Validar el puerto, arranque y metricas:

```bash
ss -ulnp | grep ':9088'

journalctl -u huawei-cgn-go-test -b --since "-5 minutes" --no-pager -l \
  | grep -E 'collector_started|alerting_started|metrics|error'
```

El arranque debe mostrar `alerts_enabled=true`, `alerts_mode=observe` y
`udp_rxq_overflow_metrics=true`.

### 9. Observar antes de activar Oracle

Mantener `ALERTS_MODE=observe` durante 24-48 horas y revisar:

```bash
journalctl -u huawei-cgn-go-test -b --no-pager \
  | grep -E 'alert_transition|alert_observe' \
  | tail -50
```

Esto permite comprobar si los umbrales generan alertas utiles antes de escribir
en la base externa.

### 10. Activar el envio a Oracle

Agregar las credenciales al archivo de entorno:

```ini
ALERTS_ENABLED=true
ALERTS_MODE=oracle

ORACLE_ALERT_HOST=IP_ORACLE
ORACLE_ALERT_PORT=1521
ORACLE_ALERT_SERVICE=SERVICE_NAME
ORACLE_ALERT_USER=USUARIO
ORACLE_ALERT_PASS=CONTRASENA
ORACLE_ALERT_TABLE=CGNAT.COLLECTOR_ALERTS

ORACLE_ALERT_CONNECT_TIMEOUT_SECONDS=5
ORACLE_ALERT_OPERATION_TIMEOUT_SECONDS=10
ORACLE_ALERT_RETRY_SECONDS=5
ORACLE_ALERT_MAX_RETRY_SECONDS=300
```

Los cambios del archivo de entorno requieren reiniciar el servicio:

```bash
systemctl restart huawei-cgn-go-test
systemctl status huawei-cgn-go-test --no-pager -l
```

### 11. Validar entregas y errores Oracle

```bash
journalctl -u huawei-cgn-go-test -b --no-pager \
  | grep -E 'alerting_started|alert_transition|alert_oracle|alert_outbox' \
  | tail -100

find /index2/huawei-cgn-go-test/alerts/outbox \
  -maxdepth 1 -type f -name '*.json' | wc -l
```

Cuando exista una alerta, `alert_oracle_delivered` confirma que Oracle acepto
el `MERGE`. Si Oracle no responde, aparece `alert_oracle_delivery_error`; el
archivo permanece en `outbox` y el collector reintenta sin detener UDP ni
ClickHouse.

El DBA puede comprobar las ultimas alertas con:

```sql
SELECT id,
       hostname,
       ip,
       fecha_inicio,
       fecha_fin,
       nombre,
       umbral,
       porcentaje_indicador,
       estado,
       fecha_envio
FROM cgnat.collector_alerts
ORDER BY fecha_envio DESC;
```

### 12. Regresar al binario anterior

Si la validacion del nuevo binario falla:

```bash
systemctl stop huawei-cgn-go-test

install -o root -g huawei-cgn -m 0755 \
  /opt/huawei-cgn-go/bin/huawei-cgn-go.bak \
  /opt/huawei-cgn-go/bin/huawei-cgn-go

systemctl start huawei-cgn-go-test
systemctl status huawei-cgn-go-test --no-pager -l
```

## Simulador UDP

Compilar el generador para ejecutarlo desde otro servidor:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
go build -mod=vendor -trimpath -ldflags="-s -w" -o bin/udp-simulator ./cmd/udp-simulator
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
