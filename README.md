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
