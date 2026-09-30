# Receptor UDP en Ubuntu ARM64

Objetivo: validar `reuseport_bpf` y `recvmmsg` en el servidor Kunpeng 920
(`aarch64`, kernel Ubuntu 6.8.0-100-generic) antes de medir capacidad. El
receptor Linux comparte codigo entre amd64 y arm64; parseo, RowBinary,
reintentos y alertas siguen en el flujo comun del collector. La version exacta
de ClickHouse 23 y su esquema se deben comprobar en el servidor.

## Recepcion y parseo sin ClickHouse

Para validar la ruta UDP y el parser antes de instalar ClickHouse, usar el
binario actualizado y `PARSE_ONLY=true` solamente en el entorno del servicio
de prueba. El servicio debe tener `ALERTS_ENABLED=false`. En este modo no se
crean tablas, no se hacen peticiones HTTP ni se guardan lotes fallidos; por
diseno `total_inserted=0`. `total_parsed` cuenta registros parseados, mientras
`total_received` cuenta datagramas UDP. En modo `multi` son unidades distintas.

En un servidor donde el servicio de prueba ya esta instalado:

```bash
systemctl stop huawei-cgn-go-test
cd /root/cgnat-arm64-install
tar -xzf /tmp/cgnat-ubuntu-arm64-install.tar.gz -C .
sha256sum -c SHA256SUMS
install -o root -g root -m 0755 huawei-cgn-go /opt/huawei-cgn-go/bin/huawei-cgn-go
sed -i '/^PARSE_ONLY=/d' /etc/huawei-cgn-go/huawei-cgn-go-test.env
printf '\nPARSE_ONLY=true\n' >> /etc/huawei-cgn-go/huawei-cgn-go-test.env
grep -E '^(PARSE_ONLY|ALERTS_ENABLED|LISTEN_ADDR)=' /etc/huawei-cgn-go/huawei-cgn-go-test.env
systemctl start huawei-cgn-go-test
journalctl -u huawei-cgn-go-test -b -n 30 --no-pager -l | grep -E 'collector_started|reuseport_bpf_attached|error'
```

El arranque debe mostrar `parse_only=true` y `reuseport_bpf_attached`.
Con `LISTEN_ADDR=0.0.0.0:9088`, ejecutar desde el mismo servidor:

```bash
./udp-simulator -target 127.0.0.1:9088 -mode legacy -pps 1000 -duration 30s -workers 4 \
  2>&1 | tee simulator-1k.log
sleep 11
journalctl -u huawei-cgn-go-test -b --since '-3 min' --no-pager -l | grep 'metrics parse_only=true'
```

Comparar `sent` con el incremento de `total_received`; revisar
`total_packet_processed`, `total_parsed`, `total_parse_errors`,
`total_udp_kernel_drops`, `total_packet_queue_drops` y `queue_packet`.
`total_inserted`, `total_insert_errors` y `total_failed_rows_spooled` deben
seguir en cero. Esta prueba en loopback no mide el rendimiento de la NIC ni de
la red. Para activar inserciones despues, configurar ClickHouse y volver a
`PARSE_ONLY=false` antes de iniciar produccion.

## Artefactos y comprobacion

Los binarios estaticos estan en `dist/cgnat-ubuntu-arm64-install.tar.gz`,
incluido en el repositorio. Se extrae con `tar` sin instalar paquetes:

```bash
mkdir -p ~/cgnat-arm64-install
tar -xzf /tmp/cgnat-ubuntu-arm64-install.tar.gz -C ~/cgnat-arm64-install
cd ~/cgnat-arm64-install
uname -m
uname -r
sha256sum -c SHA256SUMS
file huawei-cgn-go udp-simulator
chmod +x huawei-cgn-go.test udp-simulator
./huawei-cgn-go.test -test.v
./udp-simulator -h
clickhouse-client --query 'SELECT version()' # si el cliente esta disponible
```

`uname -m` debe ser `aarch64`. `file` debe mostrar ELF 64-bit ARM aarch64.
`-h` del simulador imprime sus opciones y termina con codigo 0. La
comprobacion SHA solo cubre los binarios del mismo paquete. El ejecutable
`huawei-cgn-go.test` permite correr las pruebas Linux sin instalar Go.

Si se instala Go 1.26 o posterior en el servidor, ejecutar desde el repositorio:

```bash
GOTOOLCHAIN=local GOPROXY=off go test ./...
```

La prueba Linux abre dos sockets `SO_REUSEPORT`, acopla CBPF y lee un paquete
con `recvmmsg`. Se requiere que el puerto UDP loopback y BPF esten permitidos.

## Configuracion de prueba

Usar la unidad `deploy/huawei-cgn-go-test.service` y la plantilla
`huawei-cgn-go.example` como base. Ajustar usuario, directorios y
`ReadWritePaths` segun las rutas reales, siguiendo el README principal.
Reservar un puerto para prueba (ejemplo `19088`) si produccion usa `9088`.
Configurar una tabla y spool de prueba separados, con credenciales validas:

```ini
LISTEN_ADDR=0.0.0.0:19088
CLICKHOUSE_TABLE=cgnat.huawei_cgn_nat_v2_test
FAILED_SPOOL_BASE=/index2/huawei-cgn-go-test/failed
ALERT_STATE_DIR=/index2/huawei-cgn-go-test/alerts
CLICKHOUSE_ALERT_TABLE=cgnat.collector_alerts_test
UDP_RECEIVE_MODE=reuseport_bpf
UDP_RECEIVERS=16
UDP_REUSEPORT_HASH_OFFSETS=20,36
UDP_BATCH_SIZE=128
ALERTS_ENABLED=false
```

No iniciar dos instancias en el mismo puerto durante la prueba. Si se usa la
unidad de prueba incluida, confirmar que `EnvironmentFile` apunte al archivo
de prueba y que `ExecStart` apunte al binario ARM64 instalado. Antes de cargar
trafico, confirmar que ClickHouse acepta la creacion de tabla y la insercion;
la version mayor 23 por si sola no demuestra compatibilidad del esquema.

```bash
sudo systemctl daemon-reload
sudo systemctl start huawei-cgn-go-test
sudo systemctl status huawei-cgn-go-test --no-pager -l
sudo journalctl -u huawei-cgn-go-test -b --since '-2 min' --no-pager -l | grep -E 'collector_started|reuseport_bpf_attached|error|failed'
ss -ulnp | grep ':19088'
```

Debe aparecer `reuseport_bpf_attached sockets=16` y
`udp_receive_mode=reuseport_bpf`. Si falla el acoplamiento, guardar el error
literal del journal para diagnostico; no cambiar silenciosamente a
`shared_socket` porque la medicion dejaria de evaluar la ruta objetivo.

## Simulacion y captura de evidencias

Ejecutar el simulador preferiblemente en otro host ARM64 para incluir la red
real. Si se ejecuta en el collector con `127.0.0.1`, la medicion solo evalua el
camino local. Reemplazar `IP_COLLECTOR` y conservar los logs de ambos lados:

```bash
./udp-simulator -target IP_COLLECTOR:19088 -mode legacy -pps 10000 -duration 2m -workers 8 2>&1 | tee simulator-10k.log
./udp-simulator -target IP_COLLECTOR:19088 -mode multi -records 8 -start-pps 10000 -max-pps 100000 -step-pps 10000 -step-every 30s -duration 5m -workers 16 2>&1 | tee simulator-ramp.log
```

En el collector, guardar metricas antes, durante y despues de cada corrida:

```bash
date -Is
sudo journalctl -u huawei-cgn-go-test -b --since '-15 min' --no-pager -l | grep -E 'collector_started|reuseport_bpf_attached|metrics |udp_read_error|insert_error|clickhouse_status|failed'
ss -u -n -a -p -m 'sport = :19088'
nstat -az UdpInDatagrams UdpInErrors UdpRcvbufErrors
find /index2/huawei-cgn-go-test/failed/open -maxdepth 1 -type f | wc -l
find /index2/huawei-cgn-go-test/failed/done -maxdepth 1 -type f | wc -l
```

Para confirmar inserciones, sustituir la fecha UTC usada por los eventos del
simulador en el sufijo `YYYY_MM_DD` (ver el nombre real en los logs):

```sql
SELECT count() FROM cgnat.huawei_cgn_nat_v2_test_YYYY_MM_DD;
```

Comparar `sent` del simulador con el incremento de `total_received` del
collector, y los incrementos de `total_udp_kernel_drops`,
`total_packet_queue_drops`, `total_parsed`, `total_inserted`,
`total_insert_errors`, `total_failed_rows_spooled` y
`total_insert_dropped_rows`. En modo multi, un paquete puede producir ocho
registros: comparar PPS con paquetes y RPS con registros. Revisar tambien
`udp_average_batch_10s`, `udp_receiver_min_10s`, `udp_receiver_max_10s`,
`queue_packet`, `queue_batch` y `ss`/`nstat`. Los contadores `nstat` son del
host, no de un solo proceso. Un `sent` exitoso solo confirma que el simulador
entrego el datagrama al stack local, no que el collector lo recibio.

No concluir una capacidad maxima a partir de compilacion o de una sola tasa.
Si una corrida produce drops o cola creciente, conservar la tasa y todos los
contadores anteriores para separar limite de recepcion, parseo e insercion.
