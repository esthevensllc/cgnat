# Instalacion desde cero: collector CGNAT en Ubuntu ARM64

Aplicable al servidor Huawei Kunpeng 920 (`aarch64`, Ubuntu, kernel
`6.8.0-100-generic`). Esta guia instala el collector y un servicio de prueba
desde binarios ARM64 ya compilados. No instala Go ni ClickHouse. ClickHouse
puede incorporarse despues de las pruebas UDP. El directorio `/index2` esta
preparado. Puede estar en el disco raiz durante la prueba. Sustituir
`IP_SERVIDOR`, `IP_CLICKHOUSE`, usuario y contrasena por
valores reales. El portal CGNAT es un componente distinto.

El paquete `dist/cgnat-ubuntu-arm64-install.tar.gz`, incluido en el repositorio,
contiene los binarios,
`SHA256SUMS`, la plantilla de configuracion, dos unidades systemd, el monitor
y esta guia. Los binarios proceden del commit `93213eb` o posterior; revisar
el commit indicado al recibir un paquete actualizado.

Si aun no hay ClickHouse, se puede probar recepcion y parseo con
`PARSE_ONLY=true` en el servicio de prueba. Seguir la seccion
[Recepcion y parseo sin ClickHouse](ubuntu-arm64-udp.md#recepcion-y-parseo-sin-clickhouse).
En ese caso omitir las comprobaciones y credenciales de ClickHouse hasta la
puesta en produccion. `PARSE_ONLY=false` conserva el flujo de insercion normal.

## 1. Copiar el paquete

Desde PowerShell en la raiz del repositorio local:

```powershell
scp .\dist\cgnat-ubuntu-arm64-install.tar.gz usuario@IP_SERVIDOR:/tmp/
```

En Ubuntu, con una cuenta que tenga `sudo`:

```bash
uname -m
uname -r
mkdir -p ~/cgnat-arm64-install
tar -xzf /tmp/cgnat-ubuntu-arm64-install.tar.gz -C ~/cgnat-arm64-install
cd ~/cgnat-arm64-install
sha256sum -c SHA256SUMS
file huawei-cgn-go udp-simulator huawei-cgn-go.test
chmod +x huawei-cgn-go.test udp-simulator
./huawei-cgn-go.test -test.v
```

No ejecutar `apt update` ni `apt install` en este servidor offline. `tar`,
`sha256sum` y `systemd` forman parte de la instalacion base habitual de Ubuntu;
`file`, `iproute2`, `curl` y `procps` ya estaban instalados en este servidor.
Si ya se copio el ZIP anterior, se puede extraer sin `unzip` cuando exista
`python3`:

```bash
mkdir -p ~/cgnat-arm64-install
python3 -m zipfile -e /tmp/cgnat-ubuntu-arm64-install.zip ~/cgnat-arm64-install
```

`uname -m` debe devolver `aarch64`; `sha256sum` debe mostrar tres `OK`; la
prueba debe terminar en `PASS`. Si falla la prueba de CBPF o `recvmmsg`, guardar
la salida completa antes de iniciar el servicio. La prueba usa UDP loopback y
dos sockets `SO_REUSEPORT`.

## 2. Verificar almacenamiento, hora y ClickHouse

```bash
findmnt -T /index2
df -h /index2
df -i /index2
free -h
timedatectl status
```

`/index2` guarda los lotes fallidos y el estado de alertas. Si es un directorio
del disco raiz, `findmnt -T /index2` mostrara `/` como punto de montaje; esto
permite la prueba inicial. Antes de activar produccion, dimensionar el espacio
para una caida de ClickHouse y vigilar `df -h /index2`, porque el spool puede
llenar el disco raiz. Un volumen separado reduce ese impacto, pero no es
requisito del collector. Si se decide usar otra ruta, modificar `FAILED_SPOOL_BASE`,
`ALERT_STATE_DIR`, `ExecStartPre` y `ReadWritePaths` en **ambas** unidades.

Cuando ClickHouse este disponible, comprobar el endpoint:

```bash
curl -fsS --max-time 5 http://IP_CLICKHOUSE:8123/ping
```

Debe responder `Ok.`. En el servidor ClickHouse, un
administrador debe confirmar `SELECT version()`, crear la base si falta y dar
al usuario del collector permiso para `CREATE TABLE`, `INSERT` y `SELECT` en
`cgnat.*`:

```sql
SELECT version();
CREATE DATABASE IF NOT EXISTS cgnat;
```

El collector crea las tablas NAT diarias al recibir eventos. La version mayor
23 no basta para confirmar compatibilidad: la primera insercion de prueba
verificara el DDL y RowBinary contra la version exacta del servidor.
Estos pasos de ClickHouse se omiten cuando el servicio de prueba usa
`PARSE_ONLY=true`.

## 3. Crear usuario y carpetas

Ejecutar en el servidor collector:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin huawei-cgn
sudo install -d -o root -g root -m 0755 /opt/huawei-cgn-go /opt/huawei-cgn-go/bin
sudo install -d -o root -g huawei-cgn -m 0750 /etc/huawei-cgn-go
for base in /index2/huawei-cgn-go /index2/huawei-cgn-go-test; do
  sudo install -d -o huawei-cgn -g huawei-cgn -m 0750 \
    "$base/failed" "$base/failed/open" "$base/failed/done" \
    "$base/alerts" "$base/alerts/outbox" "$base/alerts/bad"
done
id huawei-cgn
```

En una instalacion realmente nueva `useradd` debe crear el usuario. Si el
usuario ya existe, comprobar su identidad y continuar sin volver a crearlo.
Los logs del proceso van a `journald`; no se requiere `/var/log/huawei-cgn-go`.

## 4. Ajustar buffers UDP

```bash
printf '%s\n' \
  'net.core.rmem_default = 16777216' \
  'net.core.rmem_max = 536870912' \
  'net.core.netdev_max_backlog = 250000' \
  | sudo tee /etc/sysctl.d/90-huawei-cgn.conf >/dev/null
sudo sysctl --system
sysctl net.core.rmem_default net.core.rmem_max net.core.netdev_max_backlog
```

La plantilla solicita `UDP_READ_BUFFER_MB=512` para cada uno de 16 sockets.
Comprobar RAM disponible antes de elevar esos valores. Si el kernel limita el
buffer, el collector registra `udp_set_read_buffer_warning`.

## 5. Instalar binario y configuraciones

Desde `~/cgnat-arm64-install`:

```bash
cd ~/cgnat-arm64-install
sudo install -o root -g root -m 0755 huawei-cgn-go /opt/huawei-cgn-go/bin/huawei-cgn-go
sudo install -o root -g huawei-cgn -m 0640 huawei-cgn-go.example /etc/huawei-cgn-go/huawei-cgn-go.env
sudo install -o root -g huawei-cgn -m 0640 huawei-cgn-go.example /etc/huawei-cgn-go/huawei-cgn-go-test.env
sudoedit /etc/huawei-cgn-go/huawei-cgn-go.env
sudoedit /etc/huawei-cgn-go/huawei-cgn-go-test.env
```

En **ambos** archivos definir la IP real del collector y conservar el modo
optimizado:

```ini
ALERT_SERVER_IP=IP_SERVIDOR
UDP_RECEIVE_MODE=reuseport_bpf
```

Cuando ClickHouse este disponible, completar en ambos archivos:

```ini
CLICKHOUSE_URL=http://IP_CLICKHOUSE:8123
CLICKHOUSE_USER=USUARIO_REAL
CLICKHOUSE_PASS=CONTRASENA_REAL
```

Con `PARSE_ONLY=true` en el entorno de prueba, esos tres valores de
ClickHouse no se utilizan. No iniciar produccion mientras sigan como
marcadores.

En produccion, dejar `LISTEN_ADDR=0.0.0.0:9088`,
`CLICKHOUSE_TABLE=cgnat.huawei_cgn_nat_v2`, las rutas `/index2/huawei-cgn-go`
y `ALERTS_ENABLED=false` durante la puesta en marcha.

En el archivo de prueba, cambiar **ademas**:

```ini
LISTEN_ADDR=0.0.0.0:19088
CLICKHOUSE_TABLE=cgnat.huawei_cgn_nat_v2_test
FAILED_SPOOL_BASE=/index2/huawei-cgn-go-test/failed
ALERT_STATE_DIR=/index2/huawei-cgn-go-test/alerts
CLICKHOUSE_ALERT_TABLE=cgnat.collector_alerts_test
ALERTS_ENABLED=false
PARSE_ONLY=true
```

Mantener `UDP_RECEIVERS=16`, `UDP_BATCH_SIZE=128` y
`UDP_REUSEPORT_HASH_OFFSETS=20,36` inicialmente. Son valores de partida, no
una capacidad garantizada. Antes de activar produccion, verificar que no
queden marcadores en su archivo:

```bash
sudo grep -nE 'REEMPLAZAR|CLICKHOUSE_HOST' /etc/huawei-cgn-go/huawei-cgn-go.env
sudo stat -c '%U:%G %a %n' /etc/huawei-cgn-go/*.env
```

`grep` no debe imprimir lineas. No compartir el contenido del `.env` porque
contiene la contrasena.

## 6. Instalar servicios systemd

```bash
cd ~/cgnat-arm64-install
ls -l huawei-cgn-go.service huawei-cgn-go-test.service
ls -l /opt/huawei-cgn-go/bin/huawei-cgn-go /etc/huawei-cgn-go/huawei-cgn-go-test.env
sudo install -o root -g root -m 0644 huawei-cgn-go.service /etc/systemd/system/huawei-cgn-go.service
sudo install -o root -g root -m 0644 huawei-cgn-go-test.service /etc/systemd/system/huawei-cgn-go-test.service
sudo systemd-analyze verify /etc/systemd/system/huawei-cgn-go.service /etc/systemd/system/huawei-cgn-go-test.service
sudo systemctl daemon-reload
sudo systemctl start huawei-cgn-go-test
sudo systemctl status huawei-cgn-go-test --no-pager -l
sudo journalctl -u huawei-cgn-go-test -b --since '-3 min' --no-pager -l \
  | grep -E 'collector_started|reuseport_bpf_attached|error|failed'
ss -ulnp | grep ':19088'
```

El arranque correcto muestra `reuseport_bpf_attached sockets=16` y
`udp_receive_mode=reuseport_bpf`. Si no aparece, conservar el error literal del
journal. La unidad de prueba se inicia manualmente y no se habilita al boot.

## 7. Probar paquetes e inserciones

Desde el propio collector, esta prueba usa loopback. Para medir red real,
ejecutar el simulador desde otro host ARM64 y apuntar a la IP del collector.
Si `ufw` esta activo, permitir temporalmente `19088/udp` solo desde la IP del
simulador y retirar esa regla al terminar la prueba.

```bash
cd ~/cgnat-arm64-install
./udp-simulator -target 127.0.0.1:19088 -mode legacy -pps 1000 -duration 30s -workers 4 \
  2>&1 | tee simulator-1k.log
sleep 11
sudo journalctl -u huawei-cgn-go-test -b --since '-5 min' --no-pager -l \
  | grep -E 'metrics |clickhouse_table_ready|insert_error|clickhouse_status|failed'
ss -u -n -a -p -m 'sport = :19088'
nstat -az UdpInDatagrams UdpInErrors UdpRcvbufErrors
```

Con `PARSE_ONLY=true` omitir la consulta siguiente: no se crean tablas ni se
guardan filas. Con ClickHouse y `PARSE_ONLY=false`, consultar la tabla de prueba
que indiquen los logs (sustituir
`YYYY_MM_DD` por la fecha real de los eventos):

```sql
SELECT count(), min(end_time), max(end_time)
FROM cgnat.huawei_cgn_nat_v2_test_YYYY_MM_DD;
```

En modo parseo solamente, `total_received` y `total_parsed` deben aumentar;
`total_inserted` permanece en cero. Con ClickHouse y `PARSE_ONLY=false`, los
tres deben aumentar. Revisar
`total_udp_kernel_drops`, `total_packet_queue_drops`, `total_insert_errors`,
`total_failed_rows_spooled`, `queue_packet` y `queue_batch`. Guardar log del
simulador, metricas del collector y conteo de ClickHouse para comparar envios,
recepcion e insercion. El comando de rampa y el analisis de PPS estan en
[prueba ARM64](ubuntu-arm64-udp.md).

## 8. Pasar a produccion

Despues de disponer de ClickHouse, configurar sus credenciales, cambiar
`PARSE_ONLY=false` en el entorno de produccion y validar una insercion en la
tabla de prueba. Entonces activar produccion:

```bash
sudo systemctl stop huawei-cgn-go-test
sudo systemctl enable --now huawei-cgn-go
sudo systemctl status huawei-cgn-go --no-pager -l
sudo journalctl -u huawei-cgn-go -b --since '-5 min' --no-pager -l \
  | grep -E 'collector_started|reuseport_bpf_attached|metrics |error|failed'
ss -ulnp | grep ':9088'
```

Abrir `9088/udp` desde las IP de los routers en el firewall de Ubuntu o de la
red. Si `ufw` esta activo, por cada origen autorizado:

```bash
sudo ufw status
sudo ufw allow from IP_ROUTER to any port 9088 proto udp
```

Instalar el monitor incluido si se desea:

```bash
cd ~/cgnat-arm64-install
sudo install -o root -g root -m 0755 monitor-cgn.sh /usr/local/bin/monitor-cgn.sh
/usr/local/bin/monitor-cgn.sh 9088 huawei-cgn-go \
  /index2/huawei-cgn-go/failed /index2/huawei-cgn-go/alerts
```

Mantener alertas deshabilitadas durante la primera validacion. Despues pueden
habilitarse primero en `ALERTS_MODE=observe`; para escritura en ClickHouse,
configurar `ALERTS_MODE=clickhouse`, permisos y tabla de alertas segun el
README principal. Cambios al `.env` requieren `sudo systemctl restart
huawei-cgn-go`. No borrar los lotes de `failed` ni el estado de alertas al
reiniciar.
