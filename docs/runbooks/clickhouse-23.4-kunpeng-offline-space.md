# ClickHouse 23.4.6.25 offline en Kunpeng 920, datos en /space

Esta es una instalacion **para la prueba de `claro`**. La version oficial
`23.4.6.25` estuvo instalada y ejecutandose en este mismo equipo antes de
retirarla. El DEB ARM64 `26.5.3.52` falla con `Illegal instruction` porque
el CPU no anuncia `lrcpc`. El diagnostico `aarch64v80compat` funciono, pero
provenia de `master`, no de una version estable.

**23.4 es una version publicada como estable, pero ya no recibe
actualizaciones de seguridad.** Para produccion sostenida se necesitara una
compilacion ARMv8.0 compatible de una rama actual con soporte. No instalar
ni mezclar aqui los DEB 26.5.

El paquete local contiene los tres DEB oficiales `23.4.6.25` para ARM64,
`SHA256SUMS`, `90-space.xml` y `space.conf`. El servidor no necesita Internet.

## 1. Transferir el paquete

Desde PowerShell, en la raiz del repositorio:

```powershell
scp .\.artifacts\clickhouse-23.4.6.25-arm64-offline.tar.gz root@IP_CLARO:/root/
```

En `claro`, como root:

```bash
mkdir -p /root/clickhouse-23.4.6.25-arm64-offline
tar -xzf /root/clickhouse-23.4.6.25-arm64-offline.tar.gz \
  -C /root/clickhouse-23.4.6.25-arm64-offline
cd /root/clickhouse-23.4.6.25-arm64-offline
sha256sum -c SHA256SUMS
dpkg-deb -f clickhouse-common-static_23.4.6.25_arm64.deb Package Version Architecture
dpkg-deb -f clickhouse-server_23.4.6.25_arm64.deb Package Version Architecture
dpkg-deb -f clickhouse-client_23.4.6.25_arm64.deb Package Version Architecture
```

Todos los SHA256 deben indicar `OK`; los paquetes deben mostrar version
`23.4.6.25` y arquitectura `arm64`.

## 2. Verificar el estado antes de instalar

```bash
uname -m
findmnt -M /space -o TARGET,SOURCE,FSTYPE,SIZE,AVAIL
df -hT /space
dpkg --audit
dpkg-query -W -f='${Status} ${binary:Package} ${Version}\n' \
  clickhouse-client clickhouse-server clickhouse-common-static 2>/dev/null || true
systemctl is-active clickhouse-server || true
```

`/space` debe ser el montaje ext4 de aproximadamente 30 T, `dpkg --audit`
no debe listar paquetes pendientes, y `clickhouse-server` debe estar
inactivo. El usuario autorizo borrar los datos y configuraciones de la
instalacion anterior, que estaba limpia. Borrar solo esas rutas residuales
del disco de sistema; **no borrar `/space`**:

```bash
rm -rf --one-file-system -- /etc/clickhouse-server /var/lib/clickhouse \
  /var/log/clickhouse-server /etc/clickhouse-client
```

## 3. Instalar los DEB y dirigir datos a /space

```bash
cd /root/clickhouse-23.4.6.25-arm64-offline
dpkg -i clickhouse-common-static_23.4.6.25_arm64.deb \
  clickhouse-server_23.4.6.25_arm64.deb \
  clickhouse-client_23.4.6.25_arm64.deb
systemctl stop clickhouse-server
dpkg --audit
clickhouse local --query 'SELECT version()'
```

Si el instalador solicita una contrasena para `default`, definirla y
guardarla fuera del historial del shell. Si `dpkg -i`, `dpkg --audit` o la
consulta local fallan, detenerse y revisar la salida antes de continuar.

```bash
install -d -o clickhouse -g clickhouse -m 0750 \
  /space/clickhouse /space/clickhouse/data /space/clickhouse/tmp \
  /space/clickhouse/user_files /space/clickhouse/format_schemas \
  /space/clickhouse/access /space/clickhouse/logs
install -o root -g root -m 0644 90-space.xml \
  /etc/clickhouse-server/config.d/90-space.xml
install -d -o root -g root -m 0755 \
  /etc/systemd/system/clickhouse-server.service.d
install -o root -g root -m 0644 space.conf \
  /etc/systemd/system/clickhouse-server.service.d/space.conf
systemd-analyze verify clickhouse-server.service
systemctl daemon-reload
systemctl restart clickhouse-server
systemctl status clickhouse-server --no-pager -l
```

El fragmento systemd exige que `/space` este montado antes de iniciar.

## 4. Comprobar servicio, ruta y acceso

```bash
systemctl is-active clickhouse-server
clickhouse-client --password --query 'SELECT version()'
clickhouse-client --password --query \
  "SELECT name, path FROM system.disks FORMAT PrettyCompact"
clickhouse-client --password --query 'CREATE DATABASE IF NOT EXISTS cgnat'
df -hT /space
ss -lntp | grep -E ':(8123|9000) '
```

El disco `default` debe mostrar `/space/clickhouse/data/`. Los puertos
8123 y 9000 deben escuchar solo en loopback para esta validacion. El colector
puede seguir con `PARSE_ONLY=true`; no activar inserciones hasta verificar
un lote pequeno y la tabla real de CGNAT.

Si falla el arranque:

```bash
journalctl -u clickhouse-server -b -n 100 --no-pager -l
tail -n 100 /space/clickhouse/logs/clickhouse-server.err.log
```

Fuentes: [reporte Kunpeng](https://github.com/ClickHouse/ClickHouse/issues/102357),
[versiones con soporte](https://github.com/ClickHouse/ClickHouse/security),
[paquetes oficiales](https://packages.clickhouse.com/).
