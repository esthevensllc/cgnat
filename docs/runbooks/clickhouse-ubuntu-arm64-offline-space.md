# ClickHouse offline en Ubuntu ARM64 con datos en /space

> **Bloqueado en `claro` (Huawei Kunpeng 920).** El DEB ARM64 oficial
> `26.5.3.52` incluido en este paquete termino con `Illegal instruction`
> durante el `postinst` de `clickhouse-server` el 2026-10-02. No repetir
> `dpkg -i`, `dpkg --configure -a` ni los pasos 3 y 4 en este equipo. El
> paquete no es utilizable aqui. Se esta evaluando una compilacion ARMv8.0
> compatible; debe probarse antes de instalarla como servicio.

Para limpiar el estado parcial de `dpkg` en `claro` (no contiene datos a
conservar, segun confirmacion del usuario):

```bash
dpkg --purge clickhouse-client clickhouse-server clickhouse-common-static
dpkg --audit
grep -m1 '^Features' /proc/cpuinfo
```

`dpkg --audit` debe terminar sin listar paquetes pendientes. El binario
compatible de diagnostico se prueba con `--version` y `local` antes de
considerar otra instalacion. No sustituir el binario del DEB 26.5 por uno
de otra version: la version del ejecutable y la del paquete dejarian de
coincidir.

Servidor objetivo: `claro`, Ubuntu 24.04 ARM64. Version elegida: ClickHouse
`26.5.3.52`, igual a la instancia existente consultada durante la preparacion.
El paquete local `clickhouse-26.5.3.52-arm64-offline.tar.gz` contiene los tres
DEB oficiales, `SHA256SUMS`, el fragmento XML y el fragmento de systemd.
El servidor no necesita salida a Internet.

## 1. Comprobar /space antes de instalar

Ejecutar como root en `claro`:

```bash
uname -m
dpkg --print-architecture
findmnt -M /space -o TARGET,SOURCE,FSTYPE,SIZE,AVAIL
df -hT /space
dpkg-query -W 'clickhouse*' 2>/dev/null || true
```

Continuar solo si la arquitectura es `aarch64`/`arm64` y `/space` aparece como
un punto de montaje independiente de aproximadamente 30 T. En `claro` se
confirmo `ext4`, 29.9 T de capacidad y una instalacion activa de ClickHouse
`23.4.6.25` con 384 MB en `/var/lib/clickhouse`. Seguir el paso 2A para
reemplazar esa instalacion. No crear `/space` como simple directorio sobre
`/`: eso podria llenar el disco raiz.

## 2. Transferir y comprobar el paquete

Desde PowerShell, en la raiz de este repositorio, copiar el archivo preparado:

```powershell
scp .\.artifacts\clickhouse-26.5.3.52-arm64-offline.tar.gz root@IP_CLARO:/root/
```

En `claro`:

```bash
mkdir -p /root/clickhouse-26.5.3.52-arm64-offline
tar -xzf /root/clickhouse-26.5.3.52-arm64-offline.tar.gz \
  -C /root/clickhouse-26.5.3.52-arm64-offline
cd /root/clickhouse-26.5.3.52-arm64-offline
sha256sum -c SHA256SUMS
dpkg-deb -f clickhouse-common-static_26.5.3.52_arm64.deb Package Version Architecture
dpkg-deb -f clickhouse-server_26.5.3.52_arm64.deb Package Version Architecture
dpkg-deb -f clickhouse-client_26.5.3.52_arm64.deb Package Version Architecture
```

Las tres comprobaciones SHA256 deben terminar en `OK`; los DEB deben indicar
version `26.5.3.52` y arquitectura `arm64`. No ejecutar `apt update` ni
instalar dependencias desde la red.

## 2A. Retirar ClickHouse 23.4 de claro

El usuario confirmo que esta instalacion 23.4 no tiene datos que conservar y
autorizo eliminar sus tablas y configuraciones. Este paso borra de forma
definitiva la instancia anterior. Ejecutarlo solo despues de transferir y
verificar los tres DEB nuevos del paso 2.

```bash
systemctl stop clickhouse-server
systemctl is-active clickhouse-server
```

El ultimo comando debe decir `inactive`. Si indica `active`, detenerse y
revisar el servicio. Despues retirar los paquetes antiguos sin usar Internet:

```bash
dpkg --purge clickhouse-client clickhouse-server clickhouse-common-static
```

Solo si `dpkg --purge` termina sin errores, borrar las rutas residuales de la
instalacion antigua. Son rutas explicitas en el disco del sistema; no borrar
`/space`:

```bash
rm -rf --one-file-system -- /etc/clickhouse-server /var/lib/clickhouse \
  /var/log/clickhouse-server /etc/clickhouse-client
rm -rf --one-file-system -- \
  /etc/systemd/system/clickhouse-server.service.d
if [ -e /etc/systemd/system/clickhouse-server.service ]; then
  rm -- /etc/systemd/system/clickhouse-server.service
fi
systemctl daemon-reload
dpkg-query -W -f='${Status} ${binary:Package} ${Version}\n' \
  clickhouse-client clickhouse-common-static clickhouse-server 2>/dev/null || true
```

Las tres entradas deben desaparecer o figurar como no instaladas. No usar
`apt --fix-broken install`: el servidor no tiene salida a Internet.

## 3. Instalar y configurar los directorios

```bash
cd /root/clickhouse-26.5.3.52-arm64-offline
dpkg -i clickhouse-common-static_26.5.3.52_arm64.deb \
  clickhouse-server_26.5.3.52_arm64.deb \
  clickhouse-client_26.5.3.52_arm64.deb

install -d -o clickhouse -g clickhouse -m 0750 \
  /space/clickhouse \
  /space/clickhouse/data \
  /space/clickhouse/tmp \
  /space/clickhouse/user_files \
  /space/clickhouse/format_schemas \
  /space/clickhouse/access \
  /space/clickhouse/caches \
  /space/clickhouse/logs

install -o root -g root -m 0644 90-space.xml \
  /etc/clickhouse-server/config.d/90-space.xml
install -d -o root -g root -m 0755 \
  /etc/systemd/system/clickhouse-server.service.d
install -o root -g root -m 0644 space.conf \
  /etc/systemd/system/clickhouse-server.service.d/space.conf

systemctl daemon-reload
systemctl restart clickhouse-server
systemctl status clickhouse-server --no-pager -l
```

Si el instalador solicita una contrasena para el usuario `default`, definirla
y conservarla de forma segura. No escribirla como argumento de comandos ni
dejarla en el historial del shell.

El fragmento de systemd exige que `/space` sea un montaje real antes de
arrancar el servicio. Si `systemctl restart` falla, revisar el error antes de
modificar rutas o permisos:

```bash
journalctl -u clickhouse-server -b -n 100 --no-pager -l
tail -n 100 /space/clickhouse/logs/clickhouse-server.err.log
```

## 4. Verificar que los datos quedan en /space

```bash
systemctl is-active clickhouse-server
clickhouse-client --password --query 'SELECT version()'
clickhouse-client --password --query \
  "SELECT name, path FROM system.disks FORMAT PrettyCompact"
clickhouse-client --password --query 'CREATE DATABASE IF NOT EXISTS cgnat'
df -hT /space
ss -lntp | grep -E ':(8123|9000) '
```

En `system.disks`, el disco `default` debe apuntar a
`/space/clickhouse/data/`. Los puertos 8123 (HTTP) y 9000 (nativo) quedan
limitados a las interfaces locales con la configuracion predeterminada; no
abrirlos a la red durante esta primera validacion. El colector en el mismo
servidor podra usar `http://127.0.0.1:8123` cuando se habiliten inserciones.
La prueba actual con `PARSE_ONLY=true` puede seguir ejecutandose mientras se
instala ClickHouse; cambiar ese modo solo despues de configurar tabla, acceso
y validacion de inserciones.

## 5. Capacidad

Los 30 T son para datos, archivos temporales, metadatos y logs configurados
arriba. Los binarios y archivos de configuracion permanecen en el disco del
sistema. Medir crecimiento real y reservar margen para las fusiones de partes;
los 30 T no equivalen a 30 T utilizables para filas permanentes.

Fuentes: [paquetes oficiales](https://packages.clickhouse.com/) y
[configuracion en config.d](https://clickhouse.com/docs/concepts/features/configuration/server-config/configuration-files).
