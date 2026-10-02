# ClickHouse offline en Ubuntu ARM64 con datos en /space

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

## 2A. Respaldar y retirar ClickHouse 23.4 de claro

El usuario confirmo que este servidor no tiene ClickHouse en produccion y
autorizo reemplazar la instalacion 23.4. Este paso crea una instancia nueva;
**no migra automaticamente** los datos ni las credenciales viejas. Mantener
el respaldo para poder inspeccionarlos o recuperarlos despues. Ejecutar todo
el bloque siguiente en la misma terminal root, despues de verificar el paquete
del paso 2:

```bash
umask 077
backup_dir="/space/clickhouse-backups/pre-26.5-$(date +%Y%m%d-%H%M%S)"
install -d -m 0700 "$backup_dir"
dpkg-query -W -f='${Status} ${binary:Package} ${Version}\n' \
  clickhouse-client clickhouse-common-static clickhouse-server \
  > "$backup_dir/packages.txt"
systemctl cat clickhouse-server > "$backup_dir/systemd-unit.txt"
systemctl stop clickhouse-server
systemctl is-active clickhouse-server
```

El ultimo comando debe decir `inactive`. Si indica `active`, detenerse y
revisar el servicio; no respaldar datos en escritura. Continuar en la misma
terminal para conservar `backup_dir`:

```bash
tar -C / -cpf "$backup_dir/etc-clickhouse-server.tar" etc/clickhouse-server
tar -C / -cpf "$backup_dir/var-lib-clickhouse.tar" var/lib/clickhouse
if [ -d /var/log/clickhouse-server ]; then
  tar -C / -cpf "$backup_dir/var-log-clickhouse-server.tar" var/log/clickhouse-server
fi
( cd "$backup_dir" && sha256sum ./*.tar > SHA256SUMS && sha256sum -c SHA256SUMS )
```

Solo continuar si los respaldos terminan sin errores y sus comprobaciones
dicen `OK`:

```bash
dpkg --purge clickhouse-client clickhouse-server clickhouse-common-static
```

Solo continuar si `dpkg --purge` termina sin errores. Guardar los directorios
que el paquete haya dejado y quitar posibles unidades locales antiguas:

```bash
if [ -e /etc/clickhouse-server ]; then
  mv -- /etc/clickhouse-server "$backup_dir/etc-clickhouse-server-leftover"
fi
if [ -e /var/lib/clickhouse ]; then
  mv -- /var/lib/clickhouse "$backup_dir/var-lib-clickhouse-leftover"
fi
if [ -e /var/log/clickhouse-server ]; then
  mv -- /var/log/clickhouse-server "$backup_dir/var-log-clickhouse-server-leftover"
fi
if [ -e /etc/systemd/system/clickhouse-server.service.d ]; then
  mv -- /etc/systemd/system/clickhouse-server.service.d \
    "$backup_dir/systemd-service-dropins-leftover"
fi
if [ -e /etc/systemd/system/clickhouse-server.service ]; then
  mv -- /etc/systemd/system/clickhouse-server.service \
    "$backup_dir/systemd-service-override-leftover"
fi
systemctl daemon-reload
printf 'Respaldo: %s\n' "$backup_dir"
```

Los TAR contienen potencialmente credenciales y consultas: mantener el
directorio de respaldo con permisos root.

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
