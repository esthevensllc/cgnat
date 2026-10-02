# Activar inserciones CGNAT en ClickHouse de claro

Aplica al servicio de prueba `huawei-cgn-go-test` en `claro`. ClickHouse
23.4.6.25 ya debe estar activo y su disco `default` debe apuntar a
`/space/clickhouse/data/`. Ejecutar como `root` en `claro`.

## 1. Verificar el administrador y cerrar el acceso temporal

```bash
clickhouse-client --user=admin --password --query 'SHOW GRANTS'
```

Si la cuenta `admin` funciona, quitar el archivo temporal que otorgaba
permisos de administración al usuario `default` y reiniciar:

```bash
rm -f /etc/clickhouse-server/users.d/90-default-access.xml
systemctl restart clickhouse-server
clickhouse-client --user=admin --password --query 'SELECT version()'
```

Si `admin` no autentica, no quitar todavía el archivo; corregir primero su
acceso.

## 2. Apuntar el colector al ClickHouse local

ClickHouse escucha en `127.0.0.1:8123`, no en la IP `10.96.167.7:8123`.
Como ambos procesos están en `claro`, cambiar solo la URL del servicio de
prueba:

```bash
systemctl stop huawei-cgn-go-test
sed -i 's|^CLICKHOUSE_URL=.*|CLICKHOUSE_URL=http://127.0.0.1:8123|' \
  /etc/huawei-cgn-go/huawei-cgn-go-test.env
grep -E '^(CLICKHOUSE_URL|CLICKHOUSE_USER|CLICKHOUSE_TABLE|CLICKHOUSE_DAILY_TABLES|PARSE_ONLY|ALERTS_ENABLED)=' \
  /etc/huawei-cgn-go/huawei-cgn-go-test.env
```

El archivo debe contener `CLICKHOUSE_USER=admin`,
`CLICKHOUSE_TABLE=cgnat.huawei_cgn_nat_test`,
`CLICKHOUSE_DAILY_TABLES=true` y `PARSE_ONLY=false`. También debe tener
`CLICKHOUSE_PASS` con la contraseña de `admin`; comprobar su presencia sin
mostrarla:

```bash
grep -q '^CLICKHOUSE_PASS=.' /etc/huawei-cgn-go/huawei-cgn-go-test.env \
  && echo 'CLICKHOUSE_PASS presente' || echo 'Falta CLICKHOUSE_PASS'
```

Si falta, editar el archivo directamente en el servidor con un editor y
mantenerlo legible solo para `root` y el grupo que usa el colector. No pegar
la contraseña en el chat ni en un comando que quede en el historial.

## 3. Crear y comprobar las tablas de eventos y alertas

Copiar `deploy/create-cgnat-daily-table.sh` a `claro` y ejecutarlo. El script
lee los nombres reales de las dos tablas del archivo de entorno y toma la
fecha actual de eventos en `America/Lima`:

```bash
bash /root/create-cgnat-daily-table.sh
```

Para el 2 de octubre de 2026, crea
`cgnat.huawei_cgn_nat_test_2026_10_02` y `cgnat.collector_alerts_test`.
La tabla de alertas queda lista aunque `ALERTS_ENABLED=false`; no recibirá
alertas hasta activar esa función. Si se quiere crear una fecha
determinada, pasar el archivo de entorno y la fecha:

```bash
bash /root/create-cgnat-daily-table.sh \
  /etc/huawei-cgn-go/huawei-cgn-go-test.env 2026_10_02
```

El colector crea automáticamente las tablas de eventos de los días
siguientes al recibir el primer lote de cada fecha. Comprobar las dos tablas
sin mostrar las credenciales:

```bash
clickhouse-client --user=admin --password --query \
  "SELECT name, engine FROM system.tables WHERE database = 'cgnat' AND name IN ('huawei_cgn_nat_test_2026_10_02', 'collector_alerts_test') ORDER BY name"
```

## 4. Iniciar y validar una carga pequeña

```bash
systemctl start huawei-cgn-go-test
systemctl is-active huawei-cgn-go-test
journalctl -u huawei-cgn-go-test -b -n 40 --no-pager -l
```

Desde el simulador del servidor 135, enviar una prueba pequeña a la IP de
`claro` en UDP 9088. Luego comprobar las filas con la contraseña de `admin`:

```bash
clickhouse-client --user=admin --password --query \
  'SELECT count() FROM cgnat.huawei_cgn_nat_test_2026_10_02'
journalctl -u huawei-cgn-go-test -b --since '5 minutes ago' --no-pager -l \
  | grep -E 'clickhouse_table_ready|metrics|clickhouse_create_table|insert_error'
```

Si el conteo no sube, revisar primero la URL y la contraseña del entorno,
el estado del servicio y los mensajes `insert_error`. No iniciar la carga de
cinco minutos hasta que esta prueba inserte filas correctamente.
