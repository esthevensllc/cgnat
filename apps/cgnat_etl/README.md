# ETL CGNAT: cuatro nodos hacia elog

Proyecto para Python **3.7.4**, usando `clickhouse_connect` del entorno existente.
Consulta los cuatro nodos secuencialmente y publica cinco tablas en
**172.19.242.107 / elog**. No requiere pandas ni dependencias adicionales a las
que ya utiliza el driver. No instala ni actualiza paquetes del env.

## Archivos y despliegue

Copiar el contenido de `apps/` del repositorio a la carpeta `apps/` del servidor:

```text
/index1/tareas/proyectos_python/
  apps/
    run_cgnat_etl.sh
    cgnat_etl/
      main.py
      config.example.ini
      config.ini                    # copia local, no versionar
      credentials.env.example
      sql/
        01_crear_tablas.sql
        pool_ips.sql
        pool_ips_privadas.sql
        carga_nat_hora.sql
        abuso_smtp.sql
        puertos_mas_usados.sql
      tests/test_etl.py
  conf/cgnat_etl.env                 # credenciales locales, permisos 600
  env/bin/activate                   # entorno existente
  logs/cgnat_etl/                    # creado por el shell
```

El shell activa `env`, fija `TZ=America/Lima`, cambia a la carpeta del proyecto
y ejecuta su `python3`. Usa `.` para activar el entorno porque su intérprete es
`/bin/sh`. Los archivos shell deben conservar finales de línea **LF**.

## Configuración inicial

```sh
cd /index1/tareas/proyectos_python/apps/cgnat_etl
cp config.example.ini config.ini
cp credentials.env.example /index1/tareas/proyectos_python/conf/cgnat_etl.env
chmod 600 config.ini /index1/tareas/proyectos_python/conf/cgnat_etl.env
chmod 750 /index1/tareas/proyectos_python/apps/run_cgnat_etl.sh
```

Los orígenes configurados son **10.96.167.132**, **10.96.167.133**,
**10.96.167.134** y **10.96.167.135**. Editar `config.ini` para completar los usuarios.
**8123 es un valor inicial configurable**: debe ser el puerto HTTP de cada
servidor; para HTTPS configurar puerto correspondiente y `secure=true`.
El puerto nativo 9000 no sirve para este driver.

Editar `conf/cgnat_etl.env` con las contraseñas. `password_env` indica el nombre
de la variable, no la contraseña. Si cada nodo tiene credenciales distintas,
asignar variables diferentes en sus secciones y declararlas en el archivo env.
Una contraseña vacía se declara explícitamente como `VARIABLE=''`.
No copiar contraseñas al repositorio. El archivo env se interpreta como shell:
usar comillas adecuadas, especialmente si una contraseña contiene comilla simple.

Confirmar el entorno ya instalado:

```sh
. /index1/tareas/proyectos_python/env/bin/activate
python3 --version
python3 -c "import clickhouse_connect; print(clickhouse_connect.__file__)"
python3 -m pip show clickhouse-connect
```

Se usan `get_client`, `query_row_block_stream`, `query`, `command` e `insert`.
Como referencia, [clickhouse-connect 0.6.23 admite Python 3.7](https://pypi.org/project/clickhouse-connect/0.6.23/).
La versión realmente instalada debe comprobarse en el servidor; no ejecutar
una actualización indiscriminada a la última versión.

Crear las tablas ejecutando **sql/01_crear_tablas.sql en el destino**, o mediante:

```sh
/bin/sh /index1/tareas/proyectos_python/apps/run_cgnat_etl.sh --init-db
```

`--init-db` solamente conecta al destino, crea `elog` y sus cinco tablas y sale.
Los orígenes necesitan permiso `SELECT` sobre las tablas diarias. El usuario
destino necesita `SELECT`, `INSERT`, `CREATE TABLE`, `DROP TABLE` y las operaciones
`ALTER ... REPLACE/DROP PARTITION` en las tablas del ETL; para inicializar también
necesita crear la base si no existe. El administrador puede ejecutar el DDL antes.
Las tablas auxiliares se crean en `elog` con prefijo `_cgnat_etl_`.

## Ejecución y cron

Sin `--fecha`, se procesa **ayer en America/Lima (UTC-05:00)**, tanto desde Python
como desde el shell, independientemente de la zona local del servidor.
Por ejemplo, el 2026-09-10 se consulta `cgnat.huawei_cgn_nat_v2_2026_09_09`.
Para la tabla solicitada:

```sh
/bin/sh /index1/tareas/proyectos_python/apps/run_cgnat_etl.sh --fecha 2026-08-01
```

Ejecución directa de Python (cargar antes el entorno y las credenciales):

```sh
. /index1/tareas/proyectos_python/env/bin/activate
set -a
. /index1/tareas/proyectos_python/conf/cgnat_etl.env
set +a
cd /index1/tareas/proyectos_python/apps/cgnat_etl
python3 main.py --fecha 2026-08-01
# Sin parametro, consulta ayer:
python3 main.py
```

Validar SQL sin conexión, credenciales ni escrituras (puede usarse el ejemplo):

```sh
python3 main.py --config config.example.ini --fecha 2026-08-01 --dry-run
```

Reprocesar solamente un caso, o consultar otra IP pública:

```sh
/bin/sh /index1/tareas/proyectos_python/apps/run_cgnat_etl.sh --fecha 2026-08-01 --caso carga_nat_hora
/bin/sh /index1/tareas/proyectos_python/apps/run_cgnat_etl.sh --fecha 2026-08-01 --caso pool_ips_privadas --public-ip 179.6.75.138
```

Entrada para `crontab -e`: diariamente a las **06:00**, como se solicitó.
La ingestión del día anterior debe haber terminado a esa hora.

```cron
0 6 * * * /bin/sh /index1/tareas/proyectos_python/apps/run_cgnat_etl.sh
```

Cron interpreta el horario según la zona de su servicio/servidor: comprobar que
sea America/Lima o ajustar la expresión. `TZ` dentro del shell controla los logs,
no la hora a la que cron lanza el comando.

Logs: `/index1/tareas/proyectos_python/logs/cgnat_etl/cgnat_etl_YYYYMMDD.log`.
Incluyen caso, nodo, filas, duración, fallos y nombre de tabla auxiliar. Aplicar
la política habitual de retención de logs. Códigos: `0` éxito, `1` fallo de carga,
`2` configuración/dependencia, `75` ejecución simultánea bloqueada.
No se imprimen contraseñas ni excepciones completas del driver.
Para diagnosticar un fallo consultar `system.query_log` en el servidor afectado
y el intervalo horario/nombre de auxiliar del log.

Python usa `fcntl.flock` para impedir solapamientos de ejecuciones directas y
desde el shell. Se requiere permiso de escritura en la carpeta del proyecto
para `.cgnat_etl.lock`; no eliminar este archivo mientras haya procesos activos.
Mantener **un único servidor planificador** y una sola copia del proyecto.
`CGNAT_BASE_DIR` permite
modificar la raíz de despliegue y `CGNAT_ENV_FILE` la ruta de credenciales.

## Datos, resultados y recargas

| Caso / tabla en elog | Resultado |
| --- | --- |
| `cgnat_pool_ips` | Puertos, traducciones y abonados por IP pública |
| `cgnat_pool_ips_privadas` | Sesiones, destinos, puertos y bytes por IP privada para la IP pública elegida |
| `cgnat_carga_nat_hora` | Sesiones y abonados por hora e IP pública |
| `cgnat_abuso_smtp` | Top 10 000 por nodo hacia puerto 25 |
| `cgnat_puertos_mas_usados` | Métricas por IP pública y puerto de la lista indicada |

Cada fila incluye `fecha_datos` (fecha de la tabla origen), `nodo_origen` (host)
y `fecha_carga` (UTC). Se conservan IPv4 y contadores UInt64. `hour` conserva el
instante de `toStartOfHour(start_time)` del origen y se almacena en UTC; consultar
`toTimeZone(hour, 'America/Lima')` para presentación local. Las zonas de los
orígenes deben representar correctamente sus datos. No se aplica otro filtro
de tiempo además de seleccionar la tabla diaria.

Las consultas respetan los agregados indicados. Los volúmenes mencionados son
estimaciones, no límites; únicamente SMTP limita a 10 000 **en cada nodo**, por
lo que puede producir hasta 40 000 filas en destino. Sus empates se ordenan por
IP para que el top sea reproducible. Se eliminó el 3478 repetido de la lista,
sin cambiar el filtro. `bytes_totales` mantiene `sum(packet_size)` exactamente:
representa esa columna, no una estimación adicional de tráfico de sesiones.

Los cuatro nodos deben contener datos independientes, no cuatro réplicas del
mismo conjunto ni tablas Distributed que vuelvan a consultar el cluster.
**Los `uniqExact` se conservan por nodo y no se suman para producir un total
global de únicos**. Para ese total serían necesarios estados agregados o una
consulta global diferente. Los conteos de sesiones solo son aditivos entre
nodos si sus registros no se superponen. El top SMTP tampoco es un top global.

Para cada caso, se leen bloques y se insertan lotes (10 000 por defecto) en una
tabla auxiliar nueva. Solo tras completar los cuatro nodos y verificar la
cantidad insertada se publica con
[`REPLACE PARTITION`](https://clickhouse.com/docs/reference/statements/alter/partition).
Esta sustitución es atómica **por caso/partición**: repetir una fecha reemplaza
el resultado previo y elimina también grupos que ya no existen. No hay una
transacción común para las cinco tablas. Un caso fallido devuelve código 1 y
permite que los restantes se intenten; repetir la fecha o el caso para recuperar.

La partición es el día; para IPs privadas es **(día, IP pública consultada)**,
permitiendo almacenar varias IP públicas sin sobrescribirlas entre sí. Evitar
usar este caso para miles de IPs mediante miles de ejecuciones: requeriría otro
diseño de particionado. Si los cuatro orígenes devuelven cero filas, se elimina
solo esa partición anterior. Una tabla origen inexistente produce fallo, no se
interpreta como cero filas. Antes de publicar un caso fallido se conserva su
resultado anterior. Si se pierde la conexión justo al publicar, el resultado
es incierto: repetir el caso es seguro porque reemplaza la misma partición.

La tabla auxiliar se elimina al terminar. Un corte forzado del proceso o una
caída del destino puede dejar auxiliares: identificarlas mediante el log y
`SHOW TABLES FROM elog LIKE '_cgnat_etl_%'`, comprobar que ninguna ejecución las
usa y eliminar solamente esas tablas auxiliares abandonadas.

Las consultas se ejecutan secuencialmente para moderar la carga. `max_threads`,
`max_execution_time`, timeouts y lotes son configurables. Los bloques acotan
el buffer Python; `uniqExact` sigue necesitando memoria en ClickHouse según la
cardinalidad. Programar sobre tablas con ingestión terminada: la lectura de
cuatro servidores y cinco casos no constituye un snapshot transaccional global.

## Verificación

Pruebas locales sin conexiones (biblioteca estándar):

```sh
python3 -m unittest discover -s tests -v
```

Cubren los cuatro nodos, fallos de lectura/escritura, lotes, recargas, vacíos,
particiones por IP, fechas/zonas horarias y generación de consultas.
En el servidor, crear las tablas y ejecutar una carga controlada antes de
habilitar cron. Comparar conteos por nodo y fecha, por ejemplo:

```sql
SELECT fecha_datos, nodo_origen, count() AS filas,
       sum(traducciones_totales) AS traducciones
FROM elog.cgnat_pool_ips
WHERE fecha_datos = '2026-08-01'
GROUP BY fecha_datos, nodo_origen
ORDER BY nodo_origen;
```

Repetir la misma carga sobre un origen estable debe mantener los conteos.
No se ha realizado aquí una conexión a las bases privadas ni se ha instalado
el crontab del servidor; falta completar usuarios/credenciales y confirmar
los puertos HTTP (se dejó 8123 como valor inicial).
