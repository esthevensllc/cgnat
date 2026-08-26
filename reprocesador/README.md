# Reprocesador de inserts fallidos

Este directorio es un proyecto Go independiente del collector. Tiene su propio
`go.mod`, su propio `main.go`, pruebas, configuracion y servicio systemd. Ejecutar
`go build .` en la raiz del repositorio sigue compilando solamente el collector.

El reprocesador lee exclusivamente pares completos de:

```text
FAILED_SPOOL_BASE/done/*.rowbinary.done
FAILED_SPOOL_BASE/done/*.rowbinary.done.json
```

Valida la tabla, el formato, las filas y el tamano real del archivo, crea la
tabla diaria si no existe e inserta el cuerpo directamente con
`FORMAT RowBinary`. Un lote se retira de `done` solamente despues de recibir
una respuesta HTTP exitosa de ClickHouse.

Los lotes nuevos incluyen `schema_version: 2` en su metadata y usan el esquema
reducido. Los metadatos historicos sin esa propiedad se interpretan como la
version 1, por lo que pueden reprocesarse con sus columnas originales.

## Estados de los archivos

| Resultado | Accion |
|---|---|
| INSERT exitoso y `REPROCESS_SUCCESS_ACTION=delete` | Elimina el RowBinary y su JSON. |
| INSERT exitoso y `REPROCESS_SUCCESS_ACTION=archive` | Mueve ambos a `failed/reprocessed`. |
| ClickHouse no disponible o INSERT rechazado | Conserva ambos en `failed/done` para el siguiente intento. |
| JSON invalido, tabla insegura, formato incorrecto o tamano diferente | Mueve ambos a `failed/bad` para revision manual. |
| Falta el JSON del lote | No lo modifica y lo reporta como `incomplete_pairs`. |

Solo se permite una instancia en Linux mediante el archivo de bloqueo
`FAILED_SPOOL_BASE/.failed-reprocessor.lock`.

## Compilar

Entrar en esta carpeta y compilar el modulo completo, no solamente `main.go`:

```bash
cd /opt/huawei-cgn-go/reprocesador

GOTOOLCHAIN=local GOPROXY=off \
/usr/local/go/bin/go test ./...

GOTOOLCHAIN=local GOPROXY=off \
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
/usr/local/go/bin/go build \
  -trimpath -ldflags="-s -w" \
  -o /opt/huawei-cgn-go/bin/huawei-cgn-failed-reprocessor.new \
  .

install -o root -g huawei-cgn -m 0755 \
  /opt/huawei-cgn-go/bin/huawei-cgn-failed-reprocessor.new \
  /opt/huawei-cgn-go/bin/huawei-cgn-failed-reprocessor
```

Esto no reemplaza ni reinicia `/opt/huawei-cgn-go/bin/huawei-cgn-go`.

## Preparar configuracion y directorios

El servicio carga primero el mismo archivo del collector para reutilizar
`CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASS` y `FAILED_SPOOL_BASE`.
Los ajustes propios se guardan aparte:

```bash
install -o root -g huawei-cgn -m 0640 \
  /opt/huawei-cgn-go/reprocesador/deploy/huawei-cgn-failed-reprocessor.env.example \
  /etc/huawei-cgn-go/huawei-cgn-failed-reprocessor.env

install -d -o huawei-cgn -g huawei-cgn -m 0750 \
  /index2/huawei-cgn-go/failed/done \
  /index2/huawei-cgn-go/failed/reprocessed \
  /index2/huawei-cgn-go/failed/bad
```

Configuracion inicial conservadora:

```ini
REPROCESS_WORKERS=1
REPROCESS_POLL_SECONDS=30
REPROCESS_MIN_AGE_SECONDS=60
REPROCESS_SCAN_LIMIT=1000
REPROCESS_MAX_RETRIES=3
REPROCESS_RETRY_MS=1000
REPROCESS_HTTP_TIMEOUT_SECONDS=180
REPROCESS_INTER_BATCH_DELAY_MS=250
REPROCESS_AUTO_CREATE_TABLE=true
REPROCESS_SUCCESS_ACTION=delete
REPROCESS_ALLOW_HTTP_REDIRECT=false
```

Mantener inicialmente un solo worker evita competir agresivamente con los
inserts en vivo. Aumentarlo solo despues de medir ClickHouse, las colas del
collector y el crecimiento de `failed/done`.

## Validar sin insertar

Antes de instalar el servicio, cargar el entorno y revisar cinco candidatos:

```bash
set -a
source /etc/huawei-cgn-go/huawei-cgn-go.env
source /etc/huawei-cgn-go/huawei-cgn-failed-reprocessor.env
set +a

runuser -u huawei-cgn -- \
  /opt/huawei-cgn-go/bin/huawei-cgn-failed-reprocessor \
  -once -dry-run -limit 5
```

Para una primera insercion controlada, establecer temporalmente
`REPROCESS_SUCCESS_ACTION=archive`, volver a cargar el entorno y ejecutar:

```bash
runuser -u huawei-cgn -- \
  /opt/huawei-cgn-go/bin/huawei-cgn-failed-reprocessor \
  -once -limit 1
```

El log debe mostrar `reprocess_inserted` y el par debe aparecer en
`failed/reprocessed`. No ejecutar este modo manual mientras el servicio systemd
este activo; la segunda instancia sera rechazada por el bloqueo.

## Instalar y operar con systemd

```bash
install -o root -g root -m 0644 \
  /opt/huawei-cgn-go/reprocesador/deploy/huawei-cgn-failed-reprocessor.service \
  /etc/systemd/system/huawei-cgn-failed-reprocessor.service

systemctl daemon-reload
systemctl enable --now huawei-cgn-failed-reprocessor
systemctl status huawei-cgn-failed-reprocessor --no-pager -l
```

Si `FAILED_SPOOL_BASE` no es `/index2/huawei-cgn-go/failed`, ajustar tambien
`ReadWritePaths` en el unit antes de iniciarlo.

Monitorear el reproceso:

```bash
journalctl -u huawei-cgn-failed-reprocessor -f

find /index2/huawei-cgn-go/failed/done \
  -maxdepth 1 -type f -name '*.rowbinary.done' | wc -l

du -sh /index2/huawei-cgn-go/failed/{done,reprocessed,bad}
```

Detener este servicio no afecta la recepcion UDP:

```bash
systemctl stop huawei-cgn-failed-reprocessor
```

## Consideracion sobre duplicados

El reproceso ofrece semantica al menos una vez. Los lotes cuyo error es
`live_insert_queue_overload` nunca llegaron a la cola live y son candidatos
seguros para reproceso. En cambio, un timeout ocurrido despues de enviar un
INSERT puede ser ambiguo: ClickHouse podria haberlo confirmado internamente sin
que el cliente recibiera la respuesta. Antes de reprocesar masivamente esos
casos, revisar el campo `error` del JSON y definir una estrategia de
deduplicacion basada en una clave de negocio o en la configuracion de
deduplicacion de ClickHouse. El esquema reducido no conserva `event_id`.
