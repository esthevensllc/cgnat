# Captura de metricas de produccion durante 15 minutos

Objetivo: medir en cada collector los paquetes recibidos y procesados, los
registros parseados e insertados, los errores, las colas y el desvio a reproceso.
No requiere cambiar el binario, reiniciar servicios ni generar trafico artificial.
No captura contenido UDP ni consulta las tablas masivas de ClickHouse.

## Ejecutar en los cuatro servidores

Usar los scripts `deploy/capture-cgn-metrics.sh` y
`deploy/summarize-cgn-metrics.awk` de este repositorio. Deben quedar juntos en
`/opt/huawei-cgn-go/deploy/`; se pueden copiar solos, sin desplegar binarios.
Requiere Bash, awk, systemd/journald, iproute2 y utilidades basicas de Linux.
Ejecutar con root/sudo para leer el journal del sistema y los sockets.

En `10.96.167.132`, `10.96.167.133`, `10.96.167.134` y `10.96.167.135`, abrir
una sesion por servidor y ejecutar aproximadamente a la misma hora:

```bash
sudo bash /opt/huawei-cgn-go/deploy/capture-cgn-metrics.sh \
  huawei-cgn-go 900 9088
```

La consola espera 900 segundos y muestra el resultado. Mantener la sesion abierta.
No ejecutar este comando mediante `watch`: se realiza una captura por servidor.
Argumentos opcionales, en orden: servicio, segundos, puerto UDP, directorio de
resultados y `FAILED_SPOOL_BASE`. Si las rutas o el nombre del servicio difieren,
ajustarlos; no se lee ni se ejecuta el archivo `.env`.

Si al ejecutar aparece `set: pipefail: invalid option name`, comprobar los
finales de linea con `head -n 4 ARCHIVO | cat -A`: un `^M` al final indica CRLF
de Windows. Bash necesita LF. Para corregir ambos archivos, con un usuario que
tenga permiso de escritura sobre ellos:

```bash
sed -i 's/\r$//' \
  /opt/huawei-cgn-go/deploy/capture-cgn-metrics.sh \
  /opt/huawei-cgn-go/deploy/summarize-cgn-metrics.awk

bash -n /opt/huawei-cgn-go/deploy/capture-cgn-metrics.sh
```

Luego repetir la captura. El repositorio fija `eol=lf` para estas herramientas;
al copiarlas por SFTP/WinSCP usar transferencia binaria para conservar ese
formato. Si el usuario no tiene escritura en `/opt`, puede copiar ambos
archivos a un directorio propio, corregir alli las lineas y ejecutar desde ese
directorio. Aun necesitara acceso al journal y una ruta de resultados escribible.

El script toma una muestra previa y la ultima disponible al terminar. El periodo
efectivo se imprime en el resumen: puede diferir algunos segundos de 900 porque
el collector publica metricas cada 10 s. Los snapshots de kernel y spool tienen
su propia hora de inicio/fin; no corresponden exactamente a los extremos del log.

## Evidencia generada

Cada captura crea un directorio unico:

```text
/var/tmp/cgn-metrics/SERVIDOR_FECHA_IDENTIFICADOR/
```

| Archivo | Contenido |
| --- | --- |
| `summary.txt` | Diferencias de contadores, tasas y saldos del collector. |
| `metrics.csv` | Totales y deltas por muestra, aptos para comparar o graficar. |
| `collector.log` | Metricas originales y marcadores de reinicio. |
| `udp-summary.txt` | Incrementos UDP globales del servidor. |
| `spool-summary.txt` | Cantidad y bytes de archivos actuales en `failed/open` y `failed/done`. |
| `sockets.start.txt`, `sockets.end.txt` | Cola de recepcion y campo `d` de drops por socket del puerto. |
| `interfaces.*.txt` | Contadores de interfaces para revisar errores/drops en la NIC. |
| `context.txt`, `time.*.txt`, `service.*.txt` | Identidad del servidor, ventana y proceso observado. |

Si el script informa `CAPTURA NO VALIDA` (codigo 2), revisar la causa y repetir
la ventana: no combinar contadores de dos procesos. Un reinicio del servicio,
contadores decrecientes, un journal incompleto o ausencia de metricas al final
impiden usar el informe como una captura continua. Campos faltantes aparecen
como `NO_DISPONIBLE`; los intervalos sin muestras mayores de 30 s generan aviso.

Para entregar la evidencia de cada servidor:

```bash
sudo tar -czf "/var/tmp/metricas-cgn-$(hostname -s).tgz" \
  -C /var/tmp cgn-metrics
```

Se puede recalcular el resumen de una captura existente sin esperar 15 minutos:

```bash
bash /opt/huawei-cgn-go/deploy/capture-cgn-metrics.sh \
  --analyze /var/tmp/cgn-metrics/DIRECTORIO/collector.log
```

## Como interpretar los resultados

| Indicador | Interpretacion |
| --- | --- |
| Paquetes recibidos | Delta de `total_received`: paquetes entregados al collector. |
| Paquetes procesados sin error | Delta de `total_packet_processed` menos delta de `total_parse_errors`. |
| Registros parseados | Delta de `total_parsed`: registros preparados para insercion; un paquete puede contener varios. |
| Registros insertados live | Delta de `total_inserted`: filas de lotes confirmados por HTTP por ClickHouse. |
| Drops UDP y drops de cola | Cualquier incremento indica descartes observados en ese nivel. Revisar tambien los sockets. |
| Errores de parseo/serializacion | Cualquier incremento requiere revisar el formato y los logs. |
| Filas con fallo de insert live | `total_insert_dropped_rows` no equivale automaticamente a perdida: pueden estar en spool. |
| Filas guardadas para reproceso | Datos desviados de la insercion live, pendientes de confirmacion por el reprocesador. No sumarlas a las filas con fallo live. |
| Errores de spool | Hubo fallos al guardar datos para reproceso. Investigar con prioridad; el archivo puede haber quedado parcial o sin metadatos. |
| Cola y saldos | Comparar inicio/fin y maximos muestreados. Una cola creciente y sostenida indica atraso. |

No comparar paquetes recibidos directamente contra filas insertadas: son unidades
distintas. Tampoco usar `parseados - insertados` de la ventana como perdida: un
insert puede completar un lote formado antes de la ventana, y un registro del
ultimo segundo puede estar esperando su flush al terminar.

El saldo aproximado de paquetes se calcula como
`total_received - total_packet_processed - total_packet_queue_drops`. El saldo
de filas sin confirmacion live ni spool se calcula como
`total_parsed - total_inserted - total_failed_rows_spooled`. Son acumulados desde
el arranque: incluyen buffers, lotes en formacion e inserts en curso; ante errores
de spool tambien pueden incluir datos no conservados. Los contadores se leen de
forma independiente y pueden mostrar pequenas diferencias transitorias.

El collector no descuenta de sus contadores los lotes recuperados por el
reprocesador. Los archivos actuales en `failed/done` y los logs
`reprocessor_metrics` permiten revisar ese backlog; el delta de filas spooled
por si solo no indica cuantas siguen pendientes actualmente.

Una captura favorable tiene recepcion activa, cero incrementos de drops y
errores, ningun desvio nuevo a reproceso y colas/saldos acotados, sin tendencia
creciente durante las muestras. Saldo distinto de cero al final no exige detener
el collector: puede haber lotes normales en curso. Si crece, observar otra ventana.

## Limite de lo que se puede demostrar

Los drops del socket se notifican al recibir nuevos paquetes: si deja de llegar
trafico, el ultimo descarte puede verse antes en `ss` que en el log. Comparar el
campo `d` de los mismos sockets entre los dos snapshots, sin reinicios.
`UdpInErrors` y `UdpRcvbufErrors` son globales, incluyen otros puertos y se
solapan: no sumarlos ni compararlos directamente con el delta del collector.

Cero drops reportados no demuestra que todo lo enviado por los Huawei haya
llegado: puede haber perdida antes del socket (router, red o NIC). Para afirmar
ausencia de perdida de extremo a extremo se necesitan contadores del emisor y
la red en una ventana comparable. Estos logs tampoco auditan filas unicas en
ClickHouse; los reintentos pueden tener resultados ambiguos si se pierde una
respuesta HTTP. La prueba verifica la salud observada del pipeline en esa ventana.
