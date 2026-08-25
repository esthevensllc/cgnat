# Runbook: Diagnostico y optimizacion de compresion ClickHouse

Este runbook permite identificar las columnas que ocupan mas disco en las
tablas diarias `cgnat.huawei_cgn_nat_v2_YYYY_MM_DD` y validar una mejora de
compresion sin afectar el collector ni las tablas productivas existentes.

## Alcance y reglas de seguridad

- El collector inserta mediante `RowBinary`; no cambiar ese formato para una
  optimizacion de disco.
- Las tablas se crean diariamente. No requieren `PARTITION BY` adicional.
- No ejecutar `OPTIMIZE TABLE ... FINAL` ni mutaciones sobre una tabla diaria
  productiva sin una ventana de mantenimiento y espacio libre suficiente.
- No habilitar ZSTD de forma global en ClickHouse. El cambio se prueba y se
  aplica de manera explicita en las columnas de tablas nuevas.
- Las consultas de cardinalidad pueden leer muchos datos. Ejecutarlas fuera del
  periodo de mayor carga o limitar el rango a una hora representativa.

## 1. Seleccionar una tabla representativa

Usar un dia cerrado que tenga trafico normal. Reemplazar la variable antes de
ejecutar las consultas:

```bash
TABLE=huawei_cgn_nat_v2_2026_08_02
```

Verificar version, DDL y tamano actual:

```sql
SELECT version();

SHOW CREATE TABLE cgnat.huawei_cgn_nat_v2_2026_08_02;

SELECT
    table,
    formatReadableSize(sum(data_uncompressed_bytes)) AS sin_comprimir,
    formatReadableSize(sum(data_compressed_bytes)) AS comprimido,
    round(sum(data_uncompressed_bytes) /
          nullIf(sum(data_compressed_bytes), 0), 2) AS ratio
FROM system.columns
WHERE database = 'cgnat'
  AND table = 'huawei_cgn_nat_v2_2026_08_02'
GROUP BY table;
```

## 2. Determinar las columnas que consumen disco

Este resultado define la prioridad de cualquier cambio. No optimizar una
columna sin confirmar que representa una parte relevante del almacenamiento.

```sql
SELECT
    name,
    type,
    compression_codec,
    formatReadableSize(data_compressed_bytes) AS comprimido,
    formatReadableSize(data_uncompressed_bytes) AS sin_comprimir,
    round(data_uncompressed_bytes /
          nullIf(data_compressed_bytes, 0), 2) AS ratio
FROM system.columns
WHERE database = 'cgnat'
  AND table = 'huawei_cgn_nat_v2_2026_08_02'
ORDER BY data_compressed_bytes DESC;
```

Registrar el resultado antes de hacer una prueba. En particular, revisar
`event_id`, `header`, `event_type`, `protocol` y los tres campos de fecha.

## 3. Medir cardinalidad de las columnas de texto

Ejecutar preferentemente en un periodo sin carga. Para reducir el impacto,
usar una hora representativa cambiando las fechas del filtro:

```sql
SELECT
    count() AS filas,
    uniqCombined64(event_type) AS tipos_evento,
    uniqCombined64(protocol) AS protocolos,
    uniqCombined64(header) AS headers,
    uniqCombined64(router_ip) AS routers,
    uniqCombined64(event_id) AS event_ids
FROM cgnat.huawei_cgn_nat_v2_2026_08_02
WHERE event_time >= toDateTime('2026-08-02 10:00:00')
  AND event_time <  toDateTime('2026-08-02 11:00:00')
SETTINGS max_threads = 4;
```

Interpretacion:

- `event_type` y `protocol` normalmente tienen muy pocos valores: son
  candidatos a `LowCardinality(String)`.
- `header` solo es candidato a `LowCardinality(String)` si su cantidad de
  valores distintos es baja frente al total de filas.
- `event_id` suele ser casi unico. Si ademas es una de las columnas con mayor
  espacio comprimido, evaluar una representacion binaria de 20 bytes en vez de
  un hash SHA-1 hexadecimal de 40 caracteres. Esto requiere ajustar consultas,
  portal y el contrato del collector, por lo que no es un cambio automatico.

## 4. DDL candidato para una prueba

Crear una tabla de prueba con el mismo `ORDER BY` y solo codecs que hayan sido
justificados por los pasos anteriores. El siguiente ejemplo es un punto de
partida; `header` se deja como `String` hasta confirmar su cardinalidad.

```sql
CREATE TABLE cgnat.huawei_cgn_nat_v2_comp_test
(
    event_time DateTime CODEC(Delta(4), ZSTD(3)),
    start_time DateTime CODEC(ZSTD(3)),
    end_time DateTime CODEC(ZSTD(3)),
    event_id FixedString(40) CODEC(ZSTD(3)),
    event_type LowCardinality(String) CODEC(ZSTD(3)),
    header String CODEC(ZSTD(3)),
    router_ip IPv4,
    router_port UInt16,
    protocol_id UInt8,
    protocol LowCardinality(String) CODEC(ZSTD(3)),
    private_ip IPv4,
    private_port UInt16,
    public_ip IPv4,
    public_port UInt16,
    destination_ip IPv4,
    destination_port UInt16,
    packet_size UInt16
)
ENGINE = MergeTree
ORDER BY (
    event_time,
    router_ip,
    private_ip,
    public_ip,
    destination_ip,
    private_port,
    public_port
)
SETTINGS index_granularity = 8192;
```

`ZSTD(3)` ofrece una relacion razonable entre menor espacio y uso de CPU. No
usar niveles altos de ZSTD en las tablas de insercion activa sin una prueba de
carga: pueden aumentar la latencia de insercion, los merges y las consultas.

## 5. Copiar una muestra y comparar

Copiar una hora representativa a la tabla de prueba. No ejecutar este comando
contra toda la historia sin medir primero el espacio y el impacto.

```sql
INSERT INTO cgnat.huawei_cgn_nat_v2_comp_test
SELECT *
FROM cgnat.huawei_cgn_nat_v2_2026_08_02
WHERE event_time >= toDateTime('2026-08-02 10:00:00')
  AND event_time <  toDateTime('2026-08-02 11:00:00');
```

Comparar las dos tablas:

```sql
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS comprimido,
    formatReadableSize(sum(data_uncompressed_bytes)) AS sin_comprimir,
    round(sum(data_uncompressed_bytes) /
          nullIf(sum(data_compressed_bytes), 0), 2) AS ratio
FROM system.columns
WHERE database = 'cgnat'
  AND table IN ('huawei_cgn_nat_v2_2026_08_02', 'huawei_cgn_nat_v2_comp_test')
GROUP BY table
ORDER BY table;
```

Comparar tambien por columna:

```sql
SELECT
    table,
    name,
    formatReadableSize(data_compressed_bytes) AS comprimido,
    round(data_uncompressed_bytes /
          nullIf(data_compressed_bytes, 0), 2) AS ratio
FROM system.columns
WHERE database = 'cgnat'
  AND table IN ('huawei_cgn_nat_v2_2026_08_02', 'huawei_cgn_nat_v2_comp_test')
ORDER BY name, table;
```

Validar que el nuevo formato no afecte el uso real:

```sql
SELECT
    count() AS filas,
    min(event_time) AS primera_fecha,
    max(event_time) AS ultima_fecha
FROM cgnat.huawei_cgn_nat_v2_comp_test;
```

La prueba de insercion debe realizarse con el simulador y el monitor del
collector. Aceptar el cambio solo si no aparecen `total_live_insert_skipped`,
`total_failed_rows_spooled`, ni incremento de la alerta de SLA de insercion.

## 6. Aplicar el resultado de forma segura

1. Actualizar el DDL que el collector usa para crear tablas diarias nuevas.
2. Compilar, desplegar y probar antes del cambio de dia o precrear la siguiente
   tabla diaria con el DDL validado.
3. Mantener las tablas historicas sin cambios inicialmente.
4. Si se requiere recomprimir historia, migrar un dia por vez a una tabla nueva
   durante una ventana de mantenimiento. Verificar filas, tamano y consultas
   antes de eliminar la tabla antigua.

No usar `ALTER ... MODIFY COLUMN ... CODEC` sobre todos los dias en produccion
como primer paso. La reescritura de partes consume I/O, CPU y espacio temporal,
y puede competir con el collector y los merges.

## 7. Optimizaciones que requieren una decision funcional

- `protocol` duplica semanticamente `protocol_id`. Eliminarlo ahorra el 100%%
  de esa columna, pero requiere actualizar todas las consultas que lo usan.
- Reducir `event_id` a su hash binario puede ahorrar espacio si domina el
  reporte por columna, pero cambia la representacion que consumen los clientes.
- Un `TTL` para eliminar, mover o exportar datos antiguos suele ser el mayor
  ahorro total de disco. Debe definirse primero la retencion requerida por el
  negocio.
- Aumentar `index_granularity` puede reducir un poco los indices y mejorar la
  compresion, pero empeora filtros selectivos. No es una prioridad sin medir
  consultas reales.

## Resultado esperado

El resultado de este runbook es un DDL basado en mediciones, no una
configuracion generica. La reduccion adicional depende principalmente de la
columna que domine el espacio. Si el ratio global ya es alto, la prioridad es
preservar la capacidad de insercion y la estabilidad del collector.
