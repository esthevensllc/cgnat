CREATE DATABASE IF NOT EXISTS cgnat;

CREATE TABLE IF NOT EXISTS cgnat.collector_alerts
(
    id UUID,
    hostname LowCardinality(String),
    ip String,
    fecha_inicio DateTime64(6, 'America/Lima'),
    fecha_fin Nullable(DateTime64(6, 'America/Lima')),
    nombre LowCardinality(String),
    umbral Float64,
    porcentaje_indicador Float64,
    estado LowCardinality(String),
    fecha_envio DateTime64(6, 'America/Lima'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(fecha_inicio)
ORDER BY id
SETTINGS index_granularity = 8192;

-- ReplacingMergeTree conserva versiones fisicas hasta que ocurren los merges.
-- FINAL devuelve solamente el ultimo estado de cada incidente.
SELECT
    id,
    hostname,
    ip,
    fecha_inicio,
    fecha_fin,
    nombre,
    umbral,
    porcentaje_indicador,
    estado,
    fecha_envio
FROM cgnat.collector_alerts FINAL
ORDER BY fecha_envio DESC
LIMIT 100;
