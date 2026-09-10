-- Ejecutar en el destino 172.19.242.107, una sola vez.
-- nodo_origen conserva las metricas calculadas independientemente en cada servidor.
CREATE DATABASE IF NOT EXISTS elog;

CREATE TABLE IF NOT EXISTS elog.cgnat_pool_ips
(
    fecha_datos Date,
    nodo_origen LowCardinality(String),
    fecha_carga DateTime('UTC'),
    public_ip IPv4,
    puertos_en_uso UInt64,
    traducciones_totales UInt64,
    abonados_compartiendo_ip UInt64
)
ENGINE = MergeTree
PARTITION BY fecha_datos
ORDER BY (fecha_datos, nodo_origen, public_ip);

CREATE TABLE IF NOT EXISTS elog.cgnat_pool_ips_privadas
(
    fecha_datos Date,
    nodo_origen LowCardinality(String),
    fecha_carga DateTime('UTC'),
    public_ip IPv4,
    private_ip IPv4,
    sesiones UInt64,
    destinos_distintos UInt64,
    puertos_destino_distintos UInt64,
    bytes_totales UInt64
)
ENGINE = MergeTree
PARTITION BY (fecha_datos, toUInt32(public_ip))
ORDER BY (fecha_datos, public_ip, nodo_origen, private_ip);

CREATE TABLE IF NOT EXISTS elog.cgnat_carga_nat_hora
(
    fecha_datos Date,
    nodo_origen LowCardinality(String),
    fecha_carga DateTime('UTC'),
    hour DateTime('UTC'),
    ip_publica IPv4,
    sesiones UInt64,
    abonados_activos UInt64
)
ENGINE = MergeTree
PARTITION BY fecha_datos
ORDER BY (fecha_datos, nodo_origen, hour, ip_publica);

CREATE TABLE IF NOT EXISTS elog.cgnat_abuso_smtp
(
    fecha_datos Date,
    nodo_origen LowCardinality(String),
    fecha_carga DateTime('UTC'),
    private_ip IPv4,
    public_ip IPv4,
    destination_ip IPv4,
    sesiones UInt64,
    bytes_totales UInt64
)
ENGINE = MergeTree
PARTITION BY fecha_datos
ORDER BY (fecha_datos, nodo_origen, private_ip, public_ip, destination_ip);

CREATE TABLE IF NOT EXISTS elog.cgnat_puertos_mas_usados
(
    fecha_datos Date,
    nodo_origen LowCardinality(String),
    fecha_carga DateTime('UTC'),
    public_ip IPv4,
    destination_port UInt16,
    sesiones UInt64,
    bytes_totales UInt64,
    abonados_distintos UInt64
)
ENGINE = MergeTree
PARTITION BY fecha_datos
ORDER BY (fecha_datos, nodo_origen, public_ip, destination_port);
