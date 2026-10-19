-- Ejecutar en el destino para agregar solamente las dos tablas nuevas.
-- Requiere que la base elog ya exista.

-- Observaciones por hora/router: no mide usuarios simultaneos.
CREATE TABLE IF NOT EXISTS elog.cgnat_observaciones_router_hora
(
    fecha_datos Date,
    nodo_origen LowCardinality(String),
    fecha_carga DateTime('UTC'),
    hora DateTime('America/Lima'),
    router_ip IPv4,
    registros UInt64,
    ips_privadas_aprox UInt64,
    ips_publicas_aprox UInt64,
    destinos_aprox UInt64
)
ENGINE = MergeTree
PARTITION BY fecha_datos
ORDER BY (fecha_datos, nodo_origen, hora, router_ip);

-- Top 200 diario por nodo de grupos hora/IP publica, no top global del cluster.
CREATE TABLE IF NOT EXISTS elog.cgnat_comparticion_publica_hora
(
    fecha_datos Date,
    nodo_origen LowCardinality(String),
    fecha_carga DateTime('UTC'),
    hora DateTime('America/Lima'),
    public_ip IPv4,
    origenes_observados_aprox UInt64,
    routers UInt64,
    registros UInt64
)
ENGINE = MergeTree
PARTITION BY fecha_datos
ORDER BY (fecha_datos, nodo_origen, hora, public_ip);
