SELECT
    private_ip,
    count() AS sesiones,
    uniqExact(destination_ip) AS destinos_distintos,
    uniqExact(destination_port) AS puertos_destino_distintos,
    sum(packet_size) AS bytes_totales
FROM {source_table}
WHERE public_ip = %(public_ip)s
GROUP BY private_ip
ORDER BY sesiones DESC
