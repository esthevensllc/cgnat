SELECT
    private_ip,
    public_ip,
    destination_ip,
    count() AS sesiones,
    sum(packet_size) AS bytes_totales
FROM {source_table}
WHERE destination_port IN (25)
GROUP BY private_ip, public_ip, destination_ip
ORDER BY sesiones DESC, private_ip, public_ip, destination_ip
LIMIT 10000
