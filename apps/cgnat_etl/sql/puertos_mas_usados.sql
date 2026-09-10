SELECT
    public_ip,
    destination_port,
    count() AS sesiones,
    sum(packet_size) AS bytes_totales,
    uniqExact(private_ip) AS abonados_distintos
FROM {source_table}
WHERE destination_port IN
    (3478,443,53,25,587,465,110,995,143,993,5228,5229,5230,
     3479,3480,5060,5061,5222,5223,1935,500,4500,1194,22,3389,
     3074,25565,8080,8443,18080,18081)
GROUP BY public_ip, destination_port
