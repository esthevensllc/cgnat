SELECT
    toStartOfHour(start_time) AS hour,
    public_ip AS ip_publica,
    count() AS sesiones,
    uniqExact(private_ip) AS abonados_activos
FROM {source_table}
GROUP BY hour, public_ip
