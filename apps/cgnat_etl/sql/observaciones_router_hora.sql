WITH
    toDateTime(%(inicio)s, 'America/Lima') AS inicio,
    toDateTime(%(fin)s, 'America/Lima') AS fin
SELECT
    toStartOfHour(start_time, 'America/Lima') AS hora,
    router_ip,
    count() AS registros,
    uniqCombined64(private_ip) AS ips_privadas_aprox,
    uniqCombined64(public_ip) AS ips_publicas_aprox,
    uniqCombined64(destination_ip) AS destinos_aprox
FROM {source_table}
WHERE start_time >= inicio
  AND start_time < fin
GROUP BY hora, router_ip
ORDER BY hora, router_ip
