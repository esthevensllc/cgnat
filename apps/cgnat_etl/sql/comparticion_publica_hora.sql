WITH
    toDateTime(%(inicio)s, 'America/Lima') AS inicio,
    toDateTime(%(fin)s, 'America/Lima') AS fin
SELECT
    toStartOfHour(start_time, 'America/Lima') AS hora,
    public_ip,
    uniqCombined64(tuple(router_ip, private_ip)) AS origenes_observados_aprox,
    uniqExact(router_ip) AS routers,
    count() AS registros
FROM {source_table}
WHERE start_time >= inicio
  AND start_time < fin
GROUP BY hora, public_ip
ORDER BY origenes_observados_aprox DESC, hora, public_ip
LIMIT 200
