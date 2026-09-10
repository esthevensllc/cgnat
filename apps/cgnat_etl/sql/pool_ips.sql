SELECT
    public_ip,
    uniqExact(public_port) AS puertos_en_uso,
    count() AS traducciones_totales,
    uniqExact(private_ip) AS abonados_compartiendo_ip
FROM {source_table}
GROUP BY public_ip
