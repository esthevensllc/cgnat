# Runbook: acceso ClickHouse por DBeaver

Este procedimiento autoriza una estación de trabajo específica a conectarse al
ClickHouse CGNAT de `LIMSTELOGF04` (`10.96.167.135`). No abrir los puertos a
toda la red ni usar el usuario `default` desde DBeaver.

## Autorizar la estación 172.19.10.186

Ejecutar como `root` en `LIMSTELOGF04`:

```bash
firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="172.19.10.186/32" port port="8123" protocol="tcp" accept'
firewall-cmd --permanent --add-rich-rule='rule family="ipv4" source address="172.19.10.186/32" port port="9000" protocol="tcp" accept'
firewall-cmd --reload

firewall-cmd --list-rich-rules | grep '172.19.10.186'
```

El puerto `8123` corresponde a HTTP/JDBC y debe ser el valor inicial en
DBeaver. El puerto `9000` queda autorizado únicamente para la misma IP si se
usa el protocolo nativo.

## Configuración en DBeaver

```text
Host: 10.96.167.135
Puerto: 8123
Driver: ClickHouse
Usuario: una cuenta ClickHouse autorizada
```

## Retirar el acceso

Ejecutar en el servidor cuando ya no se requiera la conexión:

```bash
firewall-cmd --permanent --remove-rich-rule='rule family="ipv4" source address="172.19.10.186/32" port port="8123" protocol="tcp" accept'
firewall-cmd --permanent --remove-rich-rule='rule family="ipv4" source address="172.19.10.186/32" port port="9000" protocol="tcp" accept'
firewall-cmd --reload
```
