# ClickHouse en Kunpeng 920: fallo de los DEB ARM64 estandar

En `claro`, el paquete oficial ARM64 `26.5.3.52` termino con `Illegal
instruction` al configurar `clickhouse-server`. El CPU anuncia `asimd`,
`aes`, `atomics`, etc., pero no `lrcpc`. El paquete `clickhouse-server` quedo
a medio configurar. Existe un [reporte de ClickHouse sobre Kunpeng 920 con
el mismo fallo](https://github.com/ClickHouse/ClickHouse/issues/102357).
El [instalador de ClickHouse](https://clickhouse.com/) selecciona el binario
`aarch64v80compat` cuando falta `lrcpc`; los DEB ARM64 distribuidos en
`packages.clickhouse.com` usan la compilacion ARM64 estandar.

## Limpiar el intento fallido

El usuario confirmo que la instalacion anterior era desechable. Como el
`postinst` de `clickhouse-server` fallo, no repetir `dpkg -i` ni ejecutar
`dpkg --configure -a` con estos DEB:

```bash
dpkg --purge clickhouse-client clickhouse-server clickhouse-common-static
dpkg --audit
```

`dpkg --audit` debe terminar sin listar paquetes pendientes. El colector
Go sigue en `PARSE_ONLY=true` y no necesita ClickHouse para la prueba de
recepcion y parseo.

## Prueba de compatibilidad, sin instalar el servicio

Se descargo un binario diagnostico oficial de
`https://builds.clickhouse.com/master/aarch64v80compat/clickhouse` el
2026-10-02. **Es una compilacion de `master`, no una version 26 estable
fijada para produccion.** Se conserva localmente en
`.artifacts/clickhouse-armv80compat-diagnostic/clickhouse`, fuera de Git.
Verificar su SHA256 antes de ejecutarlo; el valor registrado para el archivo
descargado es
`aba6e8b362ed8d20de15a7a928a1f0334211a013d784d87e7cd6b29782c9827b`.

Desde PowerShell, en la raiz del repositorio:

```powershell
scp .\.artifacts\clickhouse-armv80compat-diagnostic\clickhouse root@IP_CLARO:/root/clickhouse-armv80compat-test
```

En `claro`:

```bash
echo 'aba6e8b362ed8d20de15a7a928a1f0334211a013d784d87e7cd6b29782c9827b  /root/clickhouse-armv80compat-test' | sha256sum -c -
chmod 0755 /root/clickhouse-armv80compat-test
ulimit -c 0
/root/clickhouse-armv80compat-test --version
/root/clickhouse-armv80compat-test local --query 'SELECT version()'
```

Estos comandos no instalan ni arrancan un servicio. Si terminan bien,
demuestran que la ruta ARMv8.0 compatible ejecuta en este CPU. Para instalar
ClickHouse en `/space` todavia se requiere una compilacion compatible de una
version estable fijada, o una decision explicita de usar otra version. No
mezclar este binario `master` con los DEB 26.5: la version del ejecutable y
la registrada por el paquete dejarian de coincidir.
