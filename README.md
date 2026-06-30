# Huawei CGN NAT Collector

Collector UDP escrito en Go para recibir eventos Huawei CGN NAT, conservar
paquetes RAW e insertar los eventos procesados en ClickHouse.

El flujo de alto trafico en Linux usa varios sockets UDP con `SO_REUSEPORT`,
recepcion por lotes con `recvmmsg`, colas RAW separadas por receptor, RAW
binario configurable y workers adaptativos para parseo, armado de batches e
insercion.

## Configuracion

Crear la configuracion local a partir de la plantilla:

```bash
cp huawei-cgn-go.example huawei-cgn-go
```

Completar la URL, el usuario y la contrasena de ClickHouse. El archivo
`huawei-cgn-go` contiene credenciales y esta excluido de Git.

## Compilacion

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
go build -trimpath -ldflags="-s -w" -o bin/huawei-cgn-go .
```
