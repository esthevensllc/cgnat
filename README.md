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

## Simulador UDP

Compilar el generador para ejecutarlo desde otro servidor:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
go build -trimpath -ldflags="-s -w" -o bin/udp-simulator ./cmd/udp-simulator
```

Prueba fija:

```bash
./bin/udp-simulator -target 10.96.167.132:9088 -mode legacy -pps 10000 -duration 5m -workers 8
```

Prueba incremental:

```bash
./bin/udp-simulator -target 10.96.167.132:9088 -mode multi -records 8 \
  -start-pps 10000 -max-pps 200000 -step-pps 10000 -step-every 30s \
  -duration 10m -workers 16
```
