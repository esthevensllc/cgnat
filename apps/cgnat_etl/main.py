#!/usr/bin/env python3
"""Carga diaria CGNAT, compatible con Python 3.7.4 y clickhouse_connect."""
import argparse
import configparser
import ipaddress
import logging
import os
from pathlib import Path
import re
import sys
import threading
import time
import uuid
from contextlib import ExitStack, contextmanager
from datetime import date, datetime, timedelta, timezone


ROOT = Path(__file__).resolve().parent
LOG = logging.getLogger("cgnat_etl")
LIMA = timezone(timedelta(hours=-5))
META = ("fecha_datos", "nodo_origen", "fecha_carga")
CASES = {
    "pool_ips": ("public_ip", "puertos_en_uso", "traducciones_totales",
                 "abonados_compartiendo_ip"),
    "pool_ips_privadas": ("public_ip", "private_ip", "sesiones",
                         "destinos_distintos", "puertos_destino_distintos", "bytes_totales"),
    "carga_nat_hora": ("hour", "ip_publica", "sesiones", "abonados_activos"),
    "abuso_smtp": ("private_ip", "public_ip", "destination_ip", "sesiones", "bytes_totales"),
    "puertos_mas_usados": ("public_ip", "destination_port", "sesiones",
                          "bytes_totales", "abonados_distintos"),
}


class AlreadyRunning(Exception):
    pass


@contextmanager
def process_lock():
    # fcntl pertenece a la biblioteca estandar de Python en el servidor Linux.
    import fcntl
    with open(str(ROOT / ".cgnat_etl.lock"), "a") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise AlreadyRunning()
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Identificador SQL invalido: {!r}".format(value))
    return value


def parse_date(value):
    try:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError()
        result = datetime.strptime(value, "%Y-%m-%d").date()
        if not date(1970, 1, 1) <= result <= date(2149, 6, 6):
            raise ValueError()
        return result
    except ValueError:
        raise argparse.ArgumentTypeError("Fecha invalida; use YYYY-MM-DD dentro del rango Date de ClickHouse")


def positive_int(section, name):
    value = section.getint(name)
    if value is None or value <= 0:
        raise ValueError("{} debe ser un entero positivo".format(name))
    return value


def load_config(path):
    config = configparser.ConfigParser(interpolation=None)
    with open(str(path), encoding="utf-8-sig") as handle:
        config.read_file(handle)
    origins = sorted(s for s in config.sections() if s.startswith("origen_"))
    if origins != ["origen_1", "origen_2", "origen_3", "origen_4"]:
        raise ValueError("Configure exactamente origen_1, origen_2, origen_3 y origen_4")
    process = config["proceso"]
    identifier(process["source_database"])
    identifier(process["source_table_prefix"])
    ipaddress.IPv4Address(process["public_ip"])
    for name in ("batch_size", "max_threads", "max_execution_time",
                 "connect_timeout", "send_receive_timeout"):
        positive_int(process, name)
    if process.getint("send_receive_timeout") <= process.getint("max_execution_time"):
        raise ValueError("send_receive_timeout debe superar max_execution_time")
    endpoints = []
    for name in ["destino"] + origins:
        section = config[name]
        for key in ("host", "username"):
            if not section[key].strip():
                raise ValueError("Falta {} en {}".format(key, name))
        if not section.get("password", fallback="").strip() and not section.get("password_env", fallback="").strip():
            raise ValueError("Falta password o password_env en " + name)
        port = positive_int(section, "port")
        if port > 65535:
            raise ValueError("Puerto fuera de rango en " + name)
        section.getboolean("secure")
        section.getboolean("verify")
        if name in origins:
            endpoints.append((section["host"].lower(), port))
    if len(set(endpoints)) != 4:
        raise ValueError("Los cuatro endpoints de origen deben ser distintos")
    return config


def connection_options(config, name):
    section = config[name]
    if any("CAMBIAR" in section[key] for key in ("host", "username")):
        raise ValueError("Complete host/username en " + name)
    password = section.get("password", fallback="")
    variable = section.get("password_env", fallback="")
    if not password:
        if variable not in os.environ or os.environ[variable] == "REEMPLAZAR":
            raise ValueError("Configure password o la variable de entorno " + variable)
        password = os.environ[variable]
    return dict(
        host=section["host"], port=section.getint("port"),
        username=section["username"], password=password,
        database=section.get("database", fallback="default"), secure=section.getboolean("secure"),
        verify=section.getboolean("verify"),
        connect_timeout=config["proceso"].getint("connect_timeout"),
        send_receive_timeout=config["proceso"].getint("send_receive_timeout"),
        query_retries=0,
    )


def source_table(config, data_date):
    process = config["proceso"]
    return "{}.{}".format(identifier(process["source_database"]), identifier(
        process["source_table_prefix"] + data_date.strftime("%Y_%m_%d")))


def query_for(case, table):
    return (ROOT / "sql" / (case + ".sql")).read_text(encoding="utf-8").format(source_table=table)


def partition_for(case, data_date, public_ip):
    # Valores validados, no texto libre interpolado en DDL.
    day = "'{}'".format(data_date.isoformat())
    if case == "pool_ips_privadas":
        return "({}, {})".format(day, int(ipaddress.IPv4Address(public_ip)))
    return day


def prepare_row(case, row, data_date, node, loaded_at, public_ip):
    values = list(row)
    if case == "pool_ips_privadas":
        values.insert(0, public_ip)
    if case == "carga_nat_hora":
        # query_tz='UTC' controla la conversion del driver. Insertar epoch evita
        # que un datetime naive sea reinterpretado en la zona del proceso Linux.
        hour = values[0]
        if isinstance(hour, datetime):
            if hour.tzinfo is None:
                hour = hour.replace(tzinfo=timezone.utc)
            values[0] = int(hour.timestamp())
    if len(values) != len(CASES[case]):
        raise ValueError("Numero de columnas inesperado en " + case)
    return [data_date, node, loaded_at] + values


def load_case(destination, sources, config, case, data_date, public_ip):
    """Publica una particion solo tras completar la lectura de los cuatro nodos."""
    target = "elog.cgnat_" + case
    staging = "elog._cgnat_etl_{}_{}".format(case, uuid.uuid4().hex)
    process = config["proceso"]
    batch_size = process.getint("batch_size")
    query = query_for(case, source_table(config, data_date))
    parameters = {"public_ip": public_ip} if case == "pool_ips_privadas" else None
    columns = list(META + CASES[case])
    loaded_at = int(time.time())
    total = 0
    started = time.monotonic()
    LOG.info("Inicio caso=%s fecha=%s auxiliar=%s", case, data_date, staging)
    try:
        destination.command("CREATE TABLE {} AS {}".format(staging, target))
        for name, client in sources:
            node = config[name]["host"]
            node_rows = 0
            batch = []
            LOG.info("Consulta caso=%s nodo=%s", case, node)
            with client.query_row_block_stream(
                query, parameters=parameters, query_tz="UTC",
                settings={"max_block_size": batch_size,
                          "max_threads": process.getint("max_threads"),
                          "max_execution_time": process.getint("max_execution_time"),
                          "timeout_overflow_mode": "throw",
                          "read_overflow_mode": "throw",
                          "result_overflow_mode": "throw",
                          "group_by_overflow_mode": "throw"},
            ) as stream:
                for block in stream:
                    for row in block:
                        batch.append(prepare_row(case, row, data_date, node, loaded_at, public_ip))
                        node_rows += 1
                        if len(batch) >= batch_size:
                            destination.insert(staging, batch, column_names=columns,
                                               settings={"async_insert": 0})
                            batch = []
                if batch:
                    destination.insert(staging, batch, column_names=columns,
                                       settings={"async_insert": 0})
            total += node_rows
            LOG.info("Leido caso=%s nodo=%s filas=%d", case, node, node_rows)
        # Validar que todos los INSERT sincronos persistieron la cantidad esperada.
        actual = int(destination.query("SELECT count() FROM " + staging).result_rows[0][0])
        if actual != total:
            raise RuntimeError("Conteo auxiliar incorrecto: esperado={} real={}".format(total, actual))
        partition = partition_for(case, data_date, public_ip)
        if total:
            destination.command("ALTER TABLE {} REPLACE PARTITION {} FROM {}".format(
                target, partition, staging))
        else:
            # Una lectura vacia COMPLETA tambien debe quitar un resultado anterior.
            destination.command("ALTER TABLE {} DROP PARTITION {}".format(target, partition))
        LOG.info("Publicado caso=%s fecha=%s filas=%d segundos=%.1f", case, data_date,
                 total, time.monotonic() - started)
        return total
    finally:
        try:
            destination.command("DROP TABLE IF EXISTS " + staging)
        except Exception as exc:
            LOG.error("No se pudo limpiar auxiliar=%s error=%s", staging, type(exc).__name__)


def initialize(destination):
    sql = (ROOT / "sql" / "01_crear_tablas.sql").read_text(encoding="utf-8")
    for statement in sql.split(";"):
        if statement.strip():
            destination.command(statement.strip())
    LOG.info("Esquema elog y cinco tablas inicializados")


def open_client(stack, factory, options, name, case):
    settings = options[name]
    LOG.info("Conectando %s=%s:%d usuario=%s base=%s caso=%s", name, settings["host"],
             settings["port"], settings["username"], settings["database"], case)
    client = factory(**settings)
    stack.callback(client.close)
    return client


def execute_case(config, case, data_date, public_ip, factory, options):
    """Cada caso posee sus clientes, sesiones y auxiliar; no los comparte con otros hilos."""
    try:
        with ExitStack() as stack:
            destination = open_client(stack, factory, options, "destino", case)
            sources = []
            for name in ("origen_1", "origen_2", "origen_3", "origen_4"):
                client = open_client(stack, factory, options, name, case)
                if not hasattr(client, "query_row_block_stream"):
                    raise RuntimeError("clickhouse_connect instalado no soporta query_row_block_stream")
                sources.append((name, client))
            load_case(destination, sources, config, case, data_date, public_ip)
        return True
    except Exception as exc:
        # No imprimir excepciones del driver: pueden incluir URL o datos sensibles.
        LOG.error("Fallo caso=%s fecha=%s error=%s; no se confirma publicacion. "
                  "Revisar query_log del servidor y repetir el caso.",
                  case, data_date, type(exc).__name__)
        return False


def run(config, cases, data_date, public_ip, factory, init_db=False):
    names = [] if init_db else ["origen_1", "origen_2", "origen_3", "origen_4"]
    # Validar todas las credenciales antes de abrir conexiones o iniciar hilos.
    options = {name: connection_options(config, name) for name in ["destino"] + names}
    if init_db:
        with ExitStack() as stack:
            destination = open_client(stack, factory, options, "destino", "init_db")
            initialize(destination)
        return 0

    results = [False] * len(cases)

    def execute(index, case):
        results[index] = execute_case(config, case, data_date, public_ip, factory, options)

    LOG.info("Ejecucion paralela fecha=%s casos=%d nodos_por_caso=secuenciales",
             data_date, len(cases))
    threads = []
    try:
        for index, case in enumerate(cases):
            # Un hilo fijo por caso seleccionado, sin pool ni paralelismo adicional por nodo.
            thread = threading.Thread(target=execute, args=(index, case), name="cgnat-" + case)
            thread.start()
            threads.append(thread)
    finally:
        # Mantener el proceso y su bloqueo hasta que terminen todos los casos iniciados.
        for thread in threads:
            thread.join()
    failed = [case for index, case in enumerate(cases) if not results[index]]
    LOG.info("Fin fecha=%s casos_correctos=%d casos_fallidos=%s", data_date,
             len(cases) - len(failed), ",".join(failed) or "ninguno")
    return 1 if failed else 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config.ini")
    parser.add_argument("--fecha", type=parse_date,
                        default=datetime.now(LIMA).date() - timedelta(days=1),
                        help="YYYY-MM-DD; por defecto ayer en America/Lima")
    parser.add_argument("--caso", choices=["todos"] + list(CASES), default="todos")
    parser.add_argument("--public-ip", help="IP publica para pool_ips_privadas")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Mostrar SQL sin conectar ni insertar")
    mode.add_argument("--init-db", action="store_true", help="Crear esquema/tablas solo en destino y salir")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        config = load_config(args.config)
        public_ip = str(ipaddress.IPv4Address(args.public_ip or config["proceso"]["public_ip"]))
        cases = list(CASES) if args.caso == "todos" else [args.caso]
        if args.dry_run:
            print("Fecha: {} | tabla: {} | public_ip: {}".format(
                args.fecha, source_table(config, args.fecha), public_ip))
            for case in cases:
                print("\n-- {} -> elog.cgnat_{}\n{}".format(
                    case, case, query_for(case, source_table(config, args.fecha))))
            return 0
        import clickhouse_connect
        with process_lock():
            return run(config, cases, args.fecha, public_ip, clickhouse_connect.get_client, args.init_db)
    except AlreadyRunning:
        LOG.error("Ya existe una ejecucion activa para este proyecto")
        return 75
    except (ValueError, KeyError, OSError, configparser.Error) as exc:
        LOG.error("Configuracion/archivo: %s", exc)
        return 2
    except ImportError:
        LOG.error("No se encontro clickhouse_connect en este interprete; active el env existente")
        return 2
    except Exception as exc:
        LOG.error("Ejecucion interrumpida: %s. Revisar conectividad, permisos y logs del servidor.",
                  type(exc).__name__)
        return 1


if __name__ == "__main__":
    sys.exit(main())
