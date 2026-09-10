import copy
from datetime import date, datetime, timezone
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, mock_open, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import main as etl


DAY = date(2026, 8, 1)
IP = "179.6.75.138"


class Stream:
    def __init__(self, blocks, fail=False):
        self.blocks = blocks
        self.fail = fail
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def __iter__(self):
        for block in self.blocks:
            yield block
        if self.fail:
            raise RuntimeError("Fallo de red despues de leer algunos bloques")


class Source:
    def __init__(self, blocks, fail=False):
        self.stream = Stream(blocks, fail)

    def query_row_block_stream(self, query, **kwargs):
        self.query = query
        self.options = kwargs
        return self.stream


class Destination:
    def __init__(self, fail_insert=False, wrong_count=False):
        self.commands = []
        self.batches = []
        self.fail_insert = fail_insert
        self.wrong_count = wrong_count

    def command(self, command):
        self.commands.append(command)

    def insert(self, table, rows, column_names, settings):
        if self.fail_insert:
            raise RuntimeError("Fallo de insercion")
        self.batches.append((table, copy.deepcopy(rows), column_names))
        assert settings == {"async_insert": 0}

    def query(self, query):
        count = sum(len(batch[1]) for batch in self.batches)
        self.result_rows = [[count + int(self.wrong_count)]]
        return self


class EtlTests(unittest.TestCase):
    def setUp(self):
        self.config = etl.load_config(etl.ROOT / "config.example.ini")
        self.config["proceso"]["batch_size"] = "2"
        self.sources = [("origen_" + str(i), Source([[(IP, 5, 9, 2)] * 3]))
                        for i in range(1, 5)]

    def load(self, destination, case="pool_ips", sources=None):
        return etl.load_case(destination, self.sources if sources is None else sources,
                             self.config, case, DAY, IP)

    def test_stream_batches_and_publish_all_four_nodes(self):
        destination = Destination()
        self.assertEqual(self.load(destination), 12)
        self.assertEqual([len(b[1]) for b in destination.batches], [2, 1] * 4)
        nodes = {row[1] for _, batch, _ in destination.batches for row in batch}
        self.assertEqual(len(nodes), 4)
        self.assertIn("REPLACE PARTITION '2026-08-01' FROM", destination.commands[-2])
        self.assertTrue(destination.commands[-1].startswith("DROP TABLE IF EXISTS elog._cgnat_etl_"))
        for _, source in self.sources:
            self.assertTrue(source.stream.closed)
            self.assertIn("cgnat.huawei_cgn_nat_v2_2026_08_01", source.query)
            self.assertEqual(source.options["settings"]["result_overflow_mode"], "throw")

    def test_failed_fourth_node_does_not_publish_partial_data(self):
        self.sources[-1][1].stream.fail = True
        destination = Destination()
        with self.assertRaises(RuntimeError):
            self.load(destination)
        self.assertFalse(any(c.startswith("ALTER TABLE") for c in destination.commands))
        self.assertTrue(destination.commands[-1].startswith("DROP TABLE IF EXISTS"))

    def test_failed_insert_does_not_publish(self):
        destination = Destination(fail_insert=True)
        with self.assertRaises(RuntimeError):
            self.load(destination)
        self.assertFalse(any(c.startswith("ALTER TABLE") for c in destination.commands))
        self.assertTrue(self.sources[0][1].stream.closed)

    def test_wrong_staging_count_does_not_publish(self):
        destination = Destination(wrong_count=True)
        with self.assertRaises(RuntimeError):
            self.load(destination)
        self.assertFalse(any(c.startswith("ALTER TABLE") for c in destination.commands))

    def test_complete_empty_result_removes_only_requested_partition(self):
        sources = [("origen_" + str(i), Source([])) for i in range(1, 5)]
        destination = Destination()
        self.assertEqual(self.load(destination, sources=sources), 0)
        self.assertEqual(destination.commands[-2],
                         "ALTER TABLE elog.cgnat_pool_ips DROP PARTITION '2026-08-01'")

    def test_rerun_uses_same_partition_and_new_staging_table(self):
        first, second = Destination(), Destination()
        self.load(first)
        self.load(second)
        self.assertNotEqual(first.commands[0], second.commands[0])
        self.assertEqual(first.commands[-2].split(" FROM ")[0], second.commands[-2].split(" FROM ")[0])

    def test_private_pool_keeps_filter_and_isolates_partition(self):
        sources = [("origen_" + str(i), Source([[('10.0.0.1', 5, 2, 3, 80)]]))
                   for i in range(1, 5)]
        destination = Destination()
        self.assertEqual(self.load(destination, "pool_ips_privadas", sources), 4)
        self.assertEqual(destination.batches[0][1][0][3:5], [IP, "10.0.0.1"])
        self.assertEqual(sources[0][1].options["parameters"], {"public_ip": IP})
        self.assertNotEqual(etl.partition_for("pool_ips_privadas", DAY, IP),
                            etl.partition_for("pool_ips_privadas", DAY, "179.6.75.139"))

    def test_hour_round_trip_uses_epoch_independent_of_local_timezone(self):
        for hour in (datetime(2026, 8, 1, 5), datetime(2026, 8, 1, 5, tzinfo=timezone.utc)):
            row = etl.prepare_row("carga_nat_hora", (hour, IP, 1, 1), DAY, "n1", 1, IP)
            self.assertEqual(row[3], int(datetime(2026, 8, 1, 5, tzinfo=timezone.utc).timestamp()))

    def test_sql_identifier_and_ip_reject_injection(self):
        with self.assertRaises(ValueError):
            etl.identifier("x; DROP TABLE elog.foo")
        with self.assertRaises(ValueError):
            etl.partition_for("pool_ips_privadas", DAY, "' OR 1=1")

    def test_sql_preserves_limits_and_exact_aggregates(self):
        self.assertIn("LIMIT 10000", etl.query_for("abuso_smtp", "cgnat.t"))
        self.assertNotIn("LIMIT", etl.query_for("pool_ips_privadas", "cgnat.t"))
        self.assertIn("uniqExact(public_port)", etl.query_for("pool_ips", "cgnat.t"))

    def test_dry_run_requires_no_credentials_or_driver(self):
        with patch.dict(os.environ, {}, clear=True), patch("builtins.print"):
            self.assertEqual(etl.main(["--config", str(etl.ROOT / "config.example.ini"),
                                       "--fecha", "2026-08-01", "--dry-run"]), 0)

    def test_missing_credentials_fail_before_connecting(self):
        with patch.dict(os.environ, {}, clear=True), patch("builtins.print"):
            with self.assertRaises(ValueError):
                etl.connection_options(self.config, "destino")

    def test_initialization_has_five_tables_and_database(self):
        destination = Destination()
        etl.initialize(destination)
        self.assertEqual(len(destination.commands), 6)
        for case in etl.CASES:
            self.assertTrue(any("CREATE TABLE IF NOT EXISTS elog.cgnat_" + case in c
                                for c in destination.commands))

    def test_default_date_is_previous_day_in_lima(self):
        # A las 00:30 UTC del 10 aun es 9 en Lima: debe consultar el dia 8.
        with patch.object(etl, "datetime", wraps=datetime) as clock, patch("builtins.print") as output:
            clock.now.return_value = datetime(2026, 9, 9, 19, 30, tzinfo=etl.LIMA)
            self.assertEqual(etl.main(["--config", str(etl.ROOT / "config.example.ini"),
                                       "--dry-run"]), 0)
            clock.now.assert_called_once_with(etl.LIMA)
            self.assertIn("Fecha: 2026-09-08", output.call_args_list[0][0][0])

    def test_linux_lock_rejects_overlap_and_releases_on_error(self):
        flock = Mock(side_effect=BlockingIOError)
        module = SimpleNamespace(flock=flock, LOCK_EX=2, LOCK_NB=4, LOCK_UN=8)
        with patch.dict(sys.modules, {"fcntl": module}), patch("builtins.open", mock_open()):
            with self.assertRaises(etl.AlreadyRunning):
                with etl.process_lock():
                    self.fail("No debe iniciar una segunda ejecucion")
            flock.side_effect = None
            with self.assertRaises(RuntimeError):
                with etl.process_lock():
                    raise RuntimeError("Fallo durante la carga")
            self.assertEqual(flock.call_args[0][1], module.LOCK_UN)


if __name__ == "__main__":
    unittest.main()
