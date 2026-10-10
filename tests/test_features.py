"""Transactions, prepared statements, streaming, pipelined executemany, row factories and
adapters, against a real ``nusadb-server``."""

from __future__ import annotations

import gc
import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import nusadb  # noqa: E402
import nusadb.connection as connection_module  # noqa: E402
from nusadb import _protocol as proto  # noqa: E402
from nusadb.rows import dict_row, namedtuple_row, tuple_row  # noqa: E402
from test_driver import _Server  # noqa: E402


class _ServerCase(unittest.TestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def connect(self, **kw: object) -> nusadb.Connection:
        conn = nusadb.connect(port=self.server.port, user="u", password=None, **kw)
        self.addCleanup(conn.close)
        return conn

    def table(self, conn: nusadb.Connection, name: str, columns: str = "id INT PRIMARY KEY") -> None:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {name}")
        cur.execute(f"CREATE TABLE {name} ({columns})")

    @staticmethod
    def values(conn: nusadb.Connection, sql: str) -> list:
        cur = conn.cursor()
        cur.execute(sql)
        return [row[0] for row in cur.fetchall()]


class TransactionBlockTests(_ServerCase):
    def test_a_block_commits_or_rolls_back(self) -> None:
        conn = self.connect()
        self.table(conn, "tb")
        with conn.transaction():
            conn.cursor().execute("INSERT INTO tb VALUES (1)")
            self.assertEqual(conn.transaction_status, "in_transaction")
        self.assertEqual(conn.transaction_status, "idle")
        with self.assertRaises(ZeroDivisionError):
            with conn.transaction():
                conn.cursor().execute("INSERT INTO tb VALUES (2)")
                1 / 0
        self.assertEqual(self.values(conn, "SELECT id FROM tb ORDER BY id"), [1])

    def test_a_nested_block_is_a_savepoint(self) -> None:
        conn = self.connect()
        self.table(conn, "tn")
        with conn.transaction():
            conn.cursor().execute("INSERT INTO tn VALUES (1)")
            with self.assertRaises(KeyError):
                with conn.transaction():
                    conn.cursor().execute("INSERT INTO tn VALUES (2)")
                    raise KeyError
            with conn.transaction():
                conn.cursor().execute("INSERT INTO tn VALUES (3)")
        self.assertEqual(self.values(conn, "SELECT id FROM tn ORDER BY id"), [1, 3])

    def test_isolation_and_read_only(self) -> None:
        conn = self.connect()
        self.table(conn, "ti")
        with conn.transaction(isolation="Serializable"):
            self.assertEqual(
                self.values(conn, "SHOW transaction_isolation"), ["serializable"]
            )
        with self.assertRaises(nusadb.DatabaseError):
            with conn.transaction(read_only=True):
                conn.cursor().execute("INSERT INTO ti VALUES (1)")
        self.assertEqual(self.values(conn, "SELECT count(*) FROM ti"), [0])
        with conn.transaction(read_only=False):
            conn.cursor().execute("INSERT INTO ti VALUES (1)")
        self.assertEqual(self.values(conn, "SELECT count(*) FROM ti"), [1])
        with self.assertRaises(nusadb.ProgrammingError):
            conn.transaction(isolation="chaotic")
        with conn.transaction():
            with self.assertRaises(nusadb.ProgrammingError):
                with conn.transaction(isolation="serializable"):
                    pass

    def test_a_failed_statement_makes_the_block_raise(self) -> None:
        conn = self.connect()
        self.table(conn, "tf")
        with self.assertRaises(nusadb.TransactionRolledBack):
            with conn.transaction():
                cur = conn.cursor()
                cur.execute("INSERT INTO tf VALUES (1)")
                try:
                    cur.execute("INSERT INTO tf VALUES (1)")
                except nusadb.IntegrityError:
                    pass
        self.assertEqual(self.values(conn, "SELECT count(*) FROM tf"), [0])


class InterruptedTransactionTests(_ServerCase):
    def test_an_interrupt_in_a_nested_block_keeps_its_exception_and_breaks_the_connection(self) -> None:
        conn = self.connect()
        self.table(conn, "ti2")
        real = conn._read_message

        def interrupted() -> None:
            conn._read_message = real
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            with conn.transaction():
                with conn.transaction():
                    conn._read_message = interrupted
                    conn.cursor().execute("INSERT INTO ti2 VALUES (1)")
        self.assertTrue(conn.broken)
        with self.assertRaises(nusadb.OperationalError):
            conn.cursor().execute("SELECT 7")
        other = self.connect()
        self.assertEqual(self.values(other, "SELECT count(*) FROM ti2"), [0])

    def test_a_failure_while_undoing_does_not_replace_the_error(self) -> None:
        conn = self.connect()
        with self.assertRaises(ZeroDivisionError):
            with conn.transaction():
                conn.cursor().execute("SELECT 1")
                conn._sock.close()  # the server end is gone: the rollback cannot be sent
                1 / 0
        self.assertTrue(conn.broken)


class PreparedStatementTests(_ServerCase):
    def test_prepare_and_run_many_times(self) -> None:
        conn = self.connect()
        self.table(conn, "ps", "id INT PRIMARY KEY, name TEXT")
        insert = conn.prepare("INSERT INTO ps VALUES ($1, $2)")
        cur = conn.cursor()
        cur.execute(insert, [1, "a"])
        self.assertEqual(cur.rowcount, 1)
        cur.executemany(insert, [(2, "b"), (3, "c")])
        self.assertEqual(cur.rowcount, 2)
        select = conn.prepare("SELECT name FROM ps WHERE id = $1")
        cur.execute(select, [2])
        self.assertEqual(cur.fetchall(), [("b",)])
        insert.close()
        insert.close()
        with self.assertRaisesRegex(nusadb.ProgrammingError, "closed"):
            cur.execute(insert, [4, "d"])
        other = self.connect()
        with self.assertRaisesRegex(nusadb.ProgrammingError, "another connection"):
            other.cursor().execute(select, [1])

    def test_prepare_cached_reuses_and_evicts(self) -> None:
        conn = self.connect()
        first = conn.prepare_cached("SELECT 1")
        self.assertIs(conn.prepare_cached("SELECT 1"), first)
        old = connection_module.STATEMENT_CACHE_SIZE
        connection_module.STATEMENT_CACHE_SIZE = 2
        try:
            second = conn.prepare_cached("SELECT 2")
            conn.prepare_cached("SELECT 1")  # now the most recently used
            conn.prepare_cached("SELECT 3")  # evicts SELECT 2
        finally:
            connection_module.STATEMENT_CACHE_SIZE = old
        self.assertTrue(second.closed)
        self.assertFalse(first.closed)
        self.assertIsNot(conn.prepare_cached("SELECT 2"), second)

    def test_a_bad_statement_fails_when_it_runs(self) -> None:
        conn = self.connect()
        statement = conn.prepare("SELEC 1")
        with self.assertRaises(nusadb.ProgrammingError):
            conn.cursor().execute(statement)
        self.assertEqual(self.values(conn, "SELECT 1"), [1])


class StreamTests(_ServerCase):
    def setUp(self) -> None:
        self.conn = self.connect()
        self.table(self.conn, "st", "id INT PRIMARY KEY, v TEXT")
        self.conn.cursor().execute(
            "INSERT INTO st SELECT i, 'v' || i FROM generate_series(1, 2500) AS g(i)"
        )

    def test_rows_arrive_in_batches_and_match_a_plain_query(self) -> None:
        reads = []
        original = self.conn._send

        def counting(frame: bytes) -> None:
            if frame[0] == ord("E"):
                reads.append(frame)
            original(frame)

        self.conn._send = counting  # type: ignore[method-assign]
        with self.conn.stream("SELECT id, v FROM st WHERE id > $1 ORDER BY id", [0], 1000) as rows:
            self.assertEqual([d[0] for d in rows.description], ["id", "v"])
            got = list(rows)
        self.conn._send = original  # type: ignore[method-assign]
        self.assertEqual(len(reads), 3)
        self.assertEqual(rows.rowcount, 2500)
        cur = self.conn.cursor()
        cur.execute("SELECT id, v FROM st ORDER BY id")
        self.assertEqual(got, cur.fetchall())

    def test_the_connection_waits_for_the_stream(self) -> None:
        rows = self.conn.stream("SELECT id FROM st ORDER BY id", batch_rows=10)
        self.assertEqual(next(rows), (1,))
        with self.assertRaises(nusadb.InterfaceError):
            self.conn.cursor().execute("SELECT 1")
        self.assertEqual(next(rows), (2,))
        rows.close()
        self.assertEqual(self.values(self.conn, "SELECT count(*) FROM st"), [2500])

    def test_a_dropped_stream_frees_the_connection(self) -> None:
        rows = self.conn.stream("SELECT id FROM st ORDER BY id", batch_rows=10)
        next(rows)
        del rows
        gc.collect()
        self.assertEqual(self.values(self.conn, "SELECT count(*) FROM st"), [2500])

    def test_an_error_ends_the_stream_and_keeps_the_connection(self) -> None:
        with self.assertRaises(nusadb.ProgrammingError):
            self.conn.stream("SELECT nope FROM st")
        self.assertEqual(self.values(self.conn, "SELECT 1"), [1])

    def test_a_stream_stopped_inside_a_message_breaks_the_connection(self) -> None:
        rows = self.conn.stream("SELECT id, v FROM st ORDER BY id", batch_rows=10)
        next(rows)
        real = self.conn._recv_exact
        calls = []

        def stops_in_the_payload(n: int) -> bytes:
            calls.append(n)
            if len(calls) == 2:  # the header was read; stop before its payload
                raise KeyboardInterrupt
            return real(n)

        self.conn._recv_exact = stops_in_the_payload
        with self.assertRaises(KeyboardInterrupt):
            for _ in range(20):
                next(rows)
        self.conn._recv_exact = real
        rows.close()
        self.assertTrue(self.conn.broken)

    def test_rows_take_the_row_factory_and_work_in_a_transaction(self) -> None:
        conn = self.connect(autocommit=False)
        with conn.stream("SELECT id, v FROM st WHERE id <= 3 ORDER BY id", row_factory=dict_row) as rows:
            self.assertEqual(list(rows)[0], {"id": 1, "v": "v1"})
        self.assertEqual(conn.transaction_status, "in_transaction")
        conn.rollback()


class ExecuteManyTests(_ServerCase):
    def test_a_large_batch_takes_few_round_trips(self) -> None:
        conn = self.connect()
        self.table(conn, "em", "id INT PRIMARY KEY, code TEXT")
        syncs = []
        original = conn._send

        def counting(frame: bytes) -> None:
            if frame[0] == ord("Y"):
                syncs.append(frame)
            original(frame)

        conn._send = counting  # type: ignore[method-assign]
        cur = conn.cursor()
        cur.executemany("INSERT INTO em VALUES ($1, $2)", ((i, f"{i:03d}") for i in range(1000)))
        conn._send = original  # type: ignore[method-assign]
        self.assertEqual(cur.rowcount, 1000)
        self.assertEqual(len(syncs), 4)
        self.assertEqual(self.values(conn, "SELECT code FROM em WHERE id = 7"), ["007"])

    def test_a_parameter_type_change_mid_batch(self) -> None:
        conn = self.connect()
        self.table(conn, "et", "id INT PRIMARY KEY, n NUMERIC, s TEXT")
        rows = [(1, 5, "1"), (2, None, None), (3, Decimal("1.5"), "x"), (4, 7, "007")]
        conn.cursor().executemany("INSERT INTO et VALUES ($1, $2, $3)", rows)
        cur = conn.cursor()
        cur.execute("SELECT id, n, s FROM et ORDER BY id")
        self.assertEqual(
            cur.fetchall(),
            [(1, Decimal(5), "1"), (2, None, None), (3, Decimal("1.5"), "x"), (4, Decimal(7), "007")],
        )

    def test_the_first_error_stops_the_batch(self) -> None:
        conn = self.connect()
        self.table(conn, "ee")
        rows = [(i,) for i in range(400)] + [(5,)] + [(i,) for i in range(400, 600)]
        with self.assertRaises(nusadb.IntegrityError):
            conn.cursor().executemany("INSERT INTO ee VALUES ($1)", rows)
        # Autocommit: everything before the failing row was committed, nothing after it ran.
        self.assertEqual(self.values(conn, "SELECT count(*) FROM ee"), [400])
        tx = self.connect(autocommit=False)
        with self.assertRaises(nusadb.IntegrityError):
            tx.cursor().executemany("INSERT INTO ee VALUES ($1)", [(1000,), (1000,)])
        tx.rollback()
        self.assertEqual(self.values(conn, "SELECT count(*) FROM ee WHERE id = 1000"), [0])

    def test_a_value_that_cannot_be_bound_leaves_the_connection_usable(self) -> None:
        conn = self.connect()
        self.table(conn, "eb")
        rows = [(i,) for i in range(300)] + [({1, 2},)] + [(1000,)]
        with self.assertRaises(nusadb.ProgrammingError):
            conn.cursor().executemany("INSERT INTO eb VALUES ($1)", rows)
        # The rows before the bad one ran (autocommit); nothing after it did.
        self.assertEqual(self.values(conn, "SELECT count(*) FROM eb"), [300])

        def failing():
            yield (5000,)
            raise RuntimeError("source failed")

        with self.assertRaises(RuntimeError):
            conn.cursor().executemany("INSERT INTO eb VALUES ($1)", failing())
        self.assertEqual(self.values(conn, "SELECT count(*) FROM eb WHERE id = 5000"), [1])

    def test_an_empty_batch(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.executemany("SELECT $1", [])
        self.assertEqual(cur.rowcount, -1)
        self.assertEqual(self.values(conn, "SELECT 1"), [1])


class RowFactoryTests(_ServerCase):
    def test_factories(self) -> None:
        conn = self.connect()
        sql = "SELECT 1 AS a, 'x' AS b, 2 AS a"
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(sql)
        self.assertEqual(cur.fetchone(), {"a": 2, "b": "x"})
        cur = conn.cursor(row_factory=namedtuple_row)
        cur.execute(sql)
        row = cur.fetchone()
        self.assertEqual((row.a, row.b, row._2), (1, "x", 2))
        cur = conn.cursor(row_factory=tuple_row)
        cur.execute(sql)
        self.assertEqual(cur.fetchall(), [(1, "x", 2)])

    def test_the_connection_default_and_a_new_result(self) -> None:
        conn = self.connect()
        conn.row_factory = dict_row
        cur = conn.cursor()
        cur.execute("SELECT 1 AS a")
        self.assertEqual(cur.fetchall(), [{"a": 1}])
        # Each result builds rows for its own columns.
        cur.execute("SELECT 2 AS b")
        self.assertEqual(list(cur), [{"b": 2}])


class Point:
    def __init__(self, x: int, y: int) -> None:
        self.x, self.y = x, y


class AdapterTests(_ServerCase):
    def tearDown(self) -> None:
        connection_module._ADAPTERS.clear()
        connection_module._CUSTOM_DECODERS.clear()

    def test_an_adapter_binds_a_custom_type(self) -> None:
        nusadb.register_adapter(Point, lambda p: f"{p.x},{p.y}")
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("SELECT $1, $2", [Point(1, 2), [Point(3, 4)]])
        self.assertEqual(cur.fetchone(), ("1,2", '{"3,4"}'))

        class Sub(Point):
            pass

        cur.execute("SELECT $1", [Sub(5, 6)])
        self.assertEqual(cur.fetchone(), ("5,6",))

    def test_a_decoder_replaces_the_built_in_one(self) -> None:
        nusadb.register_decoder("numeric", lambda text: ("n", text))
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("SELECT CAST(1.5 AS NUMERIC), CAST('{1.5,2}' AS NUMERIC[]), 3")
        self.assertEqual(cur.fetchone(), (("n", "1.5"), [("n", "1.5"), ("n", "2")], 3))


if __name__ == "__main__":
    unittest.main()
