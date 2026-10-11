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


class PyformatParameterTests(_ServerCase):
    def test_percent_s_binds_by_position(self) -> None:
        cur = self.connect().cursor()
        cur.execute("SELECT %s, %s + 1, '%s' AS text_kept", ["007", 41])
        self.assertEqual(cur.fetchone(), ("007", 42, "%s"))

    def test_named_markers_bind_from_a_mapping_and_may_repeat(self) -> None:
        cur = self.connect().cursor()
        cur.execute("SELECT %(a)s, %(b)s, %(a)s", {"b": "two", "a": 1})
        self.assertEqual(cur.fetchone(), (1, "two", 1))

    def test_double_percent_is_a_literal_percent(self) -> None:
        cur = self.connect().cursor()
        cur.execute("SELECT 10 %% 3, %s", [5])
        self.assertEqual(cur.fetchone(), (1, 5))
        # A query on native markers keeps its % as written, even where it reads like %s.
        cur.execute("SELECT $1 % 3", [10])
        self.assertEqual(cur.fetchone(), (1,))
        cur.execute("SELECT $1, 10 %s FROM (SELECT 3 AS s) AS t", [0])
        self.assertEqual(cur.fetchone(), (0, 1))

    def test_markers_in_strings_and_comments_are_left_alone(self) -> None:
        cur = self.connect().cursor()
        cur.execute("SELECT %s /* %(x)s */, $$ %s $$ -- %s\n", [1])
        self.assertEqual(cur.fetchone(), (1, " %s "))

    def test_a_mismatch_is_refused_before_anything_runs(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        for sql, params, message in [
            ("SELECT %s, %(a)s", {"a": 1}, "not both"),
            ("SELECT %s, %s", [1], "2 %s placeholder"),
            ("SELECT %(a)s", {"b": 1}, "no parameter named 'a'"),
            ("SELECT %(a)s", [1], "take a mapping"),
            ("SELECT $1", {"a": 1}, r"needs %\(name\)s"),
        ]:
            with self.assertRaisesRegex(nusadb.ProgrammingError, message):
                cur.execute(sql, params)
        self.assertEqual(conn.transaction_status, "idle")
        cur.execute("SELECT 1")
        self.assertEqual(cur.fetchone(), (1,))

    def test_executemany_prepare_and_stream_take_mappings(self) -> None:
        conn = self.connect()
        self.table(conn, "pf", "id INT PRIMARY KEY, name TEXT")
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO pf VALUES (%(id)s, %(name)s)",
            [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"name": "c", "id": 3}],
        )
        self.assertEqual(cur.rowcount, 3)
        statement = conn.prepare("SELECT name FROM pf WHERE id = %(id)s")
        cur.execute(statement, {"id": 2})
        self.assertEqual(cur.fetchone(), ("b",))
        cur.executemany(conn.prepare("UPDATE pf SET name = %s WHERE id = %s"), [["x", 1], ["y", 3]])
        with conn.stream("SELECT id, name FROM pf WHERE id >= %(low)s ORDER BY id", {"low": 2}) as rows:
            self.assertEqual(list(rows), [(2, "b"), (3, "y")])

    def test_a_bad_row_in_executemany_stops_after_the_rows_before_it(self) -> None:
        conn = self.connect()
        self.table(conn, "pfr")
        cur = conn.cursor()
        with self.assertRaisesRegex(nusadb.ProgrammingError, "no parameter named 'id'"):
            cur.executemany("INSERT INTO pfr VALUES (%(id)s)", [{"id": 1}, {"other": 2}])
        self.assertEqual(self.values(conn, "SELECT id FROM pfr"), [1])


class PipelineTests(_ServerCase):
    def test_results_arrive_in_their_cursors(self) -> None:
        conn = self.connect()
        self.table(conn, "pl", "id INT PRIMARY KEY, v TEXT")
        with conn.pipeline() as p:
            ins = p.execute("INSERT INTO pl VALUES ($1, $2), ($3, $4)", [1, "a", 2, "b"])
            sel = p.execute("SELECT v FROM pl WHERE id = %(id)s", {"id": 2})
            upd = p.execute("UPDATE pl SET v = 'z' WHERE id > 0")
            self.assertEqual(sel.fetchall(), [])  # nothing has been sent yet
        self.assertEqual(ins.rowcount, 2)
        self.assertEqual(sel.fetchall(), [("b",)])
        self.assertEqual(sel.description[0][:2], ("v", "TEXT"))
        self.assertEqual(upd.rowcount, 2)

    def test_one_round_trip_for_the_block(self) -> None:
        conn = self.connect()
        sends = []
        real_send = conn._send
        conn._send = lambda frame: (sends.append(frame), real_send(frame))[1]
        with conn.pipeline() as p:
            cursors = [p.execute("SELECT $1", [i]) for i in range(10)]
        self.assertEqual(len(sends), 1)
        self.assertEqual([c.fetchone() for c in cursors], [(i,) for i in range(10)])

    def test_a_long_pipeline_syncs_on_its_own(self) -> None:
        conn = self.connect()
        with conn.pipeline() as p:
            cursors = [p.execute("SELECT $1", [i]) for i in range(nusadb.PIPELINE_SYNC_STATEMENTS + 1)]
            # The first full queue has been sent and read; the last statement is still queued.
            self.assertEqual(cursors[0].fetchone(), (0,))
            self.assertEqual(cursors[-1].fetchone(), None)
        self.assertEqual(cursors[-1].fetchone(), (nusadb.PIPELINE_SYNC_STATEMENTS,))

    def test_a_failure_skips_the_rest_and_raises(self) -> None:
        conn = self.connect()
        self.table(conn, "pf")
        with self.assertRaises(nusadb.ProgrammingError):
            with conn.pipeline() as p:
                first = p.execute("INSERT INTO pf VALUES (1)")
                p.execute("SELECT nope FROM pf")
                skipped = p.execute("INSERT INTO pf VALUES (2)")
        self.assertEqual(first.rowcount, 1)
        self.assertEqual(skipped.rowcount, -1)
        cur = conn.cursor()
        cur.execute("SELECT id FROM pf")
        self.assertEqual(cur.fetchall(), [(1,)])

    def test_an_exception_in_the_block_sends_nothing(self) -> None:
        conn = self.connect()
        self.table(conn, "pe")
        with self.assertRaises(KeyError):
            with conn.pipeline() as p:
                p.execute("INSERT INTO pe VALUES (1)")
                raise KeyError("stop")
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM pe")
        self.assertEqual(cur.fetchone(), (0,))

    def test_the_connection_waits_for_queued_statements(self) -> None:
        conn = self.connect()
        with conn.pipeline() as p:
            p.execute("SELECT 1")
            with self.assertRaises(nusadb.InterfaceError):
                conn.cursor().execute("SELECT 2")
            p.sync()
            cur = conn.cursor()
            cur.execute("SELECT 2")
            self.assertEqual(cur.fetchone(), (2,))
            with self.assertRaises(nusadb.ProgrammingError):
                with conn.pipeline():
                    pass
        with self.assertRaises(nusadb.ProgrammingError):
            p.execute("SELECT 3")

    def test_a_transaction_holds_the_pipeline(self) -> None:
        conn = self.connect()
        self.table(conn, "pt")
        conn.autocommit = False
        with conn.pipeline() as p:
            p.execute("INSERT INTO pt VALUES (1)")
            p.execute("INSERT INTO pt VALUES (2)")
        self.assertEqual(conn.transaction_status, "in_transaction")
        conn.rollback()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM pt")
        self.assertEqual(cur.fetchone(), (0,))


class ServerCursorTests(_ServerCase):
    def rows_table(self, conn: nusadb.Connection, name: str, count: int = 25) -> None:
        self.table(conn, name, "id INT PRIMARY KEY, v TEXT")
        cur = conn.cursor()
        cur.execute(f"INSERT INTO {name} SELECT g, 'v' || g FROM generate_series(1, {count}) g")
        conn.commit()

    def fetches(self, conn: nusadb.Connection) -> list:
        sent = []
        real_send = conn._send
        conn._send = lambda frame: (sent.append(frame), real_send(frame))[1]
        return sent

    def test_rows_come_over_as_they_are_fetched(self) -> None:
        conn = self.connect(autocommit=False)
        self.rows_table(conn, "sc1")
        cur = conn.cursor(name="c1")
        cur.execute("SELECT id, v FROM sc1 ORDER BY id")
        self.assertIsNone(cur.description)
        self.assertEqual(cur.fetchone(), (1, "v1"))
        self.assertEqual(cur.description[0][:2], ("id", "INT"))
        self.assertEqual(cur.fetchmany(3), [(2, "v2"), (3, "v3"), (4, "v4")])
        self.assertEqual((cur.rownumber, cur.rowcount), (4, 4))
        rest = cur.fetchall()
        self.assertEqual([r[0] for r in rest], list(range(5, 26)))
        self.assertEqual((cur.rownumber, cur.rowcount), (25, 25))
        self.assertIsNone(cur.fetchone())
        self.assertEqual(cur.fetchmany(5), [])
        cur.close()

    def test_iteration_fetches_itersize_rows_at_a_time(self) -> None:
        conn = self.connect(autocommit=False)
        self.rows_table(conn, "sc2")
        cur = conn.cursor(name="c2")
        cur.itersize = 7
        cur.execute("SELECT id FROM sc2 ORDER BY id")
        sent = self.fetches(conn)
        self.assertEqual([row[0] for row in cur], list(range(1, 26)))
        self.assertEqual(sum(b"FETCH FORWARD 7" in frame for frame in sent), 5)
        # A fetch after iteration started serves the buffered rows first, and asks the server only
        # for the rest.
        cur.execute("SELECT id FROM sc2 ORDER BY id")
        self.assertEqual(next(iter(cur)), (1,))
        self.assertEqual(cur.fetchmany(2), [(2,), (3,)])
        self.assertEqual(cur.fetchone(), (4,))
        sent.clear()
        self.assertEqual([r[0] for r in cur.fetchmany(6)], [5, 6, 7, 8, 9, 10])
        self.assertTrue(any(b"FETCH FORWARD 3" in frame for frame in sent), sent)
        self.assertEqual(next(cur), (11,))
        self.assertEqual([r[0] for r in cur.fetchall()], list(range(12, 26)))
        # Exhausted, the cursor still describes its columns.
        self.assertIsNone(cur.fetchone())
        self.assertEqual(cur.description[0][0], "id")
        # A scroll drops what was buffered.
        cur.execute("SELECT id FROM sc2 ORDER BY id")
        self.assertEqual(next(cur), (1,))
        cur.scroll(5, "absolute")
        self.assertEqual(cur.fetchone(), (6,))

    def test_scroll_moves_within_the_result(self) -> None:
        conn = self.connect(autocommit=False)
        self.rows_table(conn, "sc3")
        cur = conn.cursor(name="c3", scrollable=True)
        cur.execute("SELECT id FROM sc3 ORDER BY id")
        self.assertEqual(cur.fetchmany(5)[-1], (5,))
        cur.scroll(-2)
        self.assertEqual((cur.rownumber, cur.fetchone()), (3, (4,)))
        cur.scroll(20, "absolute")
        self.assertEqual(cur.fetchone(), (21,))
        cur.scroll(0, "absolute")
        self.assertEqual(cur.fetchone(), (1,))
        cur.scroll(24, "absolute")
        self.assertEqual(cur.fetchall(), [(25,)])
        cur.scroll(-25)
        self.assertEqual(cur.rownumber, 0)
        cur.scroll(3)
        with self.assertRaises(IndexError):
            cur.scroll(30, "absolute")
        self.assertEqual((cur.rownumber, cur.fetchone()), (3, (4,)))
        with self.assertRaises(IndexError):
            cur.scroll(-10)
        self.assertEqual(cur.fetchone(), (5,))
        with self.assertRaises(nusadb.ProgrammingError):
            cur.scroll(1, "sideways")
        cur.execute("SELECT id FROM sc3 WHERE id > 23 ORDER BY id")
        self.assertEqual(cur.fetchall(), [(24,), (25,)])

    def test_a_query_takes_parameters(self) -> None:
        conn = self.connect(autocommit=False)
        self.rows_table(conn, "sc4")
        cur = conn.cursor(name="c4")
        cur.execute("SELECT v FROM sc4 WHERE id > %s ORDER BY id", (22,))
        self.assertEqual(cur.fetchall(), [("v23",), ("v24",), ("v25",)])

    def test_a_cursor_lives_as_long_as_its_transaction_unless_held(self) -> None:
        conn = self.connect(autocommit=False)
        self.rows_table(conn, "sc5", 3)
        held = conn.cursor(name="held", withhold=True)
        held.execute("SELECT id FROM sc5 ORDER BY id")
        plain = conn.cursor(name="plain")
        plain.execute("SELECT id FROM sc5 ORDER BY id")
        conn.commit()
        self.assertEqual(held.fetchall(), [(1,), (2,), (3,)])
        conn.commit()
        with self.assertRaises(nusadb.ProgrammingError):
            plain.fetchone()
        conn.rollback()
        # A held cursor declared in a transaction that rolled back is gone; closing it is quiet.
        lost = conn.cursor(name="lost", withhold=True)
        lost.execute("SELECT id FROM sc5")
        conn.rollback()
        lost.close()
        # Closing a cursor its transaction already closed sends nothing, so no transaction opens.
        sent = self.fetches(conn)
        plain.close()
        self.assertEqual(sent, [])
        held.close()
        self.assertEqual(conn.transaction_status, "idle")
        with self.assertRaises(nusadb.ProgrammingError):
            held.fetchone()

    def test_autocommit_needs_a_held_cursor(self) -> None:
        conn = self.connect(autocommit=True)
        self.rows_table(conn, "sc6", 2)
        with self.assertRaises(nusadb.OperationalError) as caught:
            conn.cursor(name="c6").execute("SELECT id FROM sc6")
        self.assertEqual(caught.exception.sqlstate, "25P01")
        held = conn.cursor(name="c6", withhold=True)
        held.execute("SELECT id FROM sc6 ORDER BY id")
        self.assertEqual(held.fetchall(), [(1,), (2,)])
        held.close()

    def test_close_in_a_failed_transaction_and_redeclare(self) -> None:
        conn = self.connect(autocommit=False)
        self.rows_table(conn, "sc7", 2)
        cur = conn.cursor(name="c7")
        cur.execute("SELECT id FROM sc7")
        cur.execute("SELECT id FROM sc7 WHERE id = 2")  # the first is closed, not a 42P03
        self.assertEqual(cur.fetchall(), [(2,)])
        with self.assertRaises(nusadb.ProgrammingError):
            conn.cursor().execute("SELECT nope FROM sc7")
        cur.close()  # in a failed transaction: nothing sent, no error
        conn.rollback()
        with self.assertRaises(nusadb.ProgrammingError):
            conn.cursor(name="c8").fetchone()
        with self.assertRaises(nusadb.NotSupportedError):
            conn.cursor(name="c8").executemany("SELECT 1", [()])


class PlaceholderCompileTests(unittest.TestCase):
    def test_a_repeated_name_is_one_parameter(self) -> None:
        compiled = connection_module._compile("SELECT %(a)s, %(b)s, %(a)s")
        self.assertEqual(compiled.sql, "SELECT $1, $2, $1")
        self.assertEqual(compiled.names, ["a", "b"])

    def test_an_empty_mapping_is_no_parameters(self) -> None:
        compiled = connection_module._compile("SELECT 1")
        self.assertEqual(connection_module._arrange(compiled, {}), ())
        with self.assertRaises(nusadb.ProgrammingError):
            connection_module._arrange(compiled, {"a": 1})

    def test_a_numeric_marker_keeps_a_percent_as_written(self) -> None:
        self.assertEqual(connection_module._compile("SELECT 7 % :1").sql, "SELECT 7 % $1")

    def test_a_native_marker_before_a_dollar_quote_stays_a_marker(self) -> None:
        spans = connection_module._sql_spans("SELECT $1$$x$$")
        self.assertEqual(spans, [(True, "SELECT $1"), (False, "$$x$$")])


class TypeTextTests(unittest.TestCase):
    def test_a_range_bound_with_a_separator_is_quoted_and_reads_back(self) -> None:
        r = nusadb.Range("a, b", 'say "hi"', "[]")
        self.assertEqual(str(r), '["a, b","say \\"hi\\""]')
        self.assertEqual(nusadb.types.decode_range(str(r)), r)
        self.assertEqual(nusadb.types.decode_range('["x ""y""",)'), nusadb.Range('x "y"', None))

    def test_a_short_offset_in_a_range_bound_is_read(self) -> None:
        import datetime as dt

        r = nusadb.types.decode_range('["2026-01-01 10:00:00+05","2026-01-02 00:00:00+0530")')
        self.assertEqual(r.lower.utcoffset(), dt.timedelta(hours=5))
        self.assertEqual(r.upper.utcoffset(), dt.timedelta(hours=5, minutes=30))

    def test_membership_honours_exclusive_bounds(self) -> None:
        r = nusadb.Range(1, 10, "()")
        self.assertNotIn(1, r)
        self.assertNotIn(10, r)
        self.assertIn(2, r)
        self.assertIn(10, nusadb.Range(1, 10, "(]"))

    def test_a_single_host_inet_is_an_address(self) -> None:
        import ipaddress

        self.assertEqual(nusadb.types.decode_inet("10.0.0.1/32"), ipaddress.ip_address("10.0.0.1"))
        self.assertEqual(nusadb.types.decode_inet("::1/128"), ipaddress.ip_address("::1"))
        self.assertEqual(
            nusadb.types.decode_inet("10.0.0.1/8"), ipaddress.ip_interface("10.0.0.1/8")
        )

    def test_a_record_keeps_empty_strings_apart_from_nulls(self) -> None:
        self.assertEqual(nusadb.parse_record('(,"",x)'), (None, "", "x"))
        self.assertEqual(nusadb.parse_record('("a ""q"" b")'), ('a "q" b',))


class CopyTextTests(unittest.TestCase):
    def test_rows_are_sent_in_bounded_chunks(self) -> None:
        rows = [(i, "x" * 100) for i in range(2000)]
        chunks = list(connection_module._copy_text_chunks(rows))
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(c) < connection_module._COPY_CHUNK + 200 for c in chunks))
        self.assertEqual(b"".join(chunks).count(b"\n"), 2000)

    def test_a_row_cut_across_chunks_is_joined(self) -> None:
        lines = connection_module._CopyLines()
        for chunk in (b"1\tab", b"c\n2\t\\", b"N\n3\tz"):
            lines(chunk)
        self.assertEqual(lines.finish(), [("1", "abc"), ("2", None), ("3", "z")])

    def test_async_rows_are_sent_in_batches(self) -> None:
        import asyncio

        from nusadb import aio

        async def rows():
            for i in range(2500):
                yield (i,)

        async def collect():
            return [c async for c in aio._async_copy_text_chunks(rows())]

        chunks = asyncio.run(collect())
        self.assertGreater(len(chunks), 1)
        self.assertEqual(b"".join(chunks).count(b"\n"), 2500)


class MoreTypeTests(_ServerCase):
    def test_ranges_decode_and_bind(self) -> None:
        import datetime as dt

        conn = self.connect()
        self.table(conn, "rg", "id INT PRIMARY KEY, i INT4RANGE, n NUMRANGE, d DATERANGE, t TSRANGE")
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO rg VALUES ($1, $2, $3, $4, $5)",
            [
                1,
                nusadb.Range(1, 10),
                nusadb.Range(Decimal("1.5"), None, "(]"),
                nusadb.Range(dt.date(2026, 1, 1), dt.date(2026, 2, 1)),
                nusadb.Range(dt.datetime(2026, 1, 1, 10), dt.datetime(2026, 1, 2)),
            ],
        )
        cur.execute("INSERT INTO rg (id, i) VALUES (2, int4range(5, 5)), (3, int4range(1, 3, '[]'))")
        cur.execute("SELECT i, n, d, t FROM rg WHERE id = 1")
        i, n, d, t = cur.fetchone()
        self.assertEqual(i, nusadb.Range(1, 10))
        self.assertTrue(5 in i and 10 not in i)
        self.assertEqual((n.lower, n.upper, n.bounds), (Decimal("1.5"), None, "()"))
        self.assertTrue(n.upper_inf)
        self.assertEqual(d, nusadb.Range(dt.date(2026, 1, 1), dt.date(2026, 2, 1)))
        self.assertEqual(t.lower, dt.datetime(2026, 1, 1, 10))
        self.assertEqual(cur.description[0][1], "RANGE")
        cur.execute("SELECT i FROM rg WHERE id IN (2, 3) ORDER BY id")
        empty, closed = (row[0] for row in cur.fetchall())
        self.assertTrue(empty.isempty)
        self.assertEqual(closed, nusadb.Range(1, 4))
        cur.execute("SELECT id FROM rg WHERE i @> $1 ORDER BY id", [2])
        self.assertEqual(cur.fetchall(), [(1,), (3,)])

    def test_network_addresses_decode_and_bind(self) -> None:
        import ipaddress

        conn = self.connect()
        self.table(conn, "net", "id INT PRIMARY KEY, a INET, n CIDR, m MACADDR")
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO net VALUES (1, $1, $2, '08:00:2b:01:02:03'), (2, $3, NULL, NULL)",
            [
                ipaddress.ip_interface("192.168.1.5/24"),
                ipaddress.ip_network("10.0.0.0/8"),
                ipaddress.ip_address("::1"),
            ],
        )
        cur.execute("SELECT a, n, m FROM net ORDER BY id")
        rows = cur.fetchall()
        self.assertEqual(rows[0][0], ipaddress.ip_interface("192.168.1.5/24"))
        self.assertEqual(rows[0][1], ipaddress.ip_network("10.0.0.0/8"))
        self.assertEqual(rows[0][2], "08:00:2b:01:02:03")
        self.assertEqual(rows[1][0], ipaddress.ip_address("::1"))
        self.assertEqual([d[1] for d in cur.description], ["INET", "CIDR", "MACADDR"])
        cur.execute("SELECT id FROM net WHERE n >> $1", [ipaddress.ip_address("10.1.2.3")])
        self.assertEqual(cur.fetchall(), [(1,)])
        # A network binds as CIDR and an address as INET, not as text.
        cur.execute("SELECT $1, $2", [ipaddress.ip_network("10.0.0.0/8"), ipaddress.ip_address("10.1.2.3")])
        self.assertEqual(
            cur.fetchone(), (ipaddress.ip_network("10.0.0.0/8"), ipaddress.ip_address("10.1.2.3"))
        )
        self.assertEqual([d[1] for d in cur.description], ["CIDR", "INET"])

    def test_vectors_decode_and_bind(self) -> None:
        conn = self.connect()
        self.table(conn, "vec", "id INT PRIMARY KEY, v VECTOR(3)")
        cur = conn.cursor()
        cur.execute("INSERT INTO vec VALUES (1, $1)", [nusadb.Vector([1, 2.5, -3])])
        cur.execute("SELECT v FROM vec")
        self.assertEqual(cur.fetchone(), ([1.0, 2.5, -3.0],))

    def test_other_types_keep_their_text_and_name(self) -> None:
        cur = self.connect().cursor()
        cur.execute(
            "SELECT B'101', '(1,2)'::POINT, to_tsvector('the cat'), '<a/>'::XML, "
            "'08:00:2b:01:02:03:04:05'::MACADDR8"
        )
        self.assertEqual(cur.fetchone(), ("101", "(1,2)", "'cat':2", "<a/>", "08:00:2b:01:02:03:04:05"))
        self.assertEqual(
            [d[1] for d in cur.description], ["BIT", "GEOMETRY", "TSVECTOR", "XML", "MACADDR8"]
        )

    def test_a_composite_value_splits_into_its_fields(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("DROP TYPE IF EXISTS pair")
        cur.execute("CREATE TYPE pair AS (a INT, b TEXT, c TEXT)")
        cur.execute("SELECT ROW(1, 'a, b', NULL)::pair")
        self.assertEqual(nusadb.parse_record(cur.fetchone()[0]), ("1", "a, b", None))
