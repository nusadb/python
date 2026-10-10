"""Parameter types, transactions, broken connections, the pool, and PEP-249 module objects,
against a real ``nusadb-server``."""

from __future__ import annotations

import datetime as dt
import enum
import io
import sys
import threading
import time
import unittest
import uuid
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import nusadb  # noqa: E402
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

    def one(self, conn: nusadb.Connection, sql: str, params=None):
        cur = conn.cursor()
        cur.execute(sql, params)
        return cur.fetchone()[0]


class ParameterTypeTests(_ServerCase):
    def test_a_string_stays_text_even_when_it_looks_like_a_number(self) -> None:
        conn = self.connect()
        self.assertEqual(self.one(conn, "SELECT $1", ["007"]), "007")
        self.assertEqual(self.one(conn, "SELECT $1 || 'x'", ["007"]), "007x")
        cur = conn.cursor()
        cur.execute("CREATE TABLE codes (c TEXT)")
        cur.execute("INSERT INTO codes VALUES ($1)", ["0042"])
        self.assertEqual(self.one(conn, "SELECT c FROM codes"), "0042")

    def test_numbers(self) -> None:
        conn = self.connect()
        self.assertEqual(self.one(conn, "SELECT $1 + 1", [41]), 42)
        big = 2**70
        self.assertEqual(self.one(conn, "SELECT $1 + 1", [big]), Decimal(big + 1))
        self.assertEqual(self.one(conn, "SELECT $1 * 2", [1.25]), 2.5)
        self.assertEqual(self.one(conn, "SELECT $1 + 0", [Decimal("1.10")]), Decimal("1.10"))
        self.assertIs(self.one(conn, "SELECT $1", [True]), True)

    def test_dates_times_and_intervals(self) -> None:
        conn = self.connect()
        utc = dt.timezone.utc
        for value in [
            dt.date(2026, 10, 8),
            dt.datetime(2026, 10, 8, 12, 30, 5, 250000),
            dt.datetime(2026, 10, 8, 12, 30, 5, tzinfo=utc),
            dt.time(12, 30, 5, 125000),
        ]:
            self.assertEqual(self.one(conn, "SELECT $1", [value]), value, value)
        for delta in [
            dt.timedelta(days=1, seconds=5),
            dt.timedelta(seconds=-5),
            dt.timedelta(days=-3, hours=4, microseconds=7),
            dt.timedelta(0),
        ]:
            self.assertEqual(self.one(conn, "SELECT $1", [delta]), delta, delta)

    def test_uuid_bytes_json_and_arrays(self) -> None:
        conn = self.connect()
        u = uuid.uuid4()
        self.assertEqual(self.one(conn, "SELECT $1", [u]), u)
        self.assertEqual(self.one(conn, "SELECT $1", [b"\x00\xffab"]), b"\x00\xffab")
        self.assertEqual(self.one(conn, "SELECT $1", [{"a": [1, "x"]}]), {"a": [1, "x"]})
        self.assertEqual(self.one(conn, "SELECT $1", [nusadb.Json([1, 2])]), [1, 2])
        cur = conn.cursor()
        cur.execute("CREATE TABLE arr (xs INT[])")
        cur.execute("INSERT INTO arr VALUES ($1)", [[1, None, 3]])
        self.assertEqual(self.one(conn, "SELECT xs FROM arr"), [1, None, 3])


class ParameterEncodingTests(_ServerCase):
    def test_a_decimal_in_exponent_form_binds_as_its_value(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE amounts (n NUMERIC(20, 4))")
        for value in [Decimal("1E+3"), Decimal("1.5E-2"), Decimal("-2.50E+1")]:
            cur.execute("INSERT INTO amounts VALUES ($1)", [value])
        cur.execute("SELECT n FROM amounts ORDER BY n")
        self.assertEqual(
            [r[0] for r in cur.fetchall()], [Decimal("-25"), Decimal("0.015"), Decimal("1000")]
        )

    def test_an_int_subclass_binds_its_number(self) -> None:
        class Level(enum.IntEnum):
            HIGH = 3

        conn = self.connect()
        self.assertEqual(self.one(conn, "SELECT $1 + 1", [Level.HIGH]), 4)
        self.assertEqual(self.one(conn, "SELECT CAST($1 AS INT[])", [[Level.HIGH, Decimal("1E+2")]]), [3, 100])

    def test_a_mapping_inside_a_list_binds_as_json_text(self) -> None:
        conn = self.connect()
        got = self.one(conn, "SELECT CAST($1 AS TEXT[])", [[{"a": 1}, "x"]])
        self.assertEqual(got, ['{"a":1}', "x"])

    def test_parameters_may_come_from_a_generator(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        # "007" stays text only when its type tag was sent alongside the value.
        cur.execute("SELECT $1, $2", (v for v in ["007", 1]))
        self.assertEqual(cur.fetchone(), ("007", 1))

    def test_an_iterator_or_set_value_is_refused(self) -> None:
        conn = self.connect()
        for value in [(v for v in [1]), iter([1]), {1, 2}]:
            with self.assertRaises(nusadb.ProgrammingError):
                self.one(conn, "SELECT $1", [value])
        self.assertEqual(self.one(conn, "SELECT 1"), 1)

    def test_executemany_reports_zero_rows(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE em (v INT)")
        cur.executemany("UPDATE em SET v = 1 WHERE v = $1", [(1,), (2,)])
        self.assertEqual(cur.rowcount, 0)
        cur.executemany("INSERT INTO em VALUES ($1)", [(1,), (2,)])
        self.assertEqual(cur.rowcount, 2)

    def test_a_driver_error_has_no_sqlstate(self) -> None:
        conn = self.connect()
        conn.close()
        with self.assertRaises(nusadb.Error) as caught:
            conn.cursor()
        self.assertIsNone(caught.exception.sqlstate)


class CopyDirectionTests(_ServerCase):
    def test_a_copy_the_other_way_is_refused_and_the_connection_stays_usable(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE cd (v INT)")
        cur.execute("INSERT INTO cd VALUES (1)")
        with self.assertRaises(nusadb.ProgrammingError):
            conn.copy_out("COPY cd FROM STDIN", io.BytesIO())
        with self.assertRaises(nusadb.ProgrammingError):
            conn.copy_in("COPY cd TO STDOUT", io.BytesIO(b"2\n"))
        self.assertEqual(self.one(conn, "SELECT count(*) FROM cd"), 1)


class TransactionTests(_ServerCase):
    def test_commit_after_a_failed_statement_raises(self) -> None:
        conn = self.connect(autocommit=False)
        cur = conn.cursor()
        cur.execute("CREATE TABLE k (id INT NOT NULL, PRIMARY KEY (id))")
        conn.commit()
        self.assertEqual(conn.transaction_status, "idle")
        cur.execute("INSERT INTO k VALUES (1)")
        self.assertEqual(conn.transaction_status, "in_transaction")
        with self.assertRaises(nusadb.IntegrityError):
            cur.execute("INSERT INTO k VALUES (1)")
        self.assertEqual(conn.transaction_status, "failed")
        with self.assertRaises(nusadb.TransactionRolledBack):
            conn.commit()
        self.assertEqual(conn.transaction_status, "idle")
        self.assertEqual(self.one(conn, "SELECT count(*) FROM k"), 0)
        conn.rollback()

    def test_autocommit_cannot_change_inside_a_transaction(self) -> None:
        conn = self.connect(autocommit=False)
        conn.cursor().execute("SELECT 1")
        with self.assertRaises(nusadb.ProgrammingError):
            conn.autocommit = True
        conn.rollback()
        conn.autocommit = True
        self.assertTrue(conn.autocommit)

    def test_commit_and_rollback_follow_a_manual_begin(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE m (id INT)")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO m VALUES (1)")
        conn.rollback()
        self.assertEqual(self.one(conn, "SELECT count(*) FROM m"), 0)


class BrokenConnectionTests(unittest.TestCase):
    def test_a_dead_server_gives_database_errors_and_a_broken_connection(self) -> None:
        server = _Server()
        conn = nusadb.connect(port=server.port, user="u", password=None)
        server.stop()
        time.sleep(0.3)
        for _ in range(2):
            with self.assertRaises(nusadb.OperationalError):
                conn.cursor().execute("SELECT 1")
        self.assertTrue(conn.broken)
        self.assertTrue(conn.closed)
        conn.close()

    def test_a_call_interrupted_part_way_breaks_the_connection(self) -> None:
        server = _Server()
        self.addCleanup(server.stop)
        conn = nusadb.connect(port=server.port, user="u", password=None)
        real = conn._read_message

        def interrupted() -> None:
            conn._read_message = real
            raise KeyboardInterrupt

        conn._read_message = interrupted
        with self.assertRaises(KeyboardInterrupt):
            conn.cursor().execute("SELECT 1")
        # The reply is still on the socket: reading on would hand it to the next statement.
        with self.assertRaises(nusadb.OperationalError):
            conn.cursor().execute("SELECT 2")
        self.assertTrue(conn.broken)

    def test_a_failed_send_is_an_operational_error(self) -> None:
        server = _Server()
        self.addCleanup(server.stop)
        conn = nusadb.connect(port=server.port, user="u", password=None)
        real = conn._sock

        class Unwritable:
            def sendall(self, _data: bytes) -> None:
                raise BrokenPipeError(32, "Broken pipe")

            def close(self) -> None:
                real.close()

        conn._sock = Unwritable()
        with self.assertRaises(nusadb.OperationalError):
            conn.cursor().execute("SELECT 1")
        self.assertTrue(conn.broken)

    def test_connect_to_nothing_is_an_operational_error(self) -> None:
        port = _Server.__init__.__globals__["_free_port"]()
        with self.assertRaises(nusadb.OperationalError):
            nusadb.connect(port=port, timeout=2)


class NotificationPollTests(_ServerCase):
    def test_a_poll_timeout_keeps_the_connection_and_its_timeout(self) -> None:
        conn = self.connect(timeout=7.0)
        conn.listen("ch")
        self.assertIsNone(conn.poll(0.05))
        self.assertEqual(conn._sock.gettimeout(), 7.0)
        self.assertEqual(self.one(conn, "SELECT 5"), 5)
        other = self.connect()
        other.notify("ch", "hi")
        note = conn.poll(5)
        self.assertEqual((note.channel, note.payload), ("ch", "hi"))


class PoolTests(_ServerCase):
    def test_a_returned_connection_is_rolled_back(self) -> None:
        pool = nusadb.Pool(max_size=1, port=self.server.port, user="u", password=None, autocommit=False)
        self.addCleanup(pool.close)
        with pool.connection() as conn:
            cur = conn.cursor()
            cur.execute("CREATE TABLE pt (id INT)")
            conn.commit()
            cur.execute("INSERT INTO pt VALUES (5)")
        with pool.connection() as conn:
            self.assertEqual(conn.transaction_status, "idle")
            self.assertEqual(self.one(conn, "SELECT count(*) FROM pt"), 0)

    def test_a_broken_connection_is_dropped(self) -> None:
        pool = nusadb.Pool(max_size=2, port=self.server.port, user="u", password=None)
        self.addCleanup(pool.close)
        conn = pool.acquire()
        conn._fail()
        pool.release(conn)
        self.assertEqual((pool.size, pool.idle_count), (0, 0))
        with pool.connection() as conn:
            self.assertEqual(self.one(conn, "SELECT 1"), 1)
        self.assertEqual((pool.size, pool.idle_count), (1, 1))


    def test_a_connection_returned_twice_is_refused(self) -> None:
        pool = nusadb.Pool(max_size=1, port=self.server.port, user="u", password=None)
        self.addCleanup(pool.close)
        conn = pool.acquire()
        pool.release(conn)
        with self.assertRaises(nusadb.InterfaceError):
            pool.release(conn)
        # The slot was returned once: a second checkout would block forever otherwise, and two
        # at once would exceed max_size.
        first = pool.acquire(timeout=1)
        with self.assertRaises(nusadb.InterfaceError):
            pool.acquire(timeout=0.2)
        pool.release(first)

    def test_a_connection_returned_after_close_is_closed(self) -> None:
        pool = nusadb.Pool(max_size=2, port=self.server.port, user="u", password=None)
        conn = pool.acquire()
        pool.close()
        pool.release(conn)
        self.assertTrue(conn.closed)
        self.assertEqual(pool.idle_count, 0)
        with self.assertRaises(nusadb.InterfaceError):
            pool.acquire()

    def test_a_checkout_waiting_when_the_pool_closes_is_refused(self) -> None:
        pool = nusadb.Pool(max_size=1, port=self.server.port, user="u", password=None)
        held = pool.acquire()
        outcome = []

        def wait_for_a_connection() -> None:
            try:
                outcome.append(pool.acquire(timeout=10))
            except nusadb.InterfaceError as exc:
                outcome.append(exc)

        waiter = threading.Thread(target=wait_for_a_connection)
        waiter.start()
        time.sleep(0.2)
        pool.close()
        pool.release(held)
        waiter.join(10)
        self.assertEqual(len(outcome), 1)
        self.assertIsInstance(outcome[0], nusadb.InterfaceError)
        self.assertEqual(pool.size, 0)

    def test_autocommit_is_restored_on_return(self) -> None:
        pool = nusadb.Pool(max_size=1, port=self.server.port, user="u", password=None)
        self.addCleanup(pool.close)
        conn = pool.acquire()
        default = conn.autocommit
        conn.autocommit = not default
        pool.release(conn)
        again = pool.acquire()
        self.assertIs(again, conn)
        self.assertEqual(again.autocommit, default)
        pool.release(again)


class ModuleTests(_ServerCase):
    def test_pep249_type_objects_and_constructors(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("SELECT 1, 'a', CAST('2026-10-08' AS DATE), CAST('\\\\x00' AS BYTEA)")
        types = [d[1] for d in cur.description]
        self.assertEqual(types[0], nusadb.NUMBER)
        self.assertEqual(types[1], nusadb.STRING)
        self.assertEqual(types[2], nusadb.DATETIME)
        self.assertEqual(types[3], nusadb.BINARY)
        self.assertNotEqual(types[0], nusadb.STRING)
        self.assertEqual(nusadb.Date(2026, 10, 8), dt.date(2026, 10, 8))
        self.assertEqual(nusadb.Binary(bytearray(b"x")), b"x")
        self.assertIsInstance(nusadb.TimestampFromTicks(0), dt.datetime)
        self.assertIs(cur.connection, conn)
        self.assertIsNone(cur.lastrowid)
        self.assertIsNone(cur.nextset())
        self.assertIs(conn.Error, nusadb.Error)

    def test_callproc(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE log (v INT)")
        cur.execute("CREATE PROCEDURE add_log(x INT) LANGUAGE SQL AS $$ INSERT INTO log VALUES ($1) $$")
        self.assertEqual(cur.callproc("add_log", [3]), [3])
        self.assertEqual(self.one(conn, "SELECT v FROM log"), 3)



class TlsTests(unittest.TestCase):
    def test_a_client_certificate_reaches_a_server_that_requires_one(self) -> None:
        import ssl

        data = Path(__file__).resolve().parent / "data"
        server = _Server(
            "--tls-cert", str(data / "localhost-cert.pem"),
            "--tls-key", str(data / "localhost-key.pem"),
            "--tls-client-ca", str(data / "client-ca.pem"),
        )
        self.addCleanup(server.stop)

        def context(with_cert: bool) -> ssl.SSLContext:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE  # the test server's certificate is self-signed
            if with_cert:
                ctx.load_cert_chain(data / "client-cert.pem", data / "client-key.pem")
            return ctx

        with self.assertRaises(nusadb.OperationalError):
            conn = nusadb.connect(port=server.port, user="u", password=None, ssl=context(False))
            conn.cursor().execute("SELECT 1")
        conn = nusadb.connect(port=server.port, user="u", password=None, ssl=context(True))
        self.addCleanup(conn.close)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        self.assertEqual(cur.fetchone(), (1,))


if __name__ == "__main__":
    unittest.main()
