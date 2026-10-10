"""Connection URLs, connection options and pool options."""

from __future__ import annotations

import socket
import ssl
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import nusadb  # noqa: E402
from nusadb.dsn import parse_dsn  # noqa: E402
from test_driver import _Server  # noqa: E402

DATA = Path(__file__).resolve().parent / "data"


class DsnTests(unittest.TestCase):
    def test_every_part(self) -> None:
        got = parse_dsn("nusadb://an%20n:p%40ss%3Aw@db.example:6000/sales%2F1")
        self.assertEqual(
            got,
            {
                "user": "an n",
                "password": "p@ss:w",
                "host": "db.example",
                "port": 6000,
                "database": "sales/1",
            },
        )

    def test_only_what_is_named(self) -> None:
        self.assertEqual(parse_dsn("nusadb://h"), {"host": "h"})
        self.assertEqual(parse_dsn("nusadb://u@h/"), {"user": "u", "host": "h"})
        self.assertEqual(parse_dsn("nusadb://u:@h"), {"user": "u", "password": "", "host": "h"})

    def test_ipv6(self) -> None:
        self.assertEqual(parse_dsn("nusadb://[::1]:5679/d"), {"host": "::1", "port": 5679, "database": "d"})
        self.assertEqual(parse_dsn("nusadb://[fe80::1]"), {"host": "fe80::1"})
        for bad in ["nusadb://::1:5678", "nusadb://[::1", "nusadb://[::1]x"]:
            with self.assertRaises(nusadb.ProgrammingError, msg=bad):
                parse_dsn(bad)

    def test_durations(self) -> None:
        got = parse_dsn("nusadb://h?connect_timeout=5&statement_timeout=1500ms&keepalive=2min")
        self.assertEqual(got["connect_timeout"], 5)
        self.assertEqual(got["statement_timeout"], 1.5)
        self.assertEqual(got["keepalive"], 120)
        self.assertEqual(parse_dsn("nusadb://h?statement_timeout=1h")["statement_timeout"], 3600)
        self.assertEqual(parse_dsn("nusadb://h?statement_timeout=0")["statement_timeout"], 0)
        for bad in ["connect_timeout=0", "keepalive=0s", "statement_timeout=5d", "connect_timeout=x"]:
            with self.assertRaises(nusadb.ProgrammingError, msg=bad):
                parse_dsn(f"nusadb://h?{bad}")

    def test_malformed(self) -> None:
        for bad in [
            "other://h",
            "nusadb://h?colour=red",
            "nusadb://h:port",
            "nusadb://h:70000",
            "nusadb://h?tls=maybe",
            "nusadb://u:%ff@h",
        ]:
            with self.assertRaises(nusadb.ProgrammingError, msg=bad):
                parse_dsn(bad)

    def test_tls(self) -> None:
        secure = parse_dsn("nusadbs://h")["ssl"]
        self.assertIsInstance(secure, ssl.SSLContext)
        self.assertEqual(secure.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(secure.check_hostname)
        self.assertIsNone(parse_dsn("nusadbs://h?tls=disable")["ssl"])
        self.assertIsNone(parse_dsn("nusadb://h?tls=disable")["ssl"])
        self.assertNotIn("ssl", parse_dsn("nusadb://h"))
        insecure = parse_dsn("nusadb://h?tls=insecure")["ssl"]
        self.assertEqual(insecure.verify_mode, ssl.CERT_NONE)
        with_ca = parse_dsn(f"nusadb://h?tls_ca={DATA / 'client-ca.pem'}")["ssl"]
        self.assertEqual(with_ca.verify_mode, ssl.CERT_REQUIRED)
        for bad in [
            f"nusadb://h?tls=disable&tls_ca={DATA / 'client-ca.pem'}",
            f"nusadb://h?tls=insecure&tls_ca={DATA / 'client-ca.pem'}",
            f"nusadb://h?tls_cert={DATA / 'client-cert.pem'}",
            f"nusadb://h?tls_key={DATA / 'client-key.pem'}",
            "nusadb://h?tls_ca=/no/such/file.pem",
        ]:
            with self.assertRaises(nusadb.ProgrammingError, msg=bad):
                parse_dsn(bad)


class _ServerCase(unittest.TestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def url(self, query: str = "") -> str:
        return f"nusadb://u@127.0.0.1:{self.server.port}/nusadb{query}"

    @staticmethod
    def show(conn: nusadb.Connection, name: str) -> str:
        cur = conn.cursor()
        cur.execute(f"SHOW {name}")
        return cur.fetchone()[0]


class ConnectOptionTests(_ServerCase):
    def test_a_url_connects(self) -> None:
        with nusadb.connect(self.url(), password=None) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            self.assertEqual(cur.fetchone(), (1,))

    def test_keywords_override_the_url(self) -> None:
        url = f"nusadb://u@127.0.0.1:1/nusadb"
        with nusadb.connect(url, port=self.server.port, password=None) as conn:
            self.assertFalse(conn.closed)

    def test_the_positional_host_and_port_still_work(self) -> None:
        with nusadb.connect("127.0.0.1", self.server.port, "u", "nusadb", None) as conn:
            self.assertFalse(conn.closed)
        with self.assertRaises(nusadb.ProgrammingError):
            nusadb.connect("127.0.0.1", self.server.port, port=1)
        with self.assertRaises(nusadb.ProgrammingError):
            nusadb.connect(self.url(), self.server.port)
        with self.assertRaises(nusadb.ProgrammingError):
            nusadb.connect(self.url(), colour="red")

    def test_statement_timeout_is_set_on_the_session(self) -> None:
        with nusadb.connect(self.url("?statement_timeout=1500ms"), password=None) as conn:
            self.assertEqual(self.show(conn, "statement_timeout"), "1500ms")
        # A fraction of a millisecond rounds up rather than to 0, which would mean no limit.
        with nusadb.connect(self.url(), password=None, statement_timeout=0.0001) as conn:
            self.assertEqual(self.show(conn, "statement_timeout"), "1ms")
        with nusadb.connect(self.url(), password=None, autocommit=False, statement_timeout=2) as conn:
            self.assertEqual(self.show(conn, "statement_timeout"), "2000ms")

    def test_keepalive_is_on_the_socket(self) -> None:
        with nusadb.connect(self.url("?keepalive=7"), password=None) as conn:
            sock = conn._sock
            self.assertEqual(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0, True)
            option = getattr(socket, "TCP_KEEPIDLE", None) or getattr(socket, "TCP_KEEPALIVE", None)
            if option is not None:
                self.assertEqual(sock.getsockopt(socket.IPPROTO_TCP, option), 7)

    def test_connect_timeout_bounds_only_the_connect(self) -> None:
        # A blackholed address never answers; the attempt gives up after connect_timeout.
        start = time.monotonic()
        with self.assertRaises(nusadb.OperationalError):
            nusadb.connect("nusadb://10.255.255.1:5678/x?connect_timeout=300ms", password=None)
        self.assertLess(time.monotonic() - start, 5)
        with nusadb.connect(self.url("?connect_timeout=5"), password=None) as conn:
            self.assertIsNone(conn._sock.gettimeout())


class PoolOptionTests(_ServerCase):
    def pool(self, **options: object) -> nusadb.Pool:
        pool = nusadb.Pool(1, self.url(), password=None, **options)
        self.addCleanup(pool.close)
        return pool

    def test_a_returned_session_is_reset_and_keeps_its_own_settings(self) -> None:
        pool = self.pool(statement_timeout=0.15)
        conn = pool.acquire()
        self.assertEqual(self.show(conn, "statement_timeout"), "150ms")
        cur = conn.cursor()
        cur.execute("SET statement_timeout = '5s'")
        cur.execute("SET search_path TO elsewhere")
        conn.listen("pool_reset")
        pool.release(conn)
        again = pool.acquire()
        self.assertIs(again, conn)
        self.assertEqual(self.show(again, "statement_timeout"), "150ms")
        self.assertNotIn("elsewhere", self.show(again, "search_path"))
        # The channel was left: a notification on it no longer reaches this connection.
        with nusadb.connect(self.url(), password=None) as other:
            other.notify("pool_reset", "x")
        self.assertIsNone(again.poll(0.3))
        pool.release(again)

    def test_without_reset_the_session_carries_over(self) -> None:
        pool = self.pool(reset_on_return=False)
        conn = pool.acquire()
        conn.cursor().execute("SET statement_timeout = '5s'")
        pool.release(conn)
        again = pool.acquire()
        self.assertEqual(self.show(again, "statement_timeout"), "5s")
        pool.release(again)

    def test_acquire_timeout_is_the_default_wait(self) -> None:
        pool = self.pool(acquire_timeout=0.2)
        held = pool.acquire()
        start = time.monotonic()
        with self.assertRaises(nusadb.InterfaceError):
            pool.acquire()
        self.assertLess(time.monotonic() - start, 2)
        pool.release(held)

    def test_idle_timeout_and_max_lifetime_replace_the_connection(self) -> None:
        for options in ({"idle_timeout": 0.1}, {"max_lifetime": 0.1}):
            pool = self.pool(**options)
            first = pool.acquire()
            pool.release(first)
            time.sleep(0.2)
            second = pool.acquire()
            self.assertIsNot(second, first, options)
            self.assertTrue(first.closed, options)
            self.assertEqual(pool.size, 1, options)
            pool.release(second)
        pool = self.pool(idle_timeout=30, max_lifetime=30)
        first = pool.acquire()
        pool.release(first)
        again = pool.acquire()
        self.assertIs(again, first)
        pool.release(again)

    def test_max_lifetime_drops_a_connection_on_return(self) -> None:
        pool = self.pool(max_lifetime=0.1)
        conn = pool.acquire()
        time.sleep(0.2)
        pool.release(conn)
        self.assertTrue(conn.closed)
        self.assertEqual(pool.size, 0)

    def test_test_on_checkout_replaces_a_dead_connection(self) -> None:
        pool = self.pool(test_on_checkout=True)
        first = pool.acquire()
        pool.release(first)
        first._sock.close()  # the server end is gone, but the pool has not noticed
        second = pool.acquire()
        self.assertIsNot(second, first)
        cur = second.cursor()
        cur.execute("SELECT 1")
        self.assertEqual(cur.fetchone(), (1,))
        self.assertEqual(pool.size, 1)
        pool.release(second)


if __name__ == "__main__":
    unittest.main()
