"""Integration tests for the nusadb Python driver, run against a real server.

The tests boot the compiled ``nusadb-server`` binary (``cargo build -p
nusadb-server``) on an ephemeral port with a temporary data directory. Run with::

    python -m unittest discover -s drivers/python/tests
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import nusadb  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[3]


def _target_dirs() -> list[Path]:
    """Candidate Cargo target directories, honouring ``CARGO_TARGET_DIR``."""
    dirs: list[Path] = []
    env = os.environ.get("CARGO_TARGET_DIR")
    if env:
        dirs.append(Path(env))
    dirs.append(REPO_ROOT / "target")
    return dirs


def _server_binary() -> Path:
    for base in _target_dirs():
        for profile in ("debug", "release"):
            for name in ("nusadb-server.exe", "nusadb-server"):
                candidate = base / profile / name
                if candidate.exists():
                    return candidate
    raise unittest.SkipTest(
        "nusadb-server binary not found; run `cargo build -p nusadb-server` first"
    )


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(port: int, deadline: float = 15.0) -> None:
    start = time.time()
    while time.time() - start < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f"server on port {port} did not become ready")


class _Server:
    """A child ``nusadb-server`` process on an ephemeral port."""

    def __init__(self, *extra_args: str) -> None:
        self.port = _free_port()
        self._tmp = tempfile.TemporaryDirectory()
        self._proc = subprocess.Popen(
            [
                str(_server_binary()),
                "--listen",
                f"127.0.0.1:{self.port}",
                "--data-dir",
                self._tmp.name,
                *extra_args,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={**os.environ, "RUST_LOG": "error"},
        )
        _wait_ready(self.port)

    def stop(self) -> None:
        self._proc.terminate()
        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
        self._tmp.cleanup()


class TrustServerTests(unittest.TestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def _connect(self) -> nusadb.Connection:
        return nusadb.connect(host="127.0.0.1", port=self.server.port, user="u", database="nusadb")

    def test_simple_query_round_trip(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE simple (id INT NOT NULL, name TEXT)")
        self.assertEqual(cur.rowcount, -1)
        cur.execute("INSERT INTO simple VALUES (5, 'alice')")
        self.assertEqual(cur.rowcount, 1)
        cur.execute("SELECT id, name FROM simple")
        self.assertEqual([d[0] for d in cur.description], ["id", "name"])
        # Protocol 1.1 typed metadata (R42-B.03): description type_code is the column type name.
        self.assertEqual([d[1] for d in cur.description], ["INT", "TEXT"])
        rows = cur.fetchall()
        # INT decodes to a Python int (type fidelity), TEXT stays str.
        self.assertEqual(rows, [(5, "alice")])
        conn.close()

    def test_copy_in_and_out(self) -> None:
        import io

        conn = self._connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE copy_t (id INT NOT NULL, name TEXT, PRIMARY KEY (id))")

        # COPY FROM STDIN: tab-delimited rows, \N for NULL.
        loaded = conn.copy_in(
            "COPY copy_t FROM STDIN", io.BytesIO(b"1\talice\n2\t\\N\n3\tcarol\n")
        )
        self.assertEqual(loaded, 3)

        cur.execute("SELECT id, name FROM copy_t ORDER BY id")
        self.assertEqual(cur.fetchall(), [(1, "alice"), (2, None), (3, "carol")])

        # COPY TO STDOUT: bytes come back in the same text format.
        sink = io.BytesIO()
        exported = conn.copy_out("COPY copy_t TO STDOUT", sink)
        self.assertEqual(exported, 3)
        self.assertEqual(
            sink.getvalue().decode("utf-8").splitlines(),
            ["1\talice", "2\t\\N", "3\tcarol"],
        )

        # A refused COPY raises, and the connection stays usable.
        with self.assertRaises(nusadb.DatabaseError):
            conn.copy_in("COPY no_such_table FROM STDIN", io.BytesIO(b""))
        cur.execute("SELECT count(*) FROM copy_t")
        self.assertEqual(cur.fetchone(), (3,))

        conn.close()

    def test_bytea_round_trip(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE bt (id INT NOT NULL, data BYTEA, PRIMARY KEY (id))")
        payload = b"\xde\xad\xbe\xef\x00\x7f"
        cur.execute("INSERT INTO bt VALUES ($1, $2)", [1, payload])
        cur.execute("INSERT INTO bt VALUES ($1, $2)", [2, b""])
        cur.execute("INSERT INTO bt VALUES ($1, $2)", [3, None])
        cur.execute("SELECT id, data FROM bt ORDER BY id")
        rows = cur.fetchall()
        self.assertEqual(rows[0][1], payload)
        self.assertEqual(rows[1][1], b"")
        self.assertIsNone(rows[2][1])
        conn.close()

    def test_typed_value_decoding(self) -> None:
        import datetime
        import uuid
        from decimal import Decimal

        conn = self._connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE typed ("
            "  b BOOL, i INT, f FLOAT, n NUMERIC(10, 2), "
            "  d DATE, ts TIMESTAMP, u UUID, j JSON, arr INT[]"
            ")"
        )
        cur.execute(
            "INSERT INTO typed VALUES "
            "(true, 42, 1.5, 12.34, DATE '2024-01-15', "
            " TIMESTAMP '2024-01-15 10:30:00', "
            " UUID '00000000-0000-0000-0000-000000000001', "
            " JSON '{\"k\": 1}', ARRAY[1, 2, 3])"
        )
        cur.execute("SELECT b, i, f, n, d, ts, u, j, arr FROM typed")
        b, i, f, n, d, ts, u, j, arr = cur.fetchone()

        self.assertIs(b, True)
        self.assertEqual(i, 42)
        self.assertIsInstance(i, int)
        self.assertEqual(f, 1.5)
        self.assertIsInstance(f, float)
        self.assertEqual(n, Decimal("12.34"))
        self.assertEqual(d, datetime.date(2024, 1, 15))
        self.assertEqual(ts, datetime.datetime(2024, 1, 15, 10, 30, 0))
        self.assertEqual(u, uuid.UUID("00000000-0000-0000-0000-000000000001"))
        self.assertEqual(j, {"k": 1})
        # An INT[] column decodes its elements as ints (protocol 1.2 carries the element type).
        self.assertEqual(arr, [1, 2, 3])
        self.assertTrue(all(isinstance(x, int) for x in arr))
        conn.close()

    def test_array_element_typing(self) -> None:
        # Protocol 1.2: an array's elements decode at the column's element type — ints as ints,
        # floats as floats, text stays str — with SQL NULL preserved as None (array-elem-text).
        conn = self._connect()
        cur = conn.cursor()
        from decimal import Decimal

        cur.execute("CREATE TABLE arrt (ints INT[], texts TEXT[], flts FLOAT[], nums NUMERIC[])")
        cur.execute(
            "INSERT INTO arrt VALUES "
            "(ARRAY[1, 2, NULL], ARRAY['a', 'b'], ARRAY[1.5, 2.5], ARRAY[1.5, 2.25])"
        )
        cur.execute("SELECT ints, texts, flts, nums FROM arrt")
        ints, texts, flts, nums = cur.fetchone()
        self.assertEqual(ints, [1, 2, None])
        self.assertTrue(all(isinstance(x, int) for x in ints if x is not None))
        self.assertEqual(texts, ["a", "b"])
        self.assertEqual(flts, [1.5, 2.5])
        self.assertTrue(all(isinstance(x, float) for x in flts))
        # NUMERIC[] elements decode as Decimal, not float.
        self.assertEqual(nums, [Decimal("1.5"), Decimal("2.25")])
        self.assertTrue(all(isinstance(x, Decimal) for x in nums))
        conn.close()

    def test_interval_decoding(self) -> None:
        import datetime

        conn = self._connect()
        cur = conn.cursor()
        # A day/time interval decodes to a timedelta (interval-text finding).
        cur.execute(
            "SELECT INTERVAL '1 day', INTERVAL '1 day' + INTERVAL '2 hours', INTERVAL '3 days'"
        )
        one_day, day_plus, three = cur.fetchone()
        self.assertEqual(one_day, datetime.timedelta(days=1))
        self.assertEqual(day_plus, datetime.timedelta(days=1, hours=2))
        self.assertEqual(three, datetime.timedelta(days=3))
        # A year/month-bearing interval stays a string (a month is not an exact number of days).
        cur.execute("SELECT age(DATE '2024-03-15', DATE '2020-01-10')")
        (months_interval,) = cur.fetchone()
        self.assertEqual(months_interval, "4 years 2 mons 5 days")
        conn.close()

    def test_null_values_decode_to_none(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE typed_null (b BOOL, i INT, d DATE, arr INT[])")
        cur.execute("INSERT INTO typed_null VALUES (NULL, NULL, NULL, NULL)")
        cur.execute("SELECT b, i, d, arr FROM typed_null")
        self.assertEqual(cur.fetchone(), (None, None, None, None))
        conn.close()

    def test_extended_query_with_parameters(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE params (id INT NOT NULL, name TEXT)")
        cur.execute("INSERT INTO params VALUES ($1, $2)", [1, "alice"])
        cur.execute("INSERT INTO params VALUES ($1, $2)", [2, None])
        cur.execute("SELECT id, name FROM params WHERE id = $1", [2])
        row = cur.fetchone()
        self.assertEqual(row, (2, None))
        self.assertIsNone(cur.fetchone())
        conn.close()

    def test_prepared_via_executemany(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE many (id INT NOT NULL)")
        cur.executemany("INSERT INTO many VALUES ($1)", [[1], [2], [3]])
        self.assertEqual(cur.rowcount, 3)
        cur.execute("SELECT id FROM many")
        self.assertEqual(sorted(r[0] for r in cur.fetchall()), [1, 2, 3])
        conn.close()

    def test_error_is_raised_and_connection_survives(self) -> None:
        conn = self._connect()
        cur = conn.cursor()
        with self.assertRaises(nusadb.DatabaseError):
            cur.execute("SELECT * FROM ghost")
        cur.execute("SELECT 1")
        self.assertEqual(len(cur.fetchall()), 1)
        conn.close()

    def test_autocommit_commit_is_a_noop(self) -> None:
        conn = self._connect()
        self.assertIsNone(conn.commit())  # no-op in autocommit mode
        self.assertIsNone(conn.rollback())  # no-op in autocommit mode
        conn.close()

    def test_transactions_commit_and_rollback(self) -> None:
        conn = nusadb.connect(
            host="127.0.0.1",
            port=self.server.port,
            user="u",
            database="nusadb",
            autocommit=False,
        )
        cur = conn.cursor()
        cur.execute("CREATE TABLE tx (id INT NOT NULL)")
        conn.commit()

        # Rolled-back insert leaves no row.
        cur.execute("INSERT INTO tx VALUES (1)")
        conn.rollback()
        cur.execute("SELECT id FROM tx")
        self.assertEqual(len(cur.fetchall()), 0)

        # Committed insert persists (a fresh autocommit connection sees it).
        cur.execute("INSERT INTO tx VALUES (2)")
        conn.commit()
        other = self._connect()
        ocur = other.cursor()
        ocur.execute("SELECT id FROM tx")
        self.assertEqual(ocur.fetchall(), [(2,)])
        other.close()
        conn.close()

    def test_savepoints(self) -> None:
        conn = nusadb.connect(
            host="127.0.0.1",
            port=self.server.port,
            user="u",
            database="nusadb",
            autocommit=False,
        )
        cur = conn.cursor()
        cur.execute("CREATE TABLE sp (id INT NOT NULL)")
        conn.commit()

        cur.execute("INSERT INTO sp VALUES (1)")
        conn.savepoint("sp1")
        cur.execute("INSERT INTO sp VALUES (2)")
        conn.rollback_to("sp1")  # undoes (2), keeps (1), transaction continues
        cur.execute("INSERT INTO sp VALUES (3)")
        conn.savepoint("sp2")
        cur.execute("INSERT INTO sp VALUES (4)")
        conn.release_savepoint("sp2")  # keeps (4)
        conn.commit()

        cur.execute("SELECT id FROM sp ORDER BY id")
        self.assertEqual(cur.fetchall(), [(1,), (3,), (4,)])

        # A savepoint is illegal without an open (non-autocommit) transaction.
        auto = self._connect()
        with self.assertRaises(nusadb.ProgrammingError):
            auto.savepoint("nope")
        auto.close()
        conn.close()

    def test_listen_notify(self) -> None:
        conn = self._connect()
        conn.listen("py_chan")
        # Self-delivery: NOTIFY on a channel this connection listens to comes back to it.
        conn.notify("py_chan", "hello")
        note = conn.poll(timeout=5.0)
        assert note is not None
        self.assertEqual(note.channel, "py_chan")
        self.assertEqual(note.payload, "hello")
        # NOTIFY without a payload delivers an empty string.
        conn.notify("py_chan")
        note = conn.poll(timeout=5.0)
        assert note is not None
        self.assertEqual(note.payload, "")
        # After UNLISTEN, a further NOTIFY is not delivered (poll returns None).
        conn.unlisten("py_chan")
        conn.notify("py_chan", "ignored")
        self.assertIsNone(conn.poll(timeout=0.3))
        conn.close()

    def test_query_surface(self) -> None:
        # The driver is a SQL-text transport: any SELECT the server accepts works
        # via raw DB-API execute/fetchall. This guards the full query surface
        # (ORDER BY / DISTINCT / LIMIT / window / CTE / subquery / set-op / JOIN).
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE surf_a (id INT NOT NULL, grp TEXT, v INT)")
        cur.executemany(
            "INSERT INTO surf_a VALUES ($1, $2, $3)",
            [[1, "a", 10], [2, "a", 30], [3, "b", 20], [4, "b", 20], [5, "a", 10]],
        )
        cur.execute("CREATE TABLE surf_b (id INT NOT NULL, a_id INT, tag TEXT)")
        cur.executemany(
            "INSERT INTO surf_b VALUES ($1, $2, $3)",
            [[10, 1, "p"], [11, 1, "q"], [12, 2, "r"]],
        )

        cases = [
            ("ORDER BY", "SELECT id FROM surf_a ORDER BY v DESC, id", 5),
            ("DISTINCT", "SELECT DISTINCT v FROM surf_a ORDER BY v", 3),
            (
                "DISTINCT ON",
                "SELECT DISTINCT ON (grp) grp, v FROM surf_a ORDER BY grp, v",
                2,
            ),
            ("LIMIT", "SELECT id FROM surf_a ORDER BY id LIMIT 2", 2),
            ("OFFSET", "SELECT id FROM surf_a ORDER BY id LIMIT 2 OFFSET 3", 2),
            (
                "GROUP/HAVING",
                "SELECT grp, count(*) FROM surf_a GROUP BY grp "
                "HAVING count(*) > 1 ORDER BY grp",
                2,
            ),
            (
                "window",
                "SELECT id, row_number() OVER (PARTITION BY grp ORDER BY v) "
                "FROM surf_a ORDER BY id",
                5,
            ),
            (
                "CTE",
                "WITH g AS (SELECT grp, count(*) c FROM surf_a GROUP BY grp) "
                "SELECT count(*) FROM g",
                1,
            ),
            (
                "subquery IN",
                "SELECT id FROM surf_a WHERE v IN (SELECT max(v) FROM surf_a) "
                "ORDER BY id",
                1,
            ),
            (
                "UNION",
                "SELECT id FROM surf_a WHERE id = 1 "
                "UNION SELECT id FROM surf_a WHERE id = 2",
                2,
            ),
            (
                "INNER JOIN",
                "SELECT surf_a.grp, surf_b.tag FROM surf_a "
                "JOIN surf_b ON surf_a.id = surf_b.a_id ORDER BY surf_b.id",
                3,
            ),
            (
                "LEFT JOIN",
                "SELECT surf_a.grp, surf_b.tag FROM surf_a "
                "LEFT JOIN surf_b ON surf_a.id = surf_b.a_id "
                "ORDER BY surf_a.id, surf_b.id",
                6,
            ),
            (
                "RIGHT JOIN",
                "SELECT surf_a.grp, surf_b.tag FROM surf_a "
                "RIGHT JOIN surf_b ON surf_a.id = surf_b.a_id ORDER BY surf_b.id",
                3,
            ),
            (
                "FULL JOIN",
                "SELECT surf_a.grp, surf_b.tag FROM surf_a "
                "FULL JOIN surf_b ON surf_a.id = surf_b.a_id "
                "ORDER BY surf_a.id, surf_b.id",
                6,
            ),
            (
                "CROSS JOIN",
                "SELECT surf_a.id, surf_b.id FROM surf_a CROSS JOIN surf_b",
                15,
            ),
            (
                "LATERAL",
                "SELECT surf_a.id, l.tag FROM surf_a JOIN LATERAL "
                "(SELECT tag FROM surf_b WHERE surf_b.a_id = surf_a.id LIMIT 1) l "
                "ON true ORDER BY surf_a.id",
                2,
            ),
        ]
        for label, sql, expected in cases:
            with self.subTest(case=label):
                cur.execute(sql)
                rows = cur.fetchall()
                self.assertEqual(len(rows), expected, f"{label}: {sql}")
        conn.close()

    def test_backend_key_and_cancel(self) -> None:
        conn = self._connect()
        self.assertIsNotNone(conn.backend_key)
        pid, _secret = conn.backend_key
        self.assertGreater(pid, 0)
        conn.cancel()  # no-op when nothing is running
        conn.close()

    def test_pool_reuses_connections(self) -> None:
        pool = nusadb.Pool(
            max_size=2, host="127.0.0.1", port=self.server.port, user="u", database="nusadb"
        )
        with pool.connection() as conn:
            conn.cursor().execute("CREATE TABLE pooled (id INT NOT NULL)")
        self.assertEqual(pool.idle_count, 1)
        with pool.connection() as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO pooled VALUES (7)")
            cur.execute("SELECT id FROM pooled")
            self.assertEqual(cur.fetchone(), (7,))
        self.assertEqual(pool.idle_count, 1)
        pool.close()

    def test_context_managers(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                self.assertEqual(list(cur), [(1,)])


class ScramAuthTests(unittest.TestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server("--auth-user", "alice:secret")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def test_correct_password_authenticates(self) -> None:
        conn = nusadb.connect(
            host="127.0.0.1",
            port=self.server.port,
            user="alice",
            database="nusadb",
            password="secret",
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        self.assertEqual(len(cur.fetchall()), 1)
        conn.close()

    def test_wrong_password_is_rejected(self) -> None:
        with self.assertRaises(nusadb.Error):
            nusadb.connect(
                host="127.0.0.1",
                port=self.server.port,
                user="alice",
                database="nusadb",
                password="wrong",
            )

    def test_missing_password_is_rejected(self) -> None:
        with self.assertRaises(nusadb.OperationalError):
            nusadb.connect(
                host="127.0.0.1",
                port=self.server.port,
                user="alice",
                database="nusadb",
                password=None,
            )


if __name__ == "__main__":
    unittest.main()
