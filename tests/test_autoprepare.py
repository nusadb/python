"""Automatic server-side prepare (``prepare_threshold``), blocking and asyncio, against a real
``nusadb-server``."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import nusadb  # noqa: E402
import nusadb.aio  # noqa: E402
import nusadb.connection as connection_module  # noqa: E402
from nusadb.dsn import parse_dsn  # noqa: E402
from test_driver import _Server  # noqa: E402

INSERT = "INSERT INTO ap VALUES ($1, $2)"


def record(conn: object) -> List[bytes]:
    """The frames the connection sends from now on."""
    sent: List[bytes] = []
    original = conn._send  # type: ignore[attr-defined]

    def recording(frame: bytes) -> None:
        sent.append(frame)
        original(frame)

    conn._send = recording  # type: ignore[attr-defined]
    return sent


def kinds(frames: List[bytes], kind: str) -> List[bytes]:
    return [f for f in frames if f[:1] == kind.encode()]


def named_parse(frames: List[bytes]) -> List[bytes]:
    """The Parse frames that name a statement (an unnamed one starts with an empty name)."""
    return [f for f in kinds(frames, "P") if f[5:9] != b"\x00\x00\x00\x00"]


class OptionTests(unittest.TestCase):
    def test_the_url_option(self) -> None:
        self.assertEqual(
            parse_dsn("nusadb://h?prepare_threshold=0"), {"host": "h", "prepare_threshold": 0}
        )
        self.assertEqual(parse_dsn("nusadb://h?prepare_threshold=none")["prepare_threshold"], None)
        self.assertEqual(parse_dsn("nusadb://h?binary=on")["binary"], True)
        self.assertEqual(parse_dsn("nusadb://h?binary=0")["binary"], False)
        for bad in ["prepare_threshold=-1", "prepare_threshold=x", "binary=maybe"]:
            with self.assertRaises(nusadb.ProgrammingError):
                parse_dsn("nusadb://h?" + bad)


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

    def table(self, conn: nusadb.Connection) -> None:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS ap")
        cur.execute("CREATE TABLE ap (id INT, v TEXT)")


class AutoPrepareTests(_ServerCase):
    def run_inserts(self, conn: nusadb.Connection, n: int = 20) -> List[bytes]:
        self.table(conn)
        frames = record(conn)
        cur = conn.cursor()
        for i in range(n):
            cur.execute(INSERT, [i, f"v{i}"])
            self.assertEqual(cur.rowcount, 1)
        self.assertEqual(
            conn.cursor().execute("SELECT count(*), sum(id) FROM ap").fetchone(),
            (n, n * (n - 1) // 2),
        )
        return frames

    def test_parse_messages_drop_after_the_threshold(self) -> None:
        conn = self.connect()
        self.assertEqual(conn.prepare_threshold, 5)
        frames = self.run_inserts(conn)
        # Five runs parse the statement each time, the sixth prepares it, the rest only bind.
        self.assertEqual(len(kinds(frames, "P")), 6)
        self.assertEqual(len(named_parse(frames)), 1)
        self.assertEqual(len(kinds(frames, "B")), 20)

    def test_the_threshold_can_be_changed_or_turned_off(self) -> None:
        self.assertEqual(
            len(kinds(self.run_inserts(self.connect(prepare_threshold=None)), "P")), 20
        )
        self.assertEqual(len(kinds(self.run_inserts(self.connect(prepare_threshold=0)), "P")), 1)
        url = nusadb.connect(
            f"nusadb://u@127.0.0.1:{self.server.port}/nusadb?prepare_threshold=2", password=None
        )
        self.addCleanup(url.close)
        self.assertEqual(len(kinds(self.run_inserts(url), "P")), 3)
        conn = self.connect()
        conn.prepare_threshold = None
        self.assertEqual(len(kinds(self.run_inserts(conn), "P")), 20)
        with self.assertRaises(nusadb.ProgrammingError):
            conn.prepare_threshold = -1
        with self.assertRaises(nusadb.ProgrammingError):
            self.connect(prepare_threshold="5")
        with nusadb.Pool(
            1, port=self.server.port, user="u", password=None, prepare_threshold=1
        ) as pool:
            with pool.connection() as pooled:
                self.assertEqual(pooled.prepare_threshold, 1)
                self.assertEqual(len(kinds(self.run_inserts(pooled), "P")), 2)

    def test_queries_without_parameters_are_left_alone(self) -> None:
        conn = self.connect(prepare_threshold=0)
        frames = record(conn)
        for _ in range(3):
            conn.cursor().execute("SELECT 1")
        self.assertEqual(kinds(frames, "P"), [])
        self.assertEqual(len(conn._statements), 0)

    def test_each_set_of_parameter_types_is_its_own_statement(self) -> None:
        conn = self.connect(prepare_threshold=1)
        self.table(conn)
        cur = conn.cursor()
        for _ in range(3):
            cur.execute(INSERT, [1, "text"])
            cur.execute(INSERT, [None, "null id"])
        self.assertEqual(len(conn._statements), 2)
        rows = cur.execute("SELECT id, v FROM ap ORDER BY v").fetchall()
        self.assertEqual(rows, [(None, "null id")] * 3 + [(1, "text")] * 3)

    def test_results_and_binary_through_a_prepared_statement(self) -> None:
        conn = self.connect(prepare_threshold=0)
        self.table(conn)
        conn.cursor().executemany(INSERT, [(i, f"v{i}") for i in range(5)])
        cur = conn.cursor()
        for i in range(5):
            cur.execute("SELECT id, v FROM ap WHERE id = %(id)s", {"id": i}, binary=bool(i % 2))
            self.assertEqual(cur.fetchall(), [(i, f"v{i}")])
            self.assertEqual(cur._result_binary, bool(i % 2))
        self.assertEqual(len(conn._statements), 1)

    def test_the_statement_cache_is_bounded(self) -> None:
        conn = self.connect(prepare_threshold=0)
        self.table(conn)
        frames = record(conn)
        original = connection_module.STATEMENT_CACHE_SIZE
        connection_module.STATEMENT_CACHE_SIZE = 2
        self.addCleanup(setattr, connection_module, "STATEMENT_CACHE_SIZE", original)
        cur = conn.cursor()
        for i in range(4):
            cur.execute(f"SELECT $1::INT + {i}", [1])
            self.assertEqual(cur.fetchone(), (1 + i,))
        self.assertEqual(len(conn._statements), 2)
        self.assertEqual(len(kinds(frames, "C")), 2)  # the two least recently used were closed
        cur.execute("SELECT $1::INT + 3", [1])
        self.assertEqual(len(named_parse(frames)), 4)

    def test_ddl_after_preparing(self) -> None:
        # The server plans a prepared statement each time it runs, so a changed table is seen.
        conn = self.connect(prepare_threshold=0)
        self.table(conn)
        cur = conn.cursor()
        cur.execute("SELECT * FROM ap WHERE id = $1", [1])
        self.assertEqual(len(cur.description), 2)
        cur.execute("ALTER TABLE ap ADD COLUMN extra INT")
        cur.execute("INSERT INTO ap VALUES ($1, $2, $3)", [1, "x", 7])
        cur.execute("SELECT * FROM ap WHERE id = $1", [1])
        self.assertEqual(cur.fetchall(), [(1, "x", 7)])
        cur.execute("DROP TABLE ap")
        with self.assertRaises(nusadb.ProgrammingError):
            cur.execute("SELECT * FROM ap WHERE id = $1", [1])
        cur.execute("CREATE TABLE ap (id INT)")
        cur.execute("SELECT * FROM ap WHERE id = $1", [1])
        self.assertEqual(cur.fetchall(), [])
        self.assertEqual(len(conn._statements), 2)  # the SELECT and the INSERT

    def test_a_statement_the_server_lost_runs_unprepared_once(self) -> None:
        conn = self.connect(prepare_threshold=0)
        self.table(conn)
        cur = conn.cursor()
        cur.execute(INSERT, [1, "a"])
        (statement,) = conn._statements.values()
        conn._close_statement(statement.name)  # behind the cache's back
        frames = record(conn)
        cur.execute(INSERT, [2, "b"])
        self.assertEqual(cur.rowcount, 1)
        # Bind to the lost name, then the retry: Parse and Bind of the unnamed statement.
        self.assertEqual(len(kinds(frames, "B")), 2)
        self.assertEqual(len(kinds(frames, "P")), 1)
        self.assertEqual(named_parse(frames), [])
        self.assertTrue(statement.closed)
        self.assertEqual(len(conn._statements), 0)
        # Prepared again on the next run.
        cur.execute(INSERT, [3, "c"])
        self.assertEqual(len(named_parse(frames)), 1)
        self.assertEqual(
            cur.execute("SELECT id FROM ap ORDER BY id").fetchall(), [(1,), (2,), (3,)]
        )

    def test_the_retry_happens_inside_a_healthy_transaction_only(self) -> None:
        conn = self.connect(prepare_threshold=0, autocommit=False)
        self.table(conn)
        conn.commit()
        cur = conn.cursor()
        cur.execute(INSERT, [1, "a"])
        conn.commit()
        (statement,) = conn._statements.values()
        conn._close_statement(statement.name)
        cur.execute(INSERT, [2, "b"])  # retried inside the open transaction
        self.assertEqual(conn.transaction_status, "in_transaction")
        conn.commit()
        cur.execute(INSERT, [3, "c"])  # prepared again
        conn.commit()
        (statement,) = conn._statements.values()
        conn._close_statement(statement.name)
        with self.assertRaises(nusadb.DataError):
            cur.execute("SELECT 1 / 0")
        self.assertEqual(conn.transaction_status, "failed")
        frames = record(conn)
        with self.assertRaises(nusadb.DatabaseError) as raised:
            cur.execute(INSERT, [4, "d"])
        self.assertEqual(raised.exception.sqlstate, "26000")
        self.assertEqual(kinds(frames, "P"), [])  # no retry in a failed transaction
        self.assertEqual(len(conn._statements), 0)
        conn.rollback()
        self.assertEqual(
            cur.execute("SELECT id FROM ap ORDER BY id").fetchall(), [(1,), (2,), (3,)]
        )

    def test_a_failing_statement_stays_prepared(self) -> None:
        conn = self.connect(prepare_threshold=0)
        cur = conn.cursor()
        for _ in range(3):
            with self.assertRaises(nusadb.DataError):
                cur.execute("SELECT 1 / $1", [0])
        self.assertEqual(len(conn._statements), 1)
        cur.execute("SELECT 6 / $1", [3])
        self.assertEqual(cur.fetchone(), (2,))


class AsyncAutoPrepareTests(unittest.IsolatedAsyncioTestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    async def connect(self, query: str = "", **kw: object) -> nusadb.aio.AsyncConnection:
        conn = await nusadb.aio.connect(
            f"nusadb://u@127.0.0.1:{self.server.port}/nusadb{query}", password=None, **kw
        )
        self.addAsyncCleanup(conn.close)
        return conn

    async def table(self, conn: nusadb.aio.AsyncConnection) -> None:
        await conn.execute("DROP TABLE IF EXISTS aap")
        await conn.execute("CREATE TABLE aap (id INT, v TEXT)")

    async def test_parse_messages_drop(self) -> None:
        for options, expected in [
            ({}, 6),
            ({"prepare_threshold": None}, 20),
            ({"prepare_threshold": 0}, 1),
        ]:
            conn = await self.connect(**options)
            await self.table(conn)
            frames = record(conn)
            for i in range(20):
                cur = await conn.execute("INSERT INTO aap VALUES ($1, $2)", [i, "x"])
                self.assertEqual(cur.rowcount, 1)
            self.assertEqual(len(kinds(frames, "P")), expected, options)
        url = await self.connect("?prepare_threshold=3")
        self.assertEqual(url.prepare_threshold, 3)

    async def test_lost_statements_and_the_cache_bound(self) -> None:
        conn = await self.connect(prepare_threshold=0)
        await self.table(conn)
        await conn.execute("INSERT INTO aap VALUES ($1, $2)", [1, "a"])
        (statement,) = conn._statements.values()
        async with conn._exchange():
            conn._send(nusadb._protocol.close_statement(statement.name))
            conn._send(nusadb._protocol.sync())
            await conn._read_until_ready()
        cur = await conn.execute("INSERT INTO aap VALUES ($1, $2)", [2, "b"])
        self.assertEqual(cur.rowcount, 1)
        self.assertTrue(statement.closed)
        original = connection_module.STATEMENT_CACHE_SIZE
        connection_module.STATEMENT_CACHE_SIZE = 2
        self.addCleanup(setattr, connection_module, "STATEMENT_CACHE_SIZE", original)
        frames = record(conn)
        for i in range(4):
            cur = await conn.execute(f"SELECT $1::INT + {i}", [1])
            self.assertEqual(await cur.fetchone(), (1 + i,))
        self.assertEqual(len(conn._statements), 2)
        self.assertEqual(len(kinds(frames, "C")), 2)
        # In a failed transaction a lost statement is not run again.
        conn.autocommit = False
        await conn.execute("SELECT $1::INT + 3", [1])
        statement = conn._statements[next(reversed(conn._statements))]
        async with conn._exchange():
            conn._send(nusadb._protocol.close_statement(statement.name))
            conn._send(nusadb._protocol.sync())
            await conn._read_until_ready()
        with self.assertRaises(nusadb.DataError):
            await conn.execute("SELECT 1 / 0")
        frames.clear()
        with self.assertRaises(nusadb.DatabaseError):
            await conn.execute("SELECT $1::INT + 3", [1])
        self.assertEqual(kinds(frames, "P"), [])
        await conn.rollback()


if __name__ == "__main__":
    unittest.main()
