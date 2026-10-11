"""The asyncio API against a real ``nusadb-server``."""

from __future__ import annotations

import asyncio
import io
import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import nusadb  # noqa: E402
import nusadb.aio  # noqa: E402
from nusadb.rows import dict_row  # noqa: E402
from test_driver import _Server  # noqa: E402


class _AsyncCase(unittest.IsolatedAsyncioTestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def url(self, query: str = "") -> str:
        return f"nusadb://u@127.0.0.1:{self.server.port}/nusadb{query}"

    async def connect(self, **kw: object) -> nusadb.aio.AsyncConnection:
        conn = await nusadb.aio.connect(self.url(), password=None, **kw)
        self.addAsyncCleanup(conn.close)
        return conn

    async def values(self, conn: nusadb.aio.AsyncConnection, sql: str) -> list:
        cur = await conn.execute(sql)
        return [row[0] for row in await cur.fetchall()]


class QueryTests(_AsyncCase):
    async def test_parameters_types_and_fetching(self) -> None:
        conn = await self.connect()
        cur = conn.cursor()
        await cur.execute("SELECT $1, $2 + 1, $3", ["007", 41, Decimal("1E+2")])
        self.assertEqual(await cur.fetchone(), ("007", 42, Decimal("100")))
        await cur.execute("CREATE TABLE q (id INT PRIMARY KEY, v TEXT)")
        syncs = []
        original = conn._send

        def counting(frame: bytes) -> None:
            if frame[0] == ord("Y"):
                syncs.append(frame)
            original(frame)

        conn._send = counting  # type: ignore[method-assign]
        await cur.executemany("INSERT INTO q VALUES ($1, $2)", ((i, f"v{i}") for i in range(600)))
        conn._send = original  # type: ignore[method-assign]
        self.assertEqual(cur.rowcount, 600)
        # Pipelined: one round trip per 256 rows, not one per row.
        self.assertEqual(len(syncs), 3)
        await cur.execute("SELECT id, v FROM q WHERE id < 3 ORDER BY id")
        self.assertEqual([d[0] for d in cur.description], ["id", "v"])
        self.assertEqual(await cur.fetchmany(2), [(0, "v0"), (1, "v1")])
        self.assertEqual([row async for row in cur], [(2, "v2")])
        cur = conn.cursor(row_factory=dict_row)
        await cur.execute("SELECT 1 AS a")
        self.assertEqual(await cur.fetchall(), [{"a": 1}])

    async def test_errors_keep_the_connection(self) -> None:
        conn = await self.connect()
        with self.assertRaises(nusadb.ProgrammingError):
            await conn.execute("SELECT nope FROM nowhere")
        self.assertEqual(await self.values(conn, "SELECT 1"), [1])
        await conn.execute("CREATE TABLE e (id INT PRIMARY KEY)")
        with self.assertRaises(nusadb.IntegrityError):
            await conn.cursor().executemany("INSERT INTO e VALUES ($1)", [(1,), (1,), (2,)])
        self.assertEqual(await self.values(conn, "SELECT count(*) FROM e"), [1])

    async def test_url_options(self) -> None:
        conn = await nusadb.aio.connect(self.url("?statement_timeout=1500ms"), password=None)
        self.assertEqual(await self.values(conn, "SHOW statement_timeout"), ["1500ms"])
        await conn.close()
        await conn.close()
        self.assertTrue(conn.closed)
        with self.assertRaises(nusadb.InterfaceError):
            conn.cursor()

    async def test_tasks_sharing_a_connection_take_turns(self) -> None:
        conn = await self.connect()

        async def one(i: int) -> int:
            cur = await conn.execute("SELECT $1 * 2", [i])
            return (await cur.fetchone())[0]

        self.assertEqual(await asyncio.gather(*(one(i) for i in range(50))), [i * 2 for i in range(50)])

    async def test_a_cancelled_call_breaks_the_connection(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE big AS SELECT i FROM generate_series(1, 200000) AS g(i)")
        task = asyncio.ensure_future(conn.execute("SELECT count(*) FROM big a, big b WHERE a.i = b.i"))
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        with self.assertRaises(nusadb.OperationalError):
            await conn.execute("SELECT 1")
        self.assertTrue(conn.broken)
        # The server is still running the statement; a cancel request on its key stops it.
        await conn.cancel()


class InterruptionTests(_AsyncCase):
    async def test_a_timeout_inside_a_block_surfaces_as_a_timeout(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE slow AS SELECT i FROM generate_series(1, 200000) AS g(i)")

        async def work() -> None:
            async with conn.transaction():
                async with conn.transaction():
                    await conn.execute("SELECT count(*) FROM slow a, slow b WHERE a.i = b.i")

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(work(), 0.05)
        self.assertTrue(conn.broken)
        await conn.cancel()

    async def test_a_bad_value_in_executemany_leaves_the_connection_usable(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE eb (id INT)")
        with self.assertRaises(nusadb.ProgrammingError):
            await conn.cursor().executemany(
                "INSERT INTO eb VALUES ($1)", [(1,), (2,), (iter([3]),), (4,)]
            )
        self.assertEqual(await self.values(conn, "SELECT count(*) FROM eb"), [2])

    async def test_a_stream_cancelled_while_it_opens_breaks_the_connection(self) -> None:
        conn = await self.connect()

        async def read() -> None:
            sql = "SELECT a, repeat('x', 2000) FROM generate_series(1, 200000) AS g(a)"
            async with conn.stream(sql, batch_rows=50000) as rows:
                async for _ in rows:
                    pass

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(read(), 0.02)
        self.assertTrue(conn.broken)
        self.assertFalse(conn._lock.locked())
        with self.assertRaises(nusadb.OperationalError):
            await asyncio.wait_for(conn.execute("SELECT 1"), 2)
        await conn.cancel()

    async def test_a_stream_on_a_failed_connection_frees_its_waiters(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE sf AS SELECT i FROM generate_series(1, 100) AS g(i)")
        rows = conn.stream("SELECT i FROM sf ORDER BY i", batch_rows=5)
        await rows.__anext__()
        conn._writer.transport.abort()  # the server end goes away under the stream
        with self.assertRaises(nusadb.OperationalError):
            for _ in range(50):
                await rows.__anext__()
        # The failed stream let go of the connection without being closed.
        self.assertFalse(conn._lock.locked())
        await rows.close()
        with self.assertRaises((nusadb.OperationalError, nusadb.InterfaceError)):
            await asyncio.wait_for(conn.execute("SELECT 1"), 2)


class PoolOutsideTheLoopTests(unittest.TestCase):
    """A pool built before (or between) event loops works in the loop that uses it."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def test_a_pool_made_outside_the_loop(self) -> None:
        url = f"nusadb://u@127.0.0.1:{self.server.port}/nusadb"
        asyncio.run(asyncio.sleep(0))  # a loop has come and gone already
        pool = nusadb.aio.AsyncPool(2, url, password=None)

        async def main() -> list:
            async def one(i: int) -> int:
                async with pool.connection() as conn:
                    return (await (await conn.execute("SELECT $1", [i])).fetchone())[0]

            try:
                return await asyncio.gather(*(one(i) for i in range(10)))
            finally:
                await pool.close()

        self.assertEqual(asyncio.run(main()), list(range(10)))


class TransactionTests(_AsyncCase):
    async def test_blocks_savepoints_and_options(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE t (id INT PRIMARY KEY)")
        async with conn.transaction():
            await conn.execute("INSERT INTO t VALUES (1)")
            with self.assertRaises(KeyError):
                async with conn.transaction():
                    await conn.execute("INSERT INTO t VALUES (2)")
                    raise KeyError
        with self.assertRaises(ZeroDivisionError):
            async with conn.transaction():
                await conn.execute("INSERT INTO t VALUES (3)")
                1 / 0
        self.assertEqual(await self.values(conn, "SELECT id FROM t ORDER BY id"), [1])
        async with conn.transaction(isolation="serializable"):
            self.assertEqual(await self.values(conn, "SHOW transaction_isolation"), ["serializable"])
        with self.assertRaises(nusadb.DatabaseError):
            async with conn.transaction(read_only=True):
                await conn.execute("INSERT INTO t VALUES (4)")
        with self.assertRaises(nusadb.TransactionRolledBack):
            async with conn.transaction():
                try:
                    await conn.execute("INSERT INTO t VALUES (1)")
                except nusadb.IntegrityError:
                    pass

    async def test_transactional_mode(self) -> None:
        conn = await self.connect(autocommit=False)
        await conn.execute("CREATE TABLE tm (id INT)")
        await conn.commit()
        await conn.execute("INSERT INTO tm VALUES (1)")
        self.assertEqual(conn.transaction_status, "in_transaction")
        await conn.rollback()
        self.assertEqual(await self.values(conn, "SELECT count(*) FROM tm"), [0])
        await conn.rollback()


class PreparedAndStreamTests(_AsyncCase):
    async def test_prepared_statements(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE p (id INT PRIMARY KEY)")
        insert = await conn.prepare_cached("INSERT INTO p VALUES ($1)")
        self.assertIs(await conn.prepare_cached("INSERT INTO p VALUES ($1)"), insert)
        await conn.cursor().executemany(insert, [(1,), (2,)])
        cur = conn.cursor()
        await cur.execute(insert, [3])
        self.assertEqual(cur.rowcount, 1)
        await insert.close()
        with self.assertRaisesRegex(nusadb.ProgrammingError, "closed"):
            await cur.execute(insert, [4])
        self.assertEqual(await self.values(conn, "SELECT count(*) FROM p"), [3])

    async def test_stream(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE s AS SELECT i AS id FROM generate_series(1, 2500) AS g(i)")
        async with conn.stream("SELECT id FROM s ORDER BY id", batch_rows=100) as rows:
            with self.assertRaises(nusadb.InterfaceError):
                await conn.execute("SELECT 1")
            got = [row[0] async for row in rows]
        self.assertEqual(got, list(range(1, 2501)))
        self.assertEqual(rows.rowcount, 2500)
        async with conn.stream("SELECT id FROM s ORDER BY id", batch_rows=10) as rows:
            first = await rows.__anext__()
        self.assertEqual(first, (1,))
        self.assertEqual(await self.values(conn, "SELECT count(*) FROM s"), [2500])
        with self.assertRaises(nusadb.ProgrammingError):
            async with conn.stream("SELECT nope FROM s"):
                pass
        self.assertEqual(await self.values(conn, "SELECT 1"), [1])


class CopyNotifyPoolTests(_AsyncCase):
    async def test_copy_in_and_out(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE c (id INT, v TEXT)")
        self.assertEqual(await conn.copy_in("COPY c FROM STDIN", b"1\ta\n2\t\\N\n"), 2)

        async def chunks():
            yield b"3\tc\n"
            yield b"4\td\n"

        self.assertEqual(await conn.copy_in("COPY c FROM STDIN", chunks()), 2)
        self.assertEqual(await conn.copy_in("COPY c FROM STDIN", io.BytesIO(b"5\te\n")), 1)
        out = []
        self.assertEqual(await conn.copy_out("COPY c TO STDOUT", out.append), 5)
        self.assertIn(b"2\t\\N\n", b"".join(out))
        with self.assertRaises(nusadb.ProgrammingError):
            await conn.copy_out("COPY c FROM STDIN", out.append)
        self.assertEqual(await self.values(conn, "SELECT count(*) FROM c"), [5])

    async def test_copy_rows_in_and_out(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE cr (id INT, v TEXT)")
        self.assertEqual(await conn.copy_in_rows("COPY cr FROM STDIN", [(1, "a\tb"), (2, None)]), 2)

        async def rows():
            for i in range(3, 1503):
                yield (i, "y")

        self.assertEqual(await conn.copy_in_rows("COPY cr FROM STDIN", rows()), 1500)
        out = await conn.copy_out_rows("COPY (SELECT id, v FROM cr ORDER BY id) TO STDOUT")
        self.assertEqual(out[:2], [("1", "a\tb"), ("2", None)])
        self.assertEqual(len(out), 1502)
        with self.assertRaises(nusadb.ProgrammingError):
            await conn.copy_in_rows("COPY cr FROM STDIN", [(5, "z"), 7])
        self.assertEqual(await self.values(conn, "SELECT count(*) FROM cr"), [1502])

    async def test_pipeline(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE ap (id INT PRIMARY KEY)")
        async with conn.pipeline() as p:
            ins = await p.execute("INSERT INTO ap VALUES ($1), ($2)", [1, 2])
            sel = await p.execute("SELECT id FROM ap ORDER BY id")
            upd = await p.execute("UPDATE ap SET id = id WHERE id > 100")
        self.assertEqual(ins.rowcount, 2)
        self.assertEqual(await sel.fetchall(), [(1,), (2,)])
        self.assertEqual((upd.rowcount, upd.description), (0, None))
        with self.assertRaises(nusadb.ProgrammingError):
            async with conn.pipeline() as p:
                await p.execute("INSERT INTO ap VALUES (3)")
                await p.execute("SELECT nope FROM ap")
                skipped = await p.execute("INSERT INTO ap VALUES (4)")
        self.assertEqual(skipped.rowcount, -1)
        async with conn.pipeline() as p:
            many = [await p.execute("SELECT $1", [i]) for i in range(nusadb.PIPELINE_SYNC_STATEMENTS + 1)]
            self.assertEqual(await many[0].fetchone(), (0,))
        self.assertEqual(await many[-1].fetchone(), (nusadb.PIPELINE_SYNC_STATEMENTS,))
        with self.assertRaises(KeyError):
            async with conn.pipeline() as p:
                await p.execute("INSERT INTO ap VALUES (9)")
                raise KeyError("stop")
        self.assertEqual(await self.values(conn, "SELECT id FROM ap ORDER BY id"), [1, 2, 3])

    async def test_named_cursor(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE nc (id INT PRIMARY KEY)")
        await conn.execute("INSERT INTO nc SELECT g FROM generate_series(1, 20) g")
        cur = conn.cursor(name="anc", scrollable=True, withhold=True)
        await cur.execute("SELECT id FROM nc WHERE id > $1 ORDER BY id", [2])
        self.assertEqual(await cur.fetchone(), (3,))
        self.assertEqual(await cur.fetchmany(2), [(4,), (5,)])
        await cur.scroll(-3)
        self.assertEqual(cur.rownumber, 0)
        cur.itersize = 4
        sent = []
        real_send = conn._send
        conn._send = lambda frame: (sent.append(frame), real_send(frame))[1]
        self.assertEqual([row[0] async for row in cur], list(range(3, 21)))
        self.assertEqual(sum(b"FETCH FORWARD 4" in frame for frame in sent), 6)
        await cur.scroll(2, "absolute")
        with self.assertRaises(IndexError):
            await cur.scroll(50, "absolute")
        self.assertEqual(await cur.fetchone(), (5,))
        await cur.scroll(0, "absolute")
        self.assertEqual(await cur.__anext__(), (3,))
        await cur.scroll(9, "absolute")
        self.assertEqual(await cur.fetchone(), (12,))
        self.assertEqual(await cur.__anext__(), (13,))
        self.assertEqual([r[0] for r in await cur.fetchall()], list(range(14, 21)))
        await cur.scroll(17, "absolute")
        self.assertEqual(await cur.fetchall(), [(20,)])
        await cur.close()
        with self.assertRaises(nusadb.ProgrammingError):
            await cur.fetchone()
        with self.assertRaises(nusadb.OperationalError):
            await conn.cursor(name="plain").execute("SELECT 1")

    async def test_listen_notify(self) -> None:
        listener = await self.connect()
        sender = await self.connect()
        await listener.listen("ch")
        self.assertIsNone(await listener.poll(0))
        await sender.notify("ch", "hello")
        note = await listener.poll(5)
        self.assertEqual((note.channel, note.payload), ("ch", "hello"))

    async def test_pool(self) -> None:
        async with nusadb.aio.AsyncPool(2, self.url(), password=None, statement_timeout=0.15) as pool:
            async with pool.connection() as conn:
                await conn.execute("SET statement_timeout = '5s'")
                first = conn
            async with pool.connection() as conn:
                self.assertIs(conn, first)
                self.assertEqual(await self.values(conn, "SHOW statement_timeout"), ["150ms"])

            async def work(i: int) -> int:
                async with pool.connection() as conn:
                    return (await (await conn.execute("SELECT $1", [i])).fetchone())[0]

            self.assertEqual(await asyncio.gather(*(work(i) for i in range(20))), list(range(20)))
            self.assertLessEqual(pool.size, 2)
            held = await pool.acquire()
            await pool.release(held)
        async with nusadb.aio.AsyncPool(1, self.url(), password=None, idle_timeout=0.1) as pool:
            first = await pool.acquire()
            await pool.release(first)
            await asyncio.sleep(0.2)
            second = await pool.acquire()
            self.assertIsNot(second, first)
            self.assertTrue(first.closed)
            await pool.release(second)
        async with nusadb.aio.AsyncPool(2, self.url(), password=None) as pool:
            held = await pool.acquire()
            await pool.release(held)
            with self.assertRaises(nusadb.InterfaceError):
                await pool.release(held)


if __name__ == "__main__":
    unittest.main()


class PyformatTests(_AsyncCase):
    async def test_named_and_positional_markers(self) -> None:
        conn = await self.connect()
        cur = await conn.execute("SELECT %(a)s, %(b)s, %(a)s", {"a": 1, "b": "x"})
        self.assertEqual(await cur.fetchone(), (1, "x", 1))
        cur = await conn.execute("SELECT %s, 7 %% 4", [3])
        self.assertEqual(await cur.fetchone(), (3, 3))
        await conn.execute("CREATE TABLE apf (id INT PRIMARY KEY)")
        await conn.cursor().executemany("INSERT INTO apf VALUES (%(id)s)", [{"id": 1}, {"id": 2}])
        statement = await conn.prepare("SELECT count(*) FROM apf WHERE id >= %(low)s")
        cur = conn.cursor()
        await cur.execute(statement, {"low": 2})
        self.assertEqual(await cur.fetchone(), (1,))
        async with conn.stream("SELECT id FROM apf WHERE id > %s ORDER BY id", [0]) as rows:
            self.assertEqual([row async for row in rows], [(1,), (2,)])
        with self.assertRaisesRegex(nusadb.ProgrammingError, "take a mapping"):
            await conn.execute("SELECT %(a)s", [1])
