"""Notices and two-phase commit: against a scripted server (``fake_server``), which can send a
notice anywhere in a reply, and against a real ``nusadb-server`` when it has both features."""

from __future__ import annotations

import datetime as dt
import io
import sys
import unittest
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import nusadb  # noqa: E402
import nusadb.aio  # noqa: E402
from fake_server import FakeServer  # noqa: E402
from nusadb import Notice, Xid  # noqa: E402
from nusadb import _protocol as proto  # noqa: E402
from test_driver import _Server  # noqa: E402


class XidTests(unittest.TestCase):
    def test_a_structured_xid_round_trips_through_its_gid(self) -> None:
        for format_id, gtrid, bqual in [
            (0, "g", "b"),
            (42, "é ☃_x", ""),
            (2**31 - 1, "a" * 64, "_=+/"),
        ]:
            xid = Xid(format_id, gtrid, bqual)
            gid = str(xid)
            self.assertRegex(gid, r"^\d+_[A-Za-z0-9+/=]*_[A-Za-z0-9+/=]*$")
            self.assertEqual(Xid.from_string(gid), xid)
            self.assertEqual(tuple(Xid.from_string(gid)), (format_id, gtrid, bqual))
        self.assertEqual(str(Xid(1, "a", "b")), "1_YQ==_Yg==")

    def test_any_other_gid_is_kept_as_it_is(self) -> None:
        for gid in [
            "plain",
            "1_x",
            "1_YQ==",
            "x_YQ==_Yg==",
            "1_YQ_Yg==",
            "1_/w==_YQ==",
            "1_YQ==_Yg==_c",
            "01_YQ==_Yg==",
            "1_YR==_Yg==",
        ]:
            xid = Xid.from_string(gid)
            self.assertEqual((xid.format_id, xid.gtrid, xid.bqual), (None, gid, None))
            self.assertEqual(str(xid), gid)

    def test_the_parts_are_checked(self) -> None:
        conn_xid = nusadb.Connection.xid
        for args in [
            (-1, "a", "b"),
            (2**31, "a", "b"),
            ("1", "a", "b"),
            (True, "a", "b"),
            (1, "a" * 65, "b"),
            (1, "a", "b" * 65),
            (1, b"a", "b"),
            (1, "a", None),
        ]:
            with self.assertRaises(nusadb.ProgrammingError):
                conn_xid(None, *args)  # type: ignore[arg-type]
        xid = conn_xid(None, 3, "g", "b")  # type: ignore[arg-type]
        self.assertEqual((xid[0], xid[1], xid[2], len(xid)), (3, "g", "b", 3))
        self.assertEqual(xid, (3, "g", "b"))
        self.assertEqual(hash(xid), hash(Xid(3, "g", "b")))


class _FakeCase(unittest.TestCase):
    def setUp(self) -> None:
        self.server = FakeServer()
        self.addCleanup(self.server.stop)

    def connect(self, **kw: object) -> nusadb.Connection:
        conn = nusadb.connect(port=self.server.port, user="u", password=None, **kw)
        self.addCleanup(conn.close)
        return conn


class NoticeTests(_FakeCase):
    def test_the_client_asks_for_minor_three(self) -> None:
        self.connect()
        self.assertEqual(self.server.minors, [3])
        self.assertEqual(proto.PROTOCOL_VERSION, (1, 3))

    def test_notices_in_every_kind_of_reply(self) -> None:
        conn = self.connect(autocommit=False)
        seen = []
        conn.add_notice_handler(seen.append)
        # A notice sent during authentication is kept too.
        self.assertEqual(conn.notices, [Notice("INFO", "00000", "welcome")])
        conn.notices.clear()
        cur = conn.cursor()
        cur.execute("SELECT notices")
        self.assertEqual(cur.fetchall(), [(1,), (2,)])
        self.assertEqual(
            [n.message for n in conn.notices],
            ["before the rows", "between the rows", "after the rows"],
        )
        self.assertEqual(conn.notices[1], Notice("WARNING", "01000", "between the rows"))
        cur.execute("SELECT $1", [1])
        self.assertEqual(cur.fetchall(), [(5,)])
        self.assertEqual([n.message for n in conn.notices[-2:]], ["described", "executed"])
        with self.assertRaises(nusadb.DataError):
            cur.execute("FAIL WITH A NOTICE")
        self.assertEqual(conn.notices[-1].message, "about to fail")
        conn.rollback()
        self.assertEqual(conn.copy_out_rows("COPY t TO STDOUT"), [("1",), ("2",)])
        self.assertEqual(conn.copy_in("COPY t FROM STDIN", io.BytesIO(b"a\nb\nc\n")), 3)
        cur.executemany("SELECT $1", [[1], [2]])
        with conn.pipeline() as p:
            queued = p.execute("SELECT $1", [1])
        self.assertEqual(queued.fetchall(), [(5,)])
        with conn.stream("SELECT $1", [1]) as rows:
            self.assertEqual(list(rows), [(5,)])
        messages = [n.message for n in conn.notices]
        for expected in ["mid copy", "copy done", "send the rows", "rows loaded"]:
            self.assertIn(expected, messages)
        self.assertEqual(seen, conn.notices)
        conn.remove_notice_handler(seen.append)
        with self.assertRaises(ValueError):
            conn.remove_notice_handler(seen.append)
        cur.execute("SELECT notices")
        self.assertEqual(len(seen), len(messages))

    def test_a_notice_on_an_idle_connection_is_kept_while_polling(self) -> None:
        conn = self.connect()
        conn.listen("ch")
        notification = conn.poll(5)
        self.assertEqual((notification.channel, notification.payload), ("ch", "hi"))
        self.assertEqual(conn.notices[-1].message, "idle notice")

    def test_notices_are_bounded_and_a_failing_handler_warns(self) -> None:
        conn = self.connect()

        def broken(_notice: Notice) -> None:
            raise RuntimeError("boom")

        conn.add_notice_handler(broken)
        cur = conn.cursor()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            for _ in range(40):
                cur.execute("SELECT notices")
        self.assertEqual(len(caught), 120)
        self.assertEqual(len(conn.notices), nusadb.MAX_NOTICES)
        conn.remove_notice_handler(broken)
        self.assertEqual(cur.execute("SELECT notices").fetchall(), [(1,), (2,)])


class TwoPhaseTests(_FakeCase):
    def test_prepare_then_commit_or_roll_back(self) -> None:
        conn = self.connect()
        xid = conn.xid(1, "global", "branch")
        conn.tpc_begin(xid)
        self.assertEqual(conn.transaction_status, "in_transaction")
        conn.cursor().execute("INSERT INTO t VALUES (1)")
        with self.assertRaises(nusadb.ProgrammingError):
            conn.commit()
        with self.assertRaises(nusadb.ProgrammingError):
            conn.rollback()
        with self.assertRaises(nusadb.ProgrammingError):
            conn.tpc_begin(conn.xid(2, "x", "y"))
        conn.tpc_prepare()
        self.assertEqual(self.server.prepared, {str(xid): "u"})
        with self.assertRaises(nusadb.ProgrammingError):
            conn.tpc_prepare()
        # Nothing else runs until the prepared transaction is ended.
        with self.assertRaises(nusadb.ProgrammingError):
            conn.cursor().execute("SELECT 1")
        conn.tpc_commit()
        self.assertEqual(self.server.prepared, {})
        self.assertEqual(
            self.server.queries,
            [
                "BEGIN",
                "INSERT INTO t VALUES (1)",
                f"PREPARE TRANSACTION '{xid}'",
                f"COMMIT PREPARED '{xid}'",
            ],
        )
        conn.tpc_begin("plain gid's")
        conn.tpc_prepare()
        conn.tpc_rollback()
        self.assertEqual(
            self.server.queries[-2:],
            ["PREPARE TRANSACTION 'plain gid''s'", "ROLLBACK PREPARED 'plain gid''s'"],
        )
        conn.cursor().execute("SELECT 1")
        conn.commit()

    def test_without_prepare_it_is_a_one_phase_commit(self) -> None:
        conn = self.connect(autocommit=False)
        conn.tpc_begin(conn.xid(1, "a", "b"))
        conn.cursor().execute("SELECT 1")
        conn.tpc_commit()
        conn.tpc_begin(conn.xid(1, "a", "c"))
        conn.tpc_rollback()
        self.assertEqual(self.server.queries, ["BEGIN", "SELECT 1", "COMMIT", "BEGIN", "ROLLBACK"])
        self.assertEqual(conn.transaction_status, "idle")
        conn.tpc_begin(conn.xid(1, "a", "d"))
        with self.assertRaises(nusadb.DataError):
            conn.cursor().execute("FAIL WITH A NOTICE")
        with self.assertRaises(nusadb.TransactionRolledBack):
            conn.tpc_commit()
        conn.tpc_begin(conn.xid(1, "a", "e"))  # the failed one is over

    def test_recover_and_finish_from_another_connection(self) -> None:
        first = self.connect()
        for bqual in ("one", "two"):
            first.tpc_begin(first.xid(9, "batch", bqual))
            first.tpc_prepare()
            first._tpc, first._tpc_prepared = None, False  # leave it prepared, as a crash would
        first.tpc_begin("legacy-gid")
        first.tpc_prepare()
        first._tpc, first._tpc_prepared = None, False
        second = self.connect(autocommit=False)
        recovered = second.tpc_recover()
        self.assertEqual(second.transaction_status, "idle")
        self.assertEqual(
            sorted(recovered, key=str),
            sorted(
                [Xid(9, "batch", "one"), Xid(9, "batch", "two"), Xid(None, "legacy-gid", None)],
                key=str,
            ),
        )
        one = [x for x in recovered if x.bqual == "one"][0]
        self.assertEqual((one.owner, one.database), ("u", "nusadb"))
        self.assertEqual(one.prepared, dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc))
        second.tpc_commit(one)
        second.tpc_rollback("legacy-gid")
        self.assertEqual(sorted(self.server.prepared), [str(Xid(9, "batch", "two"))])
        with self.assertRaises(nusadb.ProgrammingError):
            second.tpc_commit("missing")  # the server refuses an unknown gid
        second.cursor().execute("SELECT 1")
        with self.assertRaises(nusadb.ProgrammingError):
            second.tpc_commit(Xid(9, "batch", "two"))  # inside a transaction
        second.rollback()
        second.tpc_begin("x")
        with self.assertRaises(nusadb.ProgrammingError):
            second.tpc_rollback(Xid(9, "batch", "two"))  # inside a two-phase transaction
        with self.assertRaises(nusadb.ProgrammingError):
            self.connect().tpc_commit()  # nothing begun

    def test_tpc_begin_needs_no_open_transaction(self) -> None:
        conn = self.connect(autocommit=False)
        conn.cursor().execute("SELECT 1")
        with self.assertRaises(nusadb.ProgrammingError):
            conn.tpc_begin("g")
        with self.assertRaises(nusadb.ProgrammingError):
            conn.tpc_prepare()
        with self.assertRaises(nusadb.ProgrammingError):
            conn.tpc_begin(5)  # type: ignore[arg-type]


class AsyncFakeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.server = FakeServer()
        self.addCleanup(self.server.stop)

    async def connect(self, **kw: object) -> nusadb.aio.AsyncConnection:
        conn = await nusadb.aio.connect(port=self.server.port, user="u", password=None, **kw)
        self.addAsyncCleanup(conn.close)
        return conn

    async def test_notices(self) -> None:
        conn = await self.connect()
        self.assertEqual(self.server.minors, [3])
        seen = []
        conn.add_notice_handler(seen.append)
        cur = await conn.execute("SELECT notices")
        self.assertEqual(await cur.fetchall(), [(1,), (2,)])
        cur = await conn.execute("SELECT $1", [1])
        self.assertEqual(await cur.fetchall(), [(5,)])
        self.assertEqual(await conn.copy_out_rows("COPY t TO STDOUT"), [("1",), ("2",)])
        self.assertEqual(await conn.copy_in("COPY t FROM STDIN", b"a\nb\n"), 2)
        async with conn.stream("SELECT $1", [1]) as rows:
            self.assertEqual([r async for r in rows], [(5,)])
        messages = [n.message for n in seen]
        for expected in [
            "before the rows",
            "between the rows",
            "after the rows",
            "described",
            "executed",
            "mid copy",
            "copy done",
            "send the rows",
            "rows loaded",
        ]:
            self.assertIn(expected, messages)
        self.assertEqual(conn.notices[1:], seen)
        conn.remove_notice_handler(seen.append)
        handled = len(seen)
        await conn.listen("ch")
        notification = await conn.poll(5)
        self.assertEqual(notification.payload, "hi")
        self.assertEqual(conn.notices[-1].message, "idle notice")
        self.assertEqual(len(seen), handled)

    async def test_two_phase_commit(self) -> None:
        conn = await self.connect(autocommit=False)
        xid = conn.xid(4, "g", "b")
        await conn.tpc_begin(xid)
        with self.assertRaises(nusadb.ProgrammingError):
            await conn.commit()
        with self.assertRaises(nusadb.ProgrammingError):
            await conn.rollback()
        await conn.tpc_prepare()
        with self.assertRaises(nusadb.ProgrammingError):
            await conn.execute("SELECT 1")
        other = await self.connect()
        recovered = await other.tpc_recover()
        self.assertEqual(recovered, [xid])
        await conn.tpc_rollback()
        self.assertEqual(self.server.prepared, {})
        await conn.tpc_begin("one-phase")
        await conn.tpc_commit()
        await conn.tpc_begin(xid)
        await conn.tpc_prepare()
        conn._tpc, conn._tpc_prepared = None, False
        await other.tpc_commit(xid)
        self.assertEqual(self.server.queries[-1], f"COMMIT PREPARED '{xid}'")
        with self.assertRaises(nusadb.ProgrammingError):
            await other.tpc_prepare()


def _live_server() -> _Server:
    return _Server()


class LiveTwoPhaseTests(unittest.TestCase):
    """Against the real server: skipped when it has no PREPARE TRANSACTION or sends no notices."""

    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _live_server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def connect(self, **kw: object) -> nusadb.Connection:
        conn = nusadb.connect(port=self.server.port, user="nusadb-root", password=None, **kw)
        self.addCleanup(conn.close)
        return conn

    def test_two_phase_commit(self) -> None:
        conn = self.connect()
        conn.cursor().execute("CREATE TABLE IF NOT EXISTS tpc_live (id INT)")
        xid = conn.xid(1, "live", "one")
        conn.tpc_begin(xid)
        conn.cursor().execute("INSERT INTO tpc_live VALUES (1)")
        try:
            conn.tpc_prepare()
        except nusadb.ProgrammingError as exc:
            if exc.sqlstate in ("42601", "0A000"):
                self.skipTest(f"the server has no PREPARE TRANSACTION: {exc}")
            raise
        other = self.connect()
        self.assertEqual(other.cursor().execute("SELECT count(*) FROM tpc_live").fetchone(), (0,))
        recovered = other.tpc_recover()
        self.assertIn(xid, recovered)
        other.tpc_commit(xid)
        conn._tpc, conn._tpc_prepared = None, False
        self.assertEqual(other.cursor().execute("SELECT count(*) FROM tpc_live").fetchone(), (1,))
        self.assertNotIn(xid, other.tpc_recover())
        conn.tpc_begin("live-two")
        conn.cursor().execute("INSERT INTO tpc_live VALUES (2)")
        conn.tpc_prepare()
        conn.tpc_rollback()
        self.assertEqual(other.cursor().execute("SELECT count(*) FROM tpc_live").fetchone(), (1,))

    def test_notices(self) -> None:
        conn = self.connect()
        conn.cursor().execute("DROP TABLE IF EXISTS nusadb_no_such_table")
        if not conn.notices:
            self.skipTest("the server sent no notice")
        self.assertIsInstance(conn.notices[-1], Notice)
        self.assertEqual(len(conn.notices[-1].sqlstate), 5)


if __name__ == "__main__":
    unittest.main()
