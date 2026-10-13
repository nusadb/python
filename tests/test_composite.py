"""Composite types: CompositeInfo, register_composite and the composite row factory, blocking and
asyncio, against a real ``nusadb-server``."""

from __future__ import annotations

import dataclasses
import datetime as dt
import sys
import unittest
import uuid
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import nusadb  # noqa: E402
import nusadb.aio  # noqa: E402
import nusadb.connection as connection_module  # noqa: E402
from nusadb import sql  # noqa: E402
from nusadb.rows import composite_row, dict_row  # noqa: E402
from nusadb.types import CompositeInfo, register_composite  # noqa: E402
from test_driver import _Server  # noqa: E402

ITEM_FIELDS = [
    ("name", "TEXT"),
    ("qty", "INT"),
    ("price", "NUMERIC(8,2)"),
    ("added", "DATE"),
    ("ok", "BOOL"),
    ("data", "BYTEA"),
    ("seen", "TIMESTAMPTZ"),
    ("ref", "UUID"),
]
# The server reads a composite's text back for these field types (not BYTEA or INTERVAL yet).
SERVER_FIELDS = [field for field in ITEM_FIELDS if field[1] != "BYTEA"]


class InfoTests(unittest.TestCase):
    """Loading and dumping record text without a server."""

    info = CompositeInfo("item", ITEM_FIELDS)

    def test_load_converts_each_field(self) -> None:
        text = (
            '("a ""b"", c\\\\d",5,12.50,2024-01-02,t,"\\\\x00ff",'
            '"2024-01-01 10:00:00+00",a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11)'
        )
        item = self.info.load(text)
        self.assertEqual(item.name, 'a "b", c\\d')
        self.assertEqual(item.qty, 5)
        self.assertEqual(item.price, Decimal("12.50"))
        self.assertEqual(item.added, dt.date(2024, 1, 2))
        self.assertIs(item.ok, True)
        self.assertEqual(item.data, b"\x00\xff")
        self.assertEqual(item.seen, dt.datetime(2024, 1, 1, 10, tzinfo=dt.timezone.utc))
        self.assertEqual(item.ref, uuid.UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
        empty = self.info.load('("",,,,,,,)')
        self.assertEqual(empty, ("", None, None, None, None, None, None, None))
        self.assertIsNone(self.info.load(None))
        for bad in ["(1,2)", "not a record", "(a,1,1,2024-01-01,t,,,,extra)"]:
            with self.assertRaises(nusadb.DataError):
                self.info.load(bad)

    def test_dump_quotes_what_needs_it(self) -> None:
        item = self.info.python_type(
            'a "b", c\\d',
            5,
            Decimal("12.50"),
            dt.date(2024, 1, 2),
            True,
            b"\x00\xff",
            dt.datetime(2024, 1, 1, 10, tzinfo=dt.timezone.utc),
            None,
        )
        text = self.info.dump(item)
        self.assertEqual(
            text,
            '("a ""b"", c\\\\d",5,12.50,2024-01-02,true,"\\\\x00ff",'
            '"2024-01-01 10:00:00+00:00",)',
        )
        self.assertEqual(self.info.load(text), item)
        self.assertEqual(
            CompositeInfo("p", [("a", "TEXT"), ("b", "TEXT")]).dump(("", "(x)")), '("","(x)")'
        )
        with self.assertRaises(nusadb.ProgrammingError):
            self.info.dump((1, 2))
        with self.assertRaises(nusadb.ProgrammingError):
            CompositeInfo("p", [("a", "TEXT")]).dump(("a\x00",))
        with self.assertRaises(nusadb.ProgrammingError):
            CompositeInfo("p", [("a", "TEXT")]).dump(object())

    def test_fields_of_a_type_the_driver_does_not_know_stay_text(self) -> None:
        info = CompositeInfo("q", [("m", "mood"), ("tags", "TEXT[]"), ("r", "INT4RANGE")])
        self.assertEqual(
            info.load('(happy,"{a,b}","[1,3)")'), ("happy", ["a", "b"], nusadb.Range(1, 3))
        )
        with self.assertRaises(nusadb.ProgrammingError):
            CompositeInfo("empty", [])


class _ServerCase(unittest.TestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def connect(self, user: str = "u", **kw: object) -> nusadb.Connection:
        conn = nusadb.connect(port=self.server.port, user=user, password=None, **kw)
        self.addCleanup(conn.close)
        return conn

    def tearDown(self) -> None:
        connection_module._ADAPTERS.clear()


class FetchTests(_ServerCase):
    def test_before_any_type_exists(self) -> None:
        root = self.connect("nusadb-root")
        self.assertIsNone(CompositeInfo.fetch(root, "nothing"))

    def test_fetch_reads_the_fields(self) -> None:
        root = self.connect("nusadb-root")
        root.cursor().execute(
            "CREATE TYPE item AS (" + ", ".join(f"{n} {t}" for n, t in SERVER_FIELDS) + ")"
        )
        root.cursor().execute('CREATE TYPE "Mixed" AS ("A b" INT, c TEXT)')
        info = CompositeInfo.fetch(root, "item")
        assert info is not None
        self.assertEqual(info.name, "item")
        self.assertEqual(info.fields, SERVER_FIELDS)
        self.assertEqual(info.python_type._fields, tuple(n for n, _t in SERVER_FIELDS))
        self.assertEqual(CompositeInfo.fetch(root, "ITEM").name, "item")  # type: ignore[union-attr]
        mixed = CompositeInfo.fetch(root, "Mixed")
        assert mixed is not None
        self.assertEqual(mixed.fields, [("A b", "INT"), ("c", "TEXT")])
        self.assertEqual(mixed.python_type._fields, ("_0", "c"))
        self.assertIsNone(CompositeInfo.fetch(root, "missing"))
        # The catalog is a superuser's: anyone else is told how to build the info instead.
        with self.assertRaises(nusadb.ProgrammingError) as raised:
            CompositeInfo.fetch(self.connect(), "item")
        self.assertIn("superuser", str(raised.exception))


class RoundTripTests(_ServerCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        root = nusadb.connect(port=cls.server.port, user="nusadb-root", password=None)
        cur = root.cursor()
        cur.execute("CREATE TYPE item AS (" + ", ".join(f"{n} {t}" for n, t in SERVER_FIELDS) + ")")
        cur.execute("CREATE TYPE pair AS (x INT, y TEXT)")
        cur.execute("CREATE TABLE orders (id INT, line item, extra pair)")
        cur.execute("GRANT ALL ON orders TO PUBLIC")
        root.close()

    def setUp(self) -> None:
        self.connect().cursor().execute("DELETE FROM orders")

    def sample(self, info: CompositeInfo, name: str) -> object:
        return info.python_type(
            name,
            3,
            Decimal("9.99"),
            dt.date(2024, 5, 6),
            False,
            dt.datetime(2024, 5, 6, 7, 8, 9, tzinfo=dt.timezone.utc),
            uuid.UUID(int=99),
        )

    def test_values_bind_as_records_and_load_back(self) -> None:
        root = self.connect("nusadb-root")
        info = CompositeInfo.fetch(root, "item")
        assert info is not None
        register_composite(info)
        conn = self.connect()
        hostile = 'quote " comma , paren ) back \\ é'
        lines = [self.sample(info, hostile), self.sample(info, ""), info.python_type(*[None] * 7)]
        cur = conn.cursor()
        cur.executemany("INSERT INTO orders (id, line) VALUES ($1, $2)", list(enumerate(lines)))
        cur.execute("INSERT INTO orders (id, line) VALUES ($1, $2)", [9, lines[0]])
        cur = conn.cursor(row_factory=composite_row({"line": info}))
        cur.execute("SELECT id, line FROM orders ORDER BY id")
        self.assertEqual(
            cur.fetchall(), [(i, line) for i, line in enumerate(lines)] + [(9, lines[0])]
        )
        # Compared with a parameter cast to the type.
        cur.execute("SELECT id FROM orders WHERE line = $1::item ORDER BY id", [lines[0]])
        self.assertEqual(cur.fetchall(), [(0,), (9,)])
        # A field read on the server is the value written.
        cur = conn.cursor()
        cur.execute("SELECT (line).name, (line).price, (line).seen FROM orders WHERE id = 0")
        self.assertEqual(
            cur.fetchone(),
            (hostile, Decimal("9.99"), dt.datetime(2024, 5, 6, 7, 8, 9, tzinfo=dt.timezone.utc)),
        )
        # As a literal too.
        cur.execute(sql.SQL("SELECT {}::item").format(sql.Literal(lines[0])))
        self.assertEqual(info.load(cur.fetchone()[0]), lines[0])
        # And in a COPY.
        conn.copy_in_rows("COPY orders (id, line) FROM STDIN", [(20, lines[0])])
        cur.execute("SELECT line FROM orders WHERE id = 20")
        self.assertEqual(info.load(cur.fetchone()[0]), lines[0])

    def test_a_factory_class_and_a_connection_scope(self) -> None:
        @dataclasses.dataclass
        class Pair:
            x: int
            y: str

        info = register_composite(CompositeInfo("pair", [("x", "INT"), ("y", "TEXT")]), None, Pair)
        self.assertIs(info.python_type, Pair)
        connection_module._ADAPTERS.clear()
        scoped = self.connect()
        other = self.connect()
        register_composite(info, scoped)
        cur = scoped.cursor(row_factory=composite_row({"extra": info}, dict_row))
        cur.execute("INSERT INTO orders (id, extra) VALUES ($1, $2)", [1, Pair(7, "seven, 7")])
        cur.execute("SELECT id, extra FROM orders")
        self.assertEqual(cur.fetchall(), [{"id": 1, "extra": Pair(7, "seven, 7")}])
        # Only the connection it was registered on binds it as a record.
        with self.assertRaises(nusadb.DatabaseError):
            other.cursor().execute(
                "INSERT INTO orders (id, extra) VALUES ($1, $2)", [2, Pair(8, "x")]
            )
        with self.assertRaises(nusadb.ProgrammingError):
            register_composite(
                CompositeInfo("pair", [("x", "INT"), ("y", "TEXT")]), None, lambda *a: a
            )


class AsyncCompositeTests(unittest.IsolatedAsyncioTestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    async def connect(self, user: str) -> nusadb.aio.AsyncConnection:
        conn = await nusadb.aio.connect(
            f"nusadb://{user}@127.0.0.1:{self.server.port}/nusadb", password=None
        )
        self.addAsyncCleanup(conn.close)
        return conn

    async def test_fetch_register_and_load(self) -> None:
        root = await self.connect("nusadb-root")
        self.assertIsNone(await CompositeInfo.fetch_async(root, "pt"))
        await root.execute("CREATE TYPE pt AS (label TEXT, at DATE)")
        await root.execute("CREATE TABLE pts (id INT, p pt)")
        info = await CompositeInfo.fetch_async(root, "pt")
        assert info is not None
        self.assertEqual(info.fields, [("label", "TEXT"), ("at", "DATE")])
        register_composite(info, root)
        value = info.python_type('a (b), "c"', dt.date(2024, 1, 1))
        cur = root.cursor(row_factory=composite_row({"p": info}))
        await cur.execute("INSERT INTO pts VALUES ($1, $2)", [1, value])
        await cur.execute("SELECT id, p FROM pts")
        self.assertEqual(await cur.fetchall(), [(1, value)])
        user = await self.connect("u")
        with self.assertRaises(nusadb.ProgrammingError):
            await CompositeInfo.fetch_async(user, "pt")


if __name__ == "__main__":
    unittest.main()
