"""SQL composition (``nusadb.sql``): rendering on its own, and hostile names and values run
against a real ``nusadb-server``, blocking and asyncio."""

from __future__ import annotations

import datetime as dt
import io
import sys
import unittest
import uuid
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import nusadb  # noqa: E402
import nusadb.aio  # noqa: E402
from nusadb import sql  # noqa: E402
from nusadb.connection import _needs_simple_query  # noqa: E402
from test_driver import _Server  # noqa: E402

# Text meant to break out of a quoted name or string, or to be read as a marker or a comment.
HOSTILE = [
    "'",
    "''",
    '"',
    '""',
    "\\",
    "\\'",
    "'\\",
    "a'; DROP TABLE victim; --",
    'a"; DROP TABLE victim; --',
    "x' OR '1'='1",
    "$$",
    "$tag$ text $tag$",
    "$1",
    ":1",
    "%s",
    "%(name)s",
    "%%",
    "E'\\''",
    "-- comment",
    "/* open",
    "*/",
    "line\nbreak\r\n",
    "tab\there",
    "é ☃ 漢字 🙂",
    "‮​",
    " ",
    ";",
]


class RenderingTests(unittest.TestCase):
    def test_identifiers(self) -> None:
        self.assertEqual(sql.Identifier("users").as_string(None), '"users"')
        self.assertEqual(sql.Identifier("app", "users").as_string(None), '"app"."users"')
        self.assertEqual(sql.Identifier('a"b').as_string(None), '"a""b"')
        self.assertEqual(sql.Identifier("é ☃").as_string(None), '"é ☃"')
        self.assertEqual(sql.Identifier("a.b").as_string(None), '"a.b"')
        for bad in ["", "a\x00b"]:
            with self.assertRaises(nusadb.ProgrammingError):
                sql.Identifier(bad)
        with self.assertRaises(TypeError):
            sql.Identifier()
        with self.assertRaises(TypeError):
            sql.Identifier(1)  # type: ignore[arg-type]

    def test_literals(self) -> None:
        cases = [
            (None, "NULL"),
            (True, "true"),
            (False, "false"),
            (42, "42"),
            (-42, "(-42)"),
            (0, "0"),
            ("it's", "'it''s'"),
            ("a\\b", "E'a\\\\b'"),
            ("\\'", "E'\\\\'''"),
            ("", "''"),
            (1.5, "'1.5'::FLOAT"),
            (-1.5, "'-1.5'::FLOAT"),
            (Decimal("-1E+3"), "'-1000'::NUMERIC"),
            (2**70, f"'{2**70}'::NUMERIC"),
            (dt.date(2024, 1, 2), "'2024-01-02'::DATE"),
            (dt.datetime(2024, 1, 2, 3, 4, 5), "'2024-01-02 03:04:05'::TIMESTAMP"),
            (dt.timedelta(days=1), "'1 days 00:00:00.000000'::INTERVAL"),
            (uuid.UUID(int=1), "'00000000-0000-0000-0000-000000000001'::UUID"),
            (b"\x01'", "E'\\\\x0127'::BYTEA"),
            ({"a": "'"}, "'{\"a\":\"''\"}'::JSON"),
            ([1, "x'y"], "'{1,x''y}'"),
            (nusadb.Range(1, 5), "'[1,5)'"),
        ]
        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(sql.Literal(value).as_string(None), expected)
        with self.assertRaises(nusadb.ProgrammingError):
            sql.Literal("a\x00b").as_string(None)
        with self.assertRaises(nusadb.ProgrammingError):
            sql.Literal(["a\x00b"]).as_string(None)

    def test_placeholders(self) -> None:
        self.assertEqual(sql.Placeholder().as_string(None), "%s")
        self.assertEqual(sql.Placeholder("key").as_string(None), "%(key)s")
        for bad in ["", "a b", "a)s", "x%"]:
            with self.assertRaises(nusadb.ProgrammingError):
                sql.Placeholder(bad)

    def test_format(self) -> None:
        a, b = sql.Identifier("a"), sql.Literal("b")
        self.assertEqual(sql.SQL("{} = {}").format(a, b).as_string(None), "\"a\" = 'b'")
        self.assertEqual(sql.SQL("{1} = {0}").format(b, a).as_string(None), "\"a\" = 'b'")
        self.assertEqual(
            sql.SQL("{x} = {y} {x}").format(x=a, y=b).as_string(None), '"a" = \'b\' "a"'
        )
        self.assertEqual(sql.SQL("{{}} {}").format(a).as_string(None), '{} "a"')
        self.assertEqual(sql.SQL("no fields").format().as_string(None), "no fields")
        for template, args, kwargs, error in [
            ("{} {0}", (a, a), {}, ValueError),
            ("{0} {}", (a, a), {}, ValueError),
            ("{}", (), {}, IndexError),
            ("{2}", (a,), {}, IndexError),
            ("{x}", (), {}, KeyError),
            ("{!r}", (a,), {}, ValueError),
            ("{:>5}", (a,), {}, ValueError),
            ("{a.b}", (), {"a": a}, ValueError),
            ("{a[0]}", (), {"a": a}, ValueError),
            ("{}", ("raw text",), {}, TypeError),
            ("{}", (5,), {}, TypeError),
            ("{", (), {}, ValueError),
        ]:
            with self.subTest(template=template):
                with self.assertRaises(error):
                    sql.SQL(template).format(*args, **kwargs)
        with self.assertRaises(TypeError):
            sql.SQL(5)  # type: ignore[arg-type]

    def test_join_add_and_repeat(self) -> None:
        names = [sql.Identifier(n) for n in ("a", "b", "c")]
        self.assertEqual(sql.SQL(", ").join(names).as_string(None), '"a", "b", "c"')
        self.assertEqual(sql.SQL(", ").join([]).as_string(None), "")
        self.assertEqual(sql.Composed(names).join(", ").as_string(None), '"a", "b", "c"')
        joined = sql.SQL("SELECT ") + sql.Identifier("a") + sql.SQL(" FROM t")
        self.assertIsInstance(joined, sql.Composed)
        self.assertEqual(joined.as_string(None), 'SELECT "a" FROM t')
        self.assertEqual((sql.Placeholder() * 3).join(", ").as_string(None), "%s, %s, %s")
        self.assertEqual(sql.SQL("x") + sql.SQL("y"), sql.Composed([sql.SQL("x"), sql.SQL("y")]))
        self.assertNotEqual(sql.Literal(1), sql.Literal("1"))
        self.assertEqual(sql.Identifier("a", "b"), sql.Identifier("a", "b"))
        with self.assertRaises(TypeError):
            sql.SQL("x") + "y"  # type: ignore[operator]
        with self.assertRaises(TypeError):
            sql.Composed(["x"])  # type: ignore[list-item]
        with self.assertRaises(TypeError):
            sql.Composed(names).join(5)  # type: ignore[arg-type]

    def test_only_one_statement_goes_through_parse_and_bind(self) -> None:
        self.assertFalse(_needs_simple_query("SELECT 1"))
        self.assertFalse(_needs_simple_query("SELECT ';' -- ; SELECT 2"))
        self.assertFalse(_needs_simple_query("SELECT 1;"))
        self.assertTrue(_needs_simple_query("SELECT 1; SELECT 2"))
        self.assertTrue(_needs_simple_query("  fetch 2 FROM c"))
        self.assertTrue(_needs_simple_query("/* x */ DECLARE c CURSOR FOR SELECT 1"))
        self.assertTrue(_needs_simple_query("COPY t TO STDOUT"))
        self.assertFalse(_needs_simple_query("SELECT 'COPY'"))


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


class ServerTests(_ServerCase):
    def test_hostile_literals_come_back_unchanged(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE victim (id INT)")
        for text in HOSTILE:
            with self.subTest(text=text):
                cur.execute(sql.SQL("SELECT {}, {}").format(sql.Literal(text), sql.Literal([text])))
                self.assertEqual(cur.fetchall(), [(text, "{" + _array_text(text) + "}")])
                # Next to a parameter, so the query also goes through placeholder rewriting.
                cur.execute(
                    sql.SQL("SELECT {}, {}").format(sql.Literal(text), sql.Placeholder()), [7]
                )
                self.assertEqual(cur.fetchall(), [(text, 7)])
        self.assertEqual(cur.execute("SELECT count(*) FROM victim").fetchone(), (0,))

    def test_hostile_identifiers_name_exactly_one_object(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE victim2 (id INT)")
        names = [n for n in HOSTILE if n.strip() and n != "‮​"] + ["Mixed Case", "select"]
        for i, name in enumerate(names):
            with self.subTest(name=name):
                table = sql.Identifier(name)
                column = sql.Identifier(name + "_col")
                cur.execute(sql.SQL("CREATE TABLE {} ({} TEXT)").format(table, column))
                cur.execute(
                    sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        table, column, sql.Literal(name)
                    )
                )
                cur.execute(sql.SQL("SELECT {} FROM {}").format(column, table))
                self.assertEqual(cur.fetchall(), [(name,)])
                cur.execute(sql.SQL("DROP TABLE {}").format(table))
        self.assertEqual(cur.execute("SELECT count(*) FROM victim2").fetchone(), (0,))

    def test_typed_literals_round_trip(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        values = [
            None,
            True,
            0,
            -5,
            2**63 - 1,
            -(2**70),
            1.5,
            -0.25,
            float("inf"),
            Decimal("-1.50"),
            dt.date(2024, 2, 29),
            dt.time(1, 2, 3, 4),
            dt.datetime(2024, 1, 1, 1, 2, 3),
            dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            dt.timedelta(days=-1, seconds=5),
            uuid.UUID(int=7),
            b"\x00\\'\xff",
            {"k": "v'\\"},
            nusadb.Json([1, "x"]),
        ]
        for value in values:
            with self.subTest(value=value):
                cur.execute(sql.SQL("SELECT {}").format(sql.Literal(value)))
                self.assertEqual(
                    cur.fetchone(), (value.value if isinstance(value, nusadb.Json) else value,)
                )
        cur.execute(
            sql.SQL("SELECT {}, {}").format(sql.Literal(Decimal("NaN")), sql.Literal(float("nan")))
        )
        self.assertTrue(all(v != v for v in cur.fetchone()))
        # A negative number cannot run into what precedes it: `10 - -5`, not a `--` comment.
        cur.execute(sql.SQL("SELECT 10 -{}").format(sql.Literal(-5)))
        self.assertEqual(cur.fetchone(), (15,))

    def test_every_entry_point_takes_a_composed_statement(self) -> None:
        conn = self.connect()
        table = sql.Identifier("composed table")
        cur = conn.cursor()
        cur.execute(sql.SQL("CREATE TABLE {} (id INT, v TEXT)").format(table))
        insert = sql.SQL("INSERT INTO {} VALUES ({})").format(
            table, sql.SQL(", ").join([sql.Placeholder("id"), sql.Placeholder("v")])
        )
        cur.executemany(insert, [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
        cur.execute(insert, {"id": 3, "v": "c"})
        select = sql.SQL("SELECT v FROM {} WHERE id = {}").format(table, sql.Placeholder())
        statement = conn.prepare(select)
        self.assertEqual(cur.execute(statement, [2]).fetchall(), [("b",)])
        self.assertIs(conn.prepare_cached(select), conn.prepare_cached(select.as_string(conn)))
        with conn.stream(select, [3]) as rows:
            self.assertEqual(list(rows), [("c",)])
        with conn.pipeline() as p:
            queued = p.execute(select, [1])
        self.assertEqual(queued.fetchall(), [("a",)])
        copy_in = sql.SQL("COPY {} FROM STDIN").format(table)
        self.assertEqual(conn.copy_in(copy_in, io.BytesIO(b"4\td\n")), 1)
        self.assertEqual(conn.copy_in_rows(copy_in, [(5, "e")]), 1)
        copy_out = sql.SQL("COPY {} TO STDOUT").format(table)
        self.assertEqual(len(conn.copy_out_rows(copy_out)), 5)
        sink = io.BytesIO()
        self.assertEqual(conn.copy_out(copy_out, sink), 5)
        conn.autocommit = False
        named = conn.cursor(name="composed")
        named.execute(sql.SQL("SELECT id FROM {} ORDER BY id").format(table))
        self.assertEqual(named.fetchall(), [(1,), (2,), (3,), (4,), (5,)])
        conn.rollback()
        with self.assertRaises(nusadb.ProgrammingError):
            cur.execute(42)  # type: ignore[arg-type]

    def test_a_connection_adapter_applies_to_its_literals(self) -> None:
        class Money:
            def __init__(self, cents: int) -> None:
                self.cents = cents

        conn = self.connect()
        conn._adapters[Money] = lambda m: Decimal(m.cents) / 100
        cur = conn.cursor()
        cur.execute(sql.SQL("SELECT {}").format(sql.Literal(Money(250))))
        self.assertEqual(cur.fetchone(), (Decimal("2.5"),))
        self.assertEqual(sql.Literal(Money(1)).as_string(cur), "'0.01'::NUMERIC")
        self.assertTrue(sql.Literal(Money(1)).as_string(None).startswith("'<"))


def _array_text(text: str) -> str:
    """How the server writes ``text`` as an array element."""
    if text == "" or text.upper() == "NULL" or any(c in text for c in '{},"\\ \t\n\r\v\f'):
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


class AsyncServerTests(unittest.IsolatedAsyncioTestCase):
    server: _Server

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = _Server()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    async def test_composed_statements(self) -> None:
        conn = await nusadb.aio.connect(
            f"nusadb://u@127.0.0.1:{self.server.port}/nusadb", password=None
        )
        self.addAsyncCleanup(conn.close)
        table = sql.Identifier("async; DROP TABLE x; --")
        await conn.execute(sql.SQL("CREATE TABLE {} (id INT, v TEXT)").format(table))
        insert = sql.SQL("INSERT INTO {} VALUES (%s, %s)").format(table)
        await conn.cursor().executemany(insert, [(1, "'"), (2, "\\")])
        for text in HOSTILE:
            cur = await conn.execute(sql.SQL("SELECT {}").format(sql.Literal(text)))
            self.assertEqual(await cur.fetchone(), (text,))
        select = sql.SQL("SELECT v FROM {} WHERE id = {}").format(table, sql.Placeholder())
        statement = await conn.prepare(select)
        cur = conn.cursor()
        await cur.execute(statement, [1])
        self.assertEqual(await cur.fetchall(), [("'",)])
        self.assertIs(await conn.prepare_cached(select), await conn.prepare_cached(select))
        async with conn.stream(select, [2]) as rows:
            self.assertEqual([r async for r in rows], [("\\",)])
        async with conn.pipeline() as p:
            queued = await p.execute(select, [1])
        self.assertEqual(await queued.fetchall(), [("'",)])
        copy_in = sql.SQL("COPY {} FROM STDIN").format(table)
        self.assertEqual(await conn.copy_in(copy_in, b"3\tc\n"), 1)
        self.assertEqual(await conn.copy_in_rows(copy_in, [(4, "d")]), 1)
        copy_out = sql.SQL("COPY {} TO STDOUT").format(table)
        self.assertEqual(len(await conn.copy_out_rows(copy_out)), 4)
        chunks: list = []
        self.assertEqual(await conn.copy_out(copy_out, chunks.append), 4)
        conn.autocommit = False
        named = conn.cursor(name="acomposed")
        await named.execute(sql.SQL("SELECT id FROM {} ORDER BY id").format(table))
        self.assertEqual(await named.fetchall(), [(1,), (2,), (3,), (4,)])
        await conn.rollback()


if __name__ == "__main__":
    unittest.main()
