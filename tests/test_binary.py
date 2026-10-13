"""Binary results (wire-protocol §11.2) and binary COPY (§12.4), blocking and asyncio, against a
real ``nusadb-server``, plus the byte layouts checked without one."""

from __future__ import annotations

import datetime as dt
import ipaddress
import math
import struct
import sys
import unittest
import uuid
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import nusadb  # noqa: E402
import nusadb.aio  # noqa: E402
from nusadb import Range, _binary  # noqa: E402
from nusadb.connection import Cursor, _encode_param  # noqa: E402
from test_driver import _Server  # noqa: E402

UTC = dt.timezone.utc

# A table with a column of every type the server reports, and the rows it holds.
ALL_TYPES = """
    id INT, b BOOL, i BIGINT, s SMALLINT, f FLOAT, r REAL, n NUMERIC(12,3), nn NUMERIC, t TEXT,
    v VARCHAR(20), d DATE, tm TIME, ttz TIMETZ, ts TIMESTAMP, tstz TIMESTAMPTZ, iv INTERVAL,
    u UUID, j JSON, jb JSONB, ai INT[], at TEXT[], ad DATE[], af FLOAT[], ab BOOL[], vec VECTOR(3),
    by BYTEA, mac MACADDR, mac8 MACADDR8, ip INET, net CIDR, bt BIT(5), vb VARBIT,
    r4 INT4RANGE, r8 INT8RANGE, rn NUMRANGE, rd DATERANGE, rts TSRANGE, rtz TSTZRANGE,
    pt POINT, tv TSVECTOR, tq TSQUERY, x XML
"""
ALL_VALUES = [
    """(1, true, 9223372036854775807, -32768, 1.5, 2.25, 12345.678, 0.000000001, 'a "q" \\ b',
        'v', '2024-02-29', '23:59:59.999999', '13:45:30+07', '2024-01-01 10:00:00.5',
        '2024-01-01 10:00:00+02', '1 year 2 mons 3 days 04:05:06.7',
        'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a": [1, 2]}', '{"b": null}', ARRAY[1, NULL, 3],
        ARRAY['x', 'y z', ''], ARRAY['2024-01-01'::DATE], ARRAY[1.5::FLOAT], ARRAY[true, false],
        '[1,2,3]', '\\x00ff10', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05', '10.1.2.3/16',
        '10.0.0.0/8', B'10110', '101100111', '[1,5)', '(,10]', '[1.5,2.25]', '[2024-01-01,)',
        '[2024-01-01 10:00:00.25,2024-01-02 00:00:00)', '(2024-01-01 10:00+00,2024-06-01 00:00+00]',
        '(1.5,2)', 'a fat cat', 'fat & cat', '<a>b</a>')""",
    """(2, false, -1, 0, 'NaN', '-Infinity', -0.001, -12345678901234567890.12, '', 'é ☃',
        '0001-01-01', '00:00:00', '00:00:00-05:30', '1970-01-01 00:00:00', '1999-12-31 23:59:59+00',
        '-3 days -00:00:01.5', '00000000-0000-0000-0000-000000000000', '[]', '"s"', ARRAY[]::INT[],
        ARRAY['{', '}', ',', 'NULL', '\\'], ARRAY[]::DATE[], ARRAY['Infinity'::FLOAT],
        ARRAY[NULL::BOOL], '[0,0,0]', '\\x', '00:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff:ff:ff', '::1',
        '2001:db8::/32', B'00000', '', 'empty', '[-5,-1]', '(,)', 'empty',
        '(,2024-01-01 00:00:00)', '[1999-12-31 23:59:59+00,)', '(-1,-2)', '', '!x', '<x/>')""",
    "(3" + ", NULL" * 41 + ")",
    """(4, NULL, -9223372036854775808, 32767, -0.0, 1e30, 1E+3, 0, 'tab\tnl', NULL, '9999-12-31',
        '12:00:00', '12:00:00+14', '9999-12-31 23:59:59.999999', '0001-01-01 00:00:00+00',
        '0 days', NULL, 'null', '1', ARRAY[[1, 2], [3, 4]], NULL, NULL, NULL, NULL, NULL,
        '\\xdeadbeef', NULL, NULL, '192.168.0.1', '192.168.0.0/24', B'11111', '1', '[0,1)',
        '[100,200)', '[0,0]', '[2024-01-01,2024-01-02)', NULL, NULL, NULL, NULL, NULL, NULL)""",
]

# Expressions whose value the text format cannot turn into a Python object (it stays text), so the
# binary decoder has to give the same text.
TEXT_FALLBACKS = [
    "'0000-06-01'::DATE",
    "'0000-01-01 10:00:00'::TIMESTAMP",
    "'0000-01-01 10:00:00+00'::TIMESTAMPTZ",
    "'1 mon'::INTERVAL",
    "'-14 mons -1 days'::INTERVAL",
    "'1 year -00:00:00.000001'::INTERVAL",
    "'[0000-01-01,0001-01-01)'::DATERANGE",
]


def same(left: object, right: object) -> bool:
    """Equal and of the same type, recursively (NaN equal to NaN, offsets compared)."""
    if type(left) is not type(right):
        return False
    if isinstance(left, float):
        return (math.isnan(left) and math.isnan(right)) or (  # type: ignore[arg-type]
            left == right and math.copysign(1, left) == math.copysign(1, right)  # type: ignore[arg-type]
        )
    if isinstance(left, (list, tuple)):
        return len(left) == len(right) and all(  # type: ignore[arg-type]
            same(a, b) for a, b in zip(left, right)  # type: ignore[call-overload]
        )
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(same(left[k], right[k]) for k in left)  # type: ignore[attr-defined,index]
    if isinstance(left, Range):
        return left == right and all(
            same(getattr(left, a), getattr(right, a)) for a in ("lower", "upper", "bounds", "empty")
        )
    if isinstance(left, (dt.datetime, dt.time)):
        return left == right and left.utcoffset() == right.utcoffset()  # type: ignore[union-attr]
    return left == right


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

    def all_types(self, conn: nusadb.Connection, name: str) -> None:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {name}")
        cur.execute(f"CREATE TABLE {name} ({ALL_TYPES})")
        for values in ALL_VALUES:
            cur.execute(f"INSERT INTO {name} VALUES {values}")

    def frames(self, conn: nusadb.Connection, kind: str) -> list:
        """Record the frames of one kind (``"B"`` for Bind, ...) the connection sends."""
        sent: list = []
        original = conn._send

        def recording(frame: bytes) -> None:
            if frame[:1] == kind.encode():
                sent.append(frame)
            original(frame)

        conn._send = recording  # type: ignore[method-assign]
        return sent


class BinaryResultTests(_ServerCase):
    def test_every_type_decodes_the_same_in_both_formats(self) -> None:
        conn = self.connect()
        self.all_types(conn, "every_type")
        query = "SELECT * FROM every_type ORDER BY id"
        text = conn.cursor().execute(query)
        binary = conn.cursor(binary=True).execute(query)
        self.assertTrue(binary._result_binary)
        self.assertFalse(text._result_binary)
        self.assertEqual(text.description, binary.description)
        text_rows, binary_rows = text.fetchall(), binary.fetchall()
        self.assertEqual(len(text_rows), len(ALL_VALUES))
        for text_row, binary_row in zip(text_rows, binary_rows):
            for column, a, b in zip(text.description, text_row, binary_row):
                self.assertTrue(same(a, b), f"{column[0]} ({column[1]}): {a!r} != {b!r}")
        # The decoded values are typed, not text.
        first = dict(zip([c[0] for c in binary.description], binary_rows[0]))
        self.assertEqual(first["i"], 2**63 - 1)
        self.assertEqual(
            first["ttz"], dt.time(13, 45, 30, tzinfo=dt.timezone(dt.timedelta(hours=7)))
        )
        self.assertEqual(first["ts"], dt.datetime(2024, 1, 1, 10, 0, 0, 500000))
        self.assertEqual(first["ip"], ipaddress.ip_interface("10.1.2.3/16"))
        self.assertEqual(first["vb"], "101100111")
        self.assertEqual(first["rn"], Range(Decimal("1.5"), Decimal("2.25"), "[]"))
        self.assertEqual(first["by"], b"\x00\xff\x10")

    def test_values_python_cannot_hold_come_back_as_the_same_text(self) -> None:
        conn = self.connect()
        for expression in TEXT_FALLBACKS:
            text = conn.cursor().execute(f"SELECT {expression}").fetchone()[0]
            binary = conn.cursor(binary=True).execute(f"SELECT {expression}").fetchone()[0]
            self.assertTrue(same(text, binary), f"{expression}: {text!r} != {binary!r}")
        self.assertEqual(
            conn.cursor(binary=True).execute("SELECT '0000-06-01'::DATE").fetchone()[0],
            "0000-06-01",
        )

    def test_array_columns_keep_their_element_type(self) -> None:
        # Protocol 1.2: an array column's tag carries its element type in binary mode as in text,
        # and the array itself travels as its text.
        conn = self.connect()
        cur = conn.cursor(binary=True)
        cur.execute(
            "SELECT ARRAY[1, 2], ARRAY['2024-01-01'::DATE], ARRAY['10:00'::TIME], ARRAY[1.5::FLOAT]"
        )
        self.assertEqual(
            [c[1] for c in cur.description],
            ["ARRAY:INT", "ARRAY:DATE", "ARRAY:TIME", "ARRAY:FLOAT"],
        )
        self.assertEqual(cur._rows[0][0], b"{1,2}")
        self.assertEqual(cur.fetchone(), ([1, 2], [dt.date(2024, 1, 1)], [dt.time(10, 0)], [1.5]))

    def test_bind_asks_for_binary_only_when_told(self) -> None:
        conn = self.connect()
        binds = self.frames(conn, "B")
        conn.cursor().execute("SELECT $1::INT", [1])
        conn.cursor(binary=True).execute("SELECT $1::INT", [1])
        # One result-format code, 1 (binary), for every column; none (all text) otherwise.
        self.assertTrue(binds[0].endswith(b"\x00\x00"))
        self.assertTrue(binds[1].endswith(b"\x00\x01\x00\x01"))

    def test_where_the_binary_choice_comes_from(self) -> None:
        conn = self.connect()
        self.assertFalse(conn.binary)
        self.assertFalse(conn.cursor().execute("SELECT 1")._result_binary)
        self.assertTrue(conn.cursor().execute("SELECT 1", binary=True)._result_binary)
        self.assertTrue(conn.cursor(binary=True).execute("SELECT $1::INT", [1])._result_binary)
        self.assertFalse(conn.cursor(binary=True).execute("SELECT 1", binary=False)._result_binary)
        default_on = self.connect(binary=True)
        self.assertTrue(default_on.cursor().execute("SELECT 1")._result_binary)
        self.assertFalse(default_on.cursor(binary=False).execute("SELECT 1")._result_binary)
        url = nusadb.connect(
            f"nusadb://u@127.0.0.1:{self.server.port}/nusadb?binary=true", password=None
        )
        self.addCleanup(url.close)
        self.assertTrue(url.binary)
        cur = url.cursor()
        cur.execute("SELECT 2::INT")
        self.assertEqual((cur._result_binary, cur.fetchone()), (True, (2,)))
        with nusadb.Pool(2, port=self.server.port, user="u", password=None, binary=True) as pool:
            with pool.connection() as pooled:
                self.assertTrue(pooled.cursor().execute("SELECT 1")._result_binary)

    def test_prepared_streamed_and_pipelined_results(self) -> None:
        conn = self.connect(binary=True)
        statement = conn.prepare("SELECT $1::DATE + 1")
        cur = conn.cursor()
        cur.execute(statement, [dt.date(2024, 2, 28)])
        self.assertEqual((cur._result_binary, cur.fetchone()), (True, (dt.date(2024, 2, 29),)))
        with conn.stream(
            "SELECT x, x * 1.5 FROM generate_series(1, 5) AS g(x)", batch_rows=2
        ) as rows:
            self.assertEqual(
                [r for r in rows], [(x, Decimal(x) * Decimal("1.5")) for x in range(1, 6)]
            )
        with conn.stream(
            "SELECT $1::TIMESTAMPTZ", [dt.datetime(2024, 1, 1, tzinfo=UTC)], binary=False
        ) as rows:
            self.assertEqual(list(rows), [(dt.datetime(2024, 1, 1, tzinfo=UTC),)])
        with conn.pipeline() as p:
            first = p.execute("SELECT $1::INT", [7])
            second = p.execute("SELECT '[1,3)'::INT4RANGE", binary=False)
        self.assertEqual((first._result_binary, first.fetchall()), (True, [(7,)]))
        self.assertEqual((second._result_binary, second.fetchall()), (False, [(Range(1, 3),)]))

    def test_statements_only_a_simple_query_runs_still_work(self) -> None:
        conn = self.connect(binary=True)
        cur = conn.cursor()
        cur.execute("CREATE TABLE simple_only (id INT)")
        cur.execute("INSERT INTO simple_only VALUES (1), (2)")
        conn.autocommit = False
        cur.execute("DECLARE c CURSOR FOR SELECT id FROM simple_only ORDER BY id")
        cur.execute("FETCH 1 FROM c")
        self.assertEqual((cur._result_binary, cur.fetchall()), (False, [(1,)]))
        cur.execute("CLOSE c")
        cur.execute("SELECT id FROM simple_only WHERE id = 2 -- ; DECLARE")
        self.assertEqual((cur._result_binary, cur.fetchall()), (True, [(2,)]))
        conn.commit()

    def test_a_named_cursor_refuses_binary(self) -> None:
        conn = self.connect(binary=True)
        with self.assertRaises(nusadb.NotSupportedError):
            conn.cursor(name="c", binary=True)
        conn.autocommit = False
        cur = conn.cursor(name="c2")
        cur.execute("SELECT 1::INT")
        self.assertEqual(cur.fetchall(), [(1,)])
        conn.rollback()

    def test_a_malformed_binary_field_raises(self) -> None:
        for type_name, data in [
            ("INT", b"\x00\x01"),
            ("BOOL", b"\x02"),
            ("DATE", b"\x00"),
            ("UUID", b"\x00" * 15),
            ("TIMETZ", b"\x00" * 8),
            ("INTERVAL", b"\x00" * 12),
            ("MACADDR", b"\x00" * 5),
            ("INET", b"\x00\x08\x0a\x00\x00"),
            ("INET", b"\x00\x21\x0a\x00\x00\x01"),
            ("BIT", b"\x09\x00\x00\x00\xff"),
            ("RANGE", b"\x00\x08\x01"),
            ("RANGE", b"\x07\x00"),
            ("RANGE", b"\x00\x01\x00"),
        ]:
            with self.subTest(type_name=type_name, data=data):
                with self.assertRaises(nusadb.InterfaceError):
                    Cursor._decode_binary_cell(data, type_name)


class BinaryLayoutTests(unittest.TestCase):
    """The §11.2 layouts, decoded and encoded without a server."""

    def decode(self, data: bytes, type_name: str) -> object:
        return Cursor._decode_binary_cell(data, type_name)

    def test_big_endian_scalars(self) -> None:
        self.assertEqual(self.decode(struct.pack(">q", -2), "INT"), -2)
        self.assertEqual(self.decode(struct.pack(">d", 0.1), "FLOAT"), 0.1)
        self.assertEqual(self.decode(struct.pack(">i", -1), "DATE"), dt.date(1969, 12, 31))
        self.assertEqual(self.decode(struct.pack(">q", 3_600_000_001), "TIME"), dt.time(1, 0, 0, 1))
        self.assertEqual(
            self.decode(struct.pack(">qi", 0, -3600), "TIMETZ"),
            dt.time(0, tzinfo=dt.timezone(dt.timedelta(hours=1))),
        )
        self.assertEqual(
            self.decode(struct.pack(">q", 1), "TIMESTAMPTZ"),
            dt.datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
        )
        self.assertEqual(
            self.decode(struct.pack(">iiq", 0, 2, -1), "INTERVAL"),
            dt.timedelta(days=2, microseconds=-1),
        )
        self.assertEqual(self.decode(struct.pack(">iiq", 13, 0, 0), "INTERVAL"), "1 year 1 mon")

    def test_the_bit_count_is_little_endian(self) -> None:
        self.assertEqual(self.decode(b"\x09\x00\x00\x00\xb3\x80", "VARBIT"), "101100111")
        self.assertEqual(self.decode(b"\x00\x00\x00\x00", "BIT"), "")
        self.assertEqual(
            _binary.encode_value("101100111", "VARBIT", _encode_param), b"\x09\x00\x00\x00\xb3\x80"
        )

    def test_range_bounds_are_little_endian_in_the_storage_layout(self) -> None:
        def encode(value: Range, kind: str) -> bytes:
            return _binary.encode_value(value, "RANGE:" + kind, _encode_param)

        int_range = bytes([0, 0x08]) + struct.pack("<qq", 1, 5)
        self.assertEqual(encode(Range(1, 5), "INT"), int_range)
        self.assertEqual(self.decode(int_range, "RANGE"), Range(1, 5))
        numeric = bytes([1, 0x18]) + (15).to_bytes(16, "little", signed=True) + b"\x01"
        numeric += (-225).to_bytes(16, "little", signed=True) + b"\x02"
        self.assertEqual(encode(Range(Decimal("1.5"), Decimal("-2.25"), "[]"), "NUMERIC"), numeric)
        self.assertEqual(
            self.decode(numeric, "RANGE"), Range(Decimal("1.5"), Decimal("-2.25"), "[]")
        )
        dates = bytes([2, 0x0C]) + struct.pack("<i", 19723)
        self.assertEqual(encode(Range(dt.date(2024, 1, 1), None), "DATE"), dates)
        self.assertEqual(self.decode(dates, "RANGE"), Range(dt.date(2024, 1, 1), None, "[)"))
        stamps = bytes([4, 0x02]) + struct.pack("<q", 1_000_000)
        self.assertEqual(
            encode(Range(None, dt.datetime(1970, 1, 1, 0, 0, 1, tzinfo=UTC), "()"), "TIMESTAMPTZ"),
            stamps,
        )
        self.assertEqual(encode(Range(empty=True), "TIMESTAMP"), bytes([3, 0x07]))
        self.assertEqual(self.decode(bytes([3, 0x07]), "RANGE"), Range(empty=True))
        # A numeric bound's mantissa is 128 bits: no rounding at 38 digits.
        big = Decimal("1234567890123456789012345678901234567.8")
        self.assertEqual(self.decode(encode(Range(big, None), "NUMERIC"), "RANGE").lower, big)

    def test_network_layout(self) -> None:
        self.assertEqual(
            _binary.encode_value(ipaddress.ip_interface("10.1.2.3/16"), "INET", _encode_param),
            b"\x00\x10\x0a\x01\x02\x03",
        )
        self.assertEqual(
            _binary.encode_value("2001:db8::/32", "CIDR", _encode_param),
            b"\x03\x20" + ipaddress.ip_address("2001:db8::").packed,
        )
        self.assertEqual(
            self.decode(b"\x00\x20\x0a\x00\x00\x01", "INET"), ipaddress.ip_address("10.0.0.1")
        )

    def test_type_names(self) -> None:
        for name, wire in [
            ("bigint", "INT"),
            ("varchar(20)", "TEXT"),
            ("numeric(10, 2)", "NUMERIC"),
            ("double  precision", "FLOAT"),
            ("timestamp with time zone", "TIMESTAMPTZ"),
            ("text[]", "ARRAY:TEXT"),
            ("int[][]", "ARRAY:INT"),
            ("daterange", "RANGE:DATE"),
            ("jsonb", "JSON"),
            ("bytea", "BYTES"),
            ("RANGE:INT", "RANGE:INT"),
        ]:
            self.assertEqual(_binary.wire_type(name), wire, name)
        with self.assertRaises(nusadb.ProgrammingError):
            _binary.wire_type("mood")


def binary_stream(*rows: bytes) -> bytes:
    return b"NUSACOPY\xff\r\n" + struct.pack(">II", 0, 0) + b"".join(rows) + struct.pack(">h", -1)


def binary_row(*fields: object) -> bytes:
    out = struct.pack(">h", len(fields))
    for field in fields:
        out += struct.pack(">i", -1) if field is None else struct.pack(">i", len(field)) + field  # type: ignore[arg-type]
    return out


class BinaryCopyStreamTests(unittest.TestCase):
    """The §12.4 stream, written and read without a server."""

    def write(self, rows: list, types: object = None) -> bytes:
        writer = _binary.CopyWriter(types, lambda v: v, _encode_param)  # type: ignore[arg-type]
        return b"".join(_binary.copy_binary_chunks(rows, writer))

    def read(self, data: bytes, types: object = None, step: int = 0) -> list:
        reader = _binary.CopyReader(types, Cursor._decode_binary_cell)  # type: ignore[arg-type]
        if step:
            for i in range(0, len(data), step):
                reader(data[i : i + step])
        else:
            reader(data)
        return reader.finish()

    def test_the_stream_layout(self) -> None:
        stream = self.write([(1, "é", None)], ["INT", "TEXT", "DATE"])
        expected = binary_stream(binary_row(struct.pack(">q", 1), "é".encode(), None))
        self.assertEqual(stream, expected)
        self.assertEqual(self.write([]), binary_stream())

    def test_reading_back_in_any_chunks(self) -> None:
        rows = [(1, "a", dt.date(2024, 1, 1)), (None, "", None), (-7, "x\ty", dt.date(1, 1, 1))]
        stream = self.write(rows, ["INT", "TEXT", "DATE"])
        for step in (0, 1, 3, 7):
            self.assertEqual(self.read(stream, ["INT", "TEXT", "DATE"], step), rows)
        raw = self.read(stream)
        self.assertEqual(raw[0], (struct.pack(">q", 1), b"a", struct.pack(">i", 19723)))

    def test_a_malformed_stream_is_refused(self) -> None:
        good = binary_stream(binary_row(b"\x00" * 8))
        header = b"NUSACOPY\xff\r\n" + struct.pack(">II", 0, 0)
        for label, data, types in [
            ("signature", b"NUSACOPY\xff\n\n" + good[11:], None),
            ("high bit stripped", b"NUSACOPY\x7f\r\n" + good[11:], None),
            ("a flag", b"NUSACOPY\xff\r\n" + struct.pack(">II", 1, 0) + good[19:], None),
            ("short header", header[:15], None),
            ("no trailer", good[:-2], None),
            ("bytes after the trailer", good + b"\x00", None),
            ("a row cut short", good[:-5], None),
            (
                "rows of different widths",
                binary_stream(binary_row(b"a"), binary_row(b"a", b"b")),
                None,
            ),
            ("width other than the types", good, ["INT", "INT"]),
            ("length below -1", header + struct.pack(">hi", 1, -2) + struct.pack(">h", -1), None),
            ("field count below -1", header + struct.pack(">h", -2), None),
            ("a field of the wrong width", binary_stream(binary_row(b"\x00" * 4)), ["INT"]),
        ]:
            with self.subTest(label):
                with self.assertRaises(nusadb.InterfaceError):
                    self.read(data, types)
        # An extension in the header is skipped.
        extended = b"NUSACOPY\xff\r\n" + struct.pack(">II", 0, 3) + b"abc" + good[19:]
        self.assertEqual(self.read(extended, ["INT"]), [(0,)])

    def test_writing_refuses_what_does_not_fit(self) -> None:
        for label, rows, types in [
            ("str for INT", [("1",)], ["INT"]),
            ("bool for INT", [(True,)], ["INT"]),
            ("int for BOOL", [(1,)], ["BOOL"]),
            ("float for INT", [(1.0,)], ["INT"]),
            ("too big for INT", [(2**63,)], ["INT"]),
            ("datetime for DATE", [(dt.datetime(2024, 1, 1),)], ["DATE"]),
            ("naive for TIMESTAMPTZ", [(dt.datetime(2024, 1, 1),)], ["TIMESTAMPTZ"]),
            ("aware for TIMESTAMP", [(dt.datetime(2024, 1, 1, tzinfo=UTC),)], ["TIMESTAMP"]),
            ("naive for TIMETZ", [(dt.time(1),)], ["TIMETZ"]),
            ("bad bits", [("102",)], ["VARBIT"]),
            ("bad mac", [("08:00",)], ["MACADDR"]),
            ("range kind", [(Range(dt.date(2024, 1, 1), None),)], ["INT4RANGE"]),
            ("unknown type", [(1,)], ["mood"]),
            ("row width", [(1, 2)], ["INT"]),
            ("mixed column", [(1,), (1.5,)], None),
            ("text then json", [("a",), ({"k": 1},)], None),
            ("text then array", [("a",), (["a"],)], None),
            ("mixed widths", [(1,), (1, 2)], None),
            ("an object", [(object(),)], None),
            ("a range of no kind", [(Range(None, None),)], None),
            ("an empty range of no kind", [(Range(empty=True),)], None),
            ("not a row", ["ab"], None),
        ]:
            with self.subTest(label):
                with self.assertRaises(nusadb.ProgrammingError):
                    self.write(rows, types)

    def test_inferred_types(self) -> None:
        rows = [
            (
                True,
                1,
                1.5,
                Decimal("2"),
                "t",
                b"b",
                dt.date(2024, 1, 1),
                dt.datetime(2024, 1, 1),
                dt.datetime(2024, 1, 1, tzinfo=UTC),
                dt.time(1),
                dt.time(1, tzinfo=UTC),
                dt.timedelta(1),
                uuid.UUID(int=5),
                {"a": 1},
                [1, 2],
                nusadb.Vector([1, 2]),
                ipaddress.ip_network("10.0.0.0/8"),
                ipaddress.ip_address("10.0.0.1"),
                Range(1, 2),
            ),
            (None,) * 19,
        ]
        types = [
            "BOOL",
            "INT",
            "FLOAT",
            "NUMERIC",
            "TEXT",
            "BYTES",
            "DATE",
            "TIMESTAMP",
            "TIMESTAMPTZ",
            "TIME",
            "TIMETZ",
            "INTERVAL",
            "UUID",
            "JSON",
            "ARRAY",
            "VECTOR",
            "CIDR",
            "INET",
            "RANGE:INT",
        ]
        self.assertEqual(self.write(rows), self.write(rows, types))


class BinaryCopyTests(_ServerCase):
    TYPES = [
        "INT",
        "BOOL",
        "FLOAT",
        "NUMERIC(12,3)",
        "TEXT",
        "DATE",
        "TIME",
        "TIMETZ",
        "TIMESTAMP",
        "TIMESTAMPTZ",
        "INTERVAL",
        "UUID",
        "JSONB",
        "INT[]",
        "TEXT[]",
        "VECTOR(2)",
        "BYTEA",
        "MACADDR",
        "MACADDR8",
        "INET",
        "CIDR",
        "BIT(3)",
        "VARBIT",
        "INT4RANGE",
        "NUMRANGE",
        "DATERANGE",
        "TSRANGE",
        "TSTZRANGE",
    ]
    ROWS = [
        (
            1,
            True,
            1.25,
            Decimal("12.500"),
            'a\t"b"\n\\',
            dt.date(2024, 2, 29),
            dt.time(23, 59, 59, 999999),
            dt.time(13, 45, 30, tzinfo=dt.timezone(dt.timedelta(hours=-5, minutes=-30))),
            dt.datetime(2024, 1, 1, 10, 0, 0, 500000),
            dt.datetime(2024, 1, 1, 8, tzinfo=UTC),
            dt.timedelta(days=-3, seconds=5, microseconds=7),
            uuid.UUID(int=2**127 + 5),
            {"k": [1, None, "x"]},
            [1, None, 3],
            ["x", "y z", "", "NULL"],
            [1.0, -2.5],
            b"\x00\xff",
            "08:00:2b:01:02:03",
            "08:00:2b:01:02:03:04:05",
            ipaddress.ip_interface("10.1.2.3/16"),
            ipaddress.ip_network("2001:db8::/32"),
            "101",
            "1011001110001",
            Range(1, 5),
            Range(Decimal("-1.5"), Decimal("2.25"), "[]"),
            Range(dt.date(2024, 1, 1), None),
            Range(None, dt.datetime(2024, 1, 1, 10, 0, 0, 250000), "()"),
            Range(dt.datetime(2024, 1, 1, tzinfo=UTC), dt.datetime(2025, 1, 1, tzinfo=UTC)),
        ),
        (
            2,
            False,
            float("-inf"),
            Decimal("-0.001"),
            "é ☃",
            dt.date(1, 1, 1),
            dt.time(0),
            dt.time(0, tzinfo=UTC),
            dt.datetime(1, 1, 1),
            dt.datetime(9999, 12, 31, 23, 59, tzinfo=UTC),
            dt.timedelta(0),
            uuid.UUID(int=0),
            [],
            [],
            [],
            [0.0, 0.0],
            b"",
            "ff:ff:ff:ff:ff:ff",
            "00:00:00:00:00:00:00:00",
            ipaddress.ip_address("::1"),
            ipaddress.ip_network("10.0.0.0/8"),
            "000",
            "",
            Range(empty=True),
            Range(None, None, "()"),
            Range(empty=True),
            Range(empty=True),
            Range(None, None, "()"),
        ),
        (3,) + (None,) * 27,
    ]

    def table(self, conn: nusadb.Connection, name: str) -> None:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {name}")
        columns = ", ".join(f"c{i} {t}" for i, t in enumerate(self.TYPES))
        cur.execute(f"CREATE TABLE {name} ({columns})")

    def test_rows_round_trip_through_a_binary_copy(self) -> None:
        conn = self.connect()
        self.table(conn, "bcopy")
        loaded = conn.copy_in_rows(
            "COPY bcopy FROM STDIN (FORMAT binary)", self.ROWS, binary=True, types=self.TYPES
        )
        self.assertEqual(loaded, len(self.ROWS))
        selected = conn.cursor().execute("SELECT * FROM bcopy ORDER BY c0").fetchall()
        exported = conn.copy_out_rows(
            "COPY (SELECT * FROM bcopy ORDER BY c0) TO STDOUT (FORMAT binary)",
            binary=True,
            types=self.TYPES,
        )
        self.assertEqual(len(exported), len(self.ROWS))
        for expected, got_select, got_copy in zip(self.ROWS, selected, exported):
            for i, (want, a, b) in enumerate(zip(expected, got_select, got_copy)):
                self.assertTrue(same(a, b), f"c{i}: SELECT {a!r} != COPY {b!r}")
                if isinstance(want, (str, bytes)) and self.TYPES[i] in ("INET", "CIDR"):
                    continue
                if isinstance(want, (ipaddress.IPv4Interface, ipaddress.IPv6Interface)):
                    continue
                if isinstance(want, list) and self.TYPES[i].startswith("VECTOR"):
                    want = [float(x) for x in want]
                self.assertEqual(a, want, f"c{i} ({self.TYPES[i]})")

    def test_the_server_reads_what_a_text_copy_reads(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE bcopy_same (id INT, r INT4RANGE, ts TIMESTAMPTZ)")
        rows = [
            (
                1,
                Range(1, 5, "(]"),
                dt.datetime(2024, 6, 1, 12, tzinfo=dt.timezone(dt.timedelta(hours=7))),
            )
        ]
        conn.copy_in_rows("COPY bcopy_same FROM STDIN", rows)
        conn.copy_in_rows(
            "COPY bcopy_same FROM STDIN (FORMAT binary)", [(2,) + rows[0][1:]], binary=True
        )
        values = cur.execute("SELECT r, ts FROM bcopy_same ORDER BY id").fetchall()
        self.assertEqual(values[0], values[1])
        self.assertEqual(values[0][0], Range(2, 6))
        # Bounds out of order are refused as the text [10,1) is.
        with self.assertRaises(nusadb.DatabaseError):
            conn.copy_in_rows(
                "COPY bcopy_same (r) FROM STDIN (FORMAT binary)", [(Range(10, 1),)], binary=True
            )
        self.assertEqual(cur.execute("SELECT count(*) FROM bcopy_same").fetchone(), (2,))

    def test_inferred_column_types(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE bcopy_infer (i INT, t TEXT, d DATE, ts TIMESTAMPTZ, r DATERANGE, j JSON)"
        )
        rows = [
            (
                1,
                "a",
                dt.date(2024, 1, 1),
                dt.datetime(2024, 1, 1, tzinfo=UTC),
                Range(dt.date(2024, 1, 1), None),
                {"a": 1},
            ),
            (None, None, None, None, None, None),
            (
                2,
                "b",
                dt.date(2024, 1, 2),
                dt.datetime(2024, 1, 2, tzinfo=UTC),
                Range(empty=True),
                [1, 2],
            ),
        ]
        with self.assertRaises(nusadb.ProgrammingError):
            # An empty range alone does not say its type, and a list is an array, not JSON.
            conn.copy_in_rows("COPY bcopy_infer FROM STDIN (FORMAT binary)", rows, binary=True)
        rows[2] = rows[2][:4] + (Range(dt.date(2024, 2, 1), dt.date(2024, 3, 1)), {"b": [1, 2]})
        self.assertEqual(
            conn.copy_in_rows("COPY bcopy_infer FROM STDIN (FORMAT binary)", rows, binary=True), 3
        )
        self.assertEqual(
            cur.execute("SELECT * FROM bcopy_infer ORDER BY i NULLS LAST").fetchall()[0], rows[0]
        )

    def test_a_refused_row_loads_nothing_and_leaves_the_connection_usable(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE bcopy_fail (i INT, f FLOAT)")

        def rows():
            yield (1, 1.5)
            yield ("2", 2.5)

        with self.assertRaises(nusadb.ProgrammingError):
            conn.copy_in_rows(
                "COPY bcopy_fail FROM STDIN (FORMAT binary)",
                rows(),
                binary=True,
                types=["INT", "FLOAT"],
            )
        # The server checks the layout too: a FLOAT field must be eight bytes, not a DATE's four.
        with self.assertRaises(nusadb.DatabaseError) as raised:
            conn.copy_in_rows(
                "COPY bcopy_fail FROM STDIN (FORMAT binary)",
                [(1, dt.date(2024, 1, 1))],
                binary=True,
                types=["INT", "DATE"],
            )
        self.assertEqual(raised.exception.sqlstate, "22P04")
        self.assertEqual(cur.execute("SELECT count(*) FROM bcopy_fail").fetchone(), (0,))
        # A float for a FLOAT column is fine when the type is named, even from an int.
        conn.copy_in_rows(
            "COPY bcopy_fail FROM STDIN (FORMAT binary)",
            [(1, 2)],
            binary=True,
            types=["INT", "FLOAT"],
        )
        self.assertEqual(cur.execute("SELECT f FROM bcopy_fail").fetchall(), [(2.0,)])

    def test_the_format_must_match(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("CREATE TABLE bcopy_fmt (i INT)")
        with self.assertRaises(nusadb.ProgrammingError):
            conn.copy_in_rows("COPY bcopy_fmt FROM STDIN", [(1,)], binary=True)
        with self.assertRaises(nusadb.ProgrammingError):
            conn.copy_in_rows("COPY bcopy_fmt FROM STDIN (FORMAT binary)", [(1,)])
        with self.assertRaises(nusadb.ProgrammingError):
            conn.copy_out_rows("COPY bcopy_fmt TO STDOUT", binary=True)
        with self.assertRaises(nusadb.ProgrammingError):
            conn.copy_out_rows("COPY bcopy_fmt TO STDOUT (FORMAT binary)")
        with self.assertRaises(nusadb.ProgrammingError):
            conn.copy_in_rows("COPY bcopy_fmt FROM STDIN", [(1,)], types=["INT"])
        self.assertEqual(cur.execute("SELECT count(*) FROM bcopy_fmt").fetchone(), (0,))
        # The text format is as before.
        self.assertEqual(conn.copy_in_rows("COPY bcopy_fmt FROM STDIN", [(5,)]), 1)
        self.assertEqual(conn.copy_out_rows("COPY bcopy_fmt TO STDOUT"), [("5",)])
        raw = conn.copy_out_rows("COPY bcopy_fmt TO STDOUT (FORMAT binary)", binary=True)
        self.assertEqual(raw, [(struct.pack(">q", 5),)])


class _AsyncCase(unittest.IsolatedAsyncioTestCase):
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


class AsyncBinaryTests(_AsyncCase):
    async def test_every_type_decodes_the_same_in_both_formats(self) -> None:
        conn = await self.connect()
        await conn.execute(f"CREATE TABLE every_type ({ALL_TYPES})")
        for values in ALL_VALUES:
            await conn.execute(f"INSERT INTO every_type VALUES {values}")
        text = await conn.execute("SELECT * FROM every_type ORDER BY id")
        binary = await conn.execute("SELECT * FROM every_type ORDER BY id", binary=True)
        self.assertTrue(binary._rows._result_binary)
        for text_row, binary_row in zip(await text.fetchall(), await binary.fetchall()):
            for column, a, b in zip(text.description, text_row, binary_row):
                self.assertTrue(same(a, b), f"{column[0]}: {a!r} != {b!r}")

    async def test_options_streams_pipelines_and_prepared(self) -> None:
        conn = await self.connect("?binary=true")
        self.assertTrue(conn.binary)
        cur = conn.cursor()
        await cur.execute("SELECT $1::DATE", [dt.date(2024, 1, 1)])
        self.assertEqual(
            (cur._rows._result_binary, await cur.fetchone()), (True, (dt.date(2024, 1, 1),))
        )
        await cur.execute("SELECT 1", binary=False)
        self.assertFalse(cur._rows._result_binary)
        statement = await conn.prepare("SELECT $1::INT * 2")
        await cur.execute(statement, [21])
        self.assertEqual((cur._rows._result_binary, await cur.fetchone()), (True, (42,)))
        async with conn.stream("SELECT x FROM generate_series(1, 3) AS g(x)", batch_rows=2) as rows:
            self.assertEqual([r async for r in rows], [(1,), (2,), (3,)])
            self.assertTrue(rows._cursor._rows._result_binary)
        async with conn.pipeline() as p:
            first = await p.execute("SELECT '[1,2)'::INT4RANGE")
            second = await p.execute("SELECT 2", binary=False)
        self.assertEqual(await first.fetchall(), [(Range(1, 2),)])
        self.assertTrue(first._rows._result_binary)
        self.assertFalse(second._rows._result_binary)
        with self.assertRaises(nusadb.NotSupportedError):
            conn.cursor(name="c", binary=True)
        plain = await self.connect()
        self.assertFalse((await plain.execute("SELECT 1"))._rows._result_binary)
        self.assertTrue(plain.cursor(binary=True).binary)
        async with nusadb.aio.AsyncPool(
            1, f"nusadb://u@127.0.0.1:{self.server.port}/nusadb", password=None, binary=True
        ) as pool:
            async with pool.connection() as pooled:
                self.assertTrue((await pooled.execute("SELECT 1"))._rows._result_binary)

    async def test_binary_copy(self) -> None:
        conn = await self.connect()
        await conn.execute("CREATE TABLE abcopy (i INT, d DATE, r INT8RANGE, t TEXT)")

        async def rows():
            for i in range(2500):
                yield (i, dt.date(2024, 1, 1) + dt.timedelta(days=i), Range(i, None), f"row {i}")

        self.assertEqual(
            await conn.copy_in_rows("COPY abcopy FROM STDIN (FORMAT binary)", rows(), binary=True),
            2500,
        )
        self.assertEqual(
            await conn.copy_in_rows(
                "COPY abcopy FROM STDIN (FORMAT binary)",
                [(None, None, None, "x")],
                binary=True,
                types=["int", "date", "int8range", "text"],
            ),
            1,
        )
        exported = await conn.copy_out_rows(
            "COPY (SELECT * FROM abcopy ORDER BY i NULLS LAST) TO STDOUT (FORMAT binary)",
            binary=True,
            types=["int", "date", "int8range", "text"],
        )
        self.assertEqual(len(exported), 2501)
        self.assertEqual(
            exported[2499], (2499, dt.date(2030, 11, 4), Range(2499, None), "row 2499")
        )
        self.assertEqual(exported[2500], (None, None, None, "x"))

        async def mixed():
            yield (1, None, None, "a")
            for _ in range(1500):
                yield (1, None, None, "a")
            yield ("one", None, None, "a")

        with self.assertRaises(nusadb.ProgrammingError):
            await conn.copy_in_rows("COPY abcopy FROM STDIN (FORMAT binary)", mixed(), binary=True)
        with self.assertRaises(nusadb.ProgrammingError):
            await conn.copy_in_rows("COPY abcopy FROM STDIN", [(1, None, None, "a")], binary=True)
        with self.assertRaises(nusadb.ProgrammingError):
            await conn.copy_out_rows("COPY abcopy TO STDOUT (FORMAT binary)")
        cur = await conn.execute("SELECT count(*) FROM abcopy")
        self.assertEqual(await cur.fetchone(), (2501,))


if __name__ == "__main__":
    unittest.main()
