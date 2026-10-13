"""PEP-249 ``Connection`` and ``Cursor`` over the Nusa Wire Protocol."""

from __future__ import annotations

import contextlib
import contextvars
import datetime as _dt
import ipaddress as _ipaddress
import itertools
import json as _json
import math
import re as _re
import base64
import select
import socket
import ssl as ssl_module
import struct
import time
import uuid as _uuid
import warnings
from decimal import Decimal, InvalidOperation
import weakref
from collections import OrderedDict
from typing import (
    Any,
    BinaryIO,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from . import _binary
from . import _protocol as proto
from . import _scram
from . import types as _types
from .exceptions import (
    DatabaseError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    TransactionRolledBack,
    from_sqlstate,
)

_SCRAM_MECHANISM = "SCRAM-SHA-256"

# A collected statement result: column names, rows (each a list of optional bytes), command tag.
_Result = Tuple[
    List[str], List[Optional[str]], List[List[Optional[bytes]]], Optional[str]
]


def _sql_spans(sql: str) -> List[Tuple[bool, str]]:
    """Split ``sql`` into ``(is_code, text)`` spans: code, and the parts where a placeholder is never
    looked for (a single-quoted string, a double-quoted identifier, a dollar-quoted string, a line or
    block comment). Joining the texts gives ``sql`` back."""
    spans: List[Tuple[bool, str]] = []
    start, i, n = 0, 0, len(sql)

    def cut(code_end: int, quoted_end: int) -> None:
        nonlocal start
        if code_end > start:
            spans.append((True, sql[start:code_end]))
        spans.append((False, sql[code_end:quoted_end]))
        start = quoted_end

    while i < n:
        c = sql[i]
        if c == "'" or c == '"':  # quoted string / identifier ('' and "" escape the quote)
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            cut(i, j)
            i = j
            continue
        if c == "-" and sql.startswith("--", i):  # line comment
            j = sql.find("\n", i)
            j = n if j == -1 else j
            cut(i, j)
            i = j
            continue
        if c == "/" and sql.startswith("/*", i):  # block comment (the server nests)
            depth, j = 1, i + 2
            while j < n and depth > 0:
                if sql.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif sql.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            cut(i, j)
            i = j
            continue
        if c == "$":  # dollar-quoted string $tag$...$tag$ (a native $1 marker stays code)
            j = i + 1
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            if j < n and sql[j] == "$" and not sql[i + 1 : j].isdigit():
                tag = sql[i : j + 1]
                end = sql.find(tag, j + 1)
                close = n if end == -1 else end + len(tag)
                cut(i, close)
                i = close
                continue
        i += 1
    if start < n:
        spans.append((True, sql[start:]))
    return spans


# A placeholder in code: a native `$1`, a numeric `:1` (not a `::` cast), a `%%` escape, a `%s`, or a
# `%(name)s`.
_PLACEHOLDER = _re.compile(r"::|\$(\d+)|:(\d+)|%%|%s|%\((\w+)\)s")


class _Compiled:
    """SQL with its placeholders rewritten to the native ``$1`` form, and how parameters map onto
    them: by position, or by name (``names[i]`` is the parameter for ``$i+1``)."""

    __slots__ = ("sql", "names", "count")

    def __init__(self, sql: str, names: Optional[List[str]], count: Optional[int]) -> None:
        self.sql = sql
        self.names = names
        # The number of `%s` markers, checked against the parameters; None for native markers.
        self.count = count


def _compile(sql: str) -> _Compiled:
    """Rewrite the placeholders of ``sql`` to the server-native ``$1`` form.

    Three spellings are accepted. The native ``$1`` and the DB-API ``numeric`` ``:1`` (rewritten to
    ``$1``) take parameters by position. When neither appears, the ``pyformat`` markers do: ``%s``
    by position or ``%(name)s`` by name from a mapping (a name may repeat), with ``%%`` for a literal
    ``%``. A ``%`` in a query that uses ``$1`` or ``:1`` is left alone, so a modulo there needs no
    escaping. Placeholders are only recognised in code, never inside a string, an identifier, a
    dollar-quoted string or a comment.
    """
    spans = _sql_spans(sql)
    native = any(
        m.group(1) is not None or m.group(2) is not None
        for is_code, text in spans
        if is_code
        for m in _PLACEHOLDER.finditer(text)
    )
    names: List[str] = []
    positional = 0
    named = False

    def rewrite(m: "_re.Match[str]") -> str:
        nonlocal positional, named
        token = m.group(0)
        if token == "::" or m.group(1) is not None:
            return token
        if m.group(2) is not None:
            return "$" + m.group(2)
        if native:
            return token
        if token == "%%":
            return "%"
        if token == "%s":
            positional += 1
            return f"${positional}"
        named = True
        name = m.group(3)
        if name not in names:
            names.append(name)
        return f"${names.index(name) + 1}"

    out = "".join(_PLACEHOLDER.sub(rewrite, text) if is_code else text for is_code, text in spans)
    if positional and named:
        raise ProgrammingError("a query uses either %s or %(name)s placeholders, not both")
    if named:
        return _Compiled(out, names, None)
    return _Compiled(out, None, positional if positional else None)


def _sql_text(operation: Any, context: Any = None) -> str:
    """The SQL text of ``operation``: a ``str`` as it is, or a composed statement from
    :mod:`nusadb.sql` rendered for ``context`` (the connection)."""
    if isinstance(operation, str):
        return operation
    as_string = getattr(operation, "as_string", None)
    if callable(as_string):
        return as_string(context)
    raise ProgrammingError(
        f"a statement is SQL text or a nusadb.sql object, not {type(operation).__name__}"
    )


# Statements the server runs only as a simple query (not through Parse and Bind): cursor commands
# and COPY.
_SIMPLE_ONLY = frozenset({"DECLARE", "FETCH", "MOVE", "CLOSE", "COPY"})


def _needs_simple_query(sql: str) -> bool:
    """Whether ``sql`` must go as a simple query even when a binary result is asked for: a cursor
    command, a ``COPY``, or more than one statement."""
    code = "".join(text if is_code else " " for is_code, text in _sql_spans(sql))
    if sum(1 for part in code.split(";") if part.strip()) > 1:
        return True
    words = code.split(None, 1)
    return bool(words) and words[0].upper() in _SIMPLE_ONLY


def _to_native_placeholders(sql: str) -> str:
    """``sql`` with its placeholders in the native ``$1`` form (see :func:`_compile`)."""
    return _compile(sql).sql


def _arrange(compiled: _Compiled, parameters: Any) -> Sequence[Any]:
    """The parameters of one execution in ``$1``, ``$2``, ... order: taken by name from a mapping
    for ``%(name)s`` placeholders, or as given."""
    if compiled.names is not None:
        if not isinstance(parameters, Mapping):
            raise ProgrammingError("%(name)s placeholders take a mapping of parameters")
        try:
            return [parameters[name] for name in compiled.names]
        except KeyError as missing:
            raise ProgrammingError(f"no parameter named {missing.args[0]!r}") from None
    if isinstance(parameters, Mapping):
        if not parameters:
            return ()
        raise ProgrammingError("a mapping of parameters needs %(name)s placeholders")
    if compiled.count is not None and len(parameters) != compiled.count:
        raise ProgrammingError(
            f"the query has {compiled.count} %s placeholder(s) but {len(parameters)} "
            "parameter(s) were given"
        )
    return parameters


# COPY text format (wire-protocol §12): tab between fields, `\N` for NULL, one row per line, and a
# backslash before a backslash, tab, newline or carriage return inside a field.
_COPY_ESCAPES = str.maketrans({"\\": "\\\\", "\t": "\\t", "\n": "\\n", "\r": "\\r"})
_COPY_UNESCAPES = {"t": "\t", "n": "\n", "r": "\r"}
_COPY_CHUNK = 65536


def _copy_line(row: Sequence[Any]) -> str:
    """One row as a COPY text-format line: each value written as it binds as a parameter."""
    if isinstance(row, (str, bytes)) or not isinstance(row, Sequence):
        raise ProgrammingError(f"a COPY row must be a sequence of values, not {type(row).__name__}")
    fields = []
    for value in row:
        encoded = _encode_param(_adapt(value))
        fields.append("\\N" if encoded is None else encoded.decode("utf-8").translate(_COPY_ESCAPES))
    return "\t".join(fields) + "\n"


def _copy_text_chunks(rows: Iterable[Sequence[Any]]) -> Iterator[bytes]:
    """``rows`` as COPY text-format data, in chunks of about 64 KiB."""
    pending: List[str] = []
    size = 0
    for row in rows:
        line = _copy_line(row)
        pending.append(line)
        size += len(line)
        if size >= _COPY_CHUNK:
            yield "".join(pending).encode("utf-8")
            pending, size = [], 0
    if pending:
        yield "".join(pending).encode("utf-8")


def _parse_copy_line(line: str) -> Tuple[Optional[str], ...]:
    """A COPY text-format line as its fields: text with escapes decoded, ``None`` for NULL."""
    fields: List[Optional[str]] = []
    current: List[str] = []
    raw = ""
    i = 0
    while i < len(line):
        c = line[i]
        if c == "\\" and i + 1 < len(line):
            escaped = line[i + 1]
            current.append(_COPY_UNESCAPES.get(escaped, escaped))
            raw += c + escaped
            i += 2
            continue
        if c == "\t":
            fields.append(None if raw == "\\N" else "".join(current))
            current, raw = [], ""
        else:
            current.append(c)
            raw += c
        i += 1
    fields.append(None if raw == "\\N" else "".join(current))
    return tuple(fields)


class _CopyLines:
    """Collects the ``CopyData`` chunks of a ``COPY ... TO STDOUT`` and splits them into rows; a
    row may be cut across two chunks."""

    def __init__(self) -> None:
        self.rows: List[Tuple[Optional[str], ...]] = []
        self._tail = b""

    def __call__(self, chunk: bytes) -> None:
        data = self._tail + chunk
        *lines, self._tail = data.split(b"\n")
        self.rows.extend(_parse_copy_line(line.decode("utf-8")) for line in lines)

    def finish(self) -> List[Tuple[Optional[str], ...]]:
        if self._tail:
            self.rows.append(_parse_copy_line(self._tail.decode("utf-8")))
            self._tail = b""
        return self.rows


class _SinkWriter:
    """A ``write(bytes)`` file-like that hands each chunk to ``sink``."""

    def __init__(self, sink: Callable[[bytes], None]) -> None:
        self.write = sink


class _ChunkReader:
    """A ``read(size)`` file-like over an iterator of byte chunks, for :meth:`Connection.copy_in`."""

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = chunks

    def read(self, _size: int = -1) -> bytes:
        return next(self._chunks, b"")


class Json:
    """Bind ``value`` as ``JSON`` text. A ``dict`` binds as JSON on its own; wrap a ``list`` (which
    otherwise binds as an array) or any other JSON-serializable value in ``Json`` to send it as
    JSON. ``dumps`` serializes it (``json.dumps`` by default)."""

    __slots__ = ("value", "dumps")

    def __init__(self, value: Any, dumps: Any = None) -> None:
        self.value = value
        self.dumps = dumps or (lambda v: _json.dumps(v, separators=(",", ":")))

    def __repr__(self) -> str:
        return f"Json({self.value!r})"


_I64_MIN, _I64_MAX = -(2**63), 2**63 - 1

# User-registered conversions: Python type -> callable turning a value into one the driver binds
# natively, and column type name -> callable decoding the column's text.
_ADAPTERS: Dict[type, Callable[[Any], Any]] = {}
_CUSTOM_DECODERS: Dict[str, Callable[[str], Any]] = {}

# The adapters of the connection whose parameters are being bound (see `_scoped_adapters`), which
# take precedence over the global ones.
_SCOPED_ADAPTERS: "contextvars.ContextVar[Optional[Dict[type, Callable[[Any], Any]]]]" = (
    contextvars.ContextVar("nusadb_adapters", default=None)
)


@contextlib.contextmanager
def _scoped_adapters(adapters: Dict[type, Callable[[Any], Any]]) -> Iterator[None]:
    """Apply a connection's own adapters to the values bound inside the block."""
    if not adapters:
        yield
        return
    token = _SCOPED_ADAPTERS.set(adapters)
    try:
        yield
    finally:
        _SCOPED_ADAPTERS.reset(token)


def register_adapter(cls: type, adapt: Callable[[Any], Any]) -> None:
    """Bind values of ``cls`` (and its subclasses) as ``adapt(value)``, which must return a value
    the driver binds natively (``str``, ``int``, ``Decimal``, ``datetime``, a list, ...). An
    adapter for a natively supported type replaces the built-in conversion."""
    _ADAPTERS[cls] = adapt


def register_decoder(type_name: str, decode: Callable[[str], Any]) -> None:
    """Decode a column whose type is ``type_name`` (as it appears in ``cursor.description``, such
    as ``NUMERIC`` or ``ARRAY:INT``; an element type also applies inside arrays) with
    ``decode(text)`` instead of the built-in conversion."""
    _CUSTOM_DECODERS[type_name.upper()] = decode


def _adapt(value: Any) -> Any:
    if value is None:
        return value
    for adapters in (_SCOPED_ADAPTERS.get(), _ADAPTERS):
        if adapters:
            for klass in type(value).__mro__:
                adapt = adapters.get(klass)
                if adapt is not None:
                    return adapt(value)
    return value


def _bound_chunks(
    seq_of_parameters: Any,
    adapters: Optional[Dict[type, Callable[[Any], Any]]] = None,
) -> Iterator[Tuple[List[Tuple[List[Optional[bytes]], bytes]], Optional[Exception]]]:
    """The parameter sets of an ``executemany``, bound in chunks of :data:`_BATCH_ROWS`. A chunk is
    bound whole before any of it is sent, so a value that cannot be bound (or an iterable that
    fails) never leaves part of a pipeline unanswered on the connection; the rows bound before it
    are yielded with the error, to run and then raise it."""
    rows = iter(seq_of_parameters)
    while True:
        chunk: List[Tuple[List[Optional[bytes]], bytes]] = []
        failure: Optional[Exception] = None
        try:
            with _scoped_adapters(adapters or {}):
                for params in rows:
                    chunk.append(_bind(params))
                    if len(chunk) == _BATCH_ROWS:
                        break
        except Exception as exc:  # noqa: BLE001 - reported after the rows before it have run
            failure = exc
        yield chunk, failure
        if failure is not None or len(chunk) < _BATCH_ROWS:
            return


def _bind(params: Any) -> Tuple[List[Optional[bytes]], bytes]:
    """The wire text and type tag of each parameter, read once (a generator is spent by one pass)."""
    values = [_adapt(p) for p in params]
    return [_encode_param(v) for v in values], bytes(_param_tag(v) for v in values)


_IP_ADDRESS_TYPES = (
    _ipaddress.IPv4Address,
    _ipaddress.IPv6Address,
    _ipaddress.IPv4Interface,
    _ipaddress.IPv6Interface,
)


def _param_tag(value: Any) -> int:
    """The type a parameter declares in ``Parse`` (wire-protocol §9.2), so the server binds it as
    exactly that type: a ``str`` stays text even when it looks like a number. ``0`` (infer) for
    ``None`` and for types the driver has no tag for."""
    tag = proto.TAG_BYTES
    if value is None:
        return 0
    if isinstance(value, bool):
        return tag["BOOL"]
    if isinstance(value, int):
        return tag["INT"] if _I64_MIN <= value <= _I64_MAX else tag["NUMERIC"]
    if isinstance(value, float):
        return tag["FLOAT"]
    if isinstance(value, Decimal):
        return tag["NUMERIC"]
    if isinstance(value, str):
        return tag["TEXT"]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return tag["BYTES"]
    # datetime is a subclass of date: test it first.
    if isinstance(value, _dt.datetime):
        return tag["TIMESTAMPTZ"] if value.utcoffset() is not None else tag["TIMESTAMP"]
    if isinstance(value, _dt.date):
        return tag["DATE"]
    if isinstance(value, _dt.time):
        return tag["TIMETZ"] if value.utcoffset() is not None else tag["TIME"]
    if isinstance(value, _dt.timedelta):
        return tag["INTERVAL"]
    if isinstance(value, _uuid.UUID):
        return tag["UUID"]
    if isinstance(value, (dict, Json)):
        return tag["JSON"]
    if isinstance(value, (list, tuple)):
        return tag["ARRAY"]
    if isinstance(value, (_ipaddress.IPv4Network, _ipaddress.IPv6Network)):
        return tag["CIDR"]
    if isinstance(value, _IP_ADDRESS_TYPES):
        return tag["INET"]
    if isinstance(value, _types.Vector):
        return tag["VECTOR"]
    # A Range binds as its text, which the server types from where it is used (a range's tag does
    # not carry its subtype).
    return 0


def _encode_interval(value: _dt.timedelta) -> str:
    """A ``timedelta`` as interval text: ``N days HH:MM:SS.ffffff``. Python keeps the seconds
    positive and the days signed (``-1 days 23:59:55`` is minus five seconds), which the interval
    form reads the same way."""
    hours, rest = divmod(value.seconds, 3600)
    minutes, seconds = divmod(rest, 60)
    return f"{value.days} days {hours:02d}:{minutes:02d}:{seconds:02d}.{value.microseconds:06d}"


def _encode_param(value: Any) -> Optional[bytes]:
    """Encode one bound parameter as wire **text** bytes (``None`` = SQL NULL)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return b"true" if value else b"false"
    # Through the base type, so a subclass (an ``IntEnum`` member, say) binds its number rather than
    # its ``str()`` name.
    if isinstance(value, int):
        return int.__repr__(value).encode("ascii")
    if isinstance(value, float):
        return float.__repr__(value).encode("ascii")
    if isinstance(value, Decimal):
        # Positional notation: ``Decimal("1E+3")`` is ``1000``, which the server reads as NUMERIC.
        text = format(value, "f") if value.is_finite() else str(value)
        return text.encode("ascii")
    if isinstance(value, Json):
        return value.dumps(value.value).encode("utf-8")
    if isinstance(value, _dt.timedelta):
        return _encode_interval(value).encode("ascii")
    if isinstance(value, _dt.datetime):
        # ISO form with a space; an aware value carries its offset, which the server keeps exact.
        return value.isoformat(sep=" ").encode("ascii")
    if isinstance(value, (bytes, bytearray, memoryview)):
        # Bind raw bytes as the BYTEA text form \x<hex>, which the server coerces into a BYTEA column.
        return b"\\x" + bytes(value).hex().encode("ascii")
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, (list, tuple)):
        # Bind a Python sequence as a SQL array literal (``{a,b,c}``, nested and NULL-aware), the
        # text form the server coerces into an array column. Round-trips with ``_parse_array``.
        return _encode_array(value).encode("utf-8")
    if isinstance(value, dict):
        # Bind a mapping as JSON text so it coerces into a JSON / JSONB column, rather than the
        # Python ``repr`` a bare ``str()`` would produce (which the server rejects as invalid JSON).
        return _json.dumps(value, separators=(",", ":")).encode("utf-8")
    if isinstance(value, (set, frozenset)) or hasattr(value, "__next__"):
        # A set has no order to bind as an array, and an iterator would be bound as its ``repr``.
        raise ProgrammingError(
            f"cannot bind a {type(value).__name__} as a parameter; pass a list or tuple"
        )
    return str(value).encode("utf-8")


def _array_element(value: Any) -> str:
    """Render one array element for a ``{...}`` literal, quoting only when the token would
    otherwise be ambiguous (empty, ``NULL``, or containing a delimiter / quote / backslash /
    whitespace). Nested sequences recurse."""
    value = _adapt(value)
    if value is None:
        return "NULL"
    if isinstance(value, (list, tuple)):
        return _encode_array(value)
    if isinstance(value, str):
        token = value
    else:
        # The same text a scalar parameter of this type binds as (a mapping as JSON, a Decimal in
        # positional notation, an aware datetime with its offset, bytes as ``\\x`` hex).
        encoded = _encode_param(value)
        token = encoded.decode("utf-8") if encoded is not None else "NULL"
    if token == "" or token.upper() == "NULL" or any(c in token for c in '{},"\\ \t\n\r\v\f'):
        return '"' + token.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return token


def _encode_array(seq: Any) -> str:
    """Render a Python sequence as a SQL array literal ``{elem,elem,...}``."""
    return "{" + ",".join(_array_element(v) for v in seq) + "}"


def _decode_bool(text: str) -> bool:
    if text in ("true", "t", "1"):
        return True
    if text in ("false", "f", "0"):
        return False
    raise ValueError(text)


def _quote_identifier(name: str) -> str:
    """Quote an identifier (savepoint or LISTEN/NOTIFY channel) so an arbitrary name is emitted
    safely. The server folds a quoted identifier to its literal value, so the same quoting matches
    everywhere the name is used."""
    if not isinstance(name, str):
        raise ProgrammingError("identifier must be a string")
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(text: str) -> str:
    """Quote a string as a SQL literal (``'…'``, doubling ``'``), used for a NOTIFY payload."""
    return "'" + text.replace("'", "''") + "'"


def _decode_notification(r: "proto.Reader") -> "Notification":
    """Decode a ``NotificationResponse`` body: ``[pid:u32][channel:str][payload:str]``."""
    return Notification(pid=r.u32(), channel=r.string(), payload=r.string())


def _pad_tz_offset(text: str) -> str:
    """Pad a bare ``+HH`` / ``-HH`` timezone offset to ``+HH:00`` so ``fromisoformat`` (Python < 3.11)
    can parse it; any other form is left untouched."""
    return _re.sub(r"([+-]\d{2})$", r"\g<1>:00", text)


_FRACTION_RE = _re.compile(r"\.(\d{1,6})(?=[+-]|$)")


def _pad_fraction(text: str) -> str:
    """Pad fractional seconds to six digits (``05.25`` to ``05.250000``): ``fromisoformat`` before
    Python 3.11 reads only three or six."""
    return _FRACTION_RE.sub(lambda m: "." + m.group(1).ljust(6, "0"), text)


def _decode_time(text: str) -> _dt.time:
    return _dt.time.fromisoformat(_pad_fraction(_pad_tz_offset(text)))


def _decode_timestamp(text: str) -> _dt.datetime:
    return _dt.datetime.fromisoformat(_pad_fraction(_pad_tz_offset(text)))


# A NusaDB interval renders as ``[N years] [N mons] [N days] [±HH:MM:SS[.ffffff]]`` (any part may be
# absent; zero is ``00:00:00``).
_INTERVAL_RE = _re.compile(
    r"^\s*"
    r"(?:(?P<years>-?\d+)\s+years?\s*)?"
    r"(?:(?P<mons>-?\d+)\s+mons?\s*)?"
    r"(?:(?P<days>-?\d+)\s+days?\s*)?"
    r"(?:(?P<csign>-)?(?P<h>\d+):(?P<mi>\d+):(?P<sec>\d+)(?:\.(?P<frac>\d+))?)?"
    r"\s*$"
)


def _decode_interval(text: str):
    """Parse a NusaDB interval string into a :class:`datetime.timedelta`. An interval carrying years
    or months is returned unchanged as ``str``: a month is not an exact number of days, so coercing
    it to a timedelta would be silently wrong; only an exactly day/time interval becomes a timedelta.
    A string that does not parse is returned unchanged."""
    m = _INTERVAL_RE.match(text)
    if m is None:
        return text
    if m.group("years") or m.group("mons"):
        return text
    if m.group("days") is None and m.group("h") is None:
        return text  # nothing recognisable matched
    days = int(m.group("days") or 0)
    micros = 0
    if m.group("h") is not None:
        sign = -1 if m.group("csign") else 1
        magnitude = (int(m.group("h")) * 3600 + int(m.group("mi")) * 60 + int(m.group("sec"))) * 1_000_000
        frac = m.group("frac")
        if frac:
            magnitude += int((frac + "000000")[:6])
        micros = sign * magnitude
    return _dt.timedelta(days=days, microseconds=micros)


def _parse_array(text: str) -> list:
    """Parse a SQL array literal (``{a,b,c}``, possibly nested, quoted, or with ``NULL``) into
    a Python ``list``. Elements stay ``str`` (the wire array tag carries no per-element type): a quoted
    element is unescaped, and an unquoted ``NULL`` becomes ``None``."""
    pos = 0

    def parse_list() -> list:
        nonlocal pos
        if text[pos] != "{":
            raise ValueError(text)
        pos += 1
        items: list = []
        if text[pos] == "}":
            pos += 1
            return items
        while True:
            ch = text[pos]
            if ch == "{":
                items.append(parse_list())
            elif ch == '"':
                items.append(parse_quoted())
            else:
                items.append(parse_unquoted())
            sep = text[pos]
            if sep == ",":
                pos += 1
                continue
            if sep == "}":
                pos += 1
                break
            raise ValueError(text)
        return items

    def parse_quoted() -> str:
        nonlocal pos
        pos += 1  # opening quote
        chars: List[str] = []
        while text[pos] != '"':
            if text[pos] == "\\":
                pos += 1
            chars.append(text[pos])
            pos += 1
        pos += 1  # closing quote
        return "".join(chars)

    def parse_unquoted() -> Optional[str]:
        nonlocal pos
        start = pos
        while text[pos] not in ",}":
            pos += 1
        token = text[start:pos]
        return None if token == "NULL" else token

    result = parse_list()
    if pos != len(text):
        raise ValueError(text)
    return result


def _decode_array_elements(items: list, decoder) -> list:
    """Cast each leaf element of a (possibly nested) parsed array with ``decoder`` (protocol 1.2
    element typing). ``None`` (SQL NULL) and the nested-list structure are preserved; an element that
    does not parse for its type falls back to its raw ``str`` rather than failing the fetch."""
    out: list = []
    for item in items:
        if item is None:
            out.append(None)
        elif isinstance(item, list):
            out.append(_decode_array_elements(item, decoder))
        else:
            try:
                out.append(decoder(item))
            except (ValueError, InvalidOperation, IndexError, KeyError, TypeError, ArithmeticError):
                out.append(item)
    return out


# Maps a RowDescriptionTyped type name (protocol 1.1) to the function that turns the wire text into a
# typed Python value, for full typed DB-API fidelity. Tags not listed (TEXT, MACADDR, BIT, GEOMETRY,
# TSVECTOR, XML, UNKNOWN, ...) stay as ``str``; BYTES is handled separately in ``_decode_cell``.
_TYPED_DECODERS = {
    "BOOL": _decode_bool,
    "INT": int,
    "FLOAT": float,
    "NUMERIC": Decimal,
    "DATE": _dt.date.fromisoformat,
    "TIME": _decode_time,
    "TIMETZ": _decode_time,
    "TIMESTAMP": _decode_timestamp,
    "TIMESTAMPTZ": _decode_timestamp,
    "INTERVAL": _decode_interval,
    "UUID": _uuid.UUID,
    "JSON": _json.loads,
    "ARRAY": _parse_array,
    "RANGE": _types.decode_range,
    "INET": _types.decode_inet,
    "CIDR": _types.decode_cidr,
    "VECTOR": _types.decode_vector,
}


class Notice(NamedTuple):
    """A message the server sends along with a reply instead of failing it: a warning, or a note
    about what a statement did (protocol 1.3)."""

    severity: str
    """How serious it is: ``WARNING``, ``NOTICE``, ``INFO``, ``DEBUG``, ``LOG``."""
    sqlstate: str
    """The five-character SQLSTATE code (``01000`` for a plain warning)."""
    message: str
    """The text of the message."""


#: How many notices :attr:`Connection.notices` keeps; older ones are dropped first.
MAX_NOTICES = 100


def _decode_notice(r: "proto.Reader") -> Notice:
    """Decode a ``NoticeResponse`` body: ``[severity:str][code:str][message:str]``."""
    return Notice(severity=r.string(), sqlstate=r.string(), message=r.string())


def _deliver_notice(conn: Any, r: "proto.Reader") -> None:
    """Keep a notice on ``conn`` and pass it to the connection's handlers. A handler that raises is
    reported as a warning: the reply around the notice still has to be read."""
    notice = _decode_notice(r)
    conn.notices.append(notice)
    if len(conn.notices) > MAX_NOTICES:
        del conn.notices[: len(conn.notices) - MAX_NOTICES]
    for handler in list(conn._notice_handlers):
        try:
            handler(notice)
        except Exception as exc:  # noqa: BLE001 - a handler must not break the exchange
            warnings.warn(
                f"notice handler {handler!r} raised {exc!r}", RuntimeWarning, stacklevel=2
            )


class Xid:
    """A two-phase commit transaction id (PEP-249 ``xid``): ``format_id``, ``gtrid`` (global
    transaction id) and ``bqual`` (branch qualifier). It behaves as the 3-tuple
    ``(format_id, gtrid, bqual)``.

    On the server it is the transaction's gid text: ``<format_id>_<gtrid>_<bqual>``, with gtrid
    and bqual base64-encoded (UTF-8), which :meth:`from_string` reads back. A gid in any other
    form is kept as it is: ``format_id`` and ``bqual`` are ``None`` and ``gtrid`` is the whole gid.
    An xid returned by :meth:`Connection.tpc_recover` also carries ``prepared`` (when it was
    prepared), ``owner`` and ``database``."""

    __slots__ = ("format_id", "gtrid", "bqual", "prepared", "owner", "database")

    def __init__(
        self,
        format_id: Optional[int],
        gtrid: str,
        bqual: Optional[str],
        prepared: Optional[Any] = None,
        owner: Optional[str] = None,
        database: Optional[str] = None,
    ) -> None:
        self.format_id = format_id
        self.gtrid = gtrid
        self.bqual = bqual
        self.prepared = prepared
        self.owner = owner
        self.database = database

    @classmethod
    def from_string(cls, gid: str) -> "Xid":
        """The xid a gid stands for: a structured one when it is in the ``format_gtrid_bqual``
        form :class:`Xid` writes, else a plain one holding the gid."""
        match = _XID_FORM.fullmatch(gid)
        if match is not None:
            try:
                gtrid = base64.b64decode(match.group(2), validate=True).decode("utf-8")
                bqual = base64.b64decode(match.group(3), validate=True).decode("utf-8")
            except (ValueError, UnicodeDecodeError):
                pass
            else:
                xid = cls(int(match.group(1)), gtrid, bqual)
                if str(xid) == gid:
                    return xid
        return cls(None, gid, None)

    def __str__(self) -> str:
        if self.format_id is None:
            return self.gtrid
        encode = lambda text: base64.b64encode(text.encode("utf-8")).decode("ascii")  # noqa: E731
        return f"{self.format_id}_{encode(self.gtrid)}_{encode(self.bqual or '')}"

    def _key(self) -> Tuple[Optional[int], str, Optional[str]]:
        return (self.format_id, self.gtrid, self.bqual)

    def __getitem__(self, index: int) -> Any:
        return self._key()[index]

    def __len__(self) -> int:
        return 3

    def __iter__(self) -> Iterator[Any]:
        return iter(self._key())

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Xid):
            return self._key() == other._key()
        if isinstance(other, tuple):
            return self._key() == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._key())

    def __repr__(self) -> str:
        return f"Xid({self.format_id!r}, {self.gtrid!r}, {self.bqual!r})"


_XID_FORM = _re.compile(r"(\d+)_([A-Za-z0-9+/=]*)_([A-Za-z0-9+/=]*)")


def _make_xid(format_id: int, gtrid: str, bqual: str) -> Xid:
    """Check the parts of an xid (PEP-249: gtrid and bqual are at most 64 characters)."""
    if isinstance(format_id, bool) or not isinstance(format_id, int) or not 0 <= format_id < 2**31:
        raise ProgrammingError(f"format_id is a whole number below 2**31, not {format_id!r}")
    for name, part in (("gtrid", gtrid), ("bqual", bqual)):
        if not isinstance(part, str):
            raise ProgrammingError(f"{name} is a str, not {type(part).__name__}")
        if len(part) > 64:
            raise ProgrammingError(f"{name} is at most 64 characters")
    return Xid(format_id, gtrid, bqual)


def _as_xid(xid: Any) -> Xid:
    if isinstance(xid, Xid):
        return xid
    if isinstance(xid, str):
        if not xid:
            raise ProgrammingError("a transaction id cannot be empty")
        return Xid.from_string(xid)
    raise ProgrammingError(f"a transaction id is an Xid or a str, not {type(xid).__name__}")


# The prepared transactions the server holds, for `tpc_recover`.
_RECOVER_QUERY = "SELECT gid, prepared, owner, database FROM nusadb_prepared_xacts"


def _recovered(rows: List[List[Optional[bytes]]], types: List[Optional[str]]) -> List[Xid]:
    out = []
    for row in rows:
        gid, prepared, owner, database = (
            Cursor._decode_cell(cell, type_code) for cell, type_code in zip(row, types)
        )
        xid = Xid.from_string(gid)
        xid.prepared, xid.owner, xid.database = prepared, owner, database
        out.append(xid)
    return out


class Notification(NamedTuple):
    """An asynchronous ``LISTEN``/``NOTIFY`` message delivered by the server."""

    pid: int
    """Backend pid of the connection that issued the ``NOTIFY``."""
    channel: str
    """The channel the notification was sent on."""
    payload: str
    """The payload (empty string when ``NOTIFY`` carried none)."""


class Connection:
    """A PEP-249 connection: one authenticated socket to a NusaDB server.

    By default the connection is in **autocommit** mode: the server runs each
    statement on its own implicit transaction. Pass ``autocommit=False`` (as the
    SQLAlchemy dialect does) for the standard transactional model: the connection
    sends ``BEGIN`` lazily before the first statement of a transaction, and
    :meth:`commit`/:meth:`rollback` send ``COMMIT``/``ROLLBACK``.
    """

    # PEP-249 optional extension: the exception classes as connection attributes.
    from .exceptions import (  # noqa: E402
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        ProgrammingError,
        Warning,
    )

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        database: str,
        password: Optional[str] = None,
        ssl: Optional[ssl_module.SSLContext] = None,
        timeout: Optional[float] = None,
        autocommit: bool = True,
        *,
        connect_timeout: Optional[float] = None,
        statement_timeout: Optional[float] = None,
        keepalive: Optional[float] = None,
        binary: bool = False,
        prepare_threshold: Optional[int] = 5,
    ) -> None:
        self._user = user
        self._password = password
        self._closed = False
        self._pid: Optional[int] = None
        self._secret: Optional[int] = None
        self._host = host
        self._port = port
        self._ssl = ssl
        self._names = itertools.count()
        self._autocommit = autocommit
        # The transaction status of the last ReadyForQuery: "I" idle, "T" in a transaction, "E" in
        # a failed transaction.
        self._status = "I"
        # A request whose reply has not been read through its ReadyForQuery. Left set when a call
        # ends part way (an exception, a KeyboardInterrupt), which leaves the connection unusable.
        self._pending = False
        self._mid_message = False
        self._broken = False
        self._timeout = timeout
        self._statement_timeout = statement_timeout
        self._notifications: List["Notification"] = []
        #: The default ``row_factory`` of this connection's cursors (``None``: tuples).
        self.row_factory: Optional[RowFactory] = None
        # Statements kept prepared: `prepare_cached` ones under their SQL text, automatically
        # prepared ones under `(native sql, parameter type tags)`.
        self._statements: "OrderedDict[Any, PreparedStatement]" = OrderedDict()
        #: Whether this connection's cursors ask for results in the binary format by default.
        self.binary = bool(binary)
        self._prepare_threshold = _check_threshold(prepare_threshold)
        # How many times each statement not (yet) prepared has run with parameters.
        self._uses: "OrderedDict[Tuple[str, bytes], int]" = OrderedDict()
        # Adapters registered on this connection only (see `nusadb.types.register_composite`).
        self._adapters: Dict[type, Callable[[Any], Any]] = {}
        #: The :class:`Notice` messages the server sent with its replies, oldest first (the last
        #: :data:`MAX_NOTICES`). Clear it as needed. A server before protocol 1.3 sends none.
        self.notices: List[Notice] = []
        self._notice_handlers: List[Callable[[Notice], Any]] = []
        # The two-phase transaction begun with `tpc_begin`, and whether it has been prepared.
        self._tpc: Optional[Xid] = None
        self._tpc_prepared = False
        # The open row stream (weakly held: one dropped unread is ended at the next call).
        self._stream: Optional["weakref.ReferenceType[RowStream]"] = None
        self._pipeline: Optional["Pipeline"] = None

        try:
            raw = socket.create_connection(
                (host, port), timeout=connect_timeout if connect_timeout is not None else timeout
            )
        except OSError as exc:
            raise OperationalError(f"could not connect to {host}:{port}: {exc}") from exc
        # Disable Nagle's algorithm: this is a request/response protocol, so
        # coalescing small frames only adds delayed-ACK latency per round-trip.
        # Any failure from here on (an interrupt included) closes the socket rather than leak it.
        try:
            # The TLS and authentication exchanges are part of connecting: bounded the same way.
            raw.settimeout(connect_timeout if connect_timeout is not None else timeout)
            raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if keepalive is not None:
                _set_keepalive(raw, keepalive)
            if ssl is not None:
                try:
                    raw = ssl.wrap_socket(raw, server_hostname=host)
                except (OSError, ssl_module.SSLError) as exc:
                    raise OperationalError(
                        f"TLS handshake with {host}:{port} failed: {exc}"
                    ) from exc
            self._sock = raw
            self._handshake(user, database)
            self._apply_session()
            raw.settimeout(timeout)
        except BaseException:
            raw.close()
            self._closed = True
            raise

    # --- PEP-249 connection API ---

    def cursor(
        self,
        row_factory: Optional["RowFactory"] = None,
        *,
        name: Optional[str] = None,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
        binary: Optional[bool] = None,
    ) -> "Cursor":
        """A new cursor. ``row_factory`` (default: the connection's ``row_factory``) shapes the
        rows it fetches; see :mod:`nusadb.rows`. ``binary`` asks for results in the binary format
        (default: the connection's :attr:`binary`); the values decode the same either way.

        With a ``name``, the cursor is a :class:`ServerCursor`: its query stays on the server and
        rows come over as they are fetched. ``scrollable`` declares it ``SCROLL`` (``True``) or
        ``NO SCROLL`` (``False``); ``withhold`` keeps it open after its transaction commits. A named
        cursor fetches in the text format: the server runs ``FETCH`` only as a simple query."""
        self._ensure_open()
        factory = row_factory if row_factory is not None else self.row_factory
        if name is not None:
            if binary:
                raise NotSupportedError(
                    "a named cursor fetches in the text format; binary=True is not available"
                )
            return ServerCursor(self, name, factory, scrollable, withhold)
        return Cursor(self, factory, binary=self.binary if binary is None else binary)

    def prepare(self, sql: Any) -> "PreparedStatement":
        """Parse ``sql`` (text or a :mod:`nusadb.sql` object) once on the server; run it with
        :meth:`Cursor.execute` or :meth:`Cursor.executemany` as many times as needed. Its parameter
        types are inferred from the statement. The server stores the text as it is, so a mistake in
        it is reported when the statement first runs. Free it with
        :meth:`PreparedStatement.close`."""
        self._ensure_open()
        sql = _sql_text(sql, self)
        return PreparedStatement(self, self._prepare(sql), sql)

    def prepare_cached(self, sql: Any) -> "PreparedStatement":
        """:meth:`prepare`, reusing the statement already prepared for the same ``sql`` on this
        connection. Up to :data:`STATEMENT_CACHE_SIZE` statements are kept; past that the least
        recently used is closed."""
        self._ensure_open()
        sql = _sql_text(sql, self)
        cached = self._statements.get(sql)
        if cached is not None and not cached.closed:
            self._statements.move_to_end(sql)
            return cached
        statement = self.prepare(sql)
        self._statements[sql] = statement
        self._trim_statements()
        return statement

    def _trim_statements(self) -> None:
        """Close the least recently used prepared statements past :data:`STATEMENT_CACHE_SIZE`."""
        while len(self._statements) > STATEMENT_CACHE_SIZE:
            _key, oldest = self._statements.popitem(last=False)
            if not self._broken and not self._closed:
                oldest.close()

    @property
    def prepare_threshold(self) -> Optional[int]:
        """How many times a statement runs with parameters before it is prepared on the server and
        reused (``None``: never). See :func:`connect`."""
        return self._prepare_threshold

    @prepare_threshold.setter
    def prepare_threshold(self, value: Optional[int]) -> None:
        self._prepare_threshold = _check_threshold(value)

    def transaction(
        self, isolation: Optional[str] = None, read_only: Optional[bool] = None
    ) -> "Transaction":
        """A context manager around a transaction: committed when the block ends, rolled back
        when it raises. Used inside a transaction it opens a savepoint instead, released or rolled
        back the same way, so blocks nest.

        ``isolation`` is ``"read uncommitted"``, ``"read committed"``, ``"repeatable read"`` or
        ``"serializable"``; ``read_only`` makes the transaction read-only (or read-write). Both
        apply only to the outermost block."""
        return Transaction(self, isolation, read_only)

    def stream(
        self,
        sql: Any,
        parameters: Sequence[Any] = (),
        batch_rows: int = 1024,
        row_factory: Optional["RowFactory"] = None,
        *,
        binary: Optional[bool] = None,
    ) -> "RowStream":
        """Run a query and read its rows ``batch_rows`` at a time, so a result of any size is
        never held whole on the client. The connection runs nothing else until the stream is read
        to the end or closed (use it as a context manager). ``binary`` asks for the rows in the
        binary format (default: the connection's :attr:`binary`)."""
        self._ensure_open()
        return RowStream(
            self,
            _sql_text(sql, self),
            parameters,
            max(1, batch_rows),
            row_factory,
            self.binary if binary is None else binary,
        )

    def pipeline(self) -> "Pipeline":
        """A block whose statements are sent together: ``p.execute(sql, params)`` queues a
        statement and returns the cursor its result will be loaded into, and the results arrive
        when the block ends or :meth:`Pipeline.sync` is called. A whole block costs one round trip
        (one per :data:`PIPELINE_SYNC_STATEMENTS` statements). See :class:`Pipeline`."""
        self._ensure_open()
        return Pipeline(self)

    @property
    def autocommit(self) -> bool:
        """Whether each statement runs as its own transaction. Settable only outside a
        transaction."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        if self._status != "I":
            raise ProgrammingError("cannot change autocommit inside a transaction")
        self._autocommit = bool(value)

    @property
    def transaction_status(self) -> str:
        """Where the connection's transaction stands, as of the last statement: ``"idle"``,
        ``"in_transaction"``, or ``"failed"`` (a statement in the transaction failed; only a
        rollback ends it)."""
        return {"I": "idle", "T": "in_transaction", "E": "failed"}[self._status]

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` was called or the connection broke."""
        return self._closed

    @property
    def broken(self) -> bool:
        """Whether the connection failed (a socket error, a reply that could not be read, or a call
        interrupted part way). A broken connection refuses further calls; open a new one."""
        return self._broken

    def commit(self) -> None:
        """Commit the open transaction. Nothing to do outside one.

        Raises :class:`TransactionRolledBack` when a statement in the transaction had failed and the
        server rolled it back instead of committing it: its work is gone.
        """
        self._ensure_open()
        if self._tpc is not None:
            raise ProgrammingError(
                "commit() is not allowed in a two-phase transaction; use tpc_commit()"
            )
        if self._status == "I":
            return
        failed = self._status == "E"
        _cols, _types, _rows, tag = self._run_control("COMMIT")
        if failed or tag == "ROLLBACK":
            raise TransactionRolledBack(
                "a statement in the transaction failed, so COMMIT rolled it back"
            )

    def rollback(self) -> None:
        """Roll back the open transaction, discarding its uncommitted changes. Nothing to do
        outside one."""
        self._ensure_open()
        if self._tpc is not None:
            raise ProgrammingError(
                "rollback() is not allowed in a two-phase transaction; use tpc_rollback()"
            )
        if self._status != "I":
            self._run_control("ROLLBACK")

    def savepoint(self, name: str) -> None:
        """Establish a savepoint inside the current transaction (``SAVEPOINT name``).

        A savepoint is a named marker you can later roll back to without ending the
        transaction. Requires an open transaction, so it is only valid with
        ``autocommit=False``; a ``BEGIN`` is opened lazily if none is active yet.
        """
        self._ensure_open()
        if self._autocommit and self._status == "I":
            raise ProgrammingError("cannot set a savepoint in autocommit mode outside a transaction")
        self._begin_if_needed()
        self._run_control("SAVEPOINT " + _quote_identifier(name))

    def rollback_to(self, name: str) -> None:
        """Roll back to a savepoint (``ROLLBACK TO SAVEPOINT name``).

        Undoes everything done since the savepoint while leaving the transaction open
        so it can continue.
        """
        self._ensure_open()
        if self._status == "I":
            raise ProgrammingError("no active transaction to roll back to a savepoint")
        self._run_control("ROLLBACK TO SAVEPOINT " + _quote_identifier(name))

    def release_savepoint(self, name: str) -> None:
        """Release (forget) a savepoint (``RELEASE SAVEPOINT name``), keeping its work."""
        self._ensure_open()
        if self._status == "I":
            raise ProgrammingError("no active transaction to release a savepoint")
        self._run_control("RELEASE SAVEPOINT " + _quote_identifier(name))

    # --- two-phase commit (PEP-249 optional extension) ---

    def xid(self, format_id: int, gtrid: str, bqual: str) -> Xid:
        """A transaction id for :meth:`tpc_begin`: ``format_id`` a whole number below ``2**31``,
        ``gtrid`` and ``bqual`` strings of at most 64 characters. A plain ``str`` gid works in
        its place."""
        return _make_xid(format_id, gtrid, bqual)

    def tpc_begin(self, xid: Union[Xid, str]) -> None:
        """Begin a two-phase transaction under ``xid`` (an :meth:`xid` or a gid string). It must
        start outside a transaction; a transaction is opened at once, in autocommit mode too.
        End it with :meth:`tpc_prepare` then :meth:`tpc_commit` or :meth:`tpc_rollback`; until
        then :meth:`commit` and :meth:`rollback` are refused."""
        self._ensure_open()
        xid = _as_xid(xid)
        if self._tpc is not None:
            raise ProgrammingError("a two-phase transaction is already in progress")
        if self._status != "I":
            raise ProgrammingError("tpc_begin must be called outside a transaction")
        self._run_control("BEGIN")
        self._tpc, self._tpc_prepared = xid, False

    def tpc_prepare(self) -> None:
        """Prepare the two-phase transaction (``PREPARE TRANSACTION``): its work is kept on the
        server, which can then commit or roll it back by its id, from any connection, even after a
        restart. No statement may run on this connection until :meth:`tpc_commit` or
        :meth:`tpc_rollback`."""
        self._ensure_open()
        if self._tpc is None:
            raise ProgrammingError("tpc_prepare needs a transaction begun with tpc_begin")
        if self._tpc_prepared:
            raise ProgrammingError("the two-phase transaction is already prepared")
        self._run_control("PREPARE TRANSACTION " + _quote_literal(str(self._tpc)))
        self._tpc_prepared = True

    def tpc_commit(self, xid: Union[Xid, str, None] = None) -> None:
        """Commit a two-phase transaction. Without ``xid`` it is the one begun on this connection:
        ``COMMIT PREPARED`` once prepared, else an ordinary one-phase commit. With ``xid`` (one
        from :meth:`tpc_recover`, say) it is that prepared transaction, from outside a
        transaction."""
        self._tpc_end("COMMIT", xid)

    def tpc_rollback(self, xid: Union[Xid, str, None] = None) -> None:
        """Roll back a two-phase transaction, as :meth:`tpc_commit` commits one."""
        self._tpc_end("ROLLBACK", xid)

    def tpc_recover(self) -> List[Xid]:
        """The transactions prepared on the server and not yet committed or rolled back, as xids
        (with ``prepared``, ``owner`` and ``database`` set). Runs without opening a transaction."""
        self._ensure_open()
        _columns, types, rows, _tag = self._run_control(_RECOVER_QUERY)
        return _recovered(rows, types)

    def _tpc_end(self, action: str, xid: Union[Xid, str, None]) -> None:
        self._ensure_open()
        if xid is not None:
            if self._tpc is not None:
                raise ProgrammingError(
                    f"tpc_{action.lower()} with an xid cannot run inside a two-phase transaction"
                )
            if self._status != "I":
                raise ProgrammingError(
                    f"tpc_{action.lower()} with an xid must be called outside a transaction"
                )
            self._run_control(f"{action} PREPARED " + _quote_literal(str(_as_xid(xid))))
            return
        if self._tpc is None:
            raise ProgrammingError(f"tpc_{action.lower()} needs a transaction begun with tpc_begin")
        try:
            if self._tpc_prepared:
                self._run_control(f"{action} PREPARED " + _quote_literal(str(self._tpc)))
            elif self._status != "I":
                failed = self._status == "E"
                _columns, _types, _rows, tag = self._run_control(action)
                if action == "COMMIT" and (failed or tag == "ROLLBACK"):
                    raise TransactionRolledBack(
                        "a statement in the transaction failed, so COMMIT rolled it back"
                    )
        finally:
            self._tpc, self._tpc_prepared = None, False

    # --- notices ---

    def add_notice_handler(self, handler: Callable[[Notice], Any]) -> None:
        """Call ``handler(notice)`` for each :class:`Notice` the server sends from now on, as it
        arrives (also kept in :attr:`notices`). An exception it raises becomes a warning."""
        self._notice_handlers.append(handler)

    def remove_notice_handler(self, handler: Callable[[Notice], Any]) -> None:
        """Stop calling ``handler``; raises ``ValueError`` when it was not added."""
        self._notice_handlers.remove(handler)

    # --- asynchronous notifications (LISTEN/NOTIFY) ---

    def listen(self, channel: str) -> None:
        """Subscribe to asynchronous notifications on ``channel`` (``LISTEN channel``). Afterwards a
        ``NOTIFY`` from any connection on the same database is delivered here; collect notifications
        with :meth:`notifications` or :meth:`poll`."""
        self._ensure_open()
        self._run_control("LISTEN " + _quote_identifier(channel))

    def unlisten(self, channel: Optional[str] = None) -> None:
        """Stop listening on ``channel`` (``UNLISTEN channel``), or on every channel when ``channel``
        is ``None`` (``UNLISTEN *``)."""
        self._ensure_open()
        target = "*" if channel is None else _quote_identifier(channel)
        self._run_control("UNLISTEN " + target)

    def notify(self, channel: str, payload: Optional[str] = None) -> None:
        """Send a notification on ``channel`` with an optional ``payload``
        (``NOTIFY channel[, 'payload']``)."""
        self._ensure_open()
        sql = "NOTIFY " + _quote_identifier(channel)
        if payload is not None:
            sql += ", " + _quote_literal(payload)
        self._run_control(sql)

    def notifications(self) -> List[Notification]:
        """Return and clear the notifications already received (buffered while reading other
        responses). Does not touch the socket; use :meth:`poll` to wait for new ones."""
        self._ensure_open()
        pending = self._notifications
        self._notifications = []
        return pending

    def poll(self, timeout: Optional[float] = 0.0) -> Optional[Notification]:
        """Wait up to ``timeout`` seconds for the next notification (a buffered one is returned
        immediately; ``None`` on timeout). ``timeout`` of ``None`` blocks until one arrives; ``0``
        polls without blocking. Only meaningful after :meth:`listen`."""
        self._ensure_open()
        if self._notifications:
            return self._notifications.pop(0)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            # Wait for the first byte without reading any, so a timeout never cuts a message in
            # half.
            pending = isinstance(self._sock, ssl_module.SSLSocket) and self._sock.pending() > 0
            if not pending:
                wait = None if deadline is None else max(0.0, deadline - time.monotonic())
                try:
                    ready, _, _ = select.select([self._sock], [], [], wait)
                except (OSError, ValueError) as exc:
                    self._fail()
                    raise OperationalError(f"waiting for a notification failed: {exc}") from exc
                if not ready:
                    return None
            # Interrupted part way through the message, the connection could not be read again.
            self._pending = True
            mtype, r = self._read_one()
            self._pending = False
            if mtype == proto.B_NOTICE_RESPONSE:
                # A notice on an idle connection: keep it, and go on waiting for a notification.
                _deliver_notice(self, r)
                continue
            if mtype == proto.B_NOTIFICATION_RESPONSE:
                return _decode_notification(r)
            if mtype == proto.B_ERROR:
                raise from_sqlstate(r.string(), r.string())
            self._fail()
            raise InterfaceError(
                "unexpected message %r while polling for notifications" % chr(mtype)
            )

    def _run_control(self, sql: str) -> "_Result":
        """Send a transaction-control statement (`BEGIN`/`COMMIT`/`ROLLBACK`) directly."""
        self._send(proto.query(sql))
        return self._read_until_ready()

    def _begin_if_needed(self) -> None:
        """Open a transaction before the first statement when not in autocommit mode. Refused
        while a prepared two-phase transaction waits for :meth:`tpc_commit` or
        :meth:`tpc_rollback`."""
        if self._tpc_prepared:
            raise ProgrammingError(_TPC_PREPARED)
        if not self._autocommit and self._status == "I":
            self._run_control("BEGIN")

    def close(self) -> None:
        if self._closed:
            return
        try:
            if not self._broken:
                self._sock.sendall(proto.terminate())
        except OSError:
            pass
        finally:
            self._sock.close()
            self._closed = True

    @property
    def backend_key(self) -> Optional[Tuple[int, int]]:
        """This connection's ``(pid, secret)`` cancellation key, or ``None``."""
        if self._pid is None or self._secret is None:
            return None
        return (self._pid, self._secret)

    def cancel(self) -> None:
        """Cancel this connection's in-flight statement from a fresh connection
        (``docs/wire-protocol.md`` §13). Safe no-op when nothing is running.
        """
        if self._pid is None or self._secret is None:
            return
        try:
            raw = socket.create_connection((self._host, self._port), timeout=self._timeout)
            try:
                if self._ssl is not None:
                    raw = self._ssl.wrap_socket(raw, server_hostname=self._host)
                raw.sendall(proto.cancel_request(self._pid, self._secret))
            finally:
                raw.close()
        except OSError as exc:
            raise OperationalError(f"could not send the cancel request: {exc}") from exc

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # --- low-level protocol ---

    def _ensure_open(self) -> None:
        if self._broken:
            raise OperationalError("the connection is broken; open a new one")
        if self._closed:
            raise InterfaceError("connection is closed")
        if self._stream is not None:
            stream = self._stream()
            if stream is not None:
                raise InterfaceError("a row stream is open on this connection; close it first")
            # Dropped part way: end its exchange so the connection is in step again.
            self._stream = None
            if self._mid_message:
                self._fail()
                raise OperationalError(
                    "the connection is broken (a row stream stopped part way through a message); "
                    "open a new one"
                )
            self._send(proto.sync())
            self._drain_to_ready()
        if self._pending:
            # A call ended part way (an exception or an interrupt mid-reply): the next bytes on the
            # socket belong to it, so nothing after it can be read correctly.
            self._fail()
            raise OperationalError(
                "the connection is broken (an earlier call was interrupted part way); open a new one"
            )

    def _apply_session(self) -> None:
        """Apply the session settings this connection was opened with (again after a reset)."""
        if self._statement_timeout is not None:
            # Whole milliseconds, rounded up: a sub-millisecond timeout must not become 0 (none).
            ms = math.ceil(self._statement_timeout * 1000)
            self._send(proto.query(f"SET statement_timeout = '{ms}ms'"))
            self._read_until_ready()

    def _end_stream(self) -> None:
        self._stream = None

    def _reset_session(self) -> None:
        """Return the session to how it was opened: settings reset, channels left, buffered
        notifications dropped, then this connection's own settings applied again."""
        self._ensure_open()
        for sql in ("RESET ALL", "UNLISTEN *"):
            self._send(proto.query(sql))
            self._read_until_ready()
        self._notifications.clear()
        self._apply_session()

    def _fail(self) -> None:
        """Mark the connection broken and close its socket."""
        self._broken = True
        self._closed = True
        try:
            self._sock.close()
        except OSError:
            pass

    def _send(self, frame: bytes) -> None:
        if self._pipeline is not None and self._pipeline._queued:
            # Sent now, this would run before the queued statements and read their replies.
            raise InterfaceError(
                "statements are queued in a pipeline on this connection; sync it or end the block"
            )
        self._pending = True
        try:
            self._sock.sendall(frame)
        except OSError as exc:
            self._fail()
            raise OperationalError(f"sending to the server failed: {exc}") from exc

    def _recv_exact(self, n: int) -> bytes:
        chunks: List[bytes] = []
        remaining = n
        while remaining > 0:
            try:
                chunk = self._sock.recv(remaining)
            except OSError as exc:
                self._fail()
                raise OperationalError(f"reading from the server failed: {exc}") from exc
            if not chunk:
                self._fail()
                raise OperationalError("server closed the connection")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _read_message(self) -> Tuple[int, proto.Reader]:
        """The next message of a reply. A notice, which can come anywhere in one, is delivered
        (see :attr:`notices`) and skipped."""
        while True:
            mtype, r = self._read_one()
            if mtype != proto.B_NOTICE_RESPONSE:
                return mtype, r
            _deliver_notice(self, r)

    def _read_one(self) -> Tuple[int, proto.Reader]:
        # Set while a message is half read: stopped there, the socket's next bytes are not a
        # message header, and only dropping the connection gets back in step.
        self._mid_message = True
        try:
            mtype, plen = proto.decode_frame_header(self._recv_exact(proto.HEADER_LEN))
        except ValueError as exc:
            self._fail()
            raise InterfaceError(f"malformed message from the server: {exc}") from exc
        payload = self._recv_exact(plen) if plen else b""
        self._mid_message = False
        if mtype == proto.B_READY and payload:
            # Every exchange ends here: record where the transaction stands.
            self._pending = False
            status = chr(payload[0])
            if status in ("I", "T", "E"):
                self._status = status
        return mtype, proto.Reader(payload)

    def _handshake(self, user: str, database: str) -> None:
        self._send(proto.startup(user, database))
        while True:
            mtype, r = self._read_message()
            if mtype == proto.B_AUTH:
                sub = r.u32()
                if sub == proto.AUTH_SASL:
                    count = r.u16()
                    mechs = [r.string() for _ in range(count)]
                    self._scram(mechs)
                # AUTH_OK and any other ack: nothing to do.
            elif mtype == proto.B_BACKEND_KEY:
                self._pid = r.u32()
                self._secret = r.u32()
            elif mtype == proto.B_READY:
                r.u8()
                return
            elif mtype == proto.B_ERROR:
                raise from_sqlstate(r.string(), r.string())

    def _scram(self, mechs: Sequence[str]) -> None:
        if _SCRAM_MECHANISM not in mechs:
            raise OperationalError(
                f"server offered no supported SASL mechanism (got {list(mechs)})"
            )
        if self._password is None:
            raise OperationalError("server requires a password but none was given")

        bare, first = _scram.client_first(self._user)
        self._send(proto.sasl_initial(_SCRAM_MECHANISM, first.encode("utf-8")))

        mtype, r = self._read_message()
        if mtype != proto.B_AUTH or r.u32() != proto.AUTH_SASL_CONTINUE:
            raise OperationalError("expected a SASL continue message")
        server_first = r.rest().decode("utf-8")

        final, expected = _scram.client_final(self._password, bare, server_first)
        self._send(proto.sasl_response(final))

        mtype, r = self._read_message()
        if mtype == proto.B_ERROR:
            raise from_sqlstate(r.string(), r.string())
        if mtype != proto.B_AUTH or r.u32() != proto.AUTH_SASL_FINAL:
            raise OperationalError("expected a SASL final message")
        if not _scram.verify_server_final(r.rest().decode("utf-8"), expected):
            raise OperationalError("server signature did not verify")

    def _read_until_ready(self, on_parse: Optional[Callable[[], None]] = None) -> _Result:
        """Read a reply through its ``ReadyForQuery``, raising the error it carried. ``on_parse`` is
        called on a ``ParseComplete``."""
        columns: List[str] = []
        column_types: List[Optional[str]] = []
        rows: List[List[Optional[bytes]]] = []
        tag: Optional[str] = None
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = self._read_message()
            if mtype == proto.B_PARSE_COMPLETE and on_parse is not None:
                on_parse()
            elif mtype in (proto.B_ROW_DESCRIPTION, proto.B_ROW_DESCRIPTION_TYPED):
                columns, column_types = _read_description(mtype, r)
            elif mtype == proto.B_DATA_ROW:
                rows.append(r.fields())
            elif mtype == proto.B_COMMAND_COMPLETE:
                tag = r.string()
            elif mtype == proto.B_ERROR:
                error = (r.string(), r.string())
            elif mtype == proto.B_NOTIFICATION_RESPONSE:
                # A pending LISTEN/NOTIFY message can lead the next query's response; buffer it.
                self._notifications.append(_decode_notification(r))
            elif mtype == proto.B_READY:
                r.u8()
                break
            # Other acks (ParseComplete/BindComplete/NoData/...) carry no result.
        if error is not None:
            raise from_sqlstate(*error)
        return columns, column_types, rows, tag

    def _simple_query(self, sql: str) -> _Result:
        self._begin_if_needed()
        self._send(proto.query(sql))
        return self._read_until_ready()

    def _extended_query(
        self, sql: str, params: Sequence[Any], binary: bool = False, auto: bool = True
    ) -> _Result:
        """Run ``sql`` with ``params`` through Parse, Bind and Execute. With ``auto`` a statement
        that has run :attr:`prepare_threshold` times is prepared on the server and reused."""
        self._begin_if_needed()
        compiled = _compile(sql)
        with _scoped_adapters(self._adapters):
            encoded, tags = _bind(_arrange(compiled, params))
        key = (compiled.sql, tags)
        name, parse = self._auto_statement(key) if auto else (None, False)
        if name is None:
            return self._run_unnamed(compiled.sql, tags, encoded, binary)
        if parse:
            self._send(proto.parse(name, compiled.sql, tags))
        self._send(proto.bind("", name, encoded, binary))
        self._send(proto.describe_portal(""))
        self._send(proto.execute("", 0))
        self._send(proto.sync())
        try:
            return self._read_until_ready(
                lambda: self._remember(key, name, compiled.sql) if parse else None
            )
        except DatabaseError as exc:
            if parse or getattr(exc, "sqlstate", None) != _UNKNOWN_STATEMENT:
                raise
            # The server no longer knows the statement: forget it, and run the statement unprepared
            # once, unless the error failed the transaction (then nothing more may run in it).
            self._forget(key)
            if self._status == "E":
                raise
            return self._run_unnamed(compiled.sql, tags, encoded, binary)
        finally:
            if parse:
                self._trim_statements()

    def _run_unnamed(
        self, sql: str, tags: bytes, encoded: List[Optional[bytes]], binary: bool
    ) -> _Result:
        self._send(proto.parse("", sql, tags))
        self._send(proto.bind("", "", encoded, binary))
        self._send(proto.describe_portal(""))
        self._send(proto.execute("", 0))
        self._send(proto.sync())
        return self._read_until_ready()

    def _extended_plain(self, sql: str, binary: bool) -> _Result:
        """Run ``sql``, which takes no parameters, through Parse and Bind (so its result can come in
        the binary format). The text is sent as it is: no placeholder is rewritten."""
        self._begin_if_needed()
        return self._run_unnamed(sql, b"", [], binary)

    def _auto_statement(self, key: Tuple[str, bytes]) -> Tuple[Optional[str], bool]:
        """The prepared statement to run ``key`` (native SQL and parameter tags) under, and whether
        it must be parsed now: ``(None, False)`` while the statement has run fewer than
        :attr:`prepare_threshold` times, then a new name to parse, then that name."""
        threshold = self._prepare_threshold
        if threshold is None:
            return None, False
        cached = self._statements.get(key)
        if cached is not None and not cached.closed:
            self._statements.move_to_end(key)
            return cached.name, False
        uses = self._uses.pop(key, 0)
        if uses < threshold:
            self._uses[key] = uses + 1
            while len(self._uses) > _USES_TRACKED:
                self._uses.popitem(last=False)
            return None, False
        return f"nusadb_auto_{next(self._names)}", True

    def _remember(self, key: Tuple[str, bytes], name: str, sql: str) -> None:
        """Keep the statement just prepared as ``name`` for reuse."""
        self._statements[key] = PreparedStatement(self, name, sql)

    def _forget(self, key: Tuple[str, bytes]) -> None:
        """Drop a prepared statement the server no longer has (no ``Close`` is sent for it)."""
        stale = self._statements.pop(key, None)
        if stale is not None:
            stale.closed = True

    def _prepare(self, sql: str) -> str:
        name = f"nusadb_stmt_{next(self._names)}"
        self._send(proto.parse(name, _to_native_placeholders(sql)))
        self._send(proto.sync())
        self._read_until_ready()  # ParseComplete + Ready; raises on a parse error
        return name

    # --- COPY bulk load / export (wire-protocol.md §12) ---

    def copy_in(self, sql: Any, source: "BinaryIO") -> int:
        r"""Bulk-load via ``COPY ... FROM STDIN`` (§12.1).

        Stream ``source`` (a binary file-like whose ``read(size)`` yields bytes already in the
        statement's format, by default the server's text format: tab-delimited fields, ``\N`` for
        SQL NULL, one row per line) as ``CopyData`` frames, then finish. Returns the number of rows
        loaded. A read error on ``source`` sends ``CopyFail`` and leaves the connection ready; a
        refused COPY (bad SQL, an RLS-protected table) raises the server error.
        """
        return self._copy_in(_sql_text(sql, self), source, None)

    def _copy_in(self, sql: str, source: "BinaryIO", expected_format: Optional[int]) -> int:
        self._ensure_open()
        self._begin_if_needed()
        self._send(proto.query(sql))
        copy_format = self._await_copy_start(proto.B_COPY_IN)
        if expected_format is not None and copy_format != expected_format:
            self._send(proto.copy_fail("nusadb: COPY format does not match the call"))
            self._drain_to_ready()
            raise ProgrammingError(_format_mismatch(expected_format))
        while True:
            try:
                chunk = source.read(65536)
            except Exception as exc:  # noqa: BLE001 - report any source failure, stay in sync
                try:
                    self._send(proto.copy_fail("nusadb: client read error during COPY"))
                    self._drain_to_ready()
                except (OSError, DatabaseError, InterfaceError):
                    pass  # the connection is broken now; the source's error is the one to report
                if isinstance(exc, ProgrammingError):
                    raise  # a row copy_in_rows could not write
                raise OperationalError(f"COPY source read failed: {exc}") from exc
            if not chunk:
                break
            self._send(proto.copy_data(chunk))
        self._send(proto.copy_done())
        return self._finish_copy()

    def copy_out(self, sql: Any, sink: "BinaryIO") -> int:
        r"""Bulk-export via ``COPY ... TO STDOUT`` (§12.2).

        Write each ``CopyData`` chunk the server streams into ``sink`` (a binary file-like with
        ``write(bytes)``); the bytes are in the statement's format, by default the server's text
        format (tab-delimited, ``\N`` for NULL). Returns the number of rows exported.
        """
        return self._copy_out(_sql_text(sql, self), sink, None)

    def _copy_out(self, sql: str, sink: Any, expected_format: Optional[int]) -> int:
        self._ensure_open()
        self._begin_if_needed()
        self._send(proto.query(sql))
        copy_format = self._await_copy_start(proto.B_COPY_OUT)
        if expected_format is not None and copy_format != expected_format:
            self._drain_to_ready()
            raise ProgrammingError(_format_mismatch(expected_format))
        while True:
            mtype, r = self._read_message()
            if mtype == proto.B_COPY_DATA:
                sink.write(r.rest())
            elif mtype == proto.B_COPY_DONE:
                break
            elif mtype == proto.B_ERROR:
                error = (r.string(), r.string())
                self._drain_to_ready()
                raise from_sqlstate(*error)
        return self._finish_copy()

    def copy_in_rows(
        self,
        sql: Any,
        rows: Iterable[Sequence[Any]],
        binary: bool = False,
        types: Optional[Sequence[str]] = None,
    ) -> int:
        """Bulk-load Python rows via ``COPY ... FROM STDIN``.

        Each row is a sequence of values. In the default text format they are written as they
        would bind as parameters (``None`` is NULL, a ``list`` an array, a ``dict`` JSON, ``bytes``
        binary); the statement must not set ``FORMAT``, ``DELIMITER`` or ``NULL``.

        With ``binary=True`` the statement must say ``WITH (FORMAT binary)``, and each value is
        written in the binary layout of its column's type: ``types`` names them in column order
        (``["INT", "TEXT", "DATE"]``, ``"int4range"``, ``"text[]"``, ...). Without ``types`` each
        column's type follows from its Python values (``int`` is ``INT``, ``str`` is ``TEXT``, an
        aware ``datetime`` is ``TIMESTAMPTZ``, ...), and a column whose values do not agree is
        refused. A binary field carries no type, so name the types whenever a column's type is not
        the one its Python values map to (an ``int`` for a ``FLOAT`` column, say).

        ``rows`` is read lazily, so a generator streams a load of any size. Returns the number of
        rows loaded. A row that cannot be written ends the COPY with nothing loaded and raises; the
        connection stays usable.
        """
        sql = _sql_text(sql, self)
        if binary:
            writer = _binary.CopyWriter(types, _adapt, _encode_param)
            chunks = _binary.copy_binary_chunks(rows, writer)
        else:
            if types is not None:
                raise ProgrammingError("types= applies to a binary COPY (binary=True)")
            chunks = _copy_text_chunks(rows)
        with _scoped_adapters(self._adapters):
            return self._copy_in(sql, _ChunkReader(chunks), 1 if binary else 0)

    def copy_out_rows(
        self, sql: Any, binary: bool = False, types: Optional[Sequence[str]] = None
    ) -> List[Tuple[Any, ...]]:
        """Export via ``COPY ... TO STDOUT`` as a list of rows. For an export too large to hold,
        use :meth:`copy_out`.

        In the default text format each row is a tuple of each field's text, ``None`` for NULL;
        the statement must not set ``FORMAT``, ``DELIMITER`` or ``NULL``.

        With ``binary=True`` the statement must say ``WITH (FORMAT binary)``. Each field is decoded
        at its column's type in ``types`` (as for :meth:`copy_in_rows`) into the Python value a
        query returns for it; without ``types`` a field stays its raw ``bytes``. The stream is
        checked strictly (signature, flags, row widths, lengths, trailer), and a fault raises
        :class:`InterfaceError` once the COPY has ended."""
        sql = _sql_text(sql, self)
        if binary:
            reader = _binary.CopyReader(types, Cursor._decode_binary_cell)
            self._copy_out(sql, reader, 1)
            return reader.finish()
        if types is not None:
            raise ProgrammingError("types= applies to a binary COPY (binary=True)")
        lines = _CopyLines()
        self._copy_out(sql, _SinkWriter(lines), 0)
        return lines.finish()

    def _await_copy_start(self, expected: int) -> int:
        """Read until the expected ``CopyInResponse``/``CopyOutResponse`` and return its overall
        format (``0`` text, ``1`` binary); a refused COPY arrives as ``Error`` then
        ``ReadyForQuery`` and is raised, the connection left ready."""
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = self._read_message()
            if mtype == expected:
                return r.u8() if r.remaining() else 0
            if mtype in (proto.B_COPY_IN, proto.B_COPY_OUT):
                # The statement copies the other way: end that COPY and leave the connection ready.
                if mtype == proto.B_COPY_IN:
                    self._send(proto.copy_fail("nusadb: COPY direction does not match the call"))
                self._drain_to_ready()
                wanted = "copy_in() needs COPY ... FROM STDIN"
                if expected == proto.B_COPY_OUT:
                    wanted = "copy_out() needs COPY ... TO STDOUT"
                raise ProgrammingError(wanted)
            if mtype == proto.B_ERROR:
                error = (r.string(), r.string())
            elif mtype == proto.B_READY:
                r.u8()
                if error is not None:
                    raise from_sqlstate(*error)
                raise OperationalError("COPY did not start")

    def _finish_copy(self) -> int:
        """Drain to ``ReadyForQuery``, returning the row count from the ``COPY <n>`` command tag."""
        tag: Optional[str] = None
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = self._read_message()
            if mtype == proto.B_COMMAND_COMPLETE:
                tag = r.string()
            elif mtype == proto.B_ERROR:
                error = (r.string(), r.string())
            elif mtype == proto.B_READY:
                r.u8()
                if error is not None:
                    raise from_sqlstate(*error)
                return max(_affected(tag), 0)

    def _drain_to_ready(self) -> None:
        """Best-effort resync: read and discard frames until ``ReadyForQuery``."""
        while True:
            mtype, r = self._read_message()
            if mtype == proto.B_READY:
                r.u8()
                return

    def _query_prepared(self, name: str, params: Sequence[Any], binary: bool = False) -> _Result:
        self._begin_if_needed()
        with _scoped_adapters(self._adapters):
            encoded, _tags = _bind(params)
        self._send(proto.bind("", name, encoded, binary))
        self._send(proto.describe_portal(""))
        self._send(proto.execute("", 0))
        self._send(proto.sync())
        return self._read_until_ready()

    def _close_statement(self, name: str) -> None:
        self._send(proto.close_statement(name))
        self._send(proto.sync())
        self._read_until_ready()

    def _execute_batch(self, sql: str, seq_of_parameters: Any) -> Tuple[int, bool]:
        """Run ``sql`` once per parameter set, pipelined: up to :data:`_BATCH_ROWS` executions go
        out before their replies are read. Returns the affected-row total and whether any
        statement reported a count. Stops at the first error and raises it; the statements before
        it keep their effect (each commits on its own in autocommit mode)."""
        self._begin_if_needed()
        compiled = _compile(sql)
        sql = compiled.sql
        total, counted = 0, False
        rows = (_arrange(compiled, parameters) for parameters in seq_of_parameters)
        for chunk, failure in _bound_chunks(rows, self._adapters):
            if chunk:
                got, any_count = self._send_batch(sql, chunk)
                total, counted = total + got, counted or any_count
            if failure is not None:
                raise failure
        return total, counted

    def _send_batch(self, sql: str, chunk: List[Tuple[List[Optional[bytes]], bytes]]) -> Tuple[int, bool]:
        tags: Optional[bytes] = None
        for encoded, row_tags in chunk:
            if row_tags != tags:
                # The parameter types changed (a None among numbers, say): parse again for them.
                self._send(proto.parse("", sql, row_tags))
                tags = row_tags
            self._send(proto.bind("", "", encoded))
            self._send(proto.execute("", 0))
        return self._finish_batch()

    def _finish_batch(self) -> Tuple[int, bool]:
        self._send(proto.sync())
        total, counted = 0, False
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = self._read_message()
            if mtype == proto.B_COMMAND_COMPLETE:
                affected = _affected(r.string())
                if affected >= 0:
                    total, counted = total + affected, True
            elif mtype == proto.B_ERROR:
                error = (r.string(), r.string())
            elif mtype == proto.B_NOTIFICATION_RESPONSE:
                self._notifications.append(_decode_notification(r))
            elif mtype == proto.B_READY:
                r.u8()
                break
        if error is not None:
            raise from_sqlstate(*error)
        return total, counted


def _read_description(mtype: int, r: Any) -> Tuple[List[str], List[Optional[str]]]:
    """The column names and type names of a ``RowDescription`` (no types) or, protocol 1.1, a
    ``RowDescriptionTyped`` (each column a name plus a 1-byte type tag, §9.2)."""
    count = r.u16()
    if mtype == proto.B_ROW_DESCRIPTION:
        return [r.string() for _ in range(count)], [None] * count
    columns: List[str] = []
    column_types: List[Optional[str]] = []
    for _ in range(count):
        columns.append(r.string())
        column_types.append(proto.type_name(r.u8()))
    return columns, column_types


def _affected(tag: Optional[str]) -> int:
    """Parse the trailing count of a command tag (``INSERT 3`` -> 3), else -1."""
    if not tag:
        return -1
    last = tag.rsplit(" ", 1)[-1]
    try:
        return int(last)
    except ValueError:
        return -1


#: A pipeline sends its queued statements once this many are waiting (or 64 KiB of them), so the
#: replies never pile up unread while more is being sent.
PIPELINE_SYNC_STATEMENTS = 256


class Pipeline:
    """Statements sent together, their replies read together (see :meth:`Connection.pipeline`).

    ``execute`` only queues: nothing reaches the server until :meth:`sync` or the end of the
    ``with`` block, and each queued statement's cursor holds its result after that. When a
    statement fails, the server skips the rest of that sync's statements; ``sync`` raises the
    error and the cursors of the skipped statements stay empty (``rowcount`` -1). In autocommit
    mode each statement that ran before the failure has committed; inside a transaction, the
    transaction is failed as with any other error. A full queue is sent by ``execute`` itself,
    which then raises any error from it. An exception raised inside the block drops the statements
    not yet sent. The connection sends nothing else while statements are queued.
    """

    def __init__(self, connection: Connection) -> None:
        self._conn = connection
        self._queued: List[Tuple[bytes, Cursor]] = []
        self._size = 0
        self._active = False

    def __enter__(self) -> "Pipeline":
        conn = self._conn
        conn._ensure_open()
        if conn._pipeline is not None:
            raise ProgrammingError("a pipeline is already open on this connection")
        conn._pipeline = self
        self._active = True
        return self

    def __exit__(self, exc_type: Any, *_exc: object) -> None:
        try:
            if exc_type is None:
                self.sync()
        finally:
            self._queued = []
            self._size = 0
            self._active = False
            if self._conn._pipeline is self:
                self._conn._pipeline = None

    def execute(
        self,
        sql: Any,
        parameters: Any = (),
        row_factory: Optional["RowFactory"] = None,
        *,
        binary: Optional[bool] = None,
    ) -> "Cursor":
        """Queue ``sql`` with ``parameters`` (any placeholder style the connection takes) and
        return the cursor its result will be loaded into at the next sync. ``binary`` asks for the
        result in the binary format (default: the connection's :attr:`~Connection.binary`)."""
        if not self._active:
            raise ProgrammingError("use the pipeline as a context manager: with conn.pipeline() as p")
        conn = self._conn
        if conn._broken or conn._closed:
            conn._ensure_open()
        binary = conn.binary if binary is None else binary
        compiled = _compile(_sql_text(sql, conn))
        with _scoped_adapters(conn._adapters):
            encoded, tags = _bind(_arrange(compiled, parameters))
        frames = b"".join(
            (
                proto.parse("", compiled.sql, tags),
                proto.bind("", "", encoded, binary),
                proto.describe_portal(""),
                proto.execute("", 0),
            )
        )
        cursor = Cursor(
            conn, row_factory if row_factory is not None else conn.row_factory, binary=binary
        )
        self._queued.append((frames, cursor))
        self._size += len(frames)
        if len(self._queued) >= PIPELINE_SYNC_STATEMENTS or self._size >= _COPY_CHUNK:
            self.sync()
        return cursor

    def sync(self) -> None:
        """Send the queued statements and load each result into its cursor. Raises the first
        error; the statements after it in this sync did not run."""
        if not self._queued:
            return
        queued, self._queued, self._size = self._queued, [], 0
        conn = self._conn
        conn._ensure_open()
        conn._begin_if_needed()
        conn._send(b"".join(frames for frames, _cursor in queued) + proto.sync())
        cursors = iter(cursor for _frames, cursor in queued)
        columns: List[str] = []
        column_types: List[Optional[str]] = []
        rows: List[List[Optional[bytes]]] = []
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = conn._read_message()
            if mtype in (proto.B_ROW_DESCRIPTION, proto.B_ROW_DESCRIPTION_TYPED):
                columns, column_types = _read_description(mtype, r)
            elif mtype == proto.B_DATA_ROW:
                rows.append(r.fields())
            elif mtype == proto.B_COMMAND_COMPLETE:
                cursor = next(cursors)
                cursor._load(columns, column_types, rows, r.string(), cursor.binary)
                columns, column_types, rows = [], [], []
            elif mtype == proto.B_ERROR:
                error = (r.string(), r.string())
            elif mtype == proto.B_NOTIFICATION_RESPONSE:
                conn._notifications.append(_decode_notification(r))
            elif mtype == proto.B_READY:
                r.u8()
                break
        if error is not None:
            raise from_sqlstate(*error)


class Cursor:
    """A PEP-249 cursor. Placeholders are positional ``$1``, ``$2`` (the server's
    native marker); pass the bound values as the ``parameters`` sequence.
    """

    def __init__(
        self,
        connection: Connection,
        row_factory: Optional["RowFactory"] = None,
        binary: bool = False,
    ) -> None:
        self._conn = connection
        #: Builds each fetched row from the cursor (see :mod:`nusadb.rows`); ``None`` for tuples.
        self.row_factory = row_factory
        #: Whether :meth:`execute` asks for results in the binary format (wire-protocol §11.2).
        self.binary = binary
        # Whether the result loaded now is in the binary format.
        self._result_binary = False
        self._make_row: Optional[Callable[[Tuple[Any, ...]], Any]] = None
        self._rows: List[List[Optional[bytes]]] = []
        self._pos = 0
        self._columns: List[str] = []
        self._column_types: List[Optional[str]] = []
        self.arraysize = 1
        self.rowcount = -1
        # PEP-249: the server reports no row ids.
        self.lastrowid = None
        self._closed = False

    @property
    def connection(self) -> Connection:
        """The connection this cursor runs on (PEP-249 extension)."""
        return self._conn

    @property
    def description(
        self,
    ) -> Optional[List[Tuple[str, Optional[str], None, None, None, None, None]]]:
        """PEP-249 7-tuples per column: ``(name, type_code, ...)``. ``type_code`` is the
        NusaDB type name (protocol 1.1) or ``None`` if the server did not report it; the
        remaining size/precision fields are ``None``.
        """
        if not self._columns:
            return None
        return [
            (name, type_code, None, None, None, None, None)
            for name, type_code in zip(
                self._columns, self._column_types or [None] * len(self._columns)
            )
        ]

    def execute(
        self,
        operation: Union[str, "PreparedStatement", Any],
        parameters: Optional[Sequence[Any]] = None,
        *,
        binary: Optional[bool] = None,
    ) -> "Cursor":
        """Run ``operation``: SQL text, a :mod:`nusadb.sql` object, or a statement from
        :meth:`Connection.prepare`. ``binary`` asks for this result in the binary format (default:
        the cursor's :attr:`binary`). A binary result needs the statement to run through Parse and
        Bind, so a statement without parameters goes that way too, unless it is one the server runs
        only as a simple query (``DECLARE``, ``FETCH``, ``MOVE``, ``CLOSE``, ``COPY``, or several
        statements in one string), which comes back in text."""
        self._check()
        binary = self.binary if binary is None else binary
        conn = self._conn
        if isinstance(operation, PreparedStatement):
            result = conn._query_prepared(
                self._statement_name(operation),
                _arrange(operation._compiled, parameters or ()),
                binary,
            )
            self._load(*result, binary)
            return self
        operation = _sql_text(operation, conn)
        if parameters:
            result = conn._extended_query(operation, parameters, binary)
        elif binary and not _needs_simple_query(operation):
            result = conn._extended_plain(operation, True)
        else:
            result = conn._simple_query(operation)
            binary = False
        self._load(*result, binary)
        return self

    def executemany(
        self,
        operation: Union[str, "PreparedStatement", Any],
        seq_of_parameters: Sequence[Sequence[Any]],
    ) -> None:
        """Run ``operation`` once per parameter set. The executions are pipelined (sent in
        batches without waiting for each reply), so a large batch costs a few round trips rather
        than one per row. The first error stops the batch and is raised; in autocommit mode the
        statements before it have each been committed."""
        self._check()
        if not isinstance(operation, PreparedStatement):
            operation = _sql_text(operation, self._conn)
        if isinstance(operation, PreparedStatement):
            total, counted = 0, False
            for parameters in seq_of_parameters:
                *_unused, tag = self._conn._query_prepared(
                    self._statement_name(operation), _arrange(operation._compiled, parameters)
                )
                affected = _affected(tag)
                if affected >= 0:
                    total, counted = total + affected, True
        else:
            total, counted = self._conn._execute_batch(operation, seq_of_parameters)
        self._rows = []
        self._pos = 0
        self._columns = []
        # Statements that matched no rows still report 0; -1 only when no count was reported.
        self.rowcount = total if counted else -1

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        self._check()
        return self._next_row()

    def _next_row(self) -> Any:
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        types = self._column_types or [None] * len(row)
        decode = self._decode_binary_cell if self._result_binary else self._decode_cell
        values = tuple(decode(cell, type_code) for cell, type_code in zip(row, types))
        if self.row_factory is None:
            return values
        if self._make_row is None:
            self._make_row = self.row_factory(self)
        return self._make_row(values)

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        self._check()
        n = self.arraysize if size is None else size
        out: List[Tuple[Any, ...]] = []
        for _ in range(n):
            row = self.fetchone()
            if row is None:
                break
            out.append(row)
        return out

    def fetchall(self) -> List[Tuple[Any, ...]]:
        self._check()
        out: List[Tuple[Any, ...]] = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            out.append(row)
        return out

    def close(self) -> None:
        self._closed = True
        self._rows = []
        self._pos = 0

    def callproc(self, procname: str, parameters: Optional[Sequence[Any]] = None) -> Sequence[Any]:
        """Call a stored procedure (``CALL name($1, ...)``) and return the parameters, as PEP-249
        specifies; any result rows are fetched as usual."""
        params = list(parameters or [])
        placeholders = ", ".join(f"${i}" for i in range(1, len(params) + 1))
        name = ".".join(_quote_identifier(part) for part in procname.split("."))
        self.execute(f"CALL {name}({placeholders})", params)
        return params

    def nextset(self) -> Optional[bool]:
        """Every statement returns one result set: there is never a next one."""
        self._check()
        return None

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
        """No-op (PEP-249 optional)."""

    def setoutputsize(self, size: int, column: Optional[int] = None) -> None:
        """No-op (PEP-249 optional)."""

    def __iter__(self) -> "Cursor":
        return self

    def __next__(self) -> Tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # --- internals ---

    def _check(self) -> None:
        if self._closed:
            raise ProgrammingError("cursor is closed")
        self._conn._ensure_open()

    def _statement_name(self, statement: "PreparedStatement") -> str:
        if statement.connection is not self._conn:
            raise ProgrammingError("the statement was prepared on another connection")
        if statement.closed:
            raise ProgrammingError("the prepared statement is closed")
        return statement.name

    def _load(
        self,
        columns: List[str],
        column_types: List[Optional[str]],
        rows: List[List[Optional[bytes]]],
        tag: Optional[str],
        binary: bool = False,
    ) -> None:
        self._columns = columns
        self._column_types = column_types
        self._rows = rows
        self._pos = 0
        self._result_binary = binary
        self._make_row = None
        # For a SELECT, rowcount is the number of rows; for DML, the affected count.
        self.rowcount = len(rows) if columns else _affected(tag)

    @staticmethod
    def _decode_cell(cell: Optional[bytes], type_code: Optional[str] = None) -> Any:
        """Decode a text-format field into a typed Python value (``None`` for SQL NULL).

        The column's wire type tag (protocol 1.1) drives the conversion, for full typed DB-API
        fidelity: ``BOOL`` → ``bool``, ``INT`` → ``int``, ``FLOAT`` → ``float``, ``NUMERIC`` →
        ``Decimal``, ``DATE``/``TIME``/``TIMESTAMP`` (and the ``…TZ`` forms) → :mod:`datetime`
        objects, ``UUID`` → :class:`uuid.UUID`, ``JSON`` → the parsed object, ``ARRAY`` → ``list``,
        ``BYTES`` → ``bytes``. ``TEXT`` and any unrecognised tag stay ``str`` (a non-UTF-8 field
        stays raw ``bytes``). A value that does not parse as its tag falls back to the raw text, so an
        unexpected wire form never raises mid-fetch.
        """
        if cell is None:
            return None
        custom = _CUSTOM_DECODERS.get(type_code) if _CUSTOM_DECODERS and type_code else None
        if custom is not None:
            return custom(cell.decode("utf-8"))
        if type_code == "BYTES":
            text = cell.decode("ascii")
            return bytes.fromhex(text[2:] if text.startswith("\\x") else text)
        try:
            text = cell.decode("utf-8")
        except UnicodeDecodeError:
            return cell
        # An array column (protocol 1.2 names it ``ARRAY:<element>``, e.g. ``ARRAY:INT``) parses to a
        # list and then casts each element at the element type; a plain ``ARRAY`` (1.1 / unknown
        # element) keeps the elements as ``str``.
        if type_code is not None and type_code.startswith("ARRAY"):
            try:
                items = _parse_array(text)
            except (ValueError, IndexError):
                return text
            element = type_code.partition(":")[2]
            elem_decoder = _CUSTOM_DECODERS.get(element) or _TYPED_DECODERS.get(element)
            if elem_decoder is None or elem_decoder is _parse_array:
                return items
            return _decode_array_elements(items, elem_decoder)
        decoder = _TYPED_DECODERS.get(type_code)
        if decoder is None:
            return text
        try:
            return decoder(text)
        except (ValueError, InvalidOperation, IndexError, KeyError, TypeError, ArithmeticError):
            # Malformed for its declared tag → return the raw text rather than failing the fetch.
            return text

    @classmethod
    def _decode_binary_cell(cls, cell: Optional[bytes], type_code: Optional[str] = None) -> Any:
        """Decode a binary-format field (wire-protocol §11.2) into the same Python value its text
        form decodes to. The types whose binary form is their text (``NUMERIC``, ``TEXT``,
        ``JSON``, arrays, ``VECTOR``, ...) go through :meth:`_decode_cell`, registered decoders
        included. A field that does not fit its type's layout raises :class:`InterfaceError`."""
        if cell is None:
            return None
        decode = _binary.DECODERS.get(type_code) if type_code is not None else None
        if decode is None:
            return cls._decode_cell(cell, type_code)
        try:
            return decode(cell)
        except (ValueError, struct.error) as exc:
            raise InterfaceError(f"malformed binary {type_code} value: {exc}") from None


_DEFAULTS = {
    "host": "127.0.0.1",
    "port": 5678,
    "user": "nusadb-root",
    "database": "nusadb",
    "password": "nusadb-root",
    "ssl": None,
    "timeout": None,
    "autocommit": True,
    "connect_timeout": None,
    "statement_timeout": None,
    "keepalive": None,
    "binary": False,
    "prepare_threshold": 5,
}


# The positional order ``connect`` took before it read connection URLs, kept for existing callers.
_POSITIONAL = ("host", "port", "user", "database", "password", "ssl", "timeout", "autocommit")


class ServerCursor(Cursor):
    """A named cursor whose query stays on the server (``DECLARE``), from
    ``conn.cursor(name=...)``. Rows come over as they are fetched: ``fetchone`` and ``fetchmany``
    ask for just those rows, iteration :attr:`itersize` rows at a time, so a result of any size is
    never held whole on the client. ``scroll`` moves within the result.

    A cursor declared without ``withhold`` lives until its transaction ends, so outside autocommit
    mode the driver's own transaction holds it; in autocommit mode only a ``withhold`` cursor can be
    declared. ``description`` is known once the first rows have been fetched. ``rowcount`` is the
    number of rows fetched so far and ``rownumber`` the index of the next row.
    """

    def __init__(
        self,
        connection: Connection,
        name: str,
        row_factory: Optional["RowFactory"],
        scrollable: Optional[bool],
        withhold: bool,
    ) -> None:
        super().__init__(connection, row_factory)
        self.name = name
        self.scrollable = scrollable
        self.withhold = withhold
        #: Rows fetched per round trip while iterating.
        self.itersize = 2000
        self._declared = False
        self._fetched = 0
        self.rownumber: Optional[int] = None

    def execute(  # type: ignore[override]
        self, operation: Any, parameters: Optional[Any] = None
    ) -> "ServerCursor":
        """Declare the cursor for ``operation`` (a query), closing the one declared before."""
        self._check()
        if isinstance(operation, PreparedStatement):
            raise NotSupportedError("a named cursor takes SQL text, not a prepared statement")
        operation = _sql_text(operation, self._conn)
        self._close_on_server()
        scroll = {None: "", True: "SCROLL ", False: "NO SCROLL "}[self.scrollable]
        hold = " WITH HOLD" if self.withhold else ""
        sql = f"DECLARE {_quote_identifier(self.name)} {scroll}CURSOR{hold} FOR {operation}"
        if parameters:
            self._conn._extended_query(sql, parameters, auto=False)
        else:
            self._conn._simple_query(sql)
        self._declared = True
        self._rows, self._pos, self._columns, self._column_types = [], 0, [], []
        self._fetched = 0
        self.rowcount = -1
        self.rownumber = 0
        return self

    def executemany(self, operation: Any, seq_of_parameters: Any) -> None:
        raise NotSupportedError("executemany is not available on a named cursor")

    def callproc(self, procname: str, parameters: Optional[Sequence[Any]] = None) -> Sequence[Any]:
        raise NotSupportedError("callproc is not available on a named cursor")

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        self._check_declared()
        if self._pos >= len(self._rows):
            self._fetch("FORWARD 1")
        return self._deliver()

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        self._check_declared()
        n = self.arraysize if size is None else size
        out = self._take(n)
        if len(out) < n:
            self._fetch(f"FORWARD {n - len(out)}")
            out.extend(self._take(n - len(out)))
        return out

    def fetchall(self) -> List[Tuple[Any, ...]]:
        self._check_declared()
        out = self._take(len(self._rows) - self._pos)
        self._fetch("FORWARD ALL")
        out.extend(self._take(len(self._rows)))
        return out

    def __next__(self) -> Tuple[Any, ...]:
        self._check_declared()
        if self._pos >= len(self._rows):
            self._fetch(f"FORWARD {max(1, self.itersize)}")
        row = self._deliver()
        if row is None:
            raise StopIteration
        return row

    def scroll(self, value: int, mode: str = "relative") -> None:
        """Move to another row: by ``value`` rows from the current one (``mode="relative"``) or to
        index ``value`` (``"absolute"``, 0 for the first row). The next fetch starts there. Moving
        outside the result raises ``IndexError`` and leaves the position where it was. Moving back
        needs a cursor declared ``scrollable=True``."""
        self._check_declared()
        if mode not in ("relative", "absolute"):
            raise ProgrammingError(f"scroll mode must be 'relative' or 'absolute', not {mode!r}")
        current = self.rownumber or 0
        target = current + value if mode == "relative" else value
        if target < 0:
            raise IndexError("scroll would move before the first row")
        # Positioned on row `target` (counting from 1), the next fetch returns index `target`.
        if not self._position(target) and target != 0:
            self._position(current)
            raise IndexError("scroll would move past the last row")
        self._rows, self._pos = [], 0
        self.rownumber = target

    def close(self) -> None:
        if not self._closed:
            try:
                self._close_on_server()
            finally:
                super().close()

    # --- internals ---

    def _check_declared(self) -> None:
        self._check()
        if not self._declared:
            raise ProgrammingError("execute a query on the named cursor first")

    def _fetch(self, how: str) -> None:
        columns, column_types, rows, _tag = self._conn._simple_query(
            f"FETCH {how} FROM {_quote_identifier(self.name)}"
        )
        if columns:
            self._columns, self._column_types = columns, column_types
            self._make_row = None
        self._rows, self._pos = rows, 0
        self._fetched += len(rows)
        self.rowcount = self._fetched

    def _position(self, row: int) -> bool:
        """Put the server cursor on ``row`` (1-based; 0 is before the first). Whether a row is
        there."""
        _columns, _types, rows, _tag = self._conn._simple_query(
            f"FETCH ABSOLUTE {row} FROM {_quote_identifier(self.name)}"
        )
        return bool(rows)

    def _deliver(self) -> Optional[Tuple[Any, ...]]:
        row = self._next_row()
        if row is not None and self.rownumber is not None:
            self.rownumber += 1
        return row

    def _take(self, n: int) -> List[Tuple[Any, ...]]:
        out: List[Tuple[Any, ...]] = []
        while len(out) < n:
            row = self._deliver()
            if row is None:
                break
            out.append(row)
        return out

    def _close_on_server(self) -> None:
        """``CLOSE`` the declared cursor, unless its transaction already ended (which closed it)
        or failed (which refuses every statement until rolled back)."""
        if not self._declared:
            return
        self._declared = False
        conn = self._conn
        if conn._closed or conn._status == "E" or (conn._status == "I" and not self.withhold):
            return
        try:
            # Sent as it is: closing must not open a transaction outside autocommit mode.
            conn._send(proto.query(f"CLOSE {_quote_identifier(self.name)}"))
            conn._read_until_ready()
        except ProgrammingError as exc:
            if getattr(exc, "sqlstate", None) != "34000":
                raise


def connect(dsn: Optional[str] = None, *args: Any, **kwargs: Any) -> Connection:
    """Open a connection (the PEP-249 module-level constructor).

    ``dsn`` is an optional connection URL (``nusadb://user:password@host:port/database?...``, see
    :mod:`nusadb.dsn`); keyword arguments override what it names. Without either the defaults
    match a stock NusaDB server: ``127.0.0.1:5678``, database ``nusadb``, superuser
    ``nusadb-root`` with password ``nusadb-root`` (a trust-on-startup server ignores the
    password). Pass ``password=None`` to send no password at all.

    Keyword arguments:

    * ``host``, ``port``, ``user``, ``database``, ``password``
    * ``ssl``: an :class:`ssl.SSLContext` to connect over TLS
    * ``timeout``: seconds each read or write on the socket may take (``None`` waits forever)
    * ``connect_timeout``: seconds connecting may take: the TCP connection, TLS and authentication
      (default ``timeout``)
    * ``statement_timeout``: seconds a statement may run before the server cancels it
    * ``keepalive``: seconds of idleness before TCP keepalive probes start
    * ``autocommit``: ``True`` (the default) runs each statement on its own; ``False`` opens a
      transaction before the first statement, ended by :meth:`Connection.commit` or
      :meth:`Connection.rollback`
    * ``binary``: ``True`` makes the connection's cursors ask for results in the binary format
      (default ``False``); the values decode to the same Python objects either way
    * ``prepare_threshold``: after a statement has run this many times with parameters on the
      connection it is prepared on the server and reused (default 5; ``None`` never, ``0`` from
      the first run)

    A first argument that is not a URL is a host, and further positional arguments follow the
    order ``host, port, user, database, password, ssl, timeout, autocommit``.
    """
    if (dsn is not None and "://" not in dsn) or args:
        positional = ((dsn,) if dsn is not None else ()) + args
        if dsn is not None and "://" in dsn:
            raise ProgrammingError("positional arguments after a connection URL are not accepted")
        if len(positional) > len(_POSITIONAL):
            raise ProgrammingError("too many positional connection arguments")
        for name, value in zip(_POSITIONAL, positional):
            if name in kwargs:
                raise ProgrammingError(f"{name} given both positionally and by keyword")
            kwargs[name] = value
        dsn = None
    unknown = set(kwargs) - set(_DEFAULTS)
    if unknown:
        raise ProgrammingError(f"unknown connection argument(s): {', '.join(sorted(unknown))}")
    from .dsn import parse_dsn

    options = dict(_DEFAULTS)
    if dsn is not None:
        options.update(parse_dsn(dsn))
    options.update(kwargs)
    try:
        return Connection(
            options["host"],
            options["port"],
            options["user"],
            options["database"],
            options["password"],
            options["ssl"],
            options["timeout"],
            options["autocommit"],
            connect_timeout=options["connect_timeout"],
            statement_timeout=options["statement_timeout"],
            keepalive=options["keepalive"],
            binary=options["binary"],
            prepare_threshold=options["prepare_threshold"],
        )
    except (OSError, DatabaseError):
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise OperationalError(str(exc)) from exc


def _set_keepalive(sock: socket.socket, idle: float) -> None:
    """Turn TCP keepalive on, probing after ``idle`` seconds of silence where the platform lets
    the delay be set (``TCP_KEEPIDLE`` on Linux, ``TCP_KEEPALIVE`` on macOS)."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    option = getattr(socket, "TCP_KEEPIDLE", None) or getattr(socket, "TCP_KEEPALIVE", None)
    if option is not None:
        sock.setsockopt(socket.IPPROTO_TCP, option, max(1, math.ceil(idle)))


#: How many prepared statements a connection keeps: those of :meth:`Connection.prepare_cached`
#: and those prepared automatically (see ``prepare_threshold`` in :func:`connect`).
STATEMENT_CACHE_SIZE = 256

# How many statements not yet prepared a connection counts the runs of.
_USES_TRACKED = 1024

# SQLSTATE of a prepared statement the server does not know.
_UNKNOWN_STATEMENT = "26000"

_TPC_PREPARED = (
    "the two-phase transaction is prepared; end it with tpc_commit() or tpc_rollback() first"
)


def _check_threshold(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProgrammingError(f"prepare_threshold is a whole number or None, not {value!r}")
    return value


def _format_mismatch(expected: int) -> str:
    if expected == 1:
        return "binary=True needs a binary COPY: add WITH (FORMAT binary) to the statement"
    return "the statement is a binary COPY: pass binary=True"

# How many executions :meth:`Cursor.executemany` sends before reading their replies.
_BATCH_ROWS = 256

#: Builds a row from a cursor: ``row_factory(cursor)`` returns a function from the row's values
#: (a tuple) to the row the caller gets. See :mod:`nusadb.rows`.
RowFactory = Callable[["Cursor"], Callable[[Tuple[Any, ...]], Any]]


class PreparedStatement:
    """A statement parsed once on the server (:meth:`Connection.prepare`)."""

    def __init__(self, connection: Connection, name: str, sql: str) -> None:
        self.connection = connection
        self.name = name
        self.sql = sql
        self.closed = False
        # How this statement's parameters map onto its placeholders (by position or by name).
        self._compiled = _compile(sql)

    def close(self) -> None:
        """Free the statement on the server. Closing it twice is harmless."""
        if self.closed:
            return
        self.closed = True
        if not self.connection.closed and not self.connection.broken:
            self.connection._ensure_open()
            self.connection._close_statement(self.name)

    def __repr__(self) -> str:
        return f"PreparedStatement({self.sql!r})"


_ISOLATION_LEVELS = {
    "read uncommitted": "READ UNCOMMITTED",
    "read committed": "READ COMMITTED",
    "repeatable read": "REPEATABLE READ",
    "serializable": "SERIALIZABLE",
}


class Transaction:
    """The context manager :meth:`Connection.transaction` returns."""

    def __init__(
        self, connection: Connection, isolation: Optional[str], read_only: Optional[bool]
    ) -> None:
        if isolation is not None and isolation.lower() not in _ISOLATION_LEVELS:
            raise ProgrammingError(
                f"unknown isolation level {isolation!r}; use one of "
                + ", ".join(repr(level) for level in _ISOLATION_LEVELS)
            )
        self.connection = connection
        self._isolation = isolation
        self._read_only = read_only
        self._savepoint: Optional[str] = None

    def __enter__(self) -> "Transaction":
        conn = self.connection
        conn._ensure_open()
        if conn.transaction_status == "idle":
            sql = "BEGIN"
            if self._isolation is not None:
                sql += " ISOLATION LEVEL " + _ISOLATION_LEVELS[self._isolation.lower()]
            if self._read_only is not None:
                sql += " READ ONLY" if self._read_only else " READ WRITE"
            conn._run_control(sql)
        else:
            if self._isolation is not None or self._read_only is not None:
                raise ProgrammingError(
                    "isolation and read_only apply only to the outermost transaction"
                )
            self._savepoint = f"nusadb_savepoint_{next(conn._names)}"
            conn._run_control("SAVEPOINT " + self._savepoint)
        return self

    def __exit__(self, exc_type: Any, *_exc: object) -> None:
        conn = self.connection
        if exc_type is not None:
            # Leaving on an exception: undo what can be undone, but never let a failure doing so
            # replace the exception that ended the block (a Ctrl-C included).
            # (A block stopped part way through a reply is out of step: the undo below finds that
            # and breaks the connection, and the error it raises is set aside like any other.)
            if conn.closed or conn.broken:
                return
            try:
                if self._savepoint is None:
                    conn.rollback()
                else:
                    conn._ensure_open()
                    conn._run_control("ROLLBACK TO SAVEPOINT " + self._savepoint)
                    conn._run_control("RELEASE SAVEPOINT " + self._savepoint)
            except (DatabaseError, InterfaceError):
                pass
            return
        if self._savepoint is None:
            conn.commit()
        else:
            conn._ensure_open()
            conn._run_control("RELEASE SAVEPOINT " + self._savepoint)


class RowStream:
    """The rows of a query read a batch at a time (:meth:`Connection.stream`). Iterate it for the
    rows; :attr:`description` and :attr:`rowcount` (once read to the end) are as on a cursor."""

    def __init__(
        self,
        connection: Connection,
        sql: str,
        parameters: Sequence[Any],
        batch_rows: int,
        row_factory: Optional[RowFactory],
        binary: bool = False,
    ) -> None:
        self._conn = connection
        self._batch = batch_rows
        # Decoding and row building are a cursor's; this one only holds the current batch.
        self._cursor = Cursor(
            connection, row_factory if row_factory is not None else connection.row_factory, binary
        )
        self._done = False
        self.rowcount = -1
        connection._begin_if_needed()
        compiled = _compile(sql)
        with _scoped_adapters(connection._adapters):
            encoded, tags = _bind(_arrange(compiled, parameters))
        connection._send(proto.parse("", compiled.sql, tags))
        connection._send(proto.bind("", "", encoded, binary))
        connection._send(proto.describe_portal(""))
        connection._send(proto.execute("", batch_rows))
        connection._stream = weakref.ref(self)
        self._fetch(ask=False)

    @property
    def description(self) -> Any:
        return self._cursor.description

    def _fetch(self, ask: bool) -> None:
        conn = self._conn
        if ask:
            conn._send(proto.execute("", self._batch))
        rows: List[List[Optional[bytes]]] = []
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = conn._read_message()
            if mtype == proto.B_ROW_DESCRIPTION_TYPED:
                count = r.u16()
                columns, types = [], []
                for _ in range(count):
                    columns.append(r.string())
                    types.append(proto.type_name(r.u8()))
                self._cursor._load(columns, types, [], None, self._cursor.binary)
            elif mtype == proto.B_ROW_DESCRIPTION:
                count = r.u16()
                names = [r.string() for _ in range(count)]
                self._cursor._load(names, [None] * count, [], None, self._cursor.binary)
            elif mtype == proto.B_DATA_ROW:
                rows.append(r.fields())
            elif mtype == proto.B_PORTAL_SUSPENDED:
                break
            elif mtype == proto.B_COMMAND_COMPLETE:
                self.rowcount = _affected(r.string())
                self._finish()
                break
            elif mtype == proto.B_ERROR:
                error = (r.string(), r.string())
                self._finish()
                break
            elif mtype == proto.B_NOTIFICATION_RESPONSE:
                conn._notifications.append(_decode_notification(r))
        self._cursor._rows = rows
        self._cursor._pos = 0
        if error is not None:
            raise from_sqlstate(*error)

    def _finish(self) -> None:
        self._done = True
        self._conn._end_stream()
        if self._conn._mid_message:
            # Stopped inside a message: there is no message boundary to drain from.
            self._conn._fail()
            return
        self._conn._send(proto.sync())
        self._conn._drain_to_ready()

    def __iter__(self) -> Iterator[Any]:
        return self

    def __next__(self) -> Any:
        while True:
            if self._cursor._pos < len(self._cursor._rows):
                return self._cursor._next_row()
            if self._done:
                raise StopIteration
            self._fetch(ask=True)

    def close(self) -> None:
        """Stop reading: the rows not yet read are discarded and the connection is free again."""
        if not self._done and not self._conn.closed and not self._conn.broken:
            self._finish()
        self._done = True

    def __enter__(self) -> "RowStream":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
