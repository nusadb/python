"""PEP-249 ``Connection`` and ``Cursor`` over the Nusa Wire Protocol."""

from __future__ import annotations

import datetime as _dt
import itertools
import json as _json
import math
import re as _re
import select
import socket
import ssl as ssl_module
import uuid as _uuid
from decimal import Decimal, InvalidOperation
import weakref
from collections import OrderedDict
from typing import (
    Any,
    BinaryIO,
    Callable,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from . import _protocol as proto
from . import _scram
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


def _to_native_placeholders(sql: str) -> str:
    """Translate DB-API ``numeric`` placeholders (``:1``) to the server-native ``$1`` form.

    The module declares ``paramstyle = "numeric"``, so portable PEP-249 code writes ``:1``; the Nusa
    wire protocol's own marker is ``$1``. This rewrites ``:`` followed by digits, but only where it
    is a real placeholder, never inside a single-quoted string, a double-quoted identifier, a
    dollar-quoted string, a line/block comment, or a ``::`` type cast. A query that already uses the
    native ``$1`` form is left untouched (only ``:digits`` is rewritten), so existing code keeps
    working.
    """
    out: List[str] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c == "'" or c == '"':  # quoted string / identifier ('' and "" escape the quote)
            quote = c
            out.append(c)
            i += 1
            while i < n:
                out.append(sql[i])
                if sql[i] == quote:
                    if i + 1 < n and sql[i + 1] == quote:
                        out.append(sql[i + 1])
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            continue
        if c == "-" and i + 1 < n and sql[i + 1] == "-":  # line comment
            while i < n and sql[i] != "\n":
                out.append(sql[i])
                i += 1
            continue
        if c == "/" and i + 1 < n and sql[i + 1] == "*":  # block comment (the server nests)
            depth = 1
            out.append("/*")
            i += 2
            while i < n and depth > 0:
                if sql[i] == "/" and i + 1 < n and sql[i + 1] == "*":
                    depth += 1
                    out.append("/*")
                    i += 2
                elif sql[i] == "*" and i + 1 < n and sql[i + 1] == "/":
                    depth -= 1
                    out.append("*/")
                    i += 2
                else:
                    out.append(sql[i])
                    i += 1
            continue
        if c == "$":  # dollar-quoted string $tag$...$tag$, or a native $1 marker (left as-is)
            j = i + 1
            while j < n and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            if j < n and sql[j] == "$":
                tag = sql[i : j + 1]
                out.append(tag)
                i = j + 1
                end = sql.find(tag, i)
                if end == -1:
                    out.append(sql[i:])
                    i = n
                else:
                    out.append(sql[i : end + len(tag)])
                    i = end + len(tag)
                continue
            out.append(c)
            i += 1
            continue
        if c == ":":
            if i + 1 < n and sql[i + 1] == ":":  # :: type cast, never a placeholder
                out.append("::")
                i += 2
                continue
            j = i + 1
            while j < n and sql[j].isdigit():
                j += 1
            if j > i + 1:  # :<digits> → $<digits>
                out.append("$")
                out.append(sql[i + 1 : j])
                i = j
                continue
        out.append(c)
        i += 1
    return "".join(out)


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
    if _ADAPTERS and value is not None:
        for klass in type(value).__mro__:
            adapt = _ADAPTERS.get(klass)
            if adapt is not None:
                return adapt(value)
    return value


def _bound_chunks(
    seq_of_parameters: Any,
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
# typed Python value, for full typed DB-API fidelity. Tags not listed (TEXT, VECTOR, UNKNOWN) stay
# as ``str``; BYTES is handled separately in ``_decode_cell``.
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
}


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
        self._statements: "OrderedDict[str, PreparedStatement]" = OrderedDict()
        # The open row stream (weakly held: one dropped unread is ended at the next call).
        self._stream: Optional["weakref.ReferenceType[RowStream]"] = None

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

    def cursor(self, row_factory: Optional["RowFactory"] = None) -> "Cursor":
        """A new cursor. ``row_factory`` (default: the connection's ``row_factory``) shapes the
        rows it fetches; see :mod:`nusadb.rows`."""
        self._ensure_open()
        return Cursor(self, row_factory if row_factory is not None else self.row_factory)

    def prepare(self, sql: str) -> "PreparedStatement":
        """Parse ``sql`` once on the server; run it with :meth:`Cursor.execute` or
        :meth:`Cursor.executemany` as many times as needed. Its parameter types are inferred from
        the statement. The server stores the text as it is, so a mistake in it is reported when the
        statement first runs. Free it with :meth:`PreparedStatement.close`."""
        self._ensure_open()
        return PreparedStatement(self, self._prepare(sql), sql)

    def prepare_cached(self, sql: str) -> "PreparedStatement":
        """:meth:`prepare`, reusing the statement already prepared for the same ``sql`` on this
        connection. Up to :data:`STATEMENT_CACHE_SIZE` statements are kept; past that the least
        recently used is closed."""
        self._ensure_open()
        cached = self._statements.get(sql)
        if cached is not None and not cached.closed:
            self._statements.move_to_end(sql)
            return cached
        statement = self.prepare(sql)
        self._statements[sql] = statement
        while len(self._statements) > STATEMENT_CACHE_SIZE:
            _sql, oldest = self._statements.popitem(last=False)
            oldest.close()
        return statement

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
        sql: str,
        parameters: Sequence[Any] = (),
        batch_rows: int = 1024,
        row_factory: Optional["RowFactory"] = None,
    ) -> "RowStream":
        """Run a query and read its rows ``batch_rows`` at a time, so a result of any size is
        never held whole on the client. The connection runs nothing else until the stream is read
        to the end or closed (use it as a context manager)."""
        self._ensure_open()
        return RowStream(self, sql, parameters, max(1, batch_rows), row_factory)

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
        # Wait for the first byte without reading any, so a timeout never cuts a message in half.
        pending = isinstance(self._sock, ssl_module.SSLSocket) and self._sock.pending() > 0
        if not pending:
            try:
                ready, _, _ = select.select([self._sock], [], [], timeout)
            except (OSError, ValueError) as exc:
                self._fail()
                raise OperationalError(f"waiting for a notification failed: {exc}") from exc
            if not ready:
                return None
        # Interrupted part way through the message, the connection could not be read again.
        self._pending = True
        mtype, r = self._read_message()
        self._pending = False
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
        """Open a transaction before the first statement when not in autocommit mode."""
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

    def _read_until_ready(self) -> _Result:
        columns: List[str] = []
        column_types: List[Optional[str]] = []
        rows: List[List[Optional[bytes]]] = []
        tag: Optional[str] = None
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = self._read_message()
            if mtype == proto.B_ROW_DESCRIPTION:
                count = r.u16()
                columns = [r.string() for _ in range(count)]
                column_types = [None] * count
            elif mtype == proto.B_ROW_DESCRIPTION_TYPED:
                # Protocol 1.1: each column is a name plus a 1-byte type tag (§9.2).
                count = r.u16()
                columns = []
                column_types = []
                for _ in range(count):
                    columns.append(r.string())
                    column_types.append(proto.type_name(r.u8()))
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

    def _extended_query(self, sql: str, params: Sequence[Any]) -> _Result:
        self._begin_if_needed()
        sql = _to_native_placeholders(sql)
        encoded, tags = _bind(params)
        self._send(proto.parse("", sql, tags))
        self._send(proto.bind("", "", encoded))
        self._send(proto.describe_portal(""))
        self._send(proto.execute("", 0))
        self._send(proto.sync())
        return self._read_until_ready()

    def _prepare(self, sql: str) -> str:
        name = f"nusadb_stmt_{next(self._names)}"
        self._send(proto.parse(name, _to_native_placeholders(sql)))
        self._send(proto.sync())
        self._read_until_ready()  # ParseComplete + Ready; raises on a parse error
        return name

    # --- COPY bulk load / export (wire-protocol.md §12) ---

    def copy_in(self, sql: str, source: "BinaryIO") -> int:
        r"""Bulk-load via ``COPY ... FROM STDIN`` (§12.1).

        Stream ``source`` (a binary file-like whose ``read(size)`` yields bytes already in the
        server's text format: tab-delimited fields, ``\N`` for SQL NULL, one row per line) as
        ``CopyData`` frames, then finish. Returns the number of rows loaded. A read error on
        ``source`` sends ``CopyFail`` and leaves the connection ready; a refused COPY (bad SQL, an
        RLS-protected table) raises the server error.
        """
        self._ensure_open()
        self._begin_if_needed()
        self._send(proto.query(sql))
        self._await_copy_start(proto.B_COPY_IN)
        while True:
            try:
                chunk = source.read(65536)
            except Exception as exc:  # noqa: BLE001 - report any source failure, stay in sync
                try:
                    self._send(proto.copy_fail("nusadb: client read error during COPY"))
                    self._drain_to_ready()
                except (OSError, DatabaseError, InterfaceError):
                    pass  # the connection is broken now; the source's error is the one to report
                raise OperationalError(f"COPY source read failed: {exc}") from exc
            if not chunk:
                break
            self._send(proto.copy_data(chunk))
        self._send(proto.copy_done())
        return self._finish_copy()

    def copy_out(self, sql: str, sink: "BinaryIO") -> int:
        r"""Bulk-export via ``COPY ... TO STDOUT`` (§12.2).

        Write each ``CopyData`` chunk the server streams into ``sink`` (a binary file-like with
        ``write(bytes)``); the bytes are in the server's text format (tab-delimited, ``\N`` for
        NULL). Returns the number of rows exported.
        """
        self._ensure_open()
        self._begin_if_needed()
        self._send(proto.query(sql))
        self._await_copy_start(proto.B_COPY_OUT)
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

    def _await_copy_start(self, expected: int) -> None:
        """Read until the expected ``CopyInResponse``/``CopyOutResponse``; a refused COPY arrives as
        ``Error`` then ``ReadyForQuery`` and is raised, the connection left ready."""
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = self._read_message()
            if mtype == expected:
                return
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

    def _query_prepared(self, name: str, params: Sequence[Any]) -> _Result:
        self._begin_if_needed()
        encoded, _tags = _bind(params)
        self._send(proto.bind("", name, encoded))
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
        sql = _to_native_placeholders(sql)
        total, counted = 0, False
        for chunk, failure in _bound_chunks(seq_of_parameters):
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


def _affected(tag: Optional[str]) -> int:
    """Parse the trailing count of a command tag (``INSERT 3`` -> 3), else -1."""
    if not tag:
        return -1
    last = tag.rsplit(" ", 1)[-1]
    try:
        return int(last)
    except ValueError:
        return -1


class Cursor:
    """A PEP-249 cursor. Placeholders are positional ``$1``, ``$2`` (the server's
    native marker); pass the bound values as the ``parameters`` sequence.
    """

    def __init__(
        self, connection: Connection, row_factory: Optional["RowFactory"] = None
    ) -> None:
        self._conn = connection
        #: Builds each fetched row from the cursor (see :mod:`nusadb.rows`); ``None`` for tuples.
        self.row_factory = row_factory
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
        operation: Union[str, "PreparedStatement"],
        parameters: Optional[Sequence[Any]] = None,
    ) -> "Cursor":
        """Run ``operation``: SQL text, or a statement from :meth:`Connection.prepare`."""
        self._check()
        if isinstance(operation, PreparedStatement):
            result = self._conn._query_prepared(
                self._statement_name(operation), parameters or ()
            )
            self._load(*result)
            return self
        if parameters:
            columns, column_types, rows, tag = self._conn._extended_query(operation, parameters)
        else:
            columns, column_types, rows, tag = self._conn._simple_query(operation)
        self._load(columns, column_types, rows, tag)
        return self

    def executemany(
        self,
        operation: Union[str, "PreparedStatement"],
        seq_of_parameters: Sequence[Sequence[Any]],
    ) -> None:
        """Run ``operation`` once per parameter set. The executions are pipelined (sent in
        batches without waiting for each reply), so a large batch costs a few round trips rather
        than one per row. The first error stops the batch and is raised; in autocommit mode the
        statements before it have each been committed."""
        self._check()
        if isinstance(operation, PreparedStatement):
            total, counted = 0, False
            for parameters in seq_of_parameters:
                *_unused, tag = self._conn._query_prepared(
                    self._statement_name(operation), parameters
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
        values = tuple(self._decode_cell(cell, type_code) for cell, type_code in zip(row, types))
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
    ) -> None:
        self._columns = columns
        self._column_types = column_types
        self._rows = rows
        self._pos = 0
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
}


# The positional order ``connect`` took before it read connection URLs, kept for existing callers.
_POSITIONAL = ("host", "port", "user", "database", "password", "ssl", "timeout", "autocommit")


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


#: How many statements :meth:`Connection.prepare_cached` keeps per connection.
STATEMENT_CACHE_SIZE = 256

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
    ) -> None:
        self._conn = connection
        self._batch = batch_rows
        # Decoding and row building are a cursor's; this one only holds the current batch.
        self._cursor = Cursor(
            connection, row_factory if row_factory is not None else connection.row_factory
        )
        self._done = False
        self.rowcount = -1
        connection._begin_if_needed()
        encoded, tags = _bind(parameters)
        connection._send(proto.parse("", _to_native_placeholders(sql), tags))
        connection._send(proto.bind("", "", encoded))
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
                self._cursor._load(columns, types, [], None)
            elif mtype == proto.B_ROW_DESCRIPTION:
                count = r.u16()
                self._cursor._load([r.string() for _ in range(count)], [None] * count, [], None)
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
