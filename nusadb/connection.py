"""PEP-249 ``Connection`` and ``Cursor`` over the Nusa Wire Protocol."""

from __future__ import annotations

import datetime as _dt
import itertools
import json as _json
import re as _re
import socket
import ssl as ssl_module
import uuid as _uuid
from decimal import Decimal, InvalidOperation
from typing import Any, BinaryIO, List, NamedTuple, Optional, Sequence, Tuple

from . import _protocol as proto
from . import _scram
from .exceptions import (
    DatabaseError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    from_sqlstate,
)

_SCRAM_MECHANISM = "SCRAM-SHA-256"

# A collected statement result: column names, rows (each a list of optional bytes), command tag.
_Result = Tuple[
    List[str], List[Optional[str]], List[List[Optional[bytes]]], Optional[str]
]


def _encode_param(value: Any) -> Optional[bytes]:
    """Encode one bound parameter as wire **text** bytes (``None`` = SQL NULL)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return b"true" if value else b"false"
    if isinstance(value, (bytes, bytearray, memoryview)):
        # Bind raw bytes as the BYTEA text form \x<hex>, which the server coerces into a BYTEA column.
        return b"\\x" + bytes(value).hex().encode("ascii")
    if isinstance(value, str):
        return value.encode("utf-8")
    return str(value).encode("utf-8")


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
    """Quote a string as a SQL literal (``'…'``, doubling ``'``) — used for a NOTIFY payload."""
    return "'" + text.replace("'", "''") + "'"


def _decode_notification(r: "proto.Reader") -> "Notification":
    """Decode a ``NotificationResponse`` body: ``[pid:u32][channel:str][payload:str]``."""
    return Notification(pid=r.u32(), channel=r.string(), payload=r.string())


def _pad_tz_offset(text: str) -> str:
    """Pad a bare ``+HH`` / ``-HH`` timezone offset to ``+HH:00`` so ``fromisoformat`` (Python < 3.11)
    can parse it; any other form is left untouched."""
    return _re.sub(r"([+-]\d{2})$", r"\g<1>:00", text)


def _decode_time(text: str) -> _dt.time:
    return _dt.time.fromisoformat(_pad_tz_offset(text))


def _decode_timestamp(text: str) -> _dt.datetime:
    return _dt.datetime.fromisoformat(_pad_tz_offset(text))


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
    or months is returned unchanged as ``str`` — a month is not an exact number of days, so coercing
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
        self.autocommit = autocommit
        self._in_txn = False
        self._notifications: List["Notification"] = []

        raw = socket.create_connection((host, port), timeout=timeout)
        # Disable Nagle's algorithm: this is a request/response protocol, so
        # coalescing small frames only adds delayed-ACK latency per round-trip.
        raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if ssl is not None:
            raw = ssl.wrap_socket(raw, server_hostname=host)
        self._sock = raw
        try:
            self._handshake(user, database)
        except Exception:
            self._sock.close()
            self._closed = True
            raise

    # --- PEP-249 connection API ---

    def cursor(self) -> "Cursor":
        self._ensure_open()
        return Cursor(self)

    def commit(self) -> None:
        """Commit the current transaction. A no-op in autocommit mode."""
        self._ensure_open()
        if not self.autocommit and self._in_txn:
            self._run_control("COMMIT")
            self._in_txn = False

    def rollback(self) -> None:
        """Roll back the current transaction, discarding its uncommitted changes.
        A no-op in autocommit mode.
        """
        self._ensure_open()
        if not self.autocommit and self._in_txn:
            self._run_control("ROLLBACK")
            self._in_txn = False

    def savepoint(self, name: str) -> None:
        """Establish a savepoint inside the current transaction (``SAVEPOINT name``).

        A savepoint is a named marker you can later roll back to without ending the
        transaction. Requires an open transaction, so it is only valid with
        ``autocommit=False``; a ``BEGIN`` is opened lazily if none is active yet.
        """
        self._ensure_open()
        if self.autocommit:
            raise ProgrammingError("cannot set a savepoint in autocommit mode")
        self._begin_if_needed()
        self._run_control("SAVEPOINT " + _quote_identifier(name))

    def rollback_to(self, name: str) -> None:
        """Roll back to a savepoint (``ROLLBACK TO SAVEPOINT name``).

        Undoes everything done since the savepoint while leaving the transaction open
        so it can continue.
        """
        self._ensure_open()
        if self.autocommit or not self._in_txn:
            raise ProgrammingError("no active transaction to roll back to a savepoint")
        self._run_control("ROLLBACK TO SAVEPOINT " + _quote_identifier(name))

    def release_savepoint(self, name: str) -> None:
        """Release (forget) a savepoint (``RELEASE SAVEPOINT name``), keeping its work."""
        self._ensure_open()
        if self.autocommit or not self._in_txn:
            raise ProgrammingError("no active transaction to release a savepoint")
        self._run_control("RELEASE SAVEPOINT " + _quote_identifier(name))

    # --- asynchronous notifications (LISTEN/NOTIFY) ---

    def listen(self, channel: str) -> None:
        """Subscribe to asynchronous notifications on ``channel`` (``LISTEN channel``). Afterwards a
        ``NOTIFY`` from any connection on the same database is delivered here — collect notifications
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
        responses). Does not touch the socket — use :meth:`poll` to wait for new ones."""
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
        self._sock.settimeout(timeout)
        try:
            mtype, r = self._read_message()
        except (socket.timeout, BlockingIOError):
            return None
        finally:
            self._sock.settimeout(None)
        if mtype == proto.B_NOTIFICATION_RESPONSE:
            return _decode_notification(r)
        if mtype == proto.B_ERROR:
            raise from_sqlstate(r.string(), r.string())
        raise InterfaceError(
            "unexpected message %r while polling for notifications" % chr(mtype)
        )

    def _run_control(self, sql: str) -> None:
        """Send a transaction-control statement (`BEGIN`/`COMMIT`/`ROLLBACK`) directly."""
        self._send(proto.query(sql))
        self._read_until_ready()

    def _begin_if_needed(self) -> None:
        """Open a transaction before the first statement when not in autocommit mode."""
        if not self.autocommit and not self._in_txn:
            self._run_control("BEGIN")
            self._in_txn = True

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._send(proto.terminate())
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
        raw = socket.create_connection((self._host, self._port))
        try:
            if self._ssl is not None:
                raw = self._ssl.wrap_socket(raw, server_hostname=self._host)
            raw.sendall(proto.cancel_request(self._pid, self._secret))
        finally:
            raw.close()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    # --- low-level protocol ---

    def _ensure_open(self) -> None:
        if self._closed:
            raise InterfaceError("connection is closed")

    def _send(self, frame: bytes) -> None:
        self._sock.sendall(frame)

    def _recv_exact(self, n: int) -> bytes:
        chunks: List[bytes] = []
        remaining = n
        while remaining > 0:
            chunk = self._sock.recv(remaining)
            if not chunk:
                raise OperationalError("server closed the connection")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _read_message(self) -> Tuple[int, proto.Reader]:
        mtype, plen = proto.decode_frame_header(self._recv_exact(proto.HEADER_LEN))
        payload = self._recv_exact(plen) if plen else b""
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
        encoded = [_encode_param(p) for p in params]
        self._send(proto.parse("", sql))
        self._send(proto.bind("", "", encoded))
        self._send(proto.describe_portal(""))
        self._send(proto.execute("", 0))
        self._send(proto.sync())
        return self._read_until_ready()

    def _prepare(self, sql: str) -> str:
        name = f"nusa_stmt_{next(self._names)}"
        self._send(proto.parse(name, sql))
        self._send(proto.sync())
        self._read_until_ready()  # ParseComplete + Ready; raises on a parse error
        return name

    # --- COPY bulk load / export (wire-protocol.md §12) ---

    def copy_in(self, sql: str, source: "BinaryIO") -> int:
        r"""Bulk-load via ``COPY ... FROM STDIN`` (§12.1).

        Stream ``source`` (a binary file-like whose ``read(size)`` yields bytes already in the
        server's text format — tab-delimited fields, ``\N`` for SQL NULL, one row per line) as
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
                except OSError:
                    pass
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
        encoded = [_encode_param(p) for p in params]
        portal = f"nusa_portal_{next(self._names)}"
        self._send(proto.bind(portal, name, encoded))
        self._send(proto.describe_portal(portal))
        self._send(proto.execute(portal, 0))
        self._send(proto.sync())
        return self._read_until_ready()


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

    def __init__(self, connection: Connection) -> None:
        self._conn = connection
        self._rows: List[List[Optional[bytes]]] = []
        self._pos = 0
        self._columns: List[str] = []
        self._column_types: List[Optional[str]] = []
        self.arraysize = 1
        self.rowcount = -1
        self._closed = False

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

    def execute(self, operation: str, parameters: Optional[Sequence[Any]] = None) -> "Cursor":
        self._check()
        if parameters:
            columns, column_types, rows, tag = self._conn._extended_query(operation, parameters)
        else:
            columns, column_types, rows, tag = self._conn._simple_query(operation)
        self._load(columns, column_types, rows, tag)
        return self

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]
    ) -> None:
        self._check()
        total = 0
        for parameters in seq_of_parameters:
            _columns, _column_types, _rows, tag = self._conn._extended_query(
                operation, parameters
            )
            affected = _affected(tag)
            if affected >= 0:
                total += affected
        self._rows = []
        self._pos = 0
        self._columns = []
        self.rowcount = total if total else -1

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        self._check()
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        types = self._column_types or [None] * len(row)
        return tuple(self._decode_cell(cell, type_code) for cell, type_code in zip(row, types))

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
            elem_decoder = _TYPED_DECODERS.get(element)
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


def connect(
    host: str = "127.0.0.1",
    port: int = 5678,
    user: str = "nusa-root",
    database: str = "nusadb",
    password: Optional[str] = "nusa-root",
    ssl: Optional[ssl_module.SSLContext] = None,
    timeout: Optional[float] = None,
    autocommit: bool = True,
) -> Connection:
    """Open a connection (the PEP-249 module-level constructor).

    The defaults match a stock NusaDB server: database ``nusadb``, superuser
    ``nusa-root`` with password ``nusa-root`` (a trust-on-startup server ignores the
    password). Pass ``password=None`` to send no password at all.
    """
    try:
        return Connection(
            host, port, user, database, password, ssl, timeout, autocommit
        )
    except (OSError, DatabaseError):
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise OperationalError(str(exc)) from exc
