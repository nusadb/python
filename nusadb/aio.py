"""The asyncio API: the same driver, with every call that talks to the server awaited.

::

    import nusadb.aio

    conn = await nusadb.aio.connect("nusadb://app@db.example:5678/sales")
    async with conn:
        cur = conn.cursor()
        await cur.execute("SELECT id, name FROM customers WHERE region = $1", ["west"])
        for row in await cur.fetchall():
            ...
        async with conn.transaction():
            await cur.execute("UPDATE customers SET seen = now() WHERE id = $1", [7])

Connection options, URLs, parameter types, row factories and adapters are those of
:func:`nusadb.connect`. One connection runs one statement at a time; tasks that share it take turns.
A task cancelled while it waits on the server leaves the connection unusable (its reply is
unread), so the next call raises :class:`~nusadb.OperationalError`; open a new connection.
"""

from __future__ import annotations

import asyncio
import math
import socket
import ssl as ssl_module
import time
from collections import OrderedDict
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from . import _protocol as proto
from . import _scram
from .connection import (
    _DEFAULTS,
    _ISOLATION_LEVELS,
    Cursor,
    Notification,
    RowFactory,
    _affected,
    _bind,
    _bound_chunks,
    _decode_notification,
    _quote_identifier,
    _quote_literal,
    _Result,
    _set_keepalive,
    _to_native_placeholders,
)
from .exceptions import (
    DatabaseError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
    TransactionRolledBack,
    from_sqlstate,
)

_SCRAM_MECHANISM = "SCRAM-SHA-256"


async def connect(dsn: Optional[str] = None, **kwargs: Any) -> "AsyncConnection":
    """Open a connection; the arguments are those of :func:`nusadb.connect` (keyword only after
    the optional URL)."""
    unknown = set(kwargs) - set(_DEFAULTS)
    if unknown:
        raise ProgrammingError(f"unknown connection argument(s): {', '.join(sorted(unknown))}")
    from .dsn import parse_dsn

    options = dict(_DEFAULTS)
    if dsn is not None:
        options.update(parse_dsn(dsn))
    options.update(kwargs)
    conn = AsyncConnection(options)
    await conn._open()
    return conn


class AsyncConnection:
    """A connection whose server calls are coroutines. Create one with :func:`connect`."""

    def __init__(self, options: Dict[str, Any]) -> None:
        self._options = options
        self._host: str = options["host"]
        self._port: int = options["port"]
        self._user: str = options["user"]
        self._password: Optional[str] = options["password"]
        self._ssl: Optional[ssl_module.SSLContext] = options["ssl"]
        self._timeout: Optional[float] = options["timeout"]
        self._statement_timeout: Optional[float] = options["statement_timeout"]
        self._autocommit: bool = options["autocommit"]
        self._status = "I"
        self._pending = False
        self._mid_message = False
        self._broken = False
        self._closed = True
        self._pid: Optional[int] = None
        self._secret: Optional[int] = None
        self._names = iter(range(1, 1 << 62))
        self._notifications: List[Notification] = []
        self._lock = asyncio.Lock()
        self._cache_lock = asyncio.Lock()
        self._stream: Optional["AsyncRowStream"] = None
        self._statements: "OrderedDict[str, AsyncPreparedStatement]" = OrderedDict()
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        #: The default ``row_factory`` of this connection's cursors (``None``: tuples).
        self.row_factory: Optional[RowFactory] = None

    # --- opening and closing ---

    async def _open(self) -> None:
        connect_timeout = self._options["connect_timeout"]
        if connect_timeout is None:
            connect_timeout = self._timeout
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self._host,
                    self._port,
                    ssl=self._ssl,
                    server_hostname=self._host if self._ssl is not None else None,
                ),
                connect_timeout,
            )
        except asyncio.TimeoutError as exc:
            raise OperationalError(f"could not connect to {self._host}:{self._port}: timed out") from exc
        except (OSError, ssl_module.SSLError) as exc:
            raise OperationalError(f"could not connect to {self._host}:{self._port}: {exc}") from exc
        self._closed = False
        try:
            sock = self._writer.get_extra_info("socket")
            if sock is not None:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                if self._options["keepalive"] is not None:
                    _set_keepalive(sock, self._options["keepalive"])
            await self._handshake(self._user, self._options["database"])
            await self._apply_session()
        except BaseException:
            self._fail()
            raise

    async def close(self) -> None:
        """Close the connection. Closing it twice is harmless."""
        if self._closed:
            return
        self._closed = True
        writer = self._writer
        if writer is None:
            return
        try:
            if not self._broken:
                writer.write(proto.terminate())
            writer.close()
            await writer.wait_closed()
        except (OSError, ssl_module.SSLError):
            pass

    async def __aenter__(self) -> "AsyncConnection":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    # --- state ---

    @property
    def autocommit(self) -> bool:
        """Whether each statement runs as its own transaction. Settable only outside one."""
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        if self._status != "I":
            raise ProgrammingError("cannot change autocommit inside a transaction")
        self._autocommit = bool(value)

    @property
    def transaction_status(self) -> str:
        """``"idle"``, ``"in_transaction"`` or ``"failed"``, as of the last statement."""
        return {"I": "idle", "T": "in_transaction", "E": "failed"}[self._status]

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def broken(self) -> bool:
        return self._broken

    @property
    def backend_key(self) -> Optional[Tuple[int, int]]:
        if self._pid is None or self._secret is None:
            return None
        return (self._pid, self._secret)

    # --- statements ---

    def cursor(self, row_factory: Optional[RowFactory] = None) -> "AsyncCursor":
        """A new cursor (see :meth:`nusadb.Connection.cursor`)."""
        self._check_open()
        return AsyncCursor(self, row_factory if row_factory is not None else self.row_factory)

    async def execute(self, sql: str, parameters: Sequence[Any] = ()) -> "AsyncCursor":
        """Run ``sql`` on a new cursor and return it, ready to fetch from."""
        cur = self.cursor()
        await cur.execute(sql, parameters)
        return cur

    async def prepare(self, sql: str) -> "AsyncPreparedStatement":
        """Parse ``sql`` once on the server (see :meth:`nusadb.Connection.prepare`)."""
        async with self._exchange():
            name = f"nusadb_stmt_{next(self._names)}"
            self._send(proto.parse(name, _to_native_placeholders(sql)))
            self._send(proto.sync())
            await self._read_until_ready()
        return AsyncPreparedStatement(self, name, sql)

    async def prepare_cached(self, sql: str) -> "AsyncPreparedStatement":
        """:meth:`prepare`, reusing the statement already prepared for ``sql`` (up to
        :data:`nusadb.STATEMENT_CACHE_SIZE`, least recently used closed first)."""
        from . import connection as sync

        # One at a time, so two tasks asking for the same text do not both prepare it.
        async with self._cache_lock:
            cached = self._statements.get(sql)
            if cached is not None and not cached.closed:
                self._statements.move_to_end(sql)
                return cached
            statement = await self.prepare(sql)
            self._statements[sql] = statement
            while len(self._statements) > sync.STATEMENT_CACHE_SIZE:
                _sql, oldest = self._statements.popitem(last=False)
                await oldest.close()
            return statement

    def transaction(
        self, isolation: Optional[str] = None, read_only: Optional[bool] = None
    ) -> "AsyncTransaction":
        """An ``async with`` block around a transaction, or a savepoint inside one (see
        :meth:`nusadb.Connection.transaction`)."""
        return AsyncTransaction(self, isolation, read_only)

    def stream(
        self,
        sql: str,
        parameters: Sequence[Any] = (),
        batch_rows: int = 1024,
        row_factory: Optional[RowFactory] = None,
    ) -> "AsyncRowStream":
        """Read a query's rows ``batch_rows`` at a time: ``async with conn.stream(sql) as rows:``
        then ``async for row in rows``. The connection runs nothing else until the stream ends."""
        return AsyncRowStream(self, sql, parameters, max(1, batch_rows), row_factory)

    async def commit(self) -> None:
        """Commit the open transaction (see :meth:`nusadb.Connection.commit`)."""
        async with self._exchange():
            if self._status == "I":
                return
            failed = self._status == "E"
            *_unused, tag = await self._control("COMMIT")
        if failed or tag == "ROLLBACK":
            raise TransactionRolledBack(
                "a statement in the transaction failed, so COMMIT rolled it back"
            )

    async def rollback(self) -> None:
        """Roll back the open transaction. Nothing to do outside one."""
        async with self._exchange():
            if self._status != "I":
                await self._control("ROLLBACK")

    async def savepoint(self, name: str) -> None:
        async with self._exchange():
            if self._autocommit and self._status == "I":
                raise ProgrammingError(
                    "cannot set a savepoint in autocommit mode outside a transaction"
                )
            await self._begin_if_needed()
            await self._control("SAVEPOINT " + _quote_identifier(name))

    async def rollback_to(self, name: str) -> None:
        async with self._exchange():
            if self._status == "I":
                raise ProgrammingError("no active transaction to roll back to a savepoint")
            await self._control("ROLLBACK TO SAVEPOINT " + _quote_identifier(name))

    async def release_savepoint(self, name: str) -> None:
        async with self._exchange():
            if self._status == "I":
                raise ProgrammingError("no active transaction to release a savepoint")
            await self._control("RELEASE SAVEPOINT " + _quote_identifier(name))

    # --- notifications ---

    async def listen(self, channel: str) -> None:
        async with self._exchange():
            await self._control("LISTEN " + _quote_identifier(channel))

    async def unlisten(self, channel: Optional[str] = None) -> None:
        async with self._exchange():
            target = "*" if channel is None else _quote_identifier(channel)
            await self._control("UNLISTEN " + target)

    async def notify(self, channel: str, payload: Optional[str] = None) -> None:
        sql = "NOTIFY " + _quote_identifier(channel)
        if payload is not None:
            sql += ", " + _quote_literal(payload)
        async with self._exchange():
            await self._control(sql)

    def notifications(self) -> List[Notification]:
        """The notifications received so far (and clear them)."""
        pending, self._notifications = self._notifications, []
        return pending

    async def poll(self, timeout: Optional[float] = 0.0) -> Optional[Notification]:
        """Wait up to ``timeout`` seconds for the next notification (``None`` waits forever,
        ``0`` only returns one already received); ``None`` when none arrives."""
        if self._notifications:
            return self._notifications.pop(0)
        async with self._exchange(reading=True):
            assert self._reader is not None
            try:
                # readexactly takes nothing until the whole header is there, so a timeout never
                # cuts a message in half. A zero timeout still lets already-received bytes through.
                header = await asyncio.wait_for(
                    self._reader.readexactly(proto.HEADER_LEN),
                    None if timeout is None else max(timeout, 0.001),
                )
            except asyncio.TimeoutError:
                return None
            except (asyncio.IncompleteReadError, OSError) as exc:
                self._fail()
                raise OperationalError(f"waiting for a notification failed: {exc}") from exc
            self._pending = True
            mtype, r = await self._read_rest(header)
            self._pending = False
            if mtype == proto.B_NOTIFICATION_RESPONSE:
                return _decode_notification(r)
            if mtype == proto.B_ERROR:
                raise from_sqlstate(r.string(), r.string())
            self._fail()
            raise InterfaceError(
                "unexpected message %r while polling for notifications" % chr(mtype)
            )

    # --- COPY ---

    async def copy_in(
        self,
        sql: str,
        source: Union[bytes, Any],
    ) -> int:
        r"""Bulk-load through ``COPY ... FROM STDIN``. ``source`` is ``bytes``, an iterable or async
        iterable of ``bytes`` chunks, or a file-like whose ``read(size)`` returns ``bytes``
        (awaited when it is a coroutine), already in the server's text format. Returns the number
        of rows loaded."""
        async with self._exchange():
            await self._begin_if_needed()
            self._send(proto.query(sql))
            await self._await_copy_start(proto.B_COPY_IN)
            try:
                async for chunk in _chunks(source):
                    if chunk:
                        self._send(proto.copy_data(chunk))
                        await self._flush()
            except (OSError, DatabaseError, InterfaceError):
                raise
            except Exception as exc:  # noqa: BLE001 - report any source failure, stay in sync
                self._send(proto.copy_fail("nusadb: client read error during COPY"))
                await self._drain_to_ready()
                raise OperationalError(f"COPY source read failed: {exc}") from exc
            self._send(proto.copy_done())
            return await self._finish_copy()

    async def copy_out(
        self, sql: str, sink: Callable[[bytes], Union[Any, Awaitable[Any]]]
    ) -> int:
        """Bulk-export through ``COPY ... TO STDOUT``: ``sink(chunk)`` is called (and awaited
        when it returns an awaitable) with each chunk. Returns the number of rows exported."""
        async with self._exchange():
            await self._begin_if_needed()
            self._send(proto.query(sql))
            await self._await_copy_start(proto.B_COPY_OUT)
            while True:
                mtype, r = await self._read_message()
                if mtype == proto.B_COPY_DATA:
                    result = sink(r.rest())
                    if asyncio.iscoroutine(result) or isinstance(result, asyncio.Future):
                        await result
                elif mtype == proto.B_COPY_DONE:
                    break
                elif mtype == proto.B_ERROR:
                    error = (r.string(), r.string())
                    await self._drain_to_ready()
                    raise from_sqlstate(*error)
            return await self._finish_copy()

    # --- cancellation ---

    async def cancel(self) -> None:
        """Ask the server to cancel this connection's running statement, from a fresh connection.
        Safe when nothing is running."""
        if self._pid is None or self._secret is None:
            return
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self._host,
                    self._port,
                    ssl=self._ssl,
                    server_hostname=self._host if self._ssl is not None else None,
                ),
                self._timeout,
            )
            writer.write(proto.cancel_request(self._pid, self._secret))
            await writer.drain()
            writer.close()
        except (OSError, asyncio.TimeoutError, ssl_module.SSLError) as exc:
            raise OperationalError(f"could not send the cancel request: {exc}") from exc

    # --- internals: one exchange at a time ---

    def _check_open(self) -> None:
        if self._broken:
            raise OperationalError("the connection is broken; open a new one")
        if self._closed:
            raise InterfaceError("connection is closed")

    def _exchange(self, reading: bool = False) -> "_Exchange":
        return _Exchange(self, reading)

    def _fail(self) -> None:
        self._broken = True
        self._closed = True
        if self._writer is not None:
            try:
                self._writer.close()
            except (OSError, RuntimeError):
                pass

    def _send(self, frame: bytes) -> None:
        assert self._writer is not None
        self._pending = True
        self._writer.write(frame)

    async def _flush(self) -> None:
        assert self._writer is not None
        try:
            await self._writer.drain()
        except (OSError, ssl_module.SSLError) as exc:
            self._fail()
            raise OperationalError(f"sending to the server failed: {exc}") from exc

    async def _read_exact(self, n: int) -> bytes:
        assert self._reader is not None
        try:
            return await asyncio.wait_for(self._reader.readexactly(n), self._timeout)
        except asyncio.TimeoutError as exc:
            self._fail()
            raise OperationalError("reading from the server timed out") from exc
        except asyncio.IncompleteReadError as exc:
            self._fail()
            raise OperationalError("server closed the connection") from exc
        except (OSError, ssl_module.SSLError) as exc:
            self._fail()
            raise OperationalError(f"reading from the server failed: {exc}") from exc

    async def _read_message(self) -> Tuple[int, proto.Reader]:
        await self._flush()
        # Set while a message is half read: a task cancelled there leaves bytes on the socket that
        # are not a message header, and only dropping the connection gets back in step.
        self._mid_message = True
        message = await self._read_rest(await self._read_exact(proto.HEADER_LEN))
        self._mid_message = False
        return message

    async def _read_rest(self, header: bytes) -> Tuple[int, proto.Reader]:
        try:
            mtype, plen = proto.decode_frame_header(header)
        except ValueError as exc:
            self._fail()
            raise InterfaceError(f"malformed message from the server: {exc}") from exc
        payload = await self._read_exact(plen) if plen else b""
        if mtype == proto.B_READY and payload:
            self._pending = False
            status = chr(payload[0])
            if status in ("I", "T", "E"):
                self._status = status
        return mtype, proto.Reader(payload)

    async def _handshake(self, user: str, database: str) -> None:
        self._send(proto.startup(user, database))
        while True:
            mtype, r = await self._read_message()
            if mtype == proto.B_AUTH:
                if r.u32() == proto.AUTH_SASL:
                    count = r.u16()
                    await self._scram([r.string() for _ in range(count)])
            elif mtype == proto.B_BACKEND_KEY:
                self._pid = r.u32()
                self._secret = r.u32()
            elif mtype == proto.B_READY:
                r.u8()
                return
            elif mtype == proto.B_ERROR:
                raise from_sqlstate(r.string(), r.string())

    async def _scram(self, mechs: Sequence[str]) -> None:
        if _SCRAM_MECHANISM not in mechs:
            raise OperationalError(f"server offered no supported SASL mechanism (got {list(mechs)})")
        if self._password is None:
            raise OperationalError("server requires a password but none was given")
        bare, first = _scram.client_first(self._user)
        self._send(proto.sasl_initial(_SCRAM_MECHANISM, first.encode("utf-8")))
        mtype, r = await self._read_message()
        if mtype != proto.B_AUTH or r.u32() != proto.AUTH_SASL_CONTINUE:
            raise OperationalError("expected a SASL continue message")
        final, expected = _scram.client_final(self._password, bare, r.rest().decode("utf-8"))
        self._send(proto.sasl_response(final))
        mtype, r = await self._read_message()
        if mtype == proto.B_ERROR:
            raise from_sqlstate(r.string(), r.string())
        if mtype != proto.B_AUTH or r.u32() != proto.AUTH_SASL_FINAL:
            raise OperationalError("expected a SASL final message")
        if not _scram.verify_server_final(r.rest().decode("utf-8"), expected):
            raise OperationalError("server signature did not verify")

    async def _apply_session(self) -> None:
        if self._statement_timeout is not None:
            ms = math.ceil(self._statement_timeout * 1000)
            await self._control(f"SET statement_timeout = '{ms}ms'")

    async def _reset_session(self) -> None:
        """``RESET ALL``, ``UNLISTEN *``, buffered notifications dropped, own settings applied."""
        async with self._exchange():
            for sql in ("RESET ALL", "UNLISTEN *"):
                await self._control(sql)
            self._notifications.clear()
            await self._apply_session()

    async def _control(self, sql: str) -> _Result:
        self._send(proto.query(sql))
        return await self._read_until_ready()

    async def _begin_if_needed(self) -> None:
        if not self._autocommit and self._status == "I":
            await self._control("BEGIN")

    async def _read_until_ready(self) -> _Result:
        columns: List[str] = []
        column_types: List[Optional[str]] = []
        rows: List[List[Optional[bytes]]] = []
        tag: Optional[str] = None
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = await self._read_message()
            if mtype == proto.B_ROW_DESCRIPTION:
                count = r.u16()
                columns = [r.string() for _ in range(count)]
                column_types = [None] * count
            elif mtype == proto.B_ROW_DESCRIPTION_TYPED:
                count = r.u16()
                columns, column_types = [], []
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
                self._notifications.append(_decode_notification(r))
            elif mtype == proto.B_READY:
                r.u8()
                break
        if error is not None:
            raise from_sqlstate(*error)
        return columns, column_types, rows, tag

    async def _drain_to_ready(self) -> None:
        while True:
            mtype, r = await self._read_message()
            if mtype == proto.B_READY:
                return

    async def _query(self, sql: str, parameters: Sequence[Any]) -> _Result:
        await self._begin_if_needed()
        if not parameters:
            self._send(proto.query(sql))
            return await self._read_until_ready()
        encoded, tags = _bind(parameters)
        self._send(proto.parse("", _to_native_placeholders(sql), tags))
        self._send(proto.bind("", "", encoded))
        self._send(proto.describe_portal(""))
        self._send(proto.execute("", 0))
        self._send(proto.sync())
        return await self._read_until_ready()

    async def _query_prepared(self, name: str, parameters: Sequence[Any]) -> _Result:
        await self._begin_if_needed()
        encoded, _tags = _bind(parameters)
        self._send(proto.bind("", name, encoded))
        self._send(proto.describe_portal(""))
        self._send(proto.execute("", 0))
        self._send(proto.sync())
        return await self._read_until_ready()

    async def _execute_batch(self, sql: str, seq_of_parameters: Any) -> Tuple[int, bool]:
        """Pipelined executions (see :meth:`nusadb.Cursor.executemany`); each chunk is bound whole
        before any of it is sent."""
        await self._begin_if_needed()
        sql = _to_native_placeholders(sql)
        total, counted = 0, False
        for chunk, failure in _bound_chunks(seq_of_parameters):
            if chunk:
                tags: Optional[bytes] = None
                for encoded, row_tags in chunk:
                    if row_tags != tags:
                        self._send(proto.parse("", sql, row_tags))
                        tags = row_tags
                    self._send(proto.bind("", "", encoded))
                    self._send(proto.execute("", 0))
                got, any_count = await self._finish_batch()
                total, counted = total + got, counted or any_count
            if failure is not None:
                raise failure
        return total, counted

    async def _finish_batch(self) -> Tuple[int, bool]:
        self._send(proto.sync())
        total, counted = 0, False
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = await self._read_message()
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

    async def _await_copy_start(self, expected: int) -> None:
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = await self._read_message()
            if mtype == expected:
                return
            if mtype in (proto.B_COPY_IN, proto.B_COPY_OUT):
                if mtype == proto.B_COPY_IN:
                    self._send(proto.copy_fail("nusadb: COPY direction does not match the call"))
                await self._drain_to_ready()
                if expected == proto.B_COPY_OUT:
                    raise ProgrammingError("copy_out() needs COPY ... TO STDOUT")
                raise ProgrammingError("copy_in() needs COPY ... FROM STDIN")
            if mtype == proto.B_ERROR:
                error = (r.string(), r.string())
            elif mtype == proto.B_READY:
                r.u8()
                if error is not None:
                    raise from_sqlstate(*error)
                raise OperationalError("COPY did not start")

    async def _finish_copy(self) -> int:
        *_unused, tag = await self._read_until_ready()
        return max(_affected(tag), 0)


class _Exchange:
    """One request and its reply: taken in turn by the tasks sharing a connection, and refused on
    a connection that is closed, broken, streaming, or left part way through an earlier one."""

    def __init__(self, conn: AsyncConnection, reading: bool) -> None:
        self._conn = conn
        self._reading = reading

    async def __aenter__(self) -> None:
        conn = self._conn
        conn._check_open()
        if conn._stream is not None:
            raise InterfaceError("a row stream is open on this connection; close it first")
        await conn._lock.acquire()
        try:
            conn._check_open()
            if conn._pending:
                # An earlier call stopped part way (its task was cancelled, say): the next bytes
                # on the socket belong to it.
                conn._fail()
                raise OperationalError(
                    "the connection is broken (an earlier call was interrupted part way); "
                    "open a new one"
                )
        except BaseException:
            conn._lock.release()
            raise

    async def __aexit__(self, *_exc: object) -> None:
        self._conn._lock.release()


async def _chunks(source: Any) -> AsyncIterator[bytes]:
    if isinstance(source, (bytes, bytearray, memoryview)):
        yield bytes(source)
        return
    if hasattr(source, "read"):
        while True:
            chunk = source.read(65536)
            if asyncio.iscoroutine(chunk):
                chunk = await chunk
            if not chunk:
                return
            yield chunk
    elif hasattr(source, "__aiter__"):
        async for chunk in source:
            yield chunk
    else:
        for chunk in source:
            yield chunk


class AsyncCursor:
    """A cursor whose ``execute`` and ``fetch*`` are coroutines."""

    def __init__(self, connection: AsyncConnection, row_factory: Optional[RowFactory]) -> None:
        self._conn = connection
        # Decoding and row building are shared with the blocking cursor.
        self._rows = Cursor(_Detached(), row_factory)  # type: ignore[arg-type]
        self.arraysize = 1
        self.rowcount = -1
        self.lastrowid = None
        self._closed = False

    @property
    def connection(self) -> AsyncConnection:
        return self._conn

    @property
    def row_factory(self) -> Optional[RowFactory]:
        return self._rows.row_factory

    @row_factory.setter
    def row_factory(self, value: Optional[RowFactory]) -> None:
        self._rows.row_factory = value
        self._rows._make_row = None

    @property
    def description(self) -> Any:
        return self._rows.description

    def _check(self) -> None:
        if self._closed:
            raise ProgrammingError("cursor is closed")
        self._conn._check_open()

    def _load(self, result: _Result) -> None:
        self._rows._load(*result)
        self.rowcount = self._rows.rowcount

    async def execute(
        self,
        operation: Union[str, "AsyncPreparedStatement"],
        parameters: Optional[Sequence[Any]] = None,
    ) -> "AsyncCursor":
        self._check()
        async with self._conn._exchange():
            if isinstance(operation, AsyncPreparedStatement):
                result = await self._conn._query_prepared(
                    self._statement_name(operation), parameters or ()
                )
            else:
                result = await self._conn._query(operation, parameters or ())
        self._load(result)
        return self

    async def executemany(
        self,
        operation: Union[str, "AsyncPreparedStatement"],
        seq_of_parameters: Any,
    ) -> None:
        """Run ``operation`` once per parameter set, pipelined (see
        :meth:`nusadb.Cursor.executemany`)."""
        self._check()
        async with self._conn._exchange():
            if isinstance(operation, AsyncPreparedStatement):
                name = self._statement_name(operation)
                total, counted = 0, False
                for parameters in seq_of_parameters:
                    *_unused, tag = await self._conn._query_prepared(name, parameters)
                    affected = _affected(tag)
                    if affected >= 0:
                        total, counted = total + affected, True
            else:
                total, counted = await self._conn._execute_batch(operation, seq_of_parameters)
        self._rows._load([], [], [], None)
        self.rowcount = total if counted else -1

    def _statement_name(self, statement: "AsyncPreparedStatement") -> str:
        if statement.connection is not self._conn:
            raise ProgrammingError("the statement was prepared on another connection")
        if statement.closed:
            raise ProgrammingError("the prepared statement is closed")
        return statement.name

    async def fetchone(self) -> Any:
        self._check()
        return self._rows._next_row()

    async def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        self._check()
        out = []
        for _ in range(self.arraysize if size is None else size):
            row = self._rows._next_row()
            if row is None:
                break
            out.append(row)
        return out

    async def fetchall(self) -> List[Any]:
        self._check()
        out = []
        while True:
            row = self._rows._next_row()
            if row is None:
                return out
            out.append(row)

    def __aiter__(self) -> "AsyncCursor":
        return self

    async def __anext__(self) -> Any:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def close(self) -> None:
        self._closed = True

    async def __aenter__(self) -> "AsyncCursor":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()


class _Detached:
    """Stands in for the connection of the blocking cursor that only decodes rows here."""

    row_factory = None

    def _ensure_open(self) -> None:  # pragma: no cover - never called on the decode path
        pass


class AsyncPreparedStatement:
    """A statement parsed once on the server (:meth:`AsyncConnection.prepare`)."""

    def __init__(self, connection: AsyncConnection, name: str, sql: str) -> None:
        self.connection = connection
        self.name = name
        self.sql = sql
        self.closed = False

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        conn = self.connection
        if not conn.closed and not conn.broken:
            async with conn._exchange():
                conn._send(proto.close_statement(self.name))
                conn._send(proto.sync())
                await conn._read_until_ready()

    def __repr__(self) -> str:
        return f"AsyncPreparedStatement({self.sql!r})"


class AsyncTransaction:
    """The ``async with`` block :meth:`AsyncConnection.transaction` returns."""

    def __init__(
        self, connection: AsyncConnection, isolation: Optional[str], read_only: Optional[bool]
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

    async def __aenter__(self) -> "AsyncTransaction":
        conn = self.connection
        async with conn._exchange():
            if conn._status == "I":
                sql = "BEGIN"
                if self._isolation is not None:
                    sql += " ISOLATION LEVEL " + _ISOLATION_LEVELS[self._isolation.lower()]
                if self._read_only is not None:
                    sql += " READ ONLY" if self._read_only else " READ WRITE"
                await conn._control(sql)
            else:
                if self._isolation is not None or self._read_only is not None:
                    raise ProgrammingError(
                        "isolation and read_only apply only to the outermost transaction"
                    )
                self._savepoint = f"nusadb_savepoint_{next(conn._names)}"
                await conn._control("SAVEPOINT " + self._savepoint)
        return self

    async def __aexit__(self, exc_type: Any, *_exc: object) -> None:
        conn = self.connection
        if exc_type is not None:
            # Leaving on an exception (a cancellation or timeout included): undo what can be
            # undone, but never let a failure doing so replace the exception that ended the block.
            # (A block stopped part way through a reply is out of step: the undo below finds that
            # and breaks the connection, and the error it raises is set aside like any other.)
            if conn.closed or conn.broken:
                return
            try:
                if self._savepoint is None:
                    await conn.rollback()
                else:
                    async with conn._exchange():
                        await conn._control("ROLLBACK TO SAVEPOINT " + self._savepoint)
                        await conn._control("RELEASE SAVEPOINT " + self._savepoint)
            except (DatabaseError, InterfaceError):
                pass
            return
        if self._savepoint is None:
            await conn.commit()
            return
        async with conn._exchange():
            await conn._control("RELEASE SAVEPOINT " + self._savepoint)


class AsyncRowStream:
    """A query's rows read a batch at a time (:meth:`AsyncConnection.stream`). Use it as
    ``async with``; iterate it with ``async for``."""

    def __init__(
        self,
        connection: AsyncConnection,
        sql: str,
        parameters: Sequence[Any],
        batch_rows: int,
        row_factory: Optional[RowFactory],
    ) -> None:
        self._conn = connection
        self._sql = sql
        self._parameters = parameters
        self._batch = batch_rows
        self._cursor = AsyncCursor(
            connection, row_factory if row_factory is not None else connection.row_factory
        )
        self._started = False
        self._done = False
        # Whether this stream holds the connection's lock (from start until it ends).
        self._holding = False
        self.rowcount = -1

    @property
    def description(self) -> Any:
        return self._cursor.description

    async def _start(self) -> None:
        conn = self._conn
        # The exchange (and the connection's lock) is held until the stream ends: the rest of the
        # reply belongs to it. `_finish` releases it.
        await conn._exchange().__aenter__()
        try:
            await conn._begin_if_needed()
            encoded, tags = _bind(self._parameters)
            conn._send(proto.parse("", _to_native_placeholders(self._sql), tags))
            conn._send(proto.bind("", "", encoded))
            conn._send(proto.describe_portal(""))
            conn._send(proto.execute("", self._batch))
        except BaseException:
            conn._lock.release()
            raise
        conn._stream = self
        self._started = True
        self._holding = True
        await self._fetch(ask=False)

    async def _fetch(self, ask: bool) -> None:
        try:
            await self._read_batch(ask)
        except BaseException:
            if not self._done:
                # Stopped before its reply was read through (cancelled, interrupted, or the
                # connection failed; a server error has already ended the stream cleanly): the rest
                # of the reply is unread, so the connection is out of step. Break it, and let the
                # tasks waiting on it see that rather than wait for a stream that will not finish.
                self._conn._fail()
                self._done = True
                self._release()
            raise

    async def _read_batch(self, ask: bool) -> None:
        conn = self._conn
        if ask:
            conn._send(proto.execute("", self._batch))
        rows: List[List[Optional[bytes]]] = []
        error: Optional[Tuple[str, str]] = None
        rows_view = self._cursor._rows
        while True:
            mtype, r = await conn._read_message()
            if mtype == proto.B_ROW_DESCRIPTION_TYPED:
                count = r.u16()
                columns, types = [], []
                for _ in range(count):
                    columns.append(r.string())
                    types.append(proto.type_name(r.u8()))
                rows_view._load(columns, types, [], None)
            elif mtype == proto.B_ROW_DESCRIPTION:
                count = r.u16()
                rows_view._load([r.string() for _ in range(count)], [None] * count, [], None)
            elif mtype == proto.B_DATA_ROW:
                rows.append(r.fields())
            elif mtype == proto.B_PORTAL_SUSPENDED:
                break
            elif mtype == proto.B_COMMAND_COMPLETE:
                self.rowcount = _affected(r.string())
                await self._finish()
                break
            elif mtype == proto.B_ERROR:
                error = (r.string(), r.string())
                await self._finish()
                break
            elif mtype == proto.B_NOTIFICATION_RESPONSE:
                conn._notifications.append(_decode_notification(r))
        rows_view._rows = rows
        rows_view._pos = 0
        if error is not None:
            raise from_sqlstate(*error)

    async def _finish(self) -> None:
        conn = self._conn
        self._done = True
        try:
            if conn._mid_message:
                # Stopped inside a message: there is no message boundary to drain from.
                conn._fail()
            else:
                conn._send(proto.sync())
                await conn._drain_to_ready()
        finally:
            self._release()

    def _release(self) -> None:
        if self._holding:
            self._holding = False
            self._conn._stream = None
            self._conn._lock.release()

    def __aiter__(self) -> "AsyncRowStream":
        return self

    async def __anext__(self) -> Any:
        if not self._started:
            await self._start()
        rows_view = self._cursor._rows
        while True:
            if rows_view._pos < len(rows_view._rows):
                return rows_view._next_row()
            if self._done:
                raise StopAsyncIteration
            await self._fetch(ask=True)

    async def close(self) -> None:
        """Stop reading; the rows not read are discarded and the connection is free again."""
        if self._started and not self._done and not self._conn.broken:
            await self._finish()
        self._done = True
        self._release()

    async def __aenter__(self) -> "AsyncRowStream":
        if not self._started:
            await self._start()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()


class _Opened(NamedTuple):
    at: float
    autocommit: bool


class AsyncPool:
    """A bounded pool of :class:`AsyncConnection` objects, with the options of
    :class:`nusadb.Pool`. Check one out with ``async with pool.connection() as conn``.

    A pool belongs to the event loop that first uses it, like the connections it holds."""

    def __init__(
        self,
        max_size: int = 10,
        dsn: Optional[str] = None,
        *,
        acquire_timeout: Optional[float] = 30.0,
        idle_timeout: Optional[float] = None,
        max_lifetime: Optional[float] = None,
        test_on_checkout: bool = False,
        reset_on_return: bool = True,
        **connect_kwargs: Any,
    ) -> None:
        self._max = max(1, max_size)
        self._dsn = dsn
        self._kwargs = connect_kwargs
        self._acquire_timeout = acquire_timeout
        self._idle_timeout = idle_timeout
        self._max_lifetime = max_lifetime
        self._test_on_checkout = test_on_checkout
        self._reset_on_return = reset_on_return
        self._idle: List[Tuple[AsyncConnection, float]] = []
        # Made on first use: before Python 3.10 a semaphore belongs to the event loop current when
        # it is created, and the pool may be built outside the loop that uses it.
        self._slots: Optional[asyncio.Semaphore] = None
        self._opened: Dict[int, _Opened] = {}
        self._out: Dict[int, AsyncConnection] = {}
        self._live = 0
        self._closed = False

    @property
    def size(self) -> int:
        return self._live

    @property
    def idle_count(self) -> int:
        return len(self._idle)

    async def acquire(self, timeout: Optional[float] = -1.0) -> AsyncConnection:
        if timeout is not None and timeout < 0:
            timeout = self._acquire_timeout
        if self._closed:
            raise InterfaceError("pool is closed")
        if self._slots is None:
            self._slots = asyncio.Semaphore(self._max)
        try:
            await asyncio.wait_for(self._slots.acquire(), timeout)
        except asyncio.TimeoutError as exc:
            raise InterfaceError("timed out waiting for a pooled connection") from exc
        try:
            conn = await self._reuse_idle()
            if conn is None:
                conn = await connect(self._dsn, **self._kwargs)
                if self._closed:
                    await conn.close()
                    raise InterfaceError("pool is closed")
                self._live += 1
                self._opened[id(conn)] = _Opened(time.monotonic(), conn.autocommit)
            self._out[id(conn)] = conn
            return conn
        except BaseException:
            self._slots.release()
            raise

    def _expired(self, conn: AsyncConnection) -> bool:
        opened = self._opened.get(id(conn))
        return (
            self._max_lifetime is not None
            and opened is not None
            and time.monotonic() - opened.at >= self._max_lifetime
        )

    async def _reuse_idle(self) -> Optional[AsyncConnection]:
        while True:
            if self._closed:
                raise InterfaceError("pool is closed")
            if not self._idle:
                return None
            conn, since = self._idle.pop()
            stale = self._expired(conn) or (
                self._idle_timeout is not None and time.monotonic() - since >= self._idle_timeout
            )
            if not stale and (not self._test_on_checkout or await _responds(conn)):
                return conn
            await self._drop(conn)

    async def _drop(self, conn: AsyncConnection) -> None:
        self._live -= 1
        self._opened.pop(id(conn), None)
        await conn.close()

    async def release(self, conn: AsyncConnection) -> None:
        """Return a checked-out connection (twice, or one from elsewhere, raises
        :class:`InterfaceError`)."""
        if self._out.pop(id(conn), None) is None:
            raise InterfaceError("connection is not checked out from this pool")
        try:
            opened = self._opened.get(id(conn))
            clean = (
                opened is not None
                and not self._expired(conn)
                and await _clean(conn, opened.autocommit, self._reset_on_return)
            )
            if clean and not self._closed:
                self._idle.append((conn, time.monotonic()))
            else:
                await self._drop(conn)
        finally:
            if self._slots is not None:
                self._slots.release()

    def connection(self) -> "_PooledAsyncConnection":
        return _PooledAsyncConnection(self)

    async def close(self) -> None:
        """Close the idle connections; checked-out ones close when returned."""
        self._closed = True
        idle, self._idle = self._idle, []
        for conn, _since in idle:
            await self._drop(conn)

    async def __aenter__(self) -> "AsyncPool":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()


async def _responds(conn: AsyncConnection) -> bool:
    try:
        cur = await conn.execute("SELECT 1")
        await cur.fetchall()
        if conn.transaction_status != "idle":
            await conn.rollback()
    except Exception:  # noqa: BLE001 - any failure means the connection is not usable
        return False
    return True


async def _clean(conn: AsyncConnection, autocommit: bool, reset: bool) -> bool:
    if conn.closed or conn.broken:
        return False
    try:
        if conn.transaction_status != "idle":
            await conn.rollback()
        if conn.autocommit != autocommit:
            conn.autocommit = autocommit
        if reset:
            await conn._reset_session()
    except Exception:  # noqa: BLE001 - any failure here means the connection is not reusable
        return False
    return True


class _PooledAsyncConnection:
    def __init__(self, pool: AsyncPool) -> None:
        self._pool = pool
        self._conn: Optional[AsyncConnection] = None

    async def __aenter__(self) -> AsyncConnection:
        self._conn = await self._pool.acquire()
        return self._conn

    async def __aexit__(self, *_exc: object) -> None:
        if self._conn is not None:
            await self._pool.release(self._conn)
            self._conn = None
