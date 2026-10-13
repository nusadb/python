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
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from . import _binary
from . import _protocol as proto
from . import _scram
from .connection import (
    _DEFAULTS,
    _ISOLATION_LEVELS,
    _RECOVER_QUERY,
    _TPC_PREPARED,
    _UNKNOWN_STATEMENT,
    _USES_TRACKED,
    Cursor,
    Notice,
    Notification,
    RowFactory,
    Xid,
    _adapt,
    _as_xid,
    _deliver_notice,
    _make_xid,
    _recovered,
    _affected,
    _arrange,
    _bind,
    _bound_chunks,
    _check_threshold,
    _decode_notification,
    _encode_param,
    _format_mismatch,
    _needs_simple_query,
    _quote_identifier,
    _quote_literal,
    _read_description,
    _Result,
    PIPELINE_SYNC_STATEMENTS,
    _COPY_CHUNK,
    _compile,
    _copy_text_chunks,
    _CopyLines,
    _scoped_adapters,
    _set_keepalive,
    _sql_text,
    _to_native_placeholders,
)
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
        # Prepared statements kept: `prepare_cached` ones under their SQL text, automatically
        # prepared ones under `(native sql, parameter type tags)`.
        self._statements: "OrderedDict[Any, AsyncPreparedStatement]" = OrderedDict()
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        #: The default ``row_factory`` of this connection's cursors (``None``: tuples).
        self.row_factory: Optional[RowFactory] = None
        #: Whether this connection's cursors ask for results in the binary format by default.
        self.binary: bool = bool(options.get("binary", False))
        self._prepare_threshold = _check_threshold(options.get("prepare_threshold", 5))
        self._uses: "OrderedDict[Tuple[str, bytes], int]" = OrderedDict()
        # Adapters registered on this connection only (see `nusadb.types.register_composite`).
        self._adapters: Dict[type, Callable[[Any], Any]] = {}
        #: The :class:`~nusadb.Notice` messages the server sent with its replies, oldest first
        #: (see :attr:`nusadb.Connection.notices`).
        self.notices: List[Notice] = []
        self._notice_handlers: List[Callable[[Notice], Any]] = []
        self._tpc: Optional[Xid] = None
        self._tpc_prepared = False

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

    @property
    def prepare_threshold(self) -> Optional[int]:
        """How many times a statement runs with parameters before it is prepared on the server and
        reused (``None``: never). See :func:`nusadb.connect`."""
        return self._prepare_threshold

    @prepare_threshold.setter
    def prepare_threshold(self, value: Optional[int]) -> None:
        self._prepare_threshold = _check_threshold(value)

    # --- two-phase commit ---

    def xid(self, format_id: int, gtrid: str, bqual: str) -> Xid:
        """A transaction id for :meth:`tpc_begin` (see :meth:`nusadb.Connection.xid`)."""
        return _make_xid(format_id, gtrid, bqual)

    async def tpc_begin(self, xid: Union[Xid, str]) -> None:
        """Begin a two-phase transaction (see :meth:`nusadb.Connection.tpc_begin`)."""
        xid = _as_xid(xid)
        async with self._exchange():
            if self._tpc is not None:
                raise ProgrammingError("a two-phase transaction is already in progress")
            if self._status != "I":
                raise ProgrammingError("tpc_begin must be called outside a transaction")
            await self._control("BEGIN")
            self._tpc, self._tpc_prepared = xid, False

    async def tpc_prepare(self) -> None:
        """``PREPARE TRANSACTION`` (see :meth:`nusadb.Connection.tpc_prepare`)."""
        async with self._exchange():
            if self._tpc is None:
                raise ProgrammingError("tpc_prepare needs a transaction begun with tpc_begin")
            if self._tpc_prepared:
                raise ProgrammingError("the two-phase transaction is already prepared")
            await self._control("PREPARE TRANSACTION " + _quote_literal(str(self._tpc)))
            self._tpc_prepared = True

    async def tpc_commit(self, xid: Union[Xid, str, None] = None) -> None:
        """Commit a two-phase transaction (see :meth:`nusadb.Connection.tpc_commit`)."""
        await self._tpc_end("COMMIT", xid)

    async def tpc_rollback(self, xid: Union[Xid, str, None] = None) -> None:
        """Roll back a two-phase transaction (see :meth:`nusadb.Connection.tpc_rollback`)."""
        await self._tpc_end("ROLLBACK", xid)

    async def tpc_recover(self) -> List[Xid]:
        """The prepared transactions on the server (see :meth:`nusadb.Connection.tpc_recover`)."""
        async with self._exchange():
            _columns, types, rows, _tag = await self._control(_RECOVER_QUERY)
        return _recovered(rows, types)

    async def _tpc_end(self, action: str, xid: Union[Xid, str, None]) -> None:
        async with self._exchange():
            if xid is not None:
                if self._tpc is not None:
                    raise ProgrammingError(
                        f"tpc_{action.lower()} with an xid cannot run inside a two-phase "
                        "transaction"
                    )
                if self._status != "I":
                    raise ProgrammingError(
                        f"tpc_{action.lower()} with an xid must be called outside a transaction"
                    )
                await self._control(f"{action} PREPARED " + _quote_literal(str(_as_xid(xid))))
                return
            if self._tpc is None:
                raise ProgrammingError(
                    f"tpc_{action.lower()} needs a transaction begun with tpc_begin"
                )
            try:
                if self._tpc_prepared:
                    await self._control(f"{action} PREPARED " + _quote_literal(str(self._tpc)))
                elif self._status != "I":
                    failed = self._status == "E"
                    *_unused, tag = await self._control(action)
                    if action == "COMMIT" and (failed or tag == "ROLLBACK"):
                        raise TransactionRolledBack(
                            "a statement in the transaction failed, so COMMIT rolled it back"
                        )
            finally:
                self._tpc, self._tpc_prepared = None, False

    # --- notices ---

    def add_notice_handler(self, handler: Callable[[Notice], Any]) -> None:
        """Call ``handler(notice)`` for each notice from now on (see
        :meth:`nusadb.Connection.add_notice_handler`)."""
        self._notice_handlers.append(handler)

    def remove_notice_handler(self, handler: Callable[[Notice], Any]) -> None:
        """Stop calling ``handler``; raises ``ValueError`` when it was not added."""
        self._notice_handlers.remove(handler)

    # --- statements ---

    def cursor(
        self,
        row_factory: Optional[RowFactory] = None,
        *,
        name: Optional[str] = None,
        scrollable: Optional[bool] = None,
        withhold: bool = False,
        binary: Optional[bool] = None,
    ) -> "AsyncCursor":
        """A new cursor; with a ``name``, an :class:`AsyncServerCursor` (see
        :meth:`nusadb.Connection.cursor`, ``binary`` included)."""
        self._check_open()
        factory = row_factory if row_factory is not None else self.row_factory
        if name is not None:
            if binary:
                raise NotSupportedError(
                    "a named cursor fetches in the text format; binary=True is not available"
                )
            return AsyncServerCursor(self, name, factory, scrollable, withhold)
        return AsyncCursor(self, factory, self.binary if binary is None else binary)

    async def execute(
        self, sql: Any, parameters: Sequence[Any] = (), *, binary: Optional[bool] = None
    ) -> "AsyncCursor":
        """Run ``sql`` on a new cursor and return it, ready to fetch from."""
        cur = self.cursor()
        await cur.execute(sql, parameters, binary=binary)
        return cur

    async def prepare(self, sql: Any) -> "AsyncPreparedStatement":
        """Parse ``sql`` once on the server (see :meth:`nusadb.Connection.prepare`)."""
        sql = _sql_text(sql, self)
        async with self._exchange():
            name = f"nusadb_stmt_{next(self._names)}"
            self._send(proto.parse(name, _to_native_placeholders(sql)))
            self._send(proto.sync())
            await self._read_until_ready()
        return AsyncPreparedStatement(self, name, sql)

    async def prepare_cached(self, sql: Any) -> "AsyncPreparedStatement":
        """:meth:`prepare`, reusing the statement already prepared for ``sql`` (up to
        :data:`nusadb.STATEMENT_CACHE_SIZE`, least recently used closed first)."""
        from . import connection as sync

        sql = _sql_text(sql, self)
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
        sql: Any,
        parameters: Sequence[Any] = (),
        batch_rows: int = 1024,
        row_factory: Optional[RowFactory] = None,
        *,
        binary: Optional[bool] = None,
    ) -> "AsyncRowStream":
        """Read a query's rows ``batch_rows`` at a time: ``async with conn.stream(sql) as rows:``
        then ``async for row in rows``. The connection runs nothing else until the stream ends.
        ``binary`` asks for the rows in the binary format (default: the connection's
        :attr:`binary`)."""
        return AsyncRowStream(
            self,
            _sql_text(sql, self),
            parameters,
            max(1, batch_rows),
            row_factory,
            self.binary if binary is None else binary,
        )

    def pipeline(self) -> "AsyncPipeline":
        """``async with conn.pipeline() as p:`` queues statements and sends them together when the
        block ends or ``await p.sync()`` runs (see :meth:`nusadb.Connection.pipeline`)."""
        self._check_open()
        return AsyncPipeline(self)

    async def commit(self) -> None:
        """Commit the open transaction (see :meth:`nusadb.Connection.commit`)."""
        async with self._exchange():
            if self._tpc is not None:
                raise ProgrammingError(
                    "commit() is not allowed in a two-phase transaction; use tpc_commit()"
                )
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
            if self._tpc is not None:
                raise ProgrammingError(
                    "rollback() is not allowed in a two-phase transaction; use tpc_rollback()"
                )
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
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        async with self._exchange(reading=True):
            assert self._reader is not None
            while True:
                wait = None if deadline is None else max(deadline - loop.time(), 0.001)
                try:
                    # readexactly takes nothing until the whole header is there, so a timeout
                    # never cuts a message in half. A zero timeout still lets already-received
                    # bytes through.
                    header = await asyncio.wait_for(
                        self._reader.readexactly(proto.HEADER_LEN), wait
                    )
                except asyncio.TimeoutError:
                    return None
                except (asyncio.IncompleteReadError, OSError) as exc:
                    self._fail()
                    raise OperationalError(f"waiting for a notification failed: {exc}") from exc
                self._pending = True
                mtype, r = await self._read_rest(header)
                self._pending = False
                if mtype != proto.B_NOTICE_RESPONSE:
                    break
                # A notice on an idle connection: keep it, and go on waiting for a notification.
                _deliver_notice(self, r)
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
        sql: Any,
        source: Union[bytes, Any],
    ) -> int:
        r"""Bulk-load through ``COPY ... FROM STDIN``. ``source`` is ``bytes``, an iterable or async
        iterable of ``bytes`` chunks, or a file-like whose ``read(size)`` returns ``bytes``
        (awaited when it is a coroutine), already in the statement's format. Returns the number
        of rows loaded."""
        return await self._copy_in(_sql_text(sql, self), source, None)

    async def _copy_in(self, sql: str, source: Any, expected_format: Optional[int]) -> int:
        async with self._exchange():
            await self._begin_if_needed()
            self._send(proto.query(sql))
            copy_format = await self._await_copy_start(proto.B_COPY_IN)
            if expected_format is not None and copy_format != expected_format:
                self._send(proto.copy_fail("nusadb: COPY format does not match the call"))
                await self._drain_to_ready()
                raise ProgrammingError(_format_mismatch(expected_format))
            chunks = _chunks(source).__aiter__()
            while True:
                # Only the source's own failures end the COPY with CopyFail; a socket error while
                # sending propagates as it is.
                try:
                    chunk = await chunks.__anext__()
                except StopAsyncIteration:
                    break
                except Exception as exc:  # noqa: BLE001 - report any source failure, stay in sync
                    self._send(proto.copy_fail("nusadb: client read error during COPY"))
                    await self._drain_to_ready()
                    if isinstance(exc, ProgrammingError):
                        raise  # a row copy_in_rows could not write
                    raise OperationalError(f"COPY source read failed: {exc}") from exc
                if chunk:
                    self._send(proto.copy_data(chunk))
                    await self._flush()
            self._send(proto.copy_done())
            return await self._finish_copy()

    async def copy_out(
        self, sql: Any, sink: Callable[[bytes], Union[Any, Awaitable[Any]]]
    ) -> int:
        """Bulk-export through ``COPY ... TO STDOUT``: ``sink(chunk)`` is called (and awaited
        when it returns an awaitable) with each chunk. Returns the number of rows exported."""
        return await self._copy_out(_sql_text(sql, self), sink, None)

    async def _copy_out(self, sql: str, sink: Any, expected_format: Optional[int]) -> int:
        async with self._exchange():
            await self._begin_if_needed()
            self._send(proto.query(sql))
            copy_format = await self._await_copy_start(proto.B_COPY_OUT)
            if expected_format is not None and copy_format != expected_format:
                await self._drain_to_ready()
                raise ProgrammingError(_format_mismatch(expected_format))
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

    async def copy_in_rows(
        self,
        sql: Any,
        rows: Union[Iterable[Any], AsyncIterable[Any]],
        binary: bool = False,
        types: Optional[Sequence[str]] = None,
    ) -> int:
        """Bulk-load Python rows (an iterable or async iterable of value sequences) through
        ``COPY ... FROM STDIN``, in the text format or, with ``binary=True``, the binary one; see
        :meth:`nusadb.Connection.copy_in_rows`. Returns the number of rows loaded."""
        sql = _sql_text(sql, self)
        if binary:
            writer = _binary.CopyWriter(types, _adapt, _encode_param)
            if hasattr(rows, "__aiter__"):
                source: Any = _async_copy_binary_chunks(rows, writer)
            else:
                source = _binary.copy_binary_chunks(rows, writer)
        else:
            if types is not None:
                raise ProgrammingError("types= applies to a binary COPY (binary=True)")
            if hasattr(rows, "__aiter__"):
                source = _async_copy_text_chunks(rows)
            else:
                source = _copy_text_chunks(rows)
        with _scoped_adapters(self._adapters):
            return await self._copy_in(sql, source, 1 if binary else 0)

    async def copy_out_rows(
        self, sql: Any, binary: bool = False, types: Optional[Sequence[str]] = None
    ) -> List[Tuple[Any, ...]]:
        """Export through ``COPY ... TO STDOUT`` as a list of rows: field text in the text format,
        or with ``binary=True`` values decoded at ``types``; see
        :meth:`nusadb.Connection.copy_out_rows`."""
        sql = _sql_text(sql, self)
        if binary:
            reader = _binary.CopyReader(types, Cursor._decode_binary_cell)
            await self._copy_out(sql, reader, 1)
            return reader.finish()
        if types is not None:
            raise ProgrammingError("types= applies to a binary COPY (binary=True)")
        lines = _CopyLines()
        await self._copy_out(sql, lines, 0)
        return lines.finish()

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
        """The next message of a reply; a notice is delivered (see :attr:`notices`) and skipped."""
        while True:
            mtype, r = await self._read_one()
            if mtype != proto.B_NOTICE_RESPONSE:
                return mtype, r
            _deliver_notice(self, r)

    async def _read_one(self) -> Tuple[int, proto.Reader]:
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
        if self._tpc_prepared:
            raise ProgrammingError(_TPC_PREPARED)
        if not self._autocommit and self._status == "I":
            await self._control("BEGIN")

    async def _read_until_ready(self, on_parse: Optional[Callable[[], None]] = None) -> _Result:
        columns: List[str] = []
        column_types: List[Optional[str]] = []
        rows: List[List[Optional[bytes]]] = []
        tag: Optional[str] = None
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = await self._read_message()
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

    async def _query(
        self, sql: str, parameters: Sequence[Any], binary: bool = False, auto: bool = True
    ) -> Tuple[_Result, bool]:
        """Run ``sql``: a simple query without parameters (or through Parse and Bind for a binary
        result), else Parse, Bind and Execute, with a statement prepared on the server once it has
        run :attr:`prepare_threshold` times. Returns the result and whether it is binary."""
        await self._begin_if_needed()
        if not parameters:
            if binary and not _needs_simple_query(sql):
                return await self._run_unnamed(sql, b"", [], True), True
            self._send(proto.query(sql))
            return await self._read_until_ready(), False
        compiled = _compile(sql)
        with _scoped_adapters(self._adapters):
            encoded, tags = _bind(_arrange(compiled, parameters))
        key = (compiled.sql, tags)
        name, parse = self._auto_statement(key) if auto else (None, False)
        if name is None:
            return await self._run_unnamed(compiled.sql, tags, encoded, binary), binary
        if parse:
            self._send(proto.parse(name, compiled.sql, tags))
        self._send(proto.bind("", name, encoded, binary))
        self._send(proto.describe_portal(""))
        self._send(proto.execute("", 0))
        self._send(proto.sync())
        try:
            remember = (lambda: self._remember(key, name, compiled.sql)) if parse else None
            return await self._read_until_ready(remember), binary
        except DatabaseError as exc:
            if parse or getattr(exc, "sqlstate", None) != _UNKNOWN_STATEMENT:
                raise
            # The server no longer knows the statement: forget it and run it unprepared once,
            # unless the error failed the transaction.
            self._forget(key)
            if self._status == "E":
                raise
            return await self._run_unnamed(compiled.sql, tags, encoded, binary), binary
        finally:
            if parse:
                await self._trim_statements()

    async def _run_unnamed(
        self, sql: str, tags: bytes, encoded: List[Optional[bytes]], binary: bool
    ) -> _Result:
        self._send(proto.parse("", sql, tags))
        self._send(proto.bind("", "", encoded, binary))
        self._send(proto.describe_portal(""))
        self._send(proto.execute("", 0))
        self._send(proto.sync())
        return await self._read_until_ready()

    def _auto_statement(self, key: Tuple[str, bytes]) -> Tuple[Optional[str], bool]:
        """See :meth:`nusadb.Connection._auto_statement`."""
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
        self._statements[key] = AsyncPreparedStatement(self, name, sql)

    def _forget(self, key: Tuple[str, bytes]) -> None:
        stale = self._statements.pop(key, None)
        if stale is not None:
            stale.closed = True

    async def _trim_statements(self) -> None:
        """Close the least recently used prepared statements past the cache size. Called inside an
        exchange, so the statements are closed directly."""
        from . import connection as sync

        while len(self._statements) > sync.STATEMENT_CACHE_SIZE:
            _key, oldest = self._statements.popitem(last=False)
            if not oldest.closed and not self._closed and not self._broken:
                oldest.closed = True
                self._send(proto.close_statement(oldest.name))
                self._send(proto.sync())
                await self._read_until_ready()

    async def _query_prepared(
        self, name: str, parameters: Sequence[Any], binary: bool = False
    ) -> _Result:
        await self._begin_if_needed()
        with _scoped_adapters(self._adapters):
            encoded, _tags = _bind(parameters)
        self._send(proto.bind("", name, encoded, binary))
        self._send(proto.describe_portal(""))
        self._send(proto.execute("", 0))
        self._send(proto.sync())
        return await self._read_until_ready()

    async def _execute_batch(self, sql: str, seq_of_parameters: Any) -> Tuple[int, bool]:
        """Pipelined executions (see :meth:`nusadb.Cursor.executemany`); each chunk is bound whole
        before any of it is sent."""
        await self._begin_if_needed()
        compiled = _compile(sql)
        sql = compiled.sql
        total, counted = 0, False
        rows = (_arrange(compiled, parameters) for parameters in seq_of_parameters)
        for chunk, failure in _bound_chunks(rows, self._adapters):
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

    async def _await_copy_start(self, expected: int) -> int:
        error: Optional[Tuple[str, str]] = None
        while True:
            mtype, r = await self._read_message()
            if mtype == expected:
                return r.u8() if r.remaining() else 0
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


class AsyncPipeline:
    """Statements queued on the client and sent together (see :class:`nusadb.Pipeline`). Until a
    sync nothing is sent, so other tasks can keep using the connection; the queued statements run
    after whatever those tasks sent first."""

    def __init__(self, connection: AsyncConnection) -> None:
        self._conn = connection
        self._queued: List[Tuple[bytes, "AsyncCursor"]] = []
        self._size = 0
        self._active = False

    async def __aenter__(self) -> "AsyncPipeline":
        self._conn._check_open()
        self._active = True
        return self

    async def __aexit__(self, exc_type: Any, *_exc: object) -> None:
        try:
            if exc_type is None:
                await self.sync()
        finally:
            self._queued = []
            self._size = 0
            self._active = False

    async def execute(
        self,
        sql: Any,
        parameters: Any = (),
        row_factory: Optional[RowFactory] = None,
        *,
        binary: Optional[bool] = None,
    ) -> "AsyncCursor":
        """Queue ``sql`` with ``parameters``; the returned cursor holds its result after the next
        sync. Sends the queue first when it is full. ``binary`` asks for the result in the binary
        format (default: the connection's :attr:`~AsyncConnection.binary`)."""
        if not self._active:
            raise ProgrammingError(
                "use the pipeline as a context manager: async with conn.pipeline() as p"
            )
        conn = self._conn
        conn._check_open()
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
        cursor = conn.cursor(row_factory, binary=binary)
        self._queued.append((frames, cursor))
        self._size += len(frames)
        if len(self._queued) >= PIPELINE_SYNC_STATEMENTS or self._size >= _COPY_CHUNK:
            await self.sync()
        return cursor

    async def sync(self) -> None:
        """Send the queued statements and load each result into its cursor. Raises the first
        error; the statements after it in this sync did not run."""
        if not self._queued:
            return
        queued, self._queued, self._size = self._queued, [], 0
        conn = self._conn
        async with conn._exchange():
            await conn._begin_if_needed()
            conn._send(b"".join(frames for frames, _cursor in queued) + proto.sync())
            cursors = iter(cursor for _frames, cursor in queued)
            columns: List[str] = []
            column_types: List[Optional[str]] = []
            rows: List[List[Optional[bytes]]] = []
            error: Optional[Tuple[str, str]] = None
            while True:
                mtype, r = await conn._read_message()
                if mtype in (proto.B_ROW_DESCRIPTION, proto.B_ROW_DESCRIPTION_TYPED):
                    columns, column_types = _read_description(mtype, r)
                elif mtype == proto.B_DATA_ROW:
                    rows.append(r.fields())
                elif mtype == proto.B_COMMAND_COMPLETE:
                    cursor = next(cursors)
                    cursor._load((columns, column_types, rows, r.string()), cursor.binary)
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


async def _async_copy_text_chunks(rows: AsyncIterable[Any]) -> AsyncIterator[bytes]:
    """An async iterable of rows as COPY text-format data, a chunk per thousand rows or 64 KiB."""
    batch: List[Any] = []
    async for row in rows:
        batch.append(row)
        if len(batch) >= 1000:
            for chunk in _copy_text_chunks(batch):
                yield chunk
            batch = []
    for chunk in _copy_text_chunks(batch):
        yield chunk


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

    def __init__(
        self,
        connection: AsyncConnection,
        row_factory: Optional[RowFactory],
        binary: bool = False,
    ) -> None:
        self._conn = connection
        #: Whether :meth:`execute` asks for results in the binary format.
        self.binary = binary
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

    def _load(self, result: _Result, binary: bool = False) -> None:
        self._rows._load(*result, binary)
        self.rowcount = self._rows.rowcount

    async def execute(
        self,
        operation: Union[str, "AsyncPreparedStatement", Any],
        parameters: Optional[Sequence[Any]] = None,
        *,
        binary: Optional[bool] = None,
    ) -> "AsyncCursor":
        """Run ``operation`` (see :meth:`nusadb.Cursor.execute`, ``binary`` included)."""
        self._check()
        binary = self.binary if binary is None else binary
        if not isinstance(operation, AsyncPreparedStatement):
            operation = _sql_text(operation, self._conn)
        async with self._conn._exchange():
            if isinstance(operation, AsyncPreparedStatement):
                result = await self._conn._query_prepared(
                    self._statement_name(operation),
                    _arrange(operation._compiled, parameters or ()),
                    binary,
                )
            else:
                result, binary = await self._conn._query(operation, parameters or (), binary)
        self._load(result, binary)
        return self

    async def executemany(
        self,
        operation: Union[str, "AsyncPreparedStatement", Any],
        seq_of_parameters: Any,
    ) -> None:
        """Run ``operation`` once per parameter set, pipelined (see
        :meth:`nusadb.Cursor.executemany`)."""
        self._check()
        if not isinstance(operation, AsyncPreparedStatement):
            operation = _sql_text(operation, self._conn)
        async with self._conn._exchange():
            if isinstance(operation, AsyncPreparedStatement):
                name = self._statement_name(operation)
                total, counted = 0, False
                for parameters in seq_of_parameters:
                    *_unused, tag = await self._conn._query_prepared(
                        name, _arrange(operation._compiled, parameters)
                    )
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


class AsyncServerCursor(AsyncCursor):
    """A named cursor whose query stays on the server, fetched as it is read (see
    :class:`nusadb.ServerCursor`): ``await cur.execute(sql)``, then the ``fetch*`` coroutines,
    ``async for`` (:attr:`itersize` rows per round trip) and ``await cur.scroll(n)``."""

    def __init__(
        self,
        connection: AsyncConnection,
        name: str,
        row_factory: Optional[RowFactory],
        scrollable: Optional[bool],
        withhold: bool,
    ) -> None:
        super().__init__(connection, row_factory)
        self.name = name
        self.scrollable = scrollable
        self.withhold = withhold
        self.itersize = 2000
        self._declared = False
        self._fetched = 0
        self.rownumber: Optional[int] = None

    async def execute(  # type: ignore[override]
        self, operation: Any, parameters: Optional[Any] = None, *, binary: Optional[bool] = None
    ) -> "AsyncServerCursor":
        """Declare the cursor for ``operation``, closing the one declared before."""
        self._check()
        if isinstance(operation, AsyncPreparedStatement):
            raise NotSupportedError("a named cursor takes SQL text, not a prepared statement")
        if binary:
            raise NotSupportedError("a named cursor fetches in the text format")
        operation = _sql_text(operation, self._conn)
        await self._close_on_server()
        scroll = {None: "", True: "SCROLL ", False: "NO SCROLL "}[self.scrollable]
        hold = " WITH HOLD" if self.withhold else ""
        sql = f"DECLARE {_quote_identifier(self.name)} {scroll}CURSOR{hold} FOR {operation}"
        async with self._conn._exchange():
            await self._conn._query(sql, parameters or (), auto=False)
        self._declared = True
        self._rows._load([], [], [], None)
        self._fetched = 0
        self.rowcount = -1
        self.rownumber = 0
        return self

    async def executemany(self, operation: Any, seq_of_parameters: Any) -> None:
        raise NotSupportedError("executemany is not available on a named cursor")

    async def fetchone(self) -> Any:
        self._check_declared()
        if not self._buffered():
            await self._fetch("FORWARD 1")
        return self._deliver()

    async def fetchmany(self, size: Optional[int] = None) -> List[Any]:
        self._check_declared()
        n = self.arraysize if size is None else size
        out = self._take(n)
        if len(out) < n:
            await self._fetch(f"FORWARD {n - len(out)}")
            out.extend(self._take(n - len(out)))
        return out

    async def fetchall(self) -> List[Any]:
        self._check_declared()
        out = self._take(self._buffered())
        await self._fetch("FORWARD ALL")
        out.extend(self._take(self._buffered()))
        return out

    async def __anext__(self) -> Any:
        self._check_declared()
        if not self._buffered():
            await self._fetch(f"FORWARD {max(1, self.itersize)}")
        row = self._deliver()
        if row is None:
            raise StopAsyncIteration
        return row

    async def scroll(self, value: int, mode: str = "relative") -> None:
        """Move to another row (see :meth:`nusadb.ServerCursor.scroll`)."""
        self._check_declared()
        if mode not in ("relative", "absolute"):
            raise ProgrammingError(f"scroll mode must be 'relative' or 'absolute', not {mode!r}")
        current = self.rownumber or 0
        target = current + value if mode == "relative" else value
        if target < 0:
            raise IndexError("scroll would move before the first row")
        if not await self._position(target) and target != 0:
            await self._position(current)
            raise IndexError("scroll would move past the last row")
        self._rows._load([], [], [], None)
        self.rownumber = target

    async def close(self) -> None:
        if not self._closed:
            try:
                await self._close_on_server()
            finally:
                self._closed = True

    def _check_declared(self) -> None:
        self._check()
        if not self._declared:
            raise ProgrammingError("execute a query on the named cursor first")

    def _buffered(self) -> int:
        return len(self._rows._rows) - self._rows._pos

    async def _fetch(self, how: str) -> None:
        async with self._conn._exchange():
            (columns, column_types, rows, _tag), _binary_result = await self._conn._query(
                f"FETCH {how} FROM {_quote_identifier(self.name)}", ()
            )
        if not columns:
            columns, column_types = self._rows._columns, self._rows._column_types
        self._rows._load(columns, column_types, rows, None)
        self._fetched += len(rows)
        self.rowcount = self._fetched

    async def _position(self, row: int) -> bool:
        async with self._conn._exchange():
            (_columns, _types, rows, _tag), _binary_result = await self._conn._query(
                f"FETCH ABSOLUTE {row} FROM {_quote_identifier(self.name)}", ()
            )
        return bool(rows)

    def _deliver(self) -> Any:
        row = self._rows._next_row()
        if row is not None and self.rownumber is not None:
            self.rownumber += 1
        return row

    def _take(self, n: int) -> List[Any]:
        out: List[Any] = []
        while len(out) < n:
            row = self._deliver()
            if row is None:
                break
            out.append(row)
        return out

    async def _close_on_server(self) -> None:
        if not self._declared:
            return
        self._declared = False
        conn = self._conn
        if conn._closed or conn._status == "E" or (conn._status == "I" and not self.withhold):
            return
        try:
            async with conn._exchange():
                # Sent as it is: closing must not open a transaction outside autocommit mode.
                conn._send(proto.query(f"CLOSE {_quote_identifier(self.name)}"))
                await conn._read_until_ready()
        except ProgrammingError as exc:
            if getattr(exc, "sqlstate", None) != "34000":
                raise


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
        # How this statement's parameters map onto its placeholders (by position or by name).
        self._compiled = _compile(sql)

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
        binary: bool = False,
    ) -> None:
        self._conn = connection
        self._sql = sql
        self._parameters = parameters
        self._batch = batch_rows
        self._cursor = AsyncCursor(
            connection, row_factory if row_factory is not None else connection.row_factory, binary
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
            compiled = _compile(self._sql)
            with _scoped_adapters(conn._adapters):
                encoded, tags = _bind(_arrange(compiled, self._parameters))
            conn._send(proto.parse("", compiled.sql, tags))
            conn._send(proto.bind("", "", encoded, self._cursor.binary))
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
                rows_view._load(columns, types, [], None, self._cursor.binary)
            elif mtype == proto.B_ROW_DESCRIPTION:
                count = r.u16()
                names = [r.string() for _ in range(count)]
                rows_view._load(names, [None] * count, [], None, self._cursor.binary)
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


async def _async_copy_binary_chunks(
    rows: AsyncIterable[Any], writer: "_binary.CopyWriter"
) -> AsyncIterator[bytes]:
    """An async iterable of rows as a binary COPY stream, in chunks of about 64 KiB."""
    buf = bytearray(writer.HEADER)
    async for row in rows:
        buf += writer.row(row)
        if len(buf) >= _COPY_CHUNK:
            yield bytes(buf)
            buf = bytearray()
    buf += writer.TRAILER
    yield bytes(buf)
