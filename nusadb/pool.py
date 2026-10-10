"""A small thread-safe connection pool."""

from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, NamedTuple, Optional

from .connection import Connection, connect
from .exceptions import InterfaceError


class _Opened(NamedTuple):
    """What the pool remembers about a connection it opened."""

    at: float
    autocommit: bool


class _Idle(NamedTuple):
    conn: Connection
    since: float


class Pool:
    """A bounded pool of :class:`Connection` objects.

    :meth:`acquire` reuses an idle connection or opens a new one (up to ``max_size`` live
    connections); :meth:`release` returns one. Use :meth:`connection` for a context-managed
    checkout. ``dsn`` and the remaining keyword arguments are passed to :func:`nusadb.connect`.

    A returned connection comes back clean: one left inside a transaction is rolled back, its
    ``autocommit`` is restored to what it was when the pool opened it, and a closed or broken one
    is dropped (its slot freed for a new connection). With ``reset_on_return`` (the default) its
    session is reset too (``RESET ALL``, ``UNLISTEN *``, then the connection's own settings such
    as ``statement_timeout`` applied again), so the next user does not inherit what the last one
    ``SET``.

    Options:

    * ``acquire_timeout``: seconds a checkout waits for a free connection (``None``: forever)
    * ``idle_timeout``: close a connection that has been idle in the pool this long
    * ``max_lifetime``: close a connection this long after it was opened, at its next checkout
      or return
    * ``test_on_checkout``: run ``SELECT 1`` on an idle connection before handing it out, and
      replace it when that fails
    """

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
        self._idle: List[_Idle] = []
        self._live = 0
        self._lock = threading.Lock()
        self._slot = threading.Semaphore(self._max)
        self._closed = False
        # Every live connection the pool opened (by identity), and which of them are checked out.
        self._opened: Dict[int, _Opened] = {}
        self._out: Dict[int, Connection] = {}

    def acquire(self, timeout: Optional[float] = -1.0) -> Connection:
        """Check out a connection, waiting up to ``timeout`` seconds (``None`` waits forever; the
        default is the pool's ``acquire_timeout``) when every one is in use."""
        if timeout is not None and timeout < 0:
            timeout = self._acquire_timeout
        if self._closed:
            raise InterfaceError("pool is closed")
        if not self._slot.acquire(timeout=timeout):
            raise InterfaceError("timed out waiting for a pooled connection")
        try:
            conn = self._reuse_idle()
            if conn is None:
                conn = connect(self._dsn, **self._kwargs)
                with self._lock:
                    if self._closed:
                        conn.close()
                        raise InterfaceError("pool is closed")
                    self._live += 1
                    self._opened[id(conn)] = _Opened(time.monotonic(), conn.autocommit)
            with self._lock:
                self._out[id(conn)] = conn
            return conn
        except BaseException:
            self._slot.release()
            raise

    def _reuse_idle(self) -> Optional[Connection]:
        """An idle connection still fit to hand out, closing the stale or failed ones on the way."""
        while True:
            with self._lock:
                if self._closed:
                    raise InterfaceError("pool is closed")
                if not self._idle:
                    return None
                conn, since = self._idle.pop()
                stale = self._expired(conn) or (
                    self._idle_timeout is not None
                    and time.monotonic() - since >= self._idle_timeout
                )
            if not stale and (not self._test_on_checkout or _responds(conn)):
                return conn
            self._drop(conn)

    def _expired(self, conn: Connection) -> bool:
        opened = self._opened.get(id(conn))
        return (
            self._max_lifetime is not None
            and opened is not None
            and time.monotonic() - opened.at >= self._max_lifetime
        )

    def _drop(self, conn: Connection) -> None:
        with self._lock:
            self._live -= 1
            self._opened.pop(id(conn), None)
        conn.close()

    def release(self, conn: Connection) -> None:
        """Return a checked-out connection. Returning one twice, or one this pool did not hand
        out, raises :class:`InterfaceError`."""
        with self._lock:
            if self._out.pop(id(conn), None) is None:
                raise InterfaceError("connection is not checked out from this pool")
            opened = self._opened.get(id(conn))
        try:
            clean = (
                opened is not None
                and not self._expired(conn)
                and _clean(conn, opened.autocommit, self._reset_on_return)
            )
            with self._lock:
                # Decided under the lock: a pool closed meanwhile must not take it back.
                keep = clean and not self._closed
                if keep:
                    self._idle.append(_Idle(conn, time.monotonic()))
            if not keep:
                self._drop(conn)
        finally:
            self._slot.release()

    @property
    def idle_count(self) -> int:
        with self._lock:
            return len(self._idle)

    @property
    def size(self) -> int:
        """How many connections the pool holds open (idle and checked out)."""
        with self._lock:
            return self._live

    def connection(self) -> "_PooledConnection":
        return _PooledConnection(self)

    def close(self) -> None:
        """Close the idle connections; the checked-out ones close when they are returned."""
        with self._lock:
            self._closed = True
            idle = self._idle
            self._idle = []
        for conn, _since in idle:
            self._drop(conn)

    def __enter__(self) -> "Pool":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


def _responds(conn: Connection) -> bool:
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchall()
        if conn.transaction_status != "idle":
            conn.rollback()
    except Exception:  # noqa: BLE001 - any failure means the connection is not usable
        return False
    return True


def _clean(conn: Connection, autocommit: bool, reset: bool) -> bool:
    """Ready a returned connection for its next user; ``False`` when it must be dropped."""
    if conn.closed or conn.broken:
        return False
    try:
        if conn.transaction_status != "idle":
            conn.rollback()
        if conn.autocommit != autocommit:
            conn.autocommit = autocommit
        if reset:
            conn._reset_session()
    except Exception:  # noqa: BLE001 - any failure here means the connection is not reusable
        return False
    return True


class _PooledConnection:
    """Context manager that returns its connection to the pool on exit."""

    def __init__(self, pool: Pool) -> None:
        self._pool = pool
        self._conn: Optional[Connection] = None

    def __enter__(self) -> Connection:
        self._conn = self._pool.acquire()
        return self._conn

    def __exit__(self, *_exc: object) -> None:
        if self._conn is not None:
            self._pool.release(self._conn)
            self._conn = None
