"""A small thread-safe connection pool."""

from __future__ import annotations

import threading
from typing import List, Optional

from .connection import Connection, connect
from .exceptions import InterfaceError


class Pool:
    """A bounded pool of :class:`Connection` objects.

    :meth:`acquire` reuses an idle connection or opens a new one (up to
    ``max_size`` live connections); :meth:`release` returns one. Use
    :meth:`connection` for a context-managed checkout.
    """

    def __init__(self, max_size: int = 10, **connect_kwargs: object) -> None:
        self._max = max(1, max_size)
        self._kwargs = connect_kwargs
        self._idle: List[Connection] = []
        self._live = 0
        self._lock = threading.Lock()
        self._slot = threading.Semaphore(self._max)
        self._closed = False

    def acquire(self, timeout: Optional[float] = None) -> Connection:
        if self._closed:
            raise InterfaceError("pool is closed")
        if not self._slot.acquire(timeout=timeout):
            raise InterfaceError("timed out waiting for a pooled connection")
        try:
            with self._lock:
                conn = self._idle.pop() if self._idle else None
            if conn is None:
                conn = connect(**self._kwargs)  # type: ignore[arg-type]
                with self._lock:
                    self._live += 1
            return conn
        except BaseException:
            self._slot.release()
            raise

    def release(self, conn: Connection) -> None:
        with self._lock:
            self._idle.append(conn)
        self._slot.release()

    @property
    def idle_count(self) -> int:
        with self._lock:
            return len(self._idle)

    def connection(self) -> "_PooledConnection":
        return _PooledConnection(self)

    def close(self) -> None:
        self._closed = True
        with self._lock:
            idle = self._idle
            self._idle = []
        for conn in idle:
            conn.close()


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
