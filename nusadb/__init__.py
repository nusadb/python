"""nusadb — the Python driver for NusaDB (PEP-249 / DB-API 2.0).

Speaks the Nusa Wire Protocol (``PROTOCOL_VERSION 1.1``, see
``docs/wire-protocol.md``) directly over a socket — no C extension, stdlib only.

Example::

    import nusadb

    conn = nusadb.connect(host="127.0.0.1", port=5678, user="nusa-root", database="nusadb")
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INT NOT NULL, name TEXT)")
    cur.execute("INSERT INTO t VALUES ($1, $2)", [1, "alice"])
    cur.execute("SELECT id, name FROM t WHERE id = :1", [1])
    print(cur.fetchall())
    conn.close()

``paramstyle`` is reported as ``"numeric"``, so PEP-249 code uses positional
``:1``, ``:2`` markers. The server's own marker is ``$1``; the driver translates
``:1`` → ``$1`` for you, and a query written with the native ``$1`` still works.
"""

from __future__ import annotations

from .connection import Connection, Cursor, Notification, connect
from .exceptions import (
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
from .pool import Pool

# PEP-249 module globals.
apilevel = "2.0"
threadsafety = 1  # threads may share the module but not connections
paramstyle = "numeric"  # positional ``:1`` markers; the driver translates them to native ``$1``

__version__ = "0.1.0"

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "Notification",
    "Pool",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "Warning",
    "Error",
    "InterfaceError",
    "DatabaseError",
    "DataError",
    "OperationalError",
    "IntegrityError",
    "InternalError",
    "ProgrammingError",
    "NotSupportedError",
]
