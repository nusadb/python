"""nusadb: the Python driver for NusaDB (PEP-249 / DB-API 2.0).

Speaks the Nusa Wire Protocol directly over a socket, with no C extension and only the
standard library.

Example::

    import nusadb

    conn = nusadb.connect(host="127.0.0.1", port=5678, user="nusadb-root", database="nusadb")
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INT NOT NULL, name TEXT)")
    cur.execute("INSERT INTO t VALUES ($1, $2)", [1, "alice"])
    cur.execute("SELECT id, name FROM t WHERE id = :1", [1])
    print(cur.fetchall())
    conn.close()

``paramstyle`` is reported as ``"numeric"``, so PEP-249 code uses positional
``:1``, ``:2`` markers. The server's own marker is ``$1``; the driver translates
``:1`` → ``$1`` for you, and a query written with the native ``$1`` still works.
The ``pyformat`` markers are accepted too: ``%s`` by position, and ``%(name)s``
with a mapping of parameters (``%%`` is then a literal ``%``).
"""

from __future__ import annotations

from . import rows, sql
from .connection import (
    PIPELINE_SYNC_STATEMENTS,
    STATEMENT_CACHE_SIZE,
    Connection,
    Cursor,
    MAX_NOTICES,
    Json,
    Notice,
    Notification,
    Pipeline,
    PreparedStatement,
    RowStream,
    ServerCursor,
    Transaction,
    Xid,
    connect,
    register_adapter,
    register_decoder,
)
from .dbapi_types import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Binary,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)
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
    TransactionRolledBack,
    Warning,
)
from .pool import Pool
from .types import CompositeInfo, Range, Vector, parse_record, register_composite

# PEP-249 module globals.
apilevel = "2.0"
threadsafety = 1  # threads may share the module but not connections
paramstyle = "numeric"  # positional ``:1`` markers; the driver translates them to native ``$1``

__version__ = "0.1.2"

__all__ = [
    "connect",
    "Connection",
    "Cursor",
    "Pipeline",
    "PIPELINE_SYNC_STATEMENTS",
    "PreparedStatement",
    "RowStream",
    "ServerCursor",
    "Transaction",
    "STATEMENT_CACHE_SIZE",
    "register_adapter",
    "register_decoder",
    "rows",
    "sql",
    "Json",
    "Notification",
    "Notice",
    "MAX_NOTICES",
    "Xid",
    "Pool",
    "Range",
    "Vector",
    "parse_record",
    "CompositeInfo",
    "register_composite",
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "Binary",
    "STRING",
    "BINARY",
    "NUMBER",
    "DATETIME",
    "ROWID",
    "TransactionRolledBack",
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
