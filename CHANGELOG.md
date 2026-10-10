# Changelog

## 0.1.2

### Changes in behaviour

- `connect()` takes a connection URL as its first argument (`nusadb://user@host:port/db?...`).
  A first argument that is not a URL is still read as the host, with the old positional order
  after it.
- `Pool(max_size, dsn, ...)`: a checkout now waits at most 30 seconds by default (`acquire_timeout`)
  instead of forever. A returned connection has its session reset (`RESET ALL`, `UNLISTEN *`, then
  its own settings applied again) and its `autocommit` restored; pass `reset_on_return=False` to
  keep the old behaviour. Returning a connection twice raises `InterfaceError`.
- `executemany` pipelines its executions (256 per round trip). The first error still stops the
  batch. `rowcount` is `0`, not `-1`, when the statements matched no rows.
- Every parameter declares its type to the server, so a `str` stays text even when it looks like a
  number (`"007"` is no longer `7`).
- A set, a generator or any other iterator passed as a single parameter value raises
  `ProgrammingError`; it used to be bound as its `repr`.
- `commit()` raises `TransactionRolledBack` when a statement in the transaction had failed and the
  server rolled it back.
- Every `nusadb.Error` has a `sqlstate` attribute (`None` for errors raised by the driver).
- The default user and password are `nusadb-root` (they were `nusa-root`).
- Server errors map by SQLSTATE class: `XX` to `InternalError`, `25`, `40`, `53`, `57` and `58` to
  `OperationalError` (they used to fall back to `ProgrammingError`).
- The SQLAlchemy dialect needs SQLAlchemy 2.0. `metadata.create_all` now emits the server's own
  types (`VARCHAR(n)`, `NUMERIC(p, s)`, `DOUBLE PRECISION`, `TIMESTAMP WITH TIME ZONE`, `BYTEA`,
  `UUID`, `JSONB`) and an identity column for an autoincrementing integer primary key.

### Added

- Connection URLs and the `connect_timeout`, `statement_timeout` and `keepalive` options; TLS from
  the URL (`nusadbs://`, `tls`, `tls_ca`, `tls_cert`, `tls_key`).
- `conn.transaction(isolation=..., read_only=...)`: a block that commits or rolls back, nesting
  through savepoints.
- Prepared statements: `conn.prepare(sql)`, `conn.prepare_cached(sql)`.
- `conn.stream(sql, params, batch_rows)`: read a large result a batch at a time.
- Row factories (`nusadb.rows.dict_row`, `namedtuple_row`), `register_adapter` and
  `register_decoder`.
- Pool options: `acquire_timeout`, `idle_timeout`, `max_lifetime`, `test_on_checkout`,
  `reset_on_return`.
- `nusadb.aio`: the asyncio API (connections, cursors, transactions, prepared statements, streams,
  `COPY`, notifications, cancellation and `AsyncPool`).
- The `nusadb+asyncio://` SQLAlchemy dialect. Both dialects now fetch new ids through `RETURNING`
  (bulk inserts included), support `isolation_level` and `AUTOCOMMIT`, query JSON elements, reflect
  types, keys, foreign key actions, unique and check constraints, indexes, views, schemas and
  sequences, and recognise a lost connection.
- PEP-249 type objects and constructors (`Date`, `Timestamp`, `Binary`, `STRING`, `NUMBER`, ...),
  `cursor.callproc`, `cursor.connection`, the exception classes on the connection,
  `conn.transaction_status`, `conn.closed`, `conn.broken`.
- `nusadb.Json(value)` to send a value as JSON.

### Fixed

- `TIMESTAMPTZ`, `TIMESTAMP` and `TIME` values with fewer than six fractional digits decode to
  `datetime` on Python versions before 3.11 instead of staying text.
- A `Decimal` in exponent form (`Decimal("1E+3")`) binds as its value, an `IntEnum` member as its
  number, and a mapping inside a list as JSON.
- A copy in the wrong direction (`copy_out` on `COPY ... FROM STDIN`) raises instead of hanging.
- A call interrupted part way, including `poll`, marks the connection broken instead of leaving
  it out of step with the server.
- The pool no longer takes back a connection returned after `close()`, nor opens one for a checkout
  that was waiting when the pool closed.
- A connection that fails while it is being opened closes its socket.
