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
- A statement that has run five times with parameters on a connection is prepared on the server
  and reused (`prepare_threshold=None` turns this off).
- The driver asks for protocol 1.3, for notices; a server with a lower minor answers at its own.

### Added

- `nusadb.sql`: build SQL from pieces with `SQL`, `Identifier`, `Literal`, `Placeholder` and
  `Composed`, `SQL.format()` with `{}`, `{0}` and `{name}` fields, `SQL.join()` and `+`.
  Identifiers and literals are quoted so that nothing they hold can change the statement. Every
  method that takes SQL text takes these too, sync and asyncio.
- Binary results: `connect(binary=True)` (also `?binary=true`), `cursor(binary=True)`,
  `execute(..., binary=True)` and `stream(..., binary=True)` ask for the binary format and decode
  every type to the same Python value as the text format, sync and asyncio.
- Binary `COPY`: `copy_in_rows(sql, rows, binary=True, types=[...])` writes the binary stream, each
  value in its column type's layout (types named, or taken from the Python values), and
  `copy_out_rows(sql, binary=True, types=[...])` reads one into typed values, checking it strictly.
- Composite types: `nusadb.CompositeInfo` (`fetch`, `fetch_async`, `load`, `dump`),
  `nusadb.register_composite` to bind objects as records, and the `nusadb.rows.composite_row` row
  factory.
- Automatic server-side prepare with the `prepare_threshold` option (default 5; also a URL option,
  a connection property and a Django `OPTIONS` key), sharing the `STATEMENT_CACHE_SIZE` cache. A
  statement the server no longer knows is dropped and run unprepared once, outside a failed
  transaction.
- Two-phase commit: `xid()`, `tpc_begin`, `tpc_prepare`, `tpc_commit`, `tpc_rollback` and
  `tpc_recover`, with `nusadb.Xid`, sync and asyncio.
- Notices: `conn.notices` holds the `nusadb.Notice` messages the server sends (protocol 1.3), and
  `add_notice_handler` / `remove_notice_handler` register callbacks, sync and asyncio.
- `pyformat` placeholders: `%s` by position and `%(name)s` from a mapping (a name may repeat),
  with `%%` for a literal `%`, in `execute`, `executemany`, prepared statements and `stream`, sync
  and asyncio alike. The native `$1` and the `numeric` `:1` work as before.
- Range, network and vector types: a range decodes to `nusadb.Range`, `INET` and `CIDR` to
  `ipaddress` values, `VECTOR` to a list of floats, and each binds back as a parameter
  (`nusadb.Vector` for a vector). `nusadb.parse_record` splits a composite value's text. The other
  types the server reports by name (`MACADDR`, `BIT`, geometric, full-text, `XML`) keep their text.
- `nusadb.django`, a Django database backend (Django 4.2): migrations, the ORM's queries, JSON
  lookups, `select_for_update`, savepoints, introspection for `inspectdb`, test databases made
  with `CREATE DATABASE`, and `dbshell` through `nusadb-cli`. Install with `nusadb[django]`.
- `conn.pipeline()`: send a group of statements in one round trip and read each result from its
  cursor afterwards, sync and asyncio.
- Named cursors (`conn.cursor(name=..., scrollable=..., withhold=...)`): the query stays on the
  server and rows are fetched as they are read, with `scroll()`, `rownumber` and `itersize`, sync
  and asyncio. Parameters in a named cursor's query need a server that binds them in `DECLARE`.
- `conn.copy_in_rows(sql, rows)` and `conn.copy_out_rows(sql)`: `COPY` with Python rows instead of
  preformatted bytes, sync and asyncio.
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

- `copy_in_rows` in the text format applies registered adapters, as parameters do.
- `copy_in_rows` and `copy_out_rows` check the statement's `COPY` format and raise
  `ProgrammingError` when it does not match the call, instead of sending rows the server cannot
  read.
- A timestamp range bound with fewer than six fractional digits decodes to `datetime` on Python
  versions before 3.11 instead of leaving the whole range as text.

- `TIMESTAMPTZ`, `TIMESTAMP` and `TIME` values with fewer than six fractional digits decode to
  `datetime` on Python versions before 3.11 instead of staying text.
- A `Decimal` in exponent form (`Decimal("1E+3")`) binds as its value, an `IntEnum` member as its
  number, and a mapping inside a list as JSON.
- A copy in the wrong direction (`copy_out` on `COPY ... FROM STDIN`) raises instead of hanging.
- In `nusadb.aio`, a `copy_in` source that raises a database error ends the `COPY` cleanly instead of
  leaving the connection out of step.
- A call interrupted part way, including `poll`, marks the connection broken instead of leaving
  it out of step with the server.
- The pool no longer takes back a connection returned after `close()`, nor opens one for a checkout
  that was waiting when the pool closed.
- A connection that fails while it is being opened closes its socket.
