# nusadb: Python driver for NusaDB

A pure-Python [PEP-249 / DB-API 2.0](https://peps.python.org/pep-0249/) driver that speaks the
[Nusa Wire Protocol](https://github.com/nusadb/nusadb/blob/master/docs/wire-protocol.md) directly
over a socket. It has no C extension and needs only the standard library. It comes with an asyncio
API and SQLAlchemy dialects for both.

## Install

```bash
pip install nusadb                 # the driver (blocking and asyncio)
pip install "nusadb[sqlalchemy]"   # with SQLAlchemy
pip install "nusadb[asyncio]"      # with SQLAlchemy's asyncio support
```

## Usage

```python
import nusadb

conn = nusadb.connect(host="127.0.0.1", port=5678, user="nusadb-root", database="nusadb")
cur = conn.cursor()

cur.execute("CREATE TABLE t (id INT NOT NULL, name TEXT)")
cur.execute("INSERT INTO t VALUES ($1, $2)", [1, "alice"])

cur.execute("SELECT id, name FROM t WHERE id = $1", [1])
for row in cur:
    print(row)            # (1, 'alice'): INT decodes to int, TEXT to str

conn.close()
```

`connect()` defaults to `127.0.0.1:5678`, database `nusadb`, user and password `nusadb-root`. Pass
`password=None` to send none (a server that trusts its clients ignores it either way).

## Connecting

`connect()` also takes a URL, and keyword arguments override what it names:

```python
conn = nusadb.connect("nusadb://app:s3cret@db.example:5678/sales?statement_timeout=30s")
conn = nusadb.connect("nusadbs://app@db.example/sales", password=os.environ["DB_PASSWORD"])
```

`nusadbs://` turns TLS on with the system's trusted roots. Write an IPv6 host in brackets
(`nusadb://[::1]:5678/db`) and percent-encode reserved characters in the user, password and
database. The query keys are:

| key | value |
| --- | --- |
| `connect_timeout` | how long connecting may take: the TCP connection, TLS and authentication |
| `statement_timeout` | how long a statement may run before the server cancels it |
| `keepalive` | seconds of silence before TCP keepalive probes start |
| `tls` | `disable`, `require`, or `insecure` (no verification, for tests) |
| `tls_ca` | a PEM file of CA certificates to trust instead of the system roots |
| `tls_cert`, `tls_key` | the client certificate and key, for a server that requires one |

A duration is a number of seconds or a number with `ms`, `s`, `min` or `h` (`500ms`, `5min`). The
same names work as keyword arguments, in seconds: `connect(timeout=10, statement_timeout=2.5)`;
`timeout` bounds every read and write on the socket.

## Parameters

Placeholders are positional: `$1`, `$2`, ... (the server's own marker) or the DB-API `:1`, `:2`,
which the driver rewrites to `$1` outside strings, comments and `::` casts. `paramstyle` is
`"numeric"`. Pass the values as a sequence; `None` is SQL `NULL`.

The `pyformat` markers work as well, so code written for that style runs unchanged: `%s` takes
the values by position, and `%(name)s` takes them by name from a mapping, where a name may appear
more than once. In such a query write `%%` for a literal `%` (a modulo). A query that uses `$1` or
`:1` keeps its `%` as written.

```python
cur.execute("SELECT * FROM orders WHERE customer = %(id)s AND total > %(min)s", {"id": 7, "min": 100})
cur.executemany("INSERT INTO t VALUES (%s, %s)", [(1, "a"), (2, "b")])
```

Mixing `%s` and `%(name)s`, a missing name, or the wrong number of values raises
`ProgrammingError` before anything reaches the server.

Each parameter declares its type to the server, so it binds as exactly that type. A `str` stays
text even when it looks like a number (`"007"` keeps its zeros), and an `int` stays an integer.

| Python | binds as |
| --- | --- |
| `bool` | `BOOL` |
| `int` | `INT` (`NUMERIC` past the 64-bit range) |
| `float` | `FLOAT` |
| `decimal.Decimal` | `NUMERIC` |
| `str` | `TEXT` |
| `bytes`, `bytearray`, `memoryview` | `BYTEA` |
| `datetime.date` | `DATE` |
| `datetime.time` | `TIME` (`TIMETZ` when it has a `tzinfo`) |
| `datetime.datetime` | `TIMESTAMP` (`TIMESTAMPTZ` when it has a `tzinfo`) |
| `datetime.timedelta` | `INTERVAL` |
| `uuid.UUID` | `UUID` |
| `dict`, `nusadb.Json(value)` | `JSON` |
| `list`, `tuple` | an array, nested and `None`-aware |

Wrap a list in `nusadb.Json` to send it as a JSON array rather than a SQL array. Any other type is
sent as its `str()` and the server works out its type.

## Value types

Each column's type, reported by the server, decides the Python type it decodes to:

| NusaDB type | Python type |
| --- | --- |
| `BOOL` | `bool` |
| `INT` | `int` |
| `FLOAT` | `float` |
| `NUMERIC` | `decimal.Decimal` |
| `DATE` / `TIME` / `TIMESTAMP` (and the `TZ` forms) | `datetime.date` / `time` / `datetime` |
| `INTERVAL` | `datetime.timedelta` (an interval with months or years stays `str`) |
| `UUID` | `uuid.UUID` |
| `JSON` | the parsed `dict` / `list` / scalar |
| arrays | `list`, elements decoded at the array's element type |
| `BYTEA` | `bytes` |
| ranges (`INT4RANGE`, `DATERANGE`, `TSTZRANGE`, ...) | `nusadb.Range` |
| `INET` | `ipaddress` interface, or address for a single host |
| `CIDR` | `ipaddress` network |
| `VECTOR` | `list` of `float` |
| `TEXT` and anything else | `str` |

`MACADDR`, `BIT`, geometric types, `TSVECTOR`, `XML` and the rest keep their text, with the type name
in `cursor.description`. A composite (row) value also arrives as text; `nusadb.parse_record(text)`
splits it into its fields.

The same types bind as parameters: a `nusadb.Range(lower, upper, bounds="[)")` (`None` for an
unbounded side, `Range(empty=True)` for the empty range), an `ipaddress` address, interface or
network, and `nusadb.Vector([...])` for a vector (a plain list binds as an array).

A value that does not parse as its type comes back as its text rather than raising mid-fetch.
`cursor.description[i][1]` is the column's type name, and it compares equal to the PEP-249 type
objects: `cur.description[0][1] == nusadb.NUMBER`. The module also provides `Date`, `Time`,
`Timestamp`, `DateFromTicks`, `TimeFromTicks`, `TimestampFromTicks` and `Binary`.

## Transactions

By default a connection is in autocommit mode: each statement is its own transaction. With
`autocommit=False` the connection opens a transaction before the first statement, and `commit()` /
`rollback()` end it. `autocommit` can be changed between transactions, not inside one.

```python
conn = nusadb.connect(autocommit=False)
cur = conn.cursor()
cur.execute("INSERT INTO t VALUES (1, 'a')")
conn.commit()
```

When a statement inside a transaction fails, the server rolls the whole transaction back at
`COMMIT`. `commit()` then raises `nusadb.TransactionRolledBack`, so lost work is never reported as
saved. `conn.transaction_status` says where the connection stands: `"idle"`, `"in_transaction"`, or
`"failed"` after an error, until a rollback.

`conn.transaction()` wraps a block in a transaction that commits when the block ends and rolls back
when it raises. Used inside another one it becomes a savepoint, so blocks nest:

```python
with conn.transaction(isolation="serializable"):
    cur.execute("UPDATE accounts SET balance = balance - $1 WHERE id = $2", [10, 1])
    try:
        with conn.transaction():          # a savepoint
            cur.execute("INSERT INTO audit VALUES ($1)", ["moved 10"])
    except nusadb.IntegrityError:
        pass                              # only the audit insert is undone
```

`isolation` is `"read uncommitted"`, `"read committed"`, `"repeatable read"` or `"serializable"`,
and `read_only=True` makes the transaction read-only; both apply to the outermost block.

Inside a transaction, `savepoint(name)` marks a point to return to with `rollback_to(name)` (the
transaction stays open) or to forget with `release_savepoint(name)`:

```python
cur.execute("INSERT INTO t VALUES (1, 'a')")
conn.savepoint("sp1")
cur.execute("INSERT INTO t VALUES (2, 'b')")
conn.rollback_to("sp1")   # undoes (2), keeps (1)
conn.commit()
```

## Errors

Errors follow the PEP-249 hierarchy. A server error carries its five-character SQLSTATE as
`.sqlstate` and maps to the closest class: `IntegrityError` for a constraint violation, `DataError`
for bad data, `ProgrammingError` for bad SQL, `OperationalError` for a serialization failure, a
deadlock, a cancelled statement or a lost connection, `NotSupportedError` for a missing feature.

```python
try:
    cur.execute("INSERT INTO t VALUES ($1, $2)", [1, "dup"])
except nusadb.IntegrityError as e:
    print(e.sqlstate)     # 23505
```

A connection that loses its server, or whose call is interrupted part way (an exception or a
`KeyboardInterrupt` while a reply is being read), is marked broken: `conn.broken` is true and every
later call raises `OperationalError`. Open a new connection.

## Bulk insert and COPY

`cursor.executemany(sql, seq_of_parameters)` runs a statement once per parameter set and sets
`cursor.rowcount` to the total affected. The executions are pipelined, 256 to a round trip, so a
large batch costs a handful of round trips. The first error stops the batch and is raised; in
autocommit mode the statements before it have been committed.

For bulk load and export, `conn.copy_in` / `conn.copy_out` use the `COPY` sub-protocol, one round
trip for the whole dataset, in the server's text format (tab-delimited fields, `\N` for `NULL`, one
row per line):

```python
import io

loaded = conn.copy_in("COPY t (id, name) FROM STDIN", io.BytesIO(b"1\talice\n2\t\\N\n"))
sink = io.BytesIO()
exported = conn.copy_out("COPY t TO STDOUT", sink)
```

To load Python values without formatting them yourself, `conn.copy_in_rows` takes an iterable of
rows (a generator streams a load of any size); each value is written as it would bind as a
parameter. `conn.copy_out_rows` returns the rows of an export as tuples of text, `None` for `NULL`:

```python
loaded = conn.copy_in_rows("COPY t (id, name) FROM STDIN", [(1, "alice"), (2, None)])
rows = conn.copy_out_rows("COPY t TO STDOUT")  # [("1", "alice"), ("2", None)]
```

These two use the default text format, so the statement must not set `FORMAT`, `DELIMITER` or
`NULL`. A `COPY` the server refuses raises, and the connection stays usable; so does a row the
driver cannot write, which loads nothing.

## Pipelines

`conn.pipeline()` sends a group of statements in one round trip. Inside the block, `p.execute`
queues a statement and returns the cursor its result will be loaded into; the results arrive when
the block ends (or at `p.sync()`):

```python
with conn.pipeline() as p:
    inserted = p.execute("INSERT INTO t (id, name) VALUES (%s, %s)", (1, "alice"))
    found = p.execute("SELECT name FROM t WHERE id = %s", (1,))
print(inserted.rowcount, found.fetchall())
```

When a statement fails, the server skips the rest of that group and the error is raised; the
skipped statements' cursors stay empty. An exception inside the block sends nothing that was still
queued. A queue is also sent on its own once it holds `nusadb.PIPELINE_SYNC_STATEMENTS` (256)
statements. `nusadb.aio` has the same API with `async with` and `await p.execute(...)`.

## Prepared statements

`conn.prepare(sql)` parses a statement once on the server; pass the result to `execute` or
`executemany` in place of the SQL. `conn.prepare_cached(sql)` returns the statement already prepared
for the same text, keeping up to 256 per connection.

```python
insert = conn.prepare_cached("INSERT INTO t VALUES ($1, $2)")
cur.execute(insert, [3, "carol"])
```

The server stores the text as it is, so a mistake in it is reported when the statement first runs.

## Large results

`conn.stream(sql, params, batch_rows=1024)` reads a result a batch at a time, so the client never
holds all of it. The connection runs nothing else until the stream is read to the end or closed:

```python
with conn.stream("SELECT id, payload FROM events WHERE day = $1", [day]) as rows:
    for row in rows:
        handle(row)
```

A named cursor keeps the query on the server and brings rows over as they are fetched:
`fetchone` and `fetchmany(n)` ask for just those rows, and iteration fetches `cur.itersize` (2000)
at a time. Unlike a stream, the connection stays free for other statements between fetches.

```python
conn = nusadb.connect(autocommit=False)
cur = conn.cursor(name="events", scrollable=True)
cur.execute("SELECT id, payload FROM events WHERE day = %s ORDER BY id", (day,))
first = cur.fetchmany(100)
cur.scroll(-10)                 # back ten rows; scroll(0, "absolute") goes to the start
for row in cur:
    handle(row)
cur.close()
```

The cursor lives until its transaction ends. In autocommit mode, or to keep it past `commit()`,
pass `withhold=True`. `scrollable=True` allows moving back; `cur.rownumber` is the index of the next
row and `description` is set once the first rows are fetched. `nusadb.aio` has the same with
`await` and `async for`.

## Row shapes and custom types

Rows are tuples. A row factory from `nusadb.rows` changes that, per connection or per cursor:

```python
from nusadb.rows import dict_row, namedtuple_row

cur = conn.cursor(row_factory=dict_row)
cur.execute("SELECT id, name FROM t")
cur.fetchone()          # {'id': 1, 'name': 'alice'}
```

`nusadb.register_adapter(cls, fn)` binds values of your own type through `fn`, which returns
something the driver already binds (a `str`, a number, a `datetime`, ...).
`nusadb.register_decoder("NUMERIC", float)` decodes a column type your way; the type names are those
in `cursor.description`, and an element type also applies inside arrays.

## Stored procedures

`cursor.callproc("name", [args])` runs `CALL name($1, ...)` and returns the arguments, as PEP-249
specifies.

## TLS

Pass an `ssl.SSLContext` to `connect(ssl=...)`. The server uses implicit TLS 1.3, so the TLS session
starts before any protocol message. A context with `load_cert_chain(...)` presents a client
certificate to a server that requires one.

```python
import ssl, nusadb
ctx = ssl.create_default_context(cafile="ca.pem")
conn = nusadb.connect(host="db.example", user="app", password="...", ssl=ctx)
```

## Authentication

When the server requires a password, the driver runs the SCRAM-SHA-256 exchange and verifies the
server's signature.

## Connection pool

```python
from nusadb import Pool

pool = Pool(10, "nusadb://app@db.example/sales", password="...", idle_timeout=300)
with pool.connection() as conn:
    conn.cursor().execute("SELECT 1")
```

A returned connection comes back clean: one left inside a transaction is rolled back, its
`autocommit` is restored, its session is reset (`RESET ALL`, `UNLISTEN *`, then its own settings
such as `statement_timeout` applied again) and a closed or broken one is dropped, freeing its slot.
The options are `acquire_timeout` (how long a checkout waits; 30 seconds by default, `None` for
ever), `idle_timeout`, `max_lifetime`, `test_on_checkout` (run `SELECT 1` before handing out an idle
connection) and `reset_on_return`. Returning a connection twice raises `InterfaceError`.

## Notifications (LISTEN/NOTIFY)

`listen(channel)` subscribes the connection; a `notify(channel, payload)` from any connection on the
same database is then delivered to it. `poll(timeout)` waits for the next one (`0` does not wait,
`None` waits forever), and `notifications()` returns those received while reading other replies.

```python
conn.listen("orders")
note = conn.poll(5.0)          # Notification(pid, channel, payload), or None on timeout
conn.unlisten("orders")
```

## Cancellation

`conn.cancel()`, called from another thread, asks the server to stop the statement the connection
is running; the statement fails with an `OperationalError`.

## asyncio

`nusadb.aio` is the same driver with every call that talks to the server awaited:

```python
import nusadb.aio

conn = await nusadb.aio.connect("nusadb://app@db.example/sales")
async with conn:
    cur = await conn.execute("SELECT id, name FROM t WHERE id = $1", [1])
    print(await cur.fetchone())
    async with conn.transaction():
        await conn.execute("UPDATE t SET name = $1 WHERE id = $2", ["bob", 1])
    async with conn.stream("SELECT * FROM events") as rows:
        async for row in rows:
            ...
```

Prepared statements, pipelined `executemany`, `COPY`, notifications, cancellation, row factories and
`nusadb.aio.AsyncPool` (the options of `Pool`) all work the same way. Tasks sharing a connection take
turns. A task cancelled while it waits on the server leaves its reply unread, so the connection is
marked broken and the next call raises `OperationalError`.

## SQLAlchemy

Installing the package registers two dialects, `nusadb://` on the blocking driver and
`nusadb+asyncio://` on the asyncio one (SQLAlchemy 2.0; the asyncio one also needs `greenlet`):

```python
from sqlalchemy import create_engine
engine = create_engine("nusadb://app@db.example:5678/sales")

from sqlalchemy.ext.asyncio import create_async_engine
engine = create_async_engine("nusadb+asyncio://app@db.example:5678/sales")
```

An integer primary key becomes an identity column and new ids come back through `RETURNING`, also
for bulk inserts. The column types map to the server's own (`VARCHAR(n)`, `NUMERIC(p, s)`,
`TIMESTAMP WITH TIME ZONE`, `BYTEA`, `UUID`, `JSONB`, `INTERVAL`, arrays), and JSON elements can be
queried (`Book.meta["tags"]`, `Book.meta[("a", 0)].as_string()`). Reflection reports columns with
their types, primary and foreign keys with their actions, unique and check constraints, indexes,
views, schemas and sequences. `isolation_level`, including `"AUTOCOMMIT"`, works on the engine and
per connection. A connection whose socket died is recognised as lost and replaced (use
`pool_pre_ping=True` to check before each checkout). `LIMIT`/`OFFSET` are inlined as constants, which
the server requires.

## Tests

The tests start a real `nusadb-server`. Point them at one with `NUSADB_SERVER_BIN`; without it they
look for a build beside this checkout, and skip when there is none.

```bash
NUSADB_SERVER_BIN=/path/to/nusadb-server python -m unittest discover -s tests
```

## License

Apache-2.0.
