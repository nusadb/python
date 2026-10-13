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
pip install "nusadb[django]"       # with Django
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
| `binary` | `true` to ask for results in the binary format (see [Binary results](#binary-results)) |
| `prepare_threshold` | runs before a statement is prepared on the server, or `none` (see [Prepared statements](#prepared-statements)) |

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

## SQL composition

A parameter stands for a value, never for a table or column name. To build SQL with names (or
values) that come from elsewhere, compose it with `nusadb.sql` instead of pasting strings:

```python
from nusadb import sql

query = sql.SQL("SELECT {fields} FROM {table} WHERE {key} = {value}").format(
    fields=sql.SQL(", ").join(sql.Identifier(name) for name in ["id", "name"]),
    table=sql.Identifier("app", "users"),
    key=sql.Identifier("id"),
    value=sql.Placeholder(),
)
cur.execute(query, [42])
print(query.as_string(conn))  # SELECT "id", "name" FROM "app"."users" WHERE "id" = %s
```

- `sql.SQL(text)` is SQL written by the program and is used as it is. `format()` fills `{}`,
  `{0}` and `{name}` fields with other pieces (`{{` and `}}` are literal braces), `join(pieces)`
  puts the text between pieces, and `+` joins two pieces.
- `sql.Identifier("schema", "table")` quotes each part with double quotes, doubling a quote inside
  it: `"schema"."table"`. An empty name or a NUL character is refused.
- `sql.Literal(value)` writes a value as a literal, the way it would bind as a parameter: `NULL`,
  `true`, `42`, `'it''s'`, `'2024-01-01'::DATE`, `'1.5'::NUMERIC`. Text with a backslash is
  written as `E'...'` with the backslash doubled, so it reads the same whatever the server's string
  settings. A NUL character is refused.
- `sql.Placeholder()` is a `%s` marker and `sql.Placeholder("name")` a `%(name)s` one, for values
  passed with the statement.

Whatever an `Identifier` or a `Literal` holds, it cannot end its own quoting, so it cannot change
the statement around it. Every method that takes SQL text also takes one of these objects:
`execute`, `executemany`, `prepare`, `prepare_cached`, `stream`, `pipeline().execute`, the `copy_*`
methods and named cursors, blocking and asyncio alike.

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

## Binary results

Results come back as text by default. Ask for the binary format of the protocol instead and
integers, floats, dates, times, timestamps, intervals, UUIDs, network addresses, bit strings and
ranges travel in fixed binary layouts, which are smaller and need no parsing:

```python
conn = nusadb.connect(binary=True)            # every cursor of this connection
cur = conn.cursor(binary=True)                # one cursor
cur.execute("SELECT * FROM t", binary=True)   # one statement
with conn.stream("SELECT * FROM events", binary=True) as rows:
    ...
```

The URL takes `?binary=true`, and `nusadb.aio` has the same options. The values are the same Python
objects either way: a test runs every column type both ways and compares them. Two things to know:

- A `TIMESTAMPTZ` comes back in UTC. In text it carries the session's time zone offset, so with a
  zone other than UTC the two are the same instant with a different `tzinfo`.
- A decoder from `register_decoder` receives text, so in a binary result it applies only to the
  types whose binary form is their text (`NUMERIC`, `TEXT`, `JSON`, arrays, `VECTOR`, the geometric
  and full-text types, `XML`).

A binary result needs the statement to run through the extended protocol, so a binary statement
without parameters goes that way too. The statements the server runs only as a plain query
(`DECLARE`, `FETCH`, `MOVE`, `CLOSE`, `COPY`, or several statements in one string) are sent as
before and come back in text. A named cursor fetches in text for the same reason, and refuses
`binary=True`. Array columns keep their element type (`ARRAY:INT` in `description`) in both
formats: an array travels as its text even in binary.

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

### Binary COPY

With `binary=True`, `copy_in_rows` writes the binary `COPY` stream (a signature and header, a row
of length-prefixed fields per row, a trailer) and `copy_out_rows` reads one. The statement must say
`WITH (FORMAT binary)`:

```python
types = ["INT", "TEXT", "DATE", "INT4RANGE"]
conn.copy_in_rows(
    "COPY t (id, name, born, ages) FROM STDIN (FORMAT binary)",
    [(1, "alice", datetime.date(1990, 5, 1), nusadb.Range(18, 65))],
    binary=True,
    types=types,
)
rows = conn.copy_out_rows("COPY t TO STDOUT (FORMAT binary)", binary=True, types=types)
```

Each value is written in the binary layout of its column's type. `types` names the column types in
order, with any spelling the server takes (`bigint`, `varchar(20)`, `numeric(10,2)`, `text[]`,
`tstzrange`, ...). Without `types` each column's type follows from its Python values (`int` is
`INT`, `str` is `TEXT`, a `date` is `DATE`, an aware `datetime` is `TIMESTAMPTZ`, a `Range` its
range type), and a column whose values map to different types is refused. A binary field carries
no type of its own, so the server reads it at its column's type: name the types whenever a column's
type is not the one its Python values map to (an `int` for a `FLOAT` column, say). A value that does
not fit its type (a `str` for an `INT`, a naive `datetime` for a `TIMESTAMPTZ`) is refused before it
is sent, and nothing is loaded.

`copy_out_rows` decodes each field at its type in `types` into the value a query returns for it;
without `types` each field stays its raw `bytes`. The stream is checked strictly: the signature, no
flag set, rows of one width (`len(types)`), lengths, the trailer and nothing after it. A fault
raises `InterfaceError` once the `COPY` has ended. A statement whose format does not match the call
(a text `COPY` with `binary=True`, or the other way round) raises `ProgrammingError`, and the
connection stays usable either way.

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

The driver also prepares statements on its own. Once the same SQL has run with parameters
`prepare_threshold` times on a connection (5 by default), the next run prepares it on the server and
later runs only bind new values, so the statement text is no longer sent each time. The statements
share the 256-entry cache of `prepare_cached`, the least recently used closed first, and each set of
parameter types gets its own statement. Set `prepare_threshold` with `connect()`, the URL, the
`prepare_threshold` property, or `Pool` and `AsyncPool` options; `None` turns it off and `0`
prepares from the first run. Statements without parameters, `executemany`, pipelines and streams are
not prepared this way.

If the server no longer knows a prepared statement, the driver drops it and runs the statement
unprepared once, unless the error left the transaction failed, where nothing more may run. The
server plans a prepared statement each time it runs it, so a table changed by DDL needs nothing more.

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

## Composite types

The server reports a composite column (`CREATE TYPE address AS (...)`) as `TEXT`, so its values
arrive as record text such as `(Main,5)`. `nusadb.CompositeInfo` knows a type's fields and turns that
text into a Python object with each field at its type:

```python
from nusadb import CompositeInfo, register_composite
from nusadb.rows import composite_row

info = CompositeInfo.fetch(conn, "address")    # reads the fields from the server
home = info.load("(Main,5)")                   # address(street='Main', number=5)

register_composite(info)                       # address values now bind as records
cur.execute("INSERT INTO people (id, home) VALUES ($1, $2)", [1, home])

cur = conn.cursor(row_factory=composite_row({"home": info}))
cur.execute("SELECT id, home FROM people")
cur.fetchone()                                 # (1, address(street='Main', number=5))
```

`fetch` reads the server's catalog of composite types, which only a superuser may read, and returns
`None` for a type that does not exist. Any other role builds the info from the fields:
`CompositeInfo("address", [("street", "TEXT"), ("number", "INT")])`. `CompositeInfo.fetch_async`
does the same on an asyncio connection.

`load` builds a named tuple by default. `register_composite(info, conn, factory=Address)` uses your
own class instead (its constructor takes the fields in order, as a dataclass does), both for
`load` and for binding; with a connection the binding applies to that connection only, with `None`
to all of them. A bound value is sent as record text, which a composite column takes as it is; to
compare with one, cast the parameter: `WHERE home = $1::address`. `composite_row` takes a mapping of
column names to infos and, optionally, the row factory to build the rows with (`dict_row`, say).

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

## Notices

Besides errors, the server can send notices: warnings and notes about what a statement did
(protocol 1.3). Each one is kept in `conn.notices` as a `nusadb.Notice` with `severity`
(`"WARNING"`, `"NOTICE"`, ...), `sqlstate` and `message`, the most recent 100 at most, and is passed
to the handlers added with `conn.add_notice_handler(fn)` (`remove_notice_handler` takes one away):

```python
conn.add_notice_handler(lambda n: print(n.severity, n.message))
cur.execute("DROP TABLE IF EXISTS old_t")
print(conn.notices[-1])
```

A notice can come in any reply, during a `COPY`, or on an idle connection while `poll` waits. A
handler that raises is reported as a warning and does not stop the reply being read. A server before
protocol 1.3 sends none, and the list stays empty.

## Two-phase commit

The DB-API two-phase commit methods prepare a transaction on the server so that its work survives
until it is committed or rolled back by its id, from any connection:

```python
xid = conn.xid(1, "order-42", "billing")
conn.tpc_begin(xid)
cur.execute("UPDATE accounts SET balance = balance - $1 WHERE id = $2", [10, 1])
conn.tpc_prepare()        # PREPARE TRANSACTION
conn.tpc_commit()         # COMMIT PREPARED; tpc_rollback() for ROLLBACK PREPARED
```

`tpc_begin` opens a transaction at once, in autocommit mode too; until the transaction ends,
`commit()` and `rollback()` raise `ProgrammingError`, and after `tpc_prepare` nothing else runs on
the connection. `tpc_commit()` and `tpc_rollback()` without an xid end the transaction begun on the
connection, as a one-phase commit or rollback when it was not prepared. `tpc_recover()` lists the
prepared transactions the server holds, and `tpc_commit(xid)` or `tpc_rollback(xid)` ends one of
them from outside a transaction:

```python
for xid in conn.tpc_recover():
    conn.tpc_rollback(xid)
```

On the server the id is a gid string: the format id, then the global transaction id and the branch
qualifier base64-encoded, joined by `_` (`1_b3JkZXItNDI=_YmlsbGluZw==`). `nusadb.Xid.from_string`
reads it back; any other gid, such as one made by hand, is accepted too, as a plain string or as the
`gtrid` of an xid whose other parts are `None`. A recovered xid also has `prepared`, `owner` and
`database`. `nusadb.aio` has the same methods, awaited (`xid` itself is not).

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

Prepared statements, pipelined `executemany`, `COPY` (binary included), notifications, notices,
two-phase commit, SQL composition, binary results, cancellation, row factories and
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

## Django

`nusadb.django` is a database backend for Django 4.2 (`pip install "nusadb[django]"`):

```python
DATABASES = {
    "default": {
        "ENGINE": "nusadb.django",
        "NAME": "app",
        "USER": "app",
        "PASSWORD": os.environ["DB_PASSWORD"],
        "HOST": "db.example",
        "PORT": "5678",
        "OPTIONS": {"isolation_level": "serializable", "statement_timeout": 30},
    }
}
```

`OPTIONS` takes `isolation_level` (`"read committed"`, `"repeatable read"`, `"serializable"`, ...)
and the driver's `ssl`, `timeout`, `connect_timeout`, `statement_timeout`, `keepalive` and
`prepare_threshold`. An empty
`NAME` opens the default database `nusadb`. Migrations run in a transaction and roll back as a
whole. Tests get their own database through `CREATE DATABASE`, so `USER` must be allowed to create
databases (the superuser `nusadb-root`, or a role with `CREATEDB`). `manage.py dbshell` runs
`nusadb-cli`, with the password passed in `NUSADB_PASSWORD`.

Known limits:

- Constraints are checked at each statement; there are no deferrable constraints, so fixtures must
  list rows before the rows that refer to them.
- `select_for_update()` works on one table: not with `select_related()`, `of=` or `no_key=True`.
  `nowait` and `skip_locked` work.
- Regular expressions have no backreferences.
- The server knows UTC and fixed offsets only. For a named `TIME_ZONE` the backend computes the
  offsets itself from Python's zone database, for 1970 to 2050.
- With `USE_TZ = False`, `DateTimeField` is a `timestamp` column and aware datetimes are refused.
- An existing column cannot become an `AutoField`, and `SHA1()` is not available.
- Renaming a column drops and declares again the indexes and constraints that name it, since the
  server refuses the rename otherwise. An index made outside Django with an expression or a
  condition on that column has to be dropped by hand first.
- Expression indexes, index column order, column collations and comments are not read back by
  introspection.
- Tables whose names start with `nusadb_` do not appear in `information_schema`, so do not give a
  Django app a label starting with `nusadb_`.
- Not supported by the server yet: a subquery in `GROUP BY` (grouping by a `Subquery` or `Exists`
  annotation), bitwise operators on `bigint`, dividing an interval, set operations inside a
  subquery, and `OFFSET` on a combined query.

## Tests

The tests start a real `nusadb-server`. Point them at one with `NUSADB_SERVER_BIN`; without it they
look for a build beside this checkout, and skip when there is none.

```bash
NUSADB_SERVER_BIN=/path/to/nusadb-server python -m unittest discover -s tests
```

The Django backend's tests in `tests/django` run as part of that when Django is installed. They
start their own server on a free port between 5700 and 5799. The notice and two-phase commit tests
also run against a scripted server in `tests/fake_server.py`, which sends a notice anywhere in a
reply; their live counterparts skip on a server without those features.

## License

Apache-2.0.
