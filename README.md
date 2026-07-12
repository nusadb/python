# nusadb — Python driver for NusaDB

A pure-Python, dependency-free [PEP-249 / DB-API 2.0](https://peps.python.org/pep-0249/)
driver that speaks the [Nusa Wire Protocol](../../docs/wire-protocol.md)
(`PROTOCOL_VERSION 1.1`) directly over a socket. No C extension; standard library only.
`cursor.description[i][1]` (`type_code`) carries each column's NusaDB type name (protocol 1.1).

## Install

```bash
pip install ./drivers/python      # from the repo, or
pip install nusadb                 # once published
```

## Usage

```python
import nusadb

conn = nusadb.connect(host="127.0.0.1", port=5678, user="nusa-root", database="nusadb")
cur = conn.cursor()

cur.execute("CREATE TABLE t (id INT NOT NULL, name TEXT)")
cur.execute("INSERT INTO t VALUES ($1, $2)", [1, "alice"])

cur.execute("SELECT id, name FROM t WHERE id = $1", [1])
for row in cur:
    print(row)            # (1, 'alice')   — INT decodes to int, TEXT to str

conn.close()
```

### Value types

Each column's wire type tag (protocol 1.1) drives decoding, so values come back as the natural
Python type — full typed DB-API fidelity rather than everything-as-string:

| NusaDB type | Python type |
| --- | --- |
| `BOOL` | `bool` |
| `INT` | `int` |
| `FLOAT` | `float` |
| `NUMERIC` | `decimal.Decimal` |
| `DATE` / `TIME` / `TIMESTAMP` (and `…TZ`) | `datetime.date` / `time` / `datetime` |
| `UUID` | `uuid.UUID` |
| `JSON` | parsed `dict` / `list` / scalar |
| `ARRAY` | `list` (elements stay `str` — the wire array tag carries no element type) |
| `BYTEA` | `bytes` |
| `TEXT` / `VARCHAR` / everything else | `str` |

A value that does not parse as its declared tag falls back to the raw text, so an unexpected wire
form never raises mid-fetch. `cursor.description[i][1]` (`type_code`) still carries the type name.

### Bulk insert (`executemany`)

`cursor.executemany(sql, seq_of_parameters)` runs one statement once per parameter set (DB-API 2.0);
`cursor.rowcount` is the total affected. The wire protocol has no batch pipeline, so this is one
round-trip per set.

```python
cur.executemany("INSERT INTO t VALUES ($1, $2)", [[1, "a"], [2, "b"], [3, "c"]])
```

### Bulk load / export (`COPY`)

For high-throughput load/export, `conn.copy_in` / `conn.copy_out` drive the `COPY` sub-protocol —
one round-trip for the whole dataset. Move bytes in the server's text format (tab-delimited fields,
`\N` for SQL `NULL`, one row per line); you write the `COPY` statement with any `WITH (...)` options.

```python
import io

# Bulk load from any binary file-like (read(size) -> bytes).
loaded = conn.copy_in("COPY t (id, name) FROM STDIN", io.BytesIO(b"1\talice\n2\t\\N\n"))

# Bulk export into any binary file-like (write(bytes)).
sink = io.BytesIO()
exported = conn.copy_out("COPY t TO STDOUT", sink)
```

A `COPY` the server refuses (bad SQL, an RLS-protected table) raises; the connection stays usable.

### Parameters

Placeholders are positional **`$1`, `$2`, …** (the server's native
marker). `paramstyle` is reported as `"numeric"`. Pass the bound values as a
sequence to `execute` / `executemany`; `None` is SQL `NULL`. Values are sent in the
wire text format — bind with an explicit `CAST(... AS type)` when a numeric-looking
string must land in a non-numeric column.

### TLS

Pass an `ssl.SSLContext` to `connect(ssl=...)`. The server uses implicit TLS 1.3, so
the TLS session is established before any protocol frame.

```python
import ssl, nusadb
ctx = ssl.create_default_context(cafile="ca.pem")
conn = nusadb.connect(host="db.example", port=5678, user="u", database="nusadb",
                      password="…", ssl=ctx)
```

### Authentication

If the server runs with `--auth-user USER:PASSWORD`, pass `password=`; the driver
performs the SCRAM-SHA-256 handshake and verifies the server's signature (mutual auth).

### Connection pool

```python
from nusadb import Pool

pool = Pool(max_size=10, host="127.0.0.1", port=5678, user="nusa-root", database="nusadb")
with pool.connection() as conn:
    conn.cursor().execute("SELECT 1")
```

## Transactions

By default the connection is in **autocommit** mode (each statement is its own
transaction). Pass `autocommit=False` for the standard transactional model: the
connection opens a transaction lazily before the first statement, and
`commit()`/`rollback()` send `COMMIT`/`ROLLBACK`.

Inside a transaction, `savepoint(name)` marks a point you can later undo to with
`rollback_to(name)` (the transaction stays open) or forget with `release_savepoint(name)`:

```python
cur.execute("INSERT INTO t VALUES (1)")
conn.savepoint("sp1")
cur.execute("INSERT INTO t VALUES (2)")
conn.rollback_to("sp1")   # undoes (2), keeps (1); the transaction continues
conn.commit()
```

```python
conn = nusadb.connect(port=5678, user="nusa-root", database="nusadb", autocommit=False)
cur = conn.cursor()
cur.execute("INSERT INTO t VALUES (1)")
conn.rollback()   # discards the insert
cur.execute("INSERT INTO t VALUES (2)")
conn.commit()     # persists it
```

## Notifications (LISTEN/NOTIFY)

`listen(channel)` subscribes the connection; a `notify(channel, payload)` from any connection on the
same database is then delivered asynchronously. Collect delivered notifications with `poll(timeout)`
(seconds; `0` polls without blocking, `None` blocks) or drain the ones buffered during other queries
with `notifications()`:

```python
conn.listen("orders")
# ... elsewhere: other.notify("orders", "42")
note = conn.poll(5.0)          # -> Notification(pid, channel, payload), or None on timeout
print(note.channel, note.payload)
conn.unlisten("orders")
```

## SQLAlchemy

A SQLAlchemy dialect ships in `nusadb.sqlalchemy`. Once this package is installed its
`sqlalchemy.dialects` entry point registers it, so `create_engine("nusadb://…")` works
directly:

```python
from sqlalchemy import create_engine
engine = create_engine("nusadb://nusa-root@127.0.0.1:5678/nusadb")
```

Without installing, register it explicitly:

```python
from sqlalchemy.dialects import registry
registry.register("nusadb", "nusadb.sqlalchemy", "NusaDialect")
```

It supports full ORM use — declarative models, `metadata.create_all`,
insert/query/update/delete, pagination (`.limit()`/`.offset()`), joins, aggregates, and real
transactions — over the transactional connection, rewriting SQLAlchemy's `:1` markers to the
server's `$1`. `LIMIT`/`OFFSET` are inlined as constants (the server rejects a bound row count).
Reflection (`inspect(engine).get_columns(...)`) reports each column's server-side `default` and
infers `autoincrement` from a `nextval(...)` default, so Alembic autogenerate sees them.
Install the extra with `pip install nusadb[sqlalchemy]`.

## Tests

```bash
cargo build -p nusadb-server          # the tests boot this binary
python -m unittest discover -s drivers/python/tests
```

The tests start a real `nusadb-server` on an ephemeral port and exercise simple and
parameterised queries, `executemany`, errors, the pool, cancellation, and SCRAM auth.

## License

Apache-2.0.
