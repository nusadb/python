"""SQLAlchemy dialects for NusaDB: ``nusadb://`` on the blocking driver and ``nusadb+asyncio://``
on :mod:`nusadb.aio` for :mod:`sqlalchemy.ext.asyncio`.

Installing the package registers both through the ``sqlalchemy.dialects`` entry points::

    from sqlalchemy import create_engine
    engine = create_engine("nusadb://app:secret@127.0.0.1:5678/sales")

    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine("nusadb+asyncio://app:secret@127.0.0.1:5678/sales")

URL query keys (``statement_timeout``, ``connect_timeout``, ``keepalive``, ``tls``, ``tls_ca``,
``tls_cert``, ``tls_key``) are those of :mod:`nusadb.dsn`. ``isolation_level`` (including
``"AUTOCOMMIT"``) works on the engine and per connection through ``execution_options``.

Requires SQLAlchemy 2.0; the asyncio dialect also needs ``greenlet``.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy import pool as sa_pool
from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default, reflection
from sqlalchemy import sql
from sqlalchemy.sql import compiler

import nusadb
from nusadb.exceptions import InterfaceError, OperationalError, TransactionRolledBack

_ISOLATION_LEVELS = ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]


class _NusaTypeCompiler(compiler.GenericTypeCompiler):
    """DDL type names the server reads."""

    def visit_string(self, type_: Any, **kw: Any) -> str:
        return f"VARCHAR({type_.length})" if type_.length else "TEXT"

    def visit_unicode(self, type_: Any, **kw: Any) -> str:
        return self.visit_string(type_, **kw)

    def visit_text(self, type_: Any, **kw: Any) -> str:
        return "TEXT"

    visit_unicode_text = visit_text

    def visit_boolean(self, type_: Any, **kw: Any) -> str:
        return "BOOLEAN"

    def visit_float(self, type_: Any, **kw: Any) -> str:
        return "DOUBLE PRECISION"

    def visit_double(self, type_: Any, **kw: Any) -> str:
        return "DOUBLE PRECISION"

    def visit_DOUBLE_PRECISION(self, type_: Any, **kw: Any) -> str:  # noqa: N802
        return "DOUBLE PRECISION"

    def visit_datetime(self, type_: Any, **kw: Any) -> str:
        return "TIMESTAMP WITH TIME ZONE" if type_.timezone else "TIMESTAMP"

    def visit_TIMESTAMP(self, type_: Any, **kw: Any) -> str:  # noqa: N802
        return self.visit_datetime(type_, **kw)

    def visit_time(self, type_: Any, **kw: Any) -> str:
        return "TIME WITH TIME ZONE" if type_.timezone else "TIME"

    def visit_large_binary(self, type_: Any, **kw: Any) -> str:
        return "BYTEA"

    def visit_uuid(self, type_: Any, **kw: Any) -> str:
        return "UUID"

    def visit_JSON(self, type_: Any, **kw: Any) -> str:  # noqa: N802
        return "JSONB"

    def visit_interval(self, type_: Any, **kw: Any) -> str:
        return "INTERVAL"

    def visit_enum(self, type_: Any, **kw: Any) -> str:
        # Stored as text; SQLAlchemy checks the values on the way in and out.
        length = max((len(v) for v in type_.enums), default=1)
        return f"VARCHAR({length})"

    def visit_ARRAY(self, type_: Any, **kw: Any) -> str:  # noqa: N802
        return self.process(type_.item_type, **kw) + "[]" * (type_.dimensions or 1)


class _NusaJSON(sqltypes.JSON):
    """JSON bound with the driver's JSON type tag; a value the driver already decoded is kept."""

    def bind_processor(self, dialect: Any) -> Any:
        serialize = super().bind_processor(dialect)

        def process(value: Any) -> Any:
            text = serialize(value) if serialize is not None else value
            return None if text is None else nusadb.Json(text, dumps=lambda already: already)

        return process

    def result_processor(self, dialect: Any, coltype: Any) -> Any:
        if coltype == "JSON":
            # The driver decoded it already; a JSON string arrives as a Python `str` and must not be
            # parsed a second time.
            return None
        deserialize = super().result_processor(dialect, coltype)
        if deserialize is None:
            return None

        def process(value: Any) -> Any:
            # A column the server holds as text but the model calls JSON.
            return deserialize(value) if isinstance(value, (str, bytes)) else value

        return process


class _NusaJSONPathType(sqltypes.JSON.JSONPathType):
    """A JSON path (``doc[("a", 1)]``) bound as the text array the ``#>`` operator reads."""

    def bind_processor(self, dialect: Any) -> Any:
        def process(value: Any) -> Any:
            if value is None:
                return None
            parts = ['"%s"' % str(p).replace("\\", "\\\\").replace('"', '\\"') for p in value]
            return "{" + ",".join(parts) + "}"

        return process


class _NusaCompiler(compiler.SQLCompiler):
    """Render ``LIMIT``/``OFFSET`` as inline integer literals: the server takes only a constant
    row count there. JSON element access compiles to the ``->``/``->>`` and ``#>``/``#>>``
    operators."""

    def _json_access(
        self, binary: Any, operator: Any, as_json: str, as_text: str, _cast_applied: bool, **kw: Any
    ) -> str:
        if not _cast_applied and binary.type._type_affinity is not sqltypes.JSON:
            # `.as_string()`, `.as_integer()`, …: read the element as text, then cast it.
            kw["_cast_applied"] = True
            return self.process(sql.cast(binary, binary.type), **kw)
        kw["eager_grouping"] = True
        return self._generate_generic_binary(binary, as_text if _cast_applied else as_json, **kw)

    def visit_json_getitem_op_binary(
        self, binary: Any, operator: Any, _cast_applied: bool = False, **kw: Any
    ) -> str:
        return self._json_access(binary, operator, " -> ", " ->> ", _cast_applied, **kw)

    def visit_json_path_getitem_op_binary(
        self, binary: Any, operator: Any, _cast_applied: bool = False, **kw: Any
    ) -> str:
        return self._json_access(binary, operator, " #> ", " #>> ", _cast_applied, **kw)

    def limit_clause(self, select: Any, **kw: Any) -> str:
        kw["literal_binds"] = True
        text = ""
        if select._limit_clause is not None:
            text += " LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text

    def visit_sequence(self, seq: Any, **kw: Any) -> str:
        return f"nextval('{self.preparer.format_sequence(seq)}')"

    def default_from(self) -> str:
        return ""


class _NusaDDLCompiler(compiler.DDLCompiler):
    """An integer primary key that autoincrements becomes an identity column."""

    def get_column_specification(self, column: Any, **kw: Any) -> str:
        spec = self.preparer.format_column(column)
        impl = column.type.dialect_impl(self.dialect)
        autoincrement = (
            column is column.table._autoincrement_column
            and column.identity is None
            and isinstance(impl, sqltypes.Integer)
            and (column.default is None or getattr(column.default, "is_sequence", False) is False)
            and column.server_default is None
        )
        spec += " " + self.dialect.type_compiler_instance.process(
            column.type, type_expression=column
        )
        if autoincrement:
            spec += " GENERATED BY DEFAULT AS IDENTITY"
        elif column.identity is not None:
            spec += " " + self.process(column.identity)
        default_sql = self.get_column_default_string(column)
        if default_sql is not None and column.identity is None:
            spec += " DEFAULT " + default_sql
        if column.computed is not None:
            spec += " " + self.process(column.computed)
        if not column.nullable and column.identity is None and not autoincrement:
            spec += " NOT NULL"
        return spec

    def visit_identity_column(self, identity: Any, **kw: Any) -> str:
        return "GENERATED %s AS IDENTITY" % ("ALWAYS" if identity.always else "BY DEFAULT")


class NusaDialect(default.DefaultDialect):
    """The ``nusadb://`` SQLAlchemy dialect, on the blocking :mod:`nusadb` driver."""

    name = "nusadb"
    driver = "nusadb"
    default_paramstyle = "numeric"
    statement_compiler = _NusaCompiler
    ddl_compiler = _NusaDDLCompiler
    type_compiler_cls = _NusaTypeCompiler
    type_compiler = _NusaTypeCompiler
    supports_statement_cache = True
    supports_native_boolean = True
    supports_native_decimal = True
    supports_native_uuid = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_default_values = True
    supports_default_metavalue = True
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_identity_columns = True
    supports_sequences = True
    sequences_optional = True
    postfetch_lastrowid = False
    insert_returning = True
    update_returning = True
    delete_returning = True
    use_insertmanyvalues = True
    insert_executemany_returning = True
    insert_executemany_returning_sort_by_parameter_order = True
    supports_is_distinct_from = True
    supports_server_side_cursors = False
    default_schema_name = "public"
    colspecs = {sqltypes.JSON: _NusaJSON, sqltypes.JSON.JSONPathType: _NusaJSONPathType}

    def __init__(
        self,
        json_serializer: Any = None,
        json_deserializer: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._json_serializer = json_serializer
        self._json_deserializer = json_deserializer

    @classmethod
    def import_dbapi(cls) -> Any:
        return nusadb

    @classmethod
    def dbapi(cls) -> Any:  # noqa: D401 - the hook name before SQLAlchemy 2.0
        return nusadb

    def create_connect_args(self, url: Any) -> Tuple[List[Any], Dict[str, Any]]:
        opts: Dict[str, Any] = url.translate_connect_args(username="user", database="database")
        if "port" in opts:
            opts["port"] = int(opts["port"])
        query = dict(url.query)
        if query:
            # The URL's own keys (timeouts, TLS files) are read the way the driver reads them.
            from urllib.parse import urlencode

            from nusadb.dsn import parse_dsn

            parsed = parse_dsn("nusadb://h?" + urlencode(query))
            parsed.pop("host")
            opts.update(parsed)
        # SQLAlchemy manages transactions; AUTOCOMMIT comes through the isolation level.
        opts.setdefault("autocommit", False)
        return ([], opts)

    # SQLAlchemy compiles positional `:1`, `:2`, … (the `numeric` paramstyle); the driver rewrites
    # them to the server's `$1` where they are placeholders, never inside a string or comment.

    def do_begin(self, dbapi_connection: Any) -> None:
        # The driver opens the transaction before the first statement.
        pass

    def do_rollback(self, dbapi_connection: Any) -> None:
        dbapi_connection.rollback()

    def do_commit(self, dbapi_connection: Any) -> None:
        dbapi_connection.commit()

    def is_disconnect(self, e: Any, connection: Any, cursor: Any) -> bool:
        if isinstance(e, InterfaceError) and "closed" in str(e):
            return True
        if isinstance(e, OperationalError) and not isinstance(e, TransactionRolledBack):
            if getattr(e, "sqlstate", None) is None:
                # Raised by the driver itself. A lost connection is marked broken or closed; other
                # driver errors (a COPY source failing, say) leave it usable.
                broken = getattr(connection, "broken", None)
                if broken is None:
                    return True
                return bool(broken) or bool(getattr(connection, "closed", False))
            return e.sqlstate.startswith("08") or e.sqlstate in ("57P01", "57P02", "57P03")
        broken = getattr(connection, "broken", False)
        return bool(broken)

    # --- session state ---

    def _run_outside_transaction(self, dbapi_connection: Any, sql: str) -> List[Tuple[Any, ...]]:
        """Run ``sql`` without opening a transaction for it."""
        previous = dbapi_connection.autocommit
        if dbapi_connection.transaction_status == "idle" and not previous:
            dbapi_connection.autocommit = True
        try:
            cur = dbapi_connection.cursor()
            cur.execute(sql)
            return cur.fetchall() if cur.description else []
        finally:
            if dbapi_connection.transaction_status == "idle":
                dbapi_connection.autocommit = previous

    def get_isolation_level_values(self, dbapi_connection: Any) -> List[str]:
        return ["AUTOCOMMIT", *_ISOLATION_LEVELS]

    def get_isolation_level(self, dbapi_connection: Any) -> str:
        if dbapi_connection.autocommit:
            return "AUTOCOMMIT"
        rows = self._run_outside_transaction(dbapi_connection, "SHOW transaction_isolation")
        return str(rows[0][0]).upper()

    def get_default_isolation_level(self, dbapi_conn: Any) -> str:
        rows = self._run_outside_transaction(dbapi_conn, "SHOW default_transaction_isolation")
        return str(rows[0][0]).upper()

    def set_isolation_level(self, dbapi_connection: Any, level: str) -> None:
        if level == "AUTOCOMMIT":
            dbapi_connection.autocommit = True
            return
        dbapi_connection.autocommit = False
        self._run_outside_transaction(
            dbapi_connection,
            f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}",
        )

    def do_ping(self, dbapi_connection: Any) -> bool:
        # Outside a transaction: a ping must not leave one open on a connection being checked out.
        self._run_outside_transaction(dbapi_connection, "SELECT 1")
        return True

    def detect_autocommit_setting(self, dbapi_conn: Any) -> bool:
        return bool(dbapi_conn.autocommit)

    def _get_server_version_info(self, connection: Any) -> Tuple[int, ...]:
        text = connection.exec_driver_sql("SELECT version()").scalar() or ""
        match = re.search(r"(\d+)\.(\d+)(?:\.(\d+))?", str(text))
        if match is None:
            return (0, 0)
        return tuple(int(part) for part in match.groups() if part is not None)

    def _get_default_schema_name(self, connection: Any) -> str:
        return str(connection.exec_driver_sql("SELECT current_schema()").scalar())

    # --- reflection ---

    def _rows(self, connection: Any, sql: str, params: Sequence[Any] = ()) -> List[Any]:
        return list(connection.exec_driver_sql(sql, tuple(params)).fetchall())

    def _schema(self, connection: Any, schema: Optional[str]) -> str:
        return schema or self.default_schema_name

    @reflection.cache
    def get_schema_names(self, connection: Any, **kw: Any) -> List[str]:
        rows = self._rows(
            connection,
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name <> 'information_schema' ORDER BY schema_name",
        )
        return [r[0] for r in rows]

    def has_schema(self, connection: Any, schema_name: str, **kw: Any) -> bool:
        return schema_name in self.get_schema_names(connection, **kw)

    @reflection.cache
    def get_table_names(self, connection: Any, schema: Optional[str] = None, **kw: Any) -> List[str]:
        rows = self._rows(
            connection,
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY table_name",
            [self._schema(connection, schema)],
        )
        return [r[0] for r in rows]

    @reflection.cache
    def get_view_names(self, connection: Any, schema: Optional[str] = None, **kw: Any) -> List[str]:
        rows = self._rows(
            connection,
            "SELECT table_name FROM information_schema.views WHERE table_schema = $1 "
            "ORDER BY table_name",
            [self._schema(connection, schema)],
        )
        return [r[0] for r in rows]

    def get_view_definition(
        self, connection: Any, view_name: str, schema: Optional[str] = None, **kw: Any
    ) -> str:
        rows = self._rows(
            connection,
            "SELECT view_definition FROM information_schema.views "
            "WHERE table_schema = $1 AND table_name = $2",
            [self._schema(connection, schema), view_name],
        )
        if not rows:
            from sqlalchemy.exc import NoSuchTableError

            raise NoSuchTableError(view_name)
        return str(rows[0][0])

    def has_table(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> bool:
        self._ensure_has_table_connection(connection)
        rows = self._rows(
            connection,
            "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 "
            "UNION ALL SELECT 1 FROM information_schema.views "
            "WHERE table_schema = $1 AND table_name = $2",
            [self._schema(connection, schema), table_name],
        )
        return bool(rows)

    @reflection.cache
    def get_sequence_names(
        self, connection: Any, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        rows = self._rows(
            connection,
            "SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = $1 "
            "ORDER BY sequence_name",
            [self._schema(connection, schema)],
        )
        return [r[0] for r in rows]

    def has_sequence(
        self, connection: Any, sequence_name: str, schema: Optional[str] = None, **kw: Any
    ) -> bool:
        return sequence_name in self.get_sequence_names(connection, schema, **kw)

    @reflection.cache
    def get_columns(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> List[Dict[str, Any]]:
        rows = self._rows(
            connection,
            "SELECT column_name, data_type, is_nullable, column_default, "
            "character_maximum_length, numeric_precision, numeric_scale "
            "FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 "
            "ORDER BY ordinal_position",
            [self._schema(connection, schema), table_name],
        )
        if not rows:
            from sqlalchemy.exc import NoSuchTableError

            if not self.has_table(connection, table_name, schema):
                raise NoSuchTableError(table_name)
        columns = []
        for name, data_type, nullable, default_sql, length, precision, scale in rows:
            autoincrement = default_sql is not None and str(default_sql).startswith("nextval(")
            columns.append(
                {
                    "name": name,
                    "type": _reflected_type(str(data_type), length, precision, scale),
                    "nullable": str(nullable).upper() in ("YES", "TRUE", "T", "1"),
                    "default": None if autoincrement else default_sql,
                    "autoincrement": autoincrement,
                }
            )
        return columns

    def _constraints(
        self, connection: Any, table_name: str, schema: Optional[str], kind: str
    ) -> Dict[str, List[str]]:
        """``{constraint name: [column, ...]}`` for the table's constraints of ``kind``."""
        # Two queries joined here: the information_schema views are read one at a time.
        schema_name = self._schema(connection, schema)
        names = [
            r[0]
            for r in self._rows(
                connection,
                "SELECT constraint_name FROM information_schema.table_constraints "
                "WHERE table_schema = $1 AND table_name = $2 AND constraint_type = $3 "
                "ORDER BY constraint_name",
                [schema_name, table_name, kind],
            )
        ]
        out: Dict[str, List[str]] = {name: [] for name in names}
        for name, column in self._rows(
            connection,
            "SELECT constraint_name, column_name FROM information_schema.key_column_usage "
            "WHERE table_schema = $1 AND table_name = $2 ORDER BY constraint_name, ordinal_position",
            [schema_name, table_name],
        ):
            if name in out:
                out[name].append(column)
        return out

    @reflection.cache
    def get_pk_constraint(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> Dict[str, Any]:
        pks = self._constraints(connection, table_name, schema, "PRIMARY KEY")
        for name, columns in pks.items():
            return {"constrained_columns": columns, "name": name}
        return {"constrained_columns": [], "name": None}

    @reflection.cache
    def get_unique_constraints(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> List[Dict[str, Any]]:
        uniques = self._constraints(connection, table_name, schema, "UNIQUE")
        return [{"name": name, "column_names": cols} for name, cols in uniques.items()]

    @reflection.cache
    def get_foreign_keys(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> List[Dict[str, Any]]:
        schema_name = self._schema(connection, schema)
        local = self._constraints(connection, table_name, schema, "FOREIGN KEY")
        if not local:
            return []
        rules = self._rows(
            connection,
            "SELECT constraint_name, unique_constraint_schema, unique_constraint_name, "
            "update_rule, delete_rule FROM information_schema.referential_constraints "
            "WHERE constraint_schema = $1",
            [schema_name],
        )
        keys = []
        for name, ref_schema, ref_constraint, on_update, on_delete in rules:
            if name not in local:
                continue
            referred = self._rows(
                connection,
                "SELECT table_name, column_name FROM information_schema.key_column_usage "
                "WHERE constraint_schema = $1 AND constraint_name = $2 ORDER BY ordinal_position",
                [ref_schema, ref_constraint],
            )
            options = {}
            if on_update and on_update.upper() not in ("NO ACTION", "NONE"):
                options["onupdate"] = on_update.upper()
            if on_delete and on_delete.upper() not in ("NO ACTION", "NONE"):
                options["ondelete"] = on_delete.upper()
            keys.append(
                {
                    "name": name,
                    "constrained_columns": local[name],
                    "referred_schema": None if ref_schema == schema_name and schema is None else ref_schema,
                    "referred_table": referred[0][0] if referred else None,
                    "referred_columns": [r[1] for r in referred],
                    "options": options,
                }
            )
        return keys

    @reflection.cache
    def get_indexes(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> List[Dict[str, Any]]:
        schema_name = self._schema(connection, schema)
        constraint_names = {
            r[0]
            for r in self._rows(
                connection,
                "SELECT constraint_name FROM information_schema.table_constraints "
                "WHERE table_schema = $1 AND table_name = $2",
                [schema_name, table_name],
            )
        }
        rows = self._rows(
            connection,
            "SELECT index_name, non_unique, column_name FROM information_schema.statistics "
            "WHERE table_schema = $1 AND table_name = $2 ORDER BY index_name, seq_in_index",
            [schema_name, table_name],
        )
        indexes: Dict[str, Dict[str, Any]] = {}
        for name, non_unique, column in rows:
            if name in constraint_names:
                continue
            entry = indexes.setdefault(
                name, {"name": name, "column_names": [], "unique": not bool(int(non_unique))}
            )
            entry["column_names"].append(column)
        return list(indexes.values())

    @reflection.cache
    def get_check_constraints(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> List[Dict[str, Any]]:
        schema_name = self._schema(connection, schema)
        names = {
            r[0]
            for r in self._rows(
                connection,
                "SELECT constraint_name FROM information_schema.table_constraints "
                "WHERE table_schema = $1 AND table_name = $2",
                [schema_name, table_name],
            )
        }
        rows = self._rows(
            connection,
            "SELECT constraint_name, check_clause FROM information_schema.check_constraints "
            "WHERE constraint_schema = $1 ORDER BY constraint_name",
            [schema_name],
        )
        return [{"name": name, "sqltext": clause} for name, clause in rows if name in names]


def _reflected_type(data_type: str, length: Any, precision: Any, scale: Any) -> Any:
    simple = {
        "integer": sqltypes.INTEGER,
        "bigint": sqltypes.BIGINT,
        "smallint": sqltypes.SMALLINT,
        "real": sqltypes.REAL,
        "double precision": sqltypes.DOUBLE_PRECISION,
        "boolean": sqltypes.BOOLEAN,
        "date": sqltypes.DATE,
        "text": sqltypes.TEXT,
        "bytea": sqltypes.LargeBinary,
        "uuid": sqltypes.UUID,
        "json": sqltypes.JSON,
        "jsonb": sqltypes.JSON,
        "interval": sqltypes.Interval,
    }
    if data_type in simple:
        return simple[data_type]()
    if data_type == "character varying":
        return sqltypes.VARCHAR(int(length)) if length else sqltypes.VARCHAR()
    if data_type == "character":
        return sqltypes.CHAR(int(length)) if length else sqltypes.CHAR()
    if data_type == "numeric":
        if precision is None:
            return sqltypes.NUMERIC()
        return sqltypes.NUMERIC(int(precision), int(scale or 0))
    if data_type.startswith("timestamp"):
        return sqltypes.TIMESTAMP(timezone="with time zone" in data_type)
    if data_type.startswith("time"):
        return sqltypes.TIME(timezone="with time zone" in data_type)
    if data_type == "ARRAY":
        return sqltypes.ARRAY(sqltypes.NullType())
    return sqltypes.NullType()


# --- asyncio ---


class _AsyncAdaptCursor:
    """A blocking-looking cursor over :class:`nusadb.aio.AsyncCursor`, for SQLAlchemy's greenlet
    bridge."""

    def __init__(self, cursor: Any, await_: Any) -> None:
        self._cursor = cursor
        self.await_ = await_
        self.arraysize = 1
        self.lastrowid = None

    @property
    def description(self) -> Any:
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        return int(self._cursor.rowcount)

    def execute(self, operation: str, parameters: Any = None) -> None:
        self.await_(self._cursor.execute(operation, parameters))

    def executemany(self, operation: str, seq_of_parameters: Any) -> None:
        self.await_(self._cursor.executemany(operation, seq_of_parameters))

    def fetchone(self) -> Any:
        return self.await_(self._cursor.fetchone())

    def fetchmany(self, size: Optional[int] = None) -> Any:
        return self.await_(self._cursor.fetchmany(size or self.arraysize))

    def fetchall(self) -> Any:
        return self.await_(self._cursor.fetchall())

    def __iter__(self) -> Any:
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row

    def close(self) -> None:
        pass

    def setinputsizes(self, *args: Any) -> None:
        pass

    def setoutputsize(self, *args: Any) -> None:
        pass


def _adapted_connection_class() -> Any:
    from sqlalchemy.connectors.asyncio import AdaptedConnection
    from sqlalchemy.util.concurrency import await_only

    class AsyncAdaptConnection(AdaptedConnection):
        """A blocking-looking connection over :class:`nusadb.aio.AsyncConnection`."""

        __slots__ = ()
        await_ = staticmethod(await_only)

        def __init__(self, connection: Any) -> None:
            self._connection = connection

        def __getattr__(self, name: str) -> Any:
            return getattr(self._connection, name)

        def cursor(self, *args: Any, **kw: Any) -> _AsyncAdaptCursor:
            return _AsyncAdaptCursor(self._connection.cursor(), self.await_)

        def commit(self) -> None:
            self.await_(self._connection.commit())

        def rollback(self) -> None:
            self.await_(self._connection.rollback())

        def close(self) -> None:
            self.await_(self._connection.close())

        def terminate(self) -> None:
            self._connection._fail()

        @property
        def autocommit(self) -> bool:
            return bool(self._connection.autocommit)

        @autocommit.setter
        def autocommit(self, value: bool) -> None:
            self._connection.autocommit = value

    return AsyncAdaptConnection


class _AsyncAdaptModule:
    """The :mod:`nusadb` module as SQLAlchemy sees it under ``nusadb+asyncio``: everything of the
    blocking driver except ``connect``, which opens an :mod:`nusadb.aio` connection."""

    def __init__(self) -> None:
        for name, value in nusadb.__dict__.items():
            if name != "connect":
                self.__dict__[name] = value
        self._connection_class = _adapted_connection_class()

    def connect(self, *args: Any, **kwargs: Any) -> Any:
        from sqlalchemy.util.concurrency import await_only

        import nusadb.aio

        return self._connection_class(await_only(nusadb.aio.connect(*args, **kwargs)))


class NusaDialect_asyncio(NusaDialect):  # noqa: N801 - SQLAlchemy's naming for driver variants
    """The ``nusadb+asyncio://`` dialect, on :mod:`nusadb.aio`."""

    driver = "asyncio"
    is_async = True
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls) -> Any:
        return _AsyncAdaptModule()

    @classmethod
    def get_pool_class(cls, url: Any) -> Any:
        return sa_pool.AsyncAdaptedQueuePool

    def get_driver_connection(self, connection: Any) -> Any:
        return connection._connection

    def do_terminate(self, dbapi_connection: Any) -> None:
        dbapi_connection.terminate()


dialect = NusaDialect
