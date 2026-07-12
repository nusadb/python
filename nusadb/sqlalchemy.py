"""SQLAlchemy dialect for NusaDB, built on the :mod:`nusadb` DB-API driver.

Register and use it::

    from sqlalchemy.dialects import registry
    registry.register("nusadb", "nusadb.sqlalchemy", "NusaDialect")

    from sqlalchemy import create_engine
    engine = create_engine("nusadb://nusa-root@127.0.0.1:5678/nusadb")

Once the package is installed, the ``sqlalchemy.dialects`` entry point in
``pyproject.toml`` registers it automatically, so ``create_engine("nusadb://…")``
works without the explicit ``registry.register`` call.

The server's parameter marker is positional ``$1``; SQLAlchemy compiles with the
``qmark`` paramstyle (``?``) and this dialect rewrites the markers to ``$1``, ``$2``, ….
Connections run in transactional mode (``autocommit=False``) so SQLAlchemy's
``begin``/``commit``/``rollback`` map onto real server transactions.
"""

from __future__ import annotations

import re
from typing import Any, List, Sequence

from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler

import nusadb

# SQLAlchemy's `numeric` paramstyle emits positional `:1`, `:2`, …; the server uses `$1`, `$2`, ….
_PLACEHOLDER = re.compile(r":(\d+)")


class _NusaTypeCompiler(compiler.GenericTypeCompiler):
    """Emit DDL types the NusaDB parser accepts."""

    def visit_integer(self, type_, **kw: Any) -> str:
        return "INT"

    visit_INTEGER = visit_integer
    visit_BIGINT = visit_integer
    visit_SMALLINT = visit_integer
    visit_big_integer = visit_integer
    visit_small_integer = visit_integer

    def visit_string(self, type_, **kw: Any) -> str:
        return "TEXT"

    visit_VARCHAR = visit_string
    visit_NVARCHAR = visit_string
    visit_CHAR = visit_string
    visit_TEXT = visit_string
    visit_unicode = visit_string
    visit_unicode_text = visit_string
    visit_text = visit_string

    def visit_boolean(self, type_, **kw: Any) -> str:
        return "BOOL"

    visit_BOOLEAN = visit_boolean

    def visit_float(self, type_, **kw: Any) -> str:
        return "FLOAT"

    visit_FLOAT = visit_float
    visit_REAL = visit_float
    visit_numeric = compiler.GenericTypeCompiler.visit_NUMERIC

    def visit_datetime(self, type_, **kw: Any) -> str:
        return "TIMESTAMP"

    visit_DATETIME = visit_datetime
    visit_TIMESTAMP = visit_datetime


class _NusaCompiler(compiler.SQLCompiler):
    """Render ``LIMIT``/``OFFSET`` as inline integer literals.

    SQLAlchemy's default compiler emits the row count as a bound parameter (``LIMIT :param``), but
    NusaDB requires a *constant* ``LIMIT``/``OFFSET`` and rejects a bound one with
    ``non-constant LIMIT``. Forcing ``literal_binds`` on just these clauses inlines the integer
    (e.g. ``LIMIT 10 OFFSET 20``) while every other value stays safely parameterised.
    """

    def limit_clause(self, select, **kw: Any) -> str:
        kw["literal_binds"] = True
        text = ""
        if select._limit_clause is not None:
            text += " LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text


class NusaDialect(default.DefaultDialect):
    """The ``nusadb://`` SQLAlchemy dialect."""

    name = "nusadb"
    driver = "nusadb"
    # The DB-API reports paramstyle "numeric", so SQLAlchemy compiles positional ":1"; do_execute
    # rewrites those to the server's "$1", "$2", … markers. LIMIT/OFFSET are inlined as constants
    # by `_NusaCompiler` (the server rejects a bound row count).
    statement_compiler = _NusaCompiler
    type_compiler = _NusaTypeCompiler
    supports_statement_cache = True
    supports_native_boolean = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_default_values = False
    supports_empty_insert = False

    @classmethod
    def dbapi(cls):  # noqa: D401 - SQLAlchemy 1.4 hook name
        return nusadb

    # SQLAlchemy 2.0 renamed the hook; provide both.
    @classmethod
    def import_dbapi(cls):
        return nusadb

    def create_connect_args(self, url) -> tuple:
        opts = url.translate_connect_args(username="user", database="database")
        if "port" in opts:
            opts["port"] = int(opts["port"])
        # SQLAlchemy manages transactions, so run the connection transactionally.
        opts.setdefault("autocommit", False)
        return ([], opts)

    @staticmethod
    def _translate(statement: str) -> str:
        """Rewrite SQLAlchemy's positional ``:1`` markers to the server's ``$1``, ``$2``, …."""
        return _PLACEHOLDER.sub(lambda m: f"${m.group(1)}", statement)

    def do_execute(self, cursor, statement, parameters, context=None) -> None:
        cursor.execute(self._translate(statement), parameters)

    def do_executemany(self, cursor, statement, parameters, context=None) -> None:
        cursor.executemany(self._translate(statement), parameters)

    def do_begin(self, dbapi_connection) -> None:
        # The driver opens a transaction lazily before the first statement.
        pass

    # --- introspection (enough for create_all / has_table / reflection) ---

    def _get_server_version_info(self, connection):
        return (1, 0)

    def _get_default_schema_name(self, connection):
        return "public"

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def get_isolation_level(self, dbapi_connection):
        return "READ COMMITTED"

    def _table_names(self, connection) -> List[str]:
        rows = connection.exec_driver_sql("SHOW TABLES").fetchall()
        return [row[0] for row in rows]

    def has_table(self, connection, table_name, schema=None, **kw) -> bool:
        return table_name in self._table_names(connection)

    def get_table_names(self, connection, schema=None, **kw) -> List[str]:
        return self._table_names(connection)

    def get_columns(self, connection, table_name, schema=None, **kw) -> List[dict]:
        # Read from information_schema.columns so reflection sees each column's server-side DEFAULT
        # (and infers autoincrement). Filter by table in Python to avoid interpolating the name.
        rows = connection.exec_driver_sql(
            "SELECT table_name, column_name, is_nullable, column_default "
            "FROM information_schema.columns ORDER BY table_name, ordinal_position"
        ).fetchall()
        columns = []
        for row in rows:
            if row[0] != table_name:
                continue
            nullable = str(row[2]).lower() in ("true", "t", "yes", "1")
            default = row[3]  # the column's DEFAULT SQL, or None
            # A SERIAL / IDENTITY column's default is nextval(...); SQLAlchemy treats that as
            # autoincrement (matching how reflection expects to read it).
            autoincrement = default is not None and str(default).startswith("nextval(")
            columns.append(
                {
                    "name": row[1],
                    "type": sqltypes.NullType(),
                    "nullable": nullable,
                    "default": default,
                    "autoincrement": autoincrement,
                }
            )
        return columns

    def get_pk_constraint(self, connection, table_name, schema=None, **kw) -> dict:
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw) -> Sequence[dict]:
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw) -> Sequence[dict]:
        return []

    def do_rollback(self, dbapi_connection) -> None:
        dbapi_connection.rollback()

    def do_commit(self, dbapi_connection) -> None:
        dbapi_connection.commit()


dialect = NusaDialect
