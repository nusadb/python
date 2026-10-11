"""The Django ``DatabaseWrapper`` for NusaDB, on the blocking :mod:`nusadb` driver."""

from __future__ import annotations

import ipaddress
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.base import BaseDatabaseWrapper
from django.utils.asyncio import async_unsafe
from django.utils.functional import cached_property

import nusadb as Database
from nusadb.connection import Cursor as _DriverCursor

from . import lookups  # noqa: F401  (registers the NusaDB forms of JSON lookups and functions)
from .client import DatabaseClient
from .creation import DatabaseCreation
from .features import DatabaseFeatures
from .introspection import DatabaseIntrospection
from .operations import DatabaseOperations
from .schema import DatabaseSchemaEditor

# The isolation levels accepted in ``OPTIONS["isolation_level"]``.
ISOLATION_LEVELS = ("read uncommitted", "read committed", "repeatable read", "serializable")

# The connection options passed through ``OPTIONS`` to :func:`nusadb.connect`.
_CONNECT_OPTIONS = ("ssl", "timeout", "connect_timeout", "statement_timeout", "keepalive")

# A placeholder Django writes: ``%s`` by position, ``%(name)s`` by name, and ``%%`` for a ``%``.
_PYFORMAT = re.compile(r"%%|%s|%\((\w+)\)s")


def _get_varchar_column(data: Dict[str, Any]) -> str:
    if data["max_length"] is None:
        return "varchar"
    return "varchar(%(max_length)s)" % data


def _get_datetime_column(data: Dict[str, Any]) -> str:
    # Aware datetimes are instants; with USE_TZ = False Django works in naive local times.
    return "timestamp with time zone" if settings.USE_TZ else "timestamp"


class _Plan:
    """How a query's ``%s`` / ``%(name)s`` placeholders map onto the server's ``$1``, ``$2``."""

    __slots__ = ("sql", "names", "count")

    def __init__(self, sql: str, names: Optional[List[str]], count: int) -> None:
        self.sql = sql
        self.names = names
        self.count = count

    def arrange(self, params: Any) -> List[Any]:
        """The values of one execution in ``$1``, ``$2``, ... order."""
        if self.names is not None:
            if not isinstance(params, Mapping):
                raise Database.ProgrammingError("%(name)s placeholders take a mapping of parameters")
            return [params[name] for name in self.names]
        if isinstance(params, Mapping):
            if params:
                raise Database.ProgrammingError("a mapping of parameters needs %(name)s placeholders")
            return []
        values = list(params)
        if self.count and len(values) != self.count:
            raise Database.ProgrammingError(
                f"the query has {self.count} placeholder(s) but {len(values)} parameter(s) were given"
            )
        return values


def plan_placeholders(sql: str, params: Any) -> _Plan:
    """Rewrite the placeholders Django writes into the server's ``$1`` form.

    Django formats its SQL for a driver that substitutes ``%s`` and ``%(name)s`` anywhere in the
    text and turns ``%%`` into ``%``, also inside quoted strings (its LIKE patterns are written as
    ``'%%'``). This does the same, so the driver receives text that needs no further rewriting.
    A query whose parameters are given but which has no ``%`` markers at all (one written with
    the native ``$1``) is left as it is.
    """
    names: List[str] = []
    count = 0
    named = False

    def rewrite(m: "re.Match[str]") -> str:
        nonlocal count, named
        token = m.group(0)
        if token == "%%":
            return "%"
        if token == "%s":
            count += 1
            return f"${count}"
        named = True
        name = m.group(1)
        if name not in names:
            names.append(name)
        return f"${names.index(name) + 1}"

    has_markers = _PYFORMAT.search(sql) is not None
    if not has_markers:
        return _Plan(sql, None, 0)
    out = _PYFORMAT.sub(rewrite, sql)
    if count and named:
        raise Database.ProgrammingError("a query uses either %s or %(name)s placeholders, not both")
    if count == 0 and not named and params and not isinstance(params, Mapping):
        # Only `%%` escapes, yet values were given: the query uses native markers.
        return _Plan(sql, None, 0)
    return _Plan(out, names if named else None, count)


class NusaCursor(_DriverCursor):
    """The driver's cursor, with the column values Django decodes itself left as text.

    Django's ``JSONField`` parses the JSON text of a column (with the field's own decoder), so a
    ``JSON`` column comes back as text. ``GenericIPAddressField`` expects a string, so ``INET`` and
    ``CIDR`` values are returned in their text form.
    """

    @staticmethod
    def _decode_cell(cell: Optional[bytes], type_code: Optional[str] = None) -> Any:
        if cell is not None and type_code == "JSON":
            try:
                return cell.decode("utf-8")
            except UnicodeDecodeError:
                return cell
        value = _DriverCursor._decode_cell(cell, type_code)
        if type_code in ("INET", "CIDR") and value is not None and not isinstance(value, str):
            if isinstance(value, (ipaddress.IPv4Interface, ipaddress.IPv6Interface)):
                if value.network.prefixlen == value.max_prefixlen:
                    return str(value.ip)
            return str(value)
        return value


class CursorWrapper:
    """Runs Django's ``%s``-style SQL on a driver cursor (see :func:`plan_placeholders`)."""

    def __init__(self, cursor: NusaCursor) -> None:
        self.cursor = cursor

    def execute(self, sql: str, params: Any = None) -> Any:
        if params is None:
            return self.cursor.execute(sql)
        plan = plan_placeholders(sql, params)
        values = plan.arrange(params) if (plan.names is not None or plan.count) else (
            [] if isinstance(params, Mapping) else list(params)
        )
        return self.cursor.execute(plan.sql, values)

    def executemany(self, sql: str, param_list: Any) -> Any:
        param_list = list(param_list)
        if not param_list:
            return None
        plan = plan_placeholders(sql, param_list[0])
        if plan.names is None and not plan.count:
            return self.cursor.executemany(plan.sql, [list(p) for p in param_list])
        return self.cursor.executemany(plan.sql, [plan.arrange(p) for p in param_list])

    def callproc(self, procname: str, params: Optional[Sequence[Any]] = None) -> Any:
        return self.cursor.callproc(procname, list(params or ()))

    def __getattr__(self, attr: str) -> Any:
        return getattr(self.cursor, attr)

    def __iter__(self) -> Any:
        return iter(self.cursor)

    def __enter__(self) -> "CursorWrapper":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class DatabaseWrapper(BaseDatabaseWrapper):
    """A Django connection to a NusaDB database."""

    vendor = "nusadb"
    display_name = "NusaDB"
    # Field type to column type. Values are %-formatted with the field's attributes.
    data_types = {
        "AutoField": "integer",
        "BigAutoField": "bigint",
        "BinaryField": "bytea",
        "BooleanField": "boolean",
        "CharField": _get_varchar_column,
        "DateField": "date",
        "DateTimeField": _get_datetime_column,
        "DecimalField": "numeric(%(max_digits)s, %(decimal_places)s)",
        "DurationField": "interval",
        "FileField": "varchar(%(max_length)s)",
        "FilePathField": "varchar(%(max_length)s)",
        "FloatField": "double precision",
        "IntegerField": "integer",
        "BigIntegerField": "bigint",
        "IPAddressField": "inet",
        "GenericIPAddressField": "inet",
        "JSONField": "jsonb",
        "OneToOneField": "integer",
        "PositiveBigIntegerField": "bigint",
        "PositiveIntegerField": "integer",
        "PositiveSmallIntegerField": "smallint",
        "SlugField": "varchar(%(max_length)s)",
        "SmallAutoField": "smallint",
        "SmallIntegerField": "smallint",
        "TextField": "text",
        "TimeField": "time",
        "UUIDField": "uuid",
    }
    data_type_check_constraints = {
        "PositiveBigIntegerField": '"%(column)s" >= 0',
        "PositiveIntegerField": '"%(column)s" >= 0',
        "PositiveSmallIntegerField": '"%(column)s" >= 0',
    }
    data_types_suffix = {
        "AutoField": "GENERATED BY DEFAULT AS IDENTITY",
        "BigAutoField": "GENERATED BY DEFAULT AS IDENTITY",
        "SmallAutoField": "GENERATED BY DEFAULT AS IDENTITY",
    }
    operators = {
        "exact": "= %s",
        "iexact": "= UPPER(%s)",
        "contains": "LIKE %s",
        "icontains": "ILIKE %s",
        "regex": "~ %s",
        "iregex": "~* %s",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE %s",
        "endswith": "LIKE %s",
        "istartswith": "ILIKE %s",
        "iendswith": "ILIKE %s",
    }
    # For a pattern lookup whose right-hand side is an expression rather than a value: escape the
    # LIKE wildcards on the server, then add them around the result. A backslash is the server's
    # default LIKE escape character. Written for str.format(); `%%` is a literal `%`.
    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        "contains": "LIKE '%%' || {} || '%%'",
        "icontains": "ILIKE '%%' || {} || '%%'",
        "startswith": "LIKE {} || '%%'",
        "istartswith": "ILIKE {} || '%%'",
        "endswith": "LIKE '%%' || {}",
        "iendswith": "ILIKE '%%' || {}",
    }

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def get_database_version(self) -> Tuple[int, ...]:
        """The server's version, from ``SELECT version()`` (``"NusaDB 0.1.0"`` is ``(0, 1, 0)``)."""
        match = re.search(r"(\d+)\.(\d+)(?:\.(\d+))?", self.nusadb_version)
        if match is None:
            return (0,)
        return tuple(int(part) for part in match.groups() if part is not None)

    @cached_property
    def nusadb_version(self) -> str:
        """The text of ``SELECT version()``."""
        with self.temporary_connection() as cursor:
            cursor.execute("SELECT version()")
            return str(cursor.fetchone()[0])

    def get_connection_params(self) -> Dict[str, Any]:
        """The keyword arguments for :func:`nusadb.connect`, from ``NAME``, ``USER``,
        ``PASSWORD``, ``HOST``, ``PORT`` and ``OPTIONS``.

        ``OPTIONS`` may hold ``isolation_level`` and the driver's ``ssl``, ``timeout``,
        ``connect_timeout``, ``statement_timeout`` and ``keepalive``. An empty ``NAME`` connects
        to the server's default database, ``nusadb``; an empty ``PASSWORD`` sends none.
        """
        settings_dict = self.settings_dict
        options = dict(settings_dict.get("OPTIONS") or {})
        unknown = set(options) - set(_CONNECT_OPTIONS) - {"isolation_level"}
        if unknown:
            raise ImproperlyConfigured(
                "Unknown NusaDB OPTIONS: %s. Accepted: isolation_level, %s."
                % (", ".join(sorted(unknown)), ", ".join(_CONNECT_OPTIONS))
            )
        name = settings_dict.get("NAME")
        if name and len(name) > self.ops.max_name_length():
            raise ImproperlyConfigured(
                "The database name '%s' (%d characters) is longer than the limit of %d characters."
                % (name, len(name), self.ops.max_name_length())
            )
        params: Dict[str, Any] = {
            "database": name or "nusadb",
            "password": settings_dict.get("PASSWORD") or None,
        }
        if settings_dict.get("USER"):
            params["user"] = settings_dict["USER"]
        if settings_dict.get("HOST"):
            params["host"] = settings_dict["HOST"]
        if settings_dict.get("PORT"):
            params["port"] = int(settings_dict["PORT"])
        for key in _CONNECT_OPTIONS:
            if key in options:
                params[key] = options[key]
        return params

    @cached_property
    def isolation_level(self) -> Optional[str]:
        """The isolation level from ``OPTIONS``, lower case, or ``None`` for the server default."""
        level = (self.settings_dict.get("OPTIONS") or {}).get("isolation_level")
        if level is None:
            return None
        normalized = " ".join(str(level).replace("_", " ").split()).lower()
        if normalized not in ISOLATION_LEVELS:
            raise ImproperlyConfigured(
                "Invalid transaction isolation level %r. Use one of: %s."
                % (level, ", ".join(ISOLATION_LEVELS))
            )
        return normalized

    @async_unsafe
    def get_new_connection(self, conn_params: Dict[str, Any]) -> Database.Connection:
        level = self.isolation_level
        connection = Database.connect(autocommit=True, **conn_params)
        if level is not None:
            cursor = connection.cursor()
            cursor.execute(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " + level.upper()
            )
        return connection

    def init_connection_state(self) -> None:
        super().init_connection_state()
        # Times are exchanged in UTC: the server keeps its session in UTC, and the backend converts
        # to a named zone itself where Django asks for one (see DatabaseOperations).
        self.connection.cursor().execute("SET TIME ZONE 'UTC'")
        if not self.get_autocommit():
            self.connection.commit()

    @async_unsafe
    def create_cursor(self, name: Optional[str] = None) -> CursorWrapper:
        return CursorWrapper(NusaCursor(self.connection))

    def _set_autocommit(self, autocommit: bool) -> None:
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    def is_usable(self) -> bool:
        if self.connection is None or self.connection.closed or self.connection.broken:
            return False
        try:
            self.connection.cursor().execute("SELECT 1")
        except Database.Error:
            return False
        return True

    def check_constraints(self, table_names: Optional[List[str]] = None) -> None:
        """Constraints are checked when each statement runs; nothing is deferred."""
