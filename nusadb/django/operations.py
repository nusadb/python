"""SQL generation details for NusaDB: quoting, dates and times, casts, sequences, bulk inserts."""

from __future__ import annotations

import datetime
import json
import re
from functools import lru_cache, partial
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from django.conf import settings
from django.db import NotSupportedError
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.utils import split_tzname_delta
from django.db.models.constants import OnConflict

import nusadb

# The lookup types the server's DATE_TRUNC and EXTRACT accept, as Django names them.
_TRUNC_KINDS = frozenset(
    ["year", "quarter", "month", "week", "day", "hour", "minute", "second"]
)
_EXTRACT_KINDS = re.compile(r"[A-Z_]+")

# The window, in years, over which a named time zone's offset changes are followed.
_ZONE_FIRST_YEAR = 1970
_ZONE_LAST_YEAR = 2050

_UTC_NAMES = frozenset(["UTC", "GMT", "Z", "ETC/UTC", "ETC/GMT", "UCT", "ZULU", "UNIVERSAL"])


@lru_cache(maxsize=None)
def _json_dumps(encoder: Any) -> Callable[[Any], str]:
    if encoder is None:
        return json.dumps
    return partial(json.dumps, cls=encoder)


@lru_cache(maxsize=64)
def zone_offsets(tzname: str) -> Tuple[Tuple[datetime.datetime, ...], Tuple[int, ...]]:
    """The UTC offsets of a time zone, as ``(changes, offsets)``.

    ``offsets[0]`` applies before ``changes[0]``, ``offsets[i]`` from ``changes[i - 1]`` up to
    ``changes[i]``, and the last offset from the last change on. Offsets are in seconds, changes
    are UTC instants between 1970 and 2050. A zone with a fixed offset has no changes.

    Raises :class:`django.db.NotSupportedError` for a name that is not a known time zone.
    """
    name, sign, offset = split_tzname_delta(tzname)
    if offset:
        hours, _, minutes = offset.partition(":")
        seconds = int(hours) * 3600 + int(minutes or 0) * 60
        base = 0 if name.upper() in _UTC_NAMES or not name else zone_offsets(name)[1][0]
        return (), (base + (seconds if sign == "+" else -seconds),)
    if name.upper() in _UTC_NAMES:
        return (), (0,)
    try:
        import zoneinfo
    except ImportError:  # Python 3.8
        from backports import zoneinfo  # type: ignore[no-redef]
    try:
        zone = zoneinfo.ZoneInfo(name)
    except (zoneinfo.ZoneInfoNotFoundError, ValueError) as exc:
        raise NotSupportedError(f"unknown time zone {name!r}") from exc

    def offset_at(instant: datetime.datetime) -> int:
        delta = instant.astimezone(zone).utcoffset()
        return int(delta.total_seconds()) if delta is not None else 0

    utc = datetime.timezone.utc
    start = datetime.datetime(_ZONE_FIRST_YEAR, 1, 1, tzinfo=utc)
    end = datetime.datetime(_ZONE_LAST_YEAR, 1, 1, tzinfo=utc)
    day = datetime.timedelta(days=1)
    changes: List[datetime.datetime] = []
    offsets = [offset_at(start)]
    instant = start
    while instant < end:
        following = instant + day
        current = offset_at(following)
        if current != offsets[-1]:
            # Narrow the change down to the second.
            low, high = instant, following
            while (high - low) > datetime.timedelta(seconds=1):
                middle = low + (high - low) / 2
                if offset_at(middle) == offsets[-1]:
                    low = middle
                else:
                    high = middle
            changes.append(high.replace(microsecond=0))
            offsets.append(current)
        instant = following
    return tuple(changes), tuple(offsets)


def _interval(seconds: int) -> str:
    return "INTERVAL '%d seconds'" % seconds


class DatabaseOperations(BaseDatabaseOperations):
    """How Django's ORM writes SQL for NusaDB."""

    cast_char_field_without_max_length = "varchar"
    explain_prefix = "EXPLAIN"
    explain_options = frozenset(["ANALYZE"])
    cast_data_types = {
        "AutoField": "integer",
        "BigAutoField": "bigint",
        "SmallAutoField": "smallint",
    }

    # --- names and values ---

    def quote_name(self, name: str) -> str:
        if name.startswith('"') and name.endswith('"'):
            return name
        return '"%s"' % name.replace('"', '""')

    def max_name_length(self) -> int:
        return 63

    def no_limit_value(self) -> None:
        return None

    def prep_for_iexact_query(self, x: str) -> str:
        return x

    def lookup_cast(self, lookup_type: str, internal_type: Optional[str] = None) -> str:
        lookup = "%s"
        if lookup_type in (
            "iexact",
            "contains",
            "icontains",
            "startswith",
            "istartswith",
            "endswith",
            "iendswith",
            "regex",
            "iregex",
        ):
            if internal_type in ("IPAddressField", "GenericIPAddressField"):
                lookup = "HOST(%s)"
            else:
                lookup = "(%s)::text"
        if lookup_type == "iexact":
            lookup = "UPPER(%s)" % lookup
        return lookup

    def unification_cast_sql(self, output_field: Any) -> str:
        internal_type = output_field.get_internal_type()
        if internal_type in (
            "GenericIPAddressField",
            "IPAddressField",
            "TimeField",
            "UUIDField",
        ):
            return "CAST(%%s AS %s)" % output_field.db_type(self.connection).split("(")[0]
        return "%s"

    def adapt_datefield_value(self, value: Any) -> Any:
        return value

    def adapt_datetimefield_value(self, value: Any) -> Any:
        if value is None or hasattr(value, "resolve_expression"):
            return value
        if not isinstance(value, datetime.datetime):
            return value
        if settings.USE_TZ:
            # The column holds instants: a naive value is taken to be UTC.
            if value.tzinfo is None:
                return value.replace(tzinfo=datetime.timezone.utc)
            return value
        if value.tzinfo is not None:
            raise ValueError(
                "NusaDB backend does not support timezone-aware datetimes when USE_TZ is False."
            )
        return value

    def adapt_timefield_value(self, value: Any) -> Any:
        if value is None or hasattr(value, "resolve_expression"):
            return value
        if isinstance(value, datetime.time) and value.tzinfo is not None:
            raise ValueError("NusaDB backend does not support timezone-aware times.")
        return value

    def adapt_decimalfield_value(
        self, value: Any, max_digits: Optional[int] = None, decimal_places: Optional[int] = None
    ) -> Any:
        return value

    def adapt_ipaddressfield_value(self, value: Any) -> Any:
        return value or None

    def adapt_json_value(self, value: Any, encoder: Any) -> Any:
        return nusadb.Json(value, dumps=_json_dumps(encoder))

    def get_db_converters(self, expression: Any) -> List[Any]:
        converters = super().get_db_converters(expression)
        internal_type = expression.output_field.get_internal_type()
        if internal_type == "DateTimeField" and not settings.USE_TZ:
            converters.append(self.convert_datetimefield_value)
        return converters

    def convert_datetimefield_value(self, value: Any, expression: Any, connection: Any) -> Any:
        # A column made while USE_TZ was True holds instants; give them back as naive UTC.
        if isinstance(value, datetime.datetime) and value.tzinfo is not None:
            return value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return value

    # --- dates and times ---

    def _convert_sql_to_tz(
        self, sql: str, params: Sequence[Any], tzname: Optional[str]
    ) -> Tuple[str, Tuple[Any, ...]]:
        """The local time in ``tzname`` of the ``timestamp with time zone`` that ``sql`` yields, as
        a ``timestamp``.

        The server converts only to UTC and fixed offsets, so a named zone is applied here: its
        UTC offset at each instant comes from Python's zone database, as a CASE over the zone's
        offset changes between 1970 and 2050 (outside that window the nearest offset is used).
        """
        if not tzname or not settings.USE_TZ:
            return sql, tuple(params)
        return self.local_time_sql(sql, params, tzname)

    def local_time_sql(
        self, sql: str, params: Sequence[Any], tzname: str
    ) -> Tuple[str, Tuple[Any, ...]]:
        """The local time in ``tzname`` of the ``timestamp with time zone`` that ``sql`` yields."""
        params = tuple(params)
        changes, offsets = zone_offsets(tzname)
        utc = f"(({sql}) AT TIME ZONE 'UTC')"
        if not changes:
            if offsets[0] == 0:
                return utc, params
            return f"({utc} + {_interval(offsets[0])})", params
        whens = " ".join(
            "WHEN (%s) < TIMESTAMPTZ '%s' THEN %s"
            % (sql, change.strftime("%Y-%m-%d %H:%M:%S+00"), _interval(offset))
            for change, offset in zip(changes, offsets)
        )
        case = f"CASE {whens} ELSE {_interval(offsets[-1])} END"
        return f"({utc} + {case})", params * (len(changes) + 1)

    def date_extract_sql(
        self, lookup_type: str, sql: str, params: Sequence[Any]
    ) -> Tuple[str, Tuple[Any, ...]]:
        params = tuple(params)
        if lookup_type == "week_day":
            # Sunday is 1 and Saturday 7, as on every Django backend.
            return f"(EXTRACT(DOW FROM {sql}) + 1)", params
        if lookup_type == "iso_week_day":
            return f"EXTRACT(ISODOW FROM {sql})", params
        if lookup_type == "iso_year":
            return f"EXTRACT(ISOYEAR FROM {sql})", params
        kind = lookup_type.upper()
        if not _EXTRACT_KINDS.fullmatch(kind):
            raise ValueError(f"Invalid lookup type: {lookup_type!r}")
        return f"EXTRACT({kind} FROM {sql})", params

    def _trunc_kind(self, lookup_type: str) -> str:
        if lookup_type not in _TRUNC_KINDS:
            raise ValueError(f"Invalid lookup type: {lookup_type!r}")
        return lookup_type

    def date_trunc_sql(
        self, lookup_type: str, sql: str, params: Sequence[Any], tzname: Optional[str] = None
    ) -> Tuple[str, Tuple[Any, ...]]:
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"CAST(DATE_TRUNC('{self._trunc_kind(lookup_type)}', {sql}) AS date)", params

    def datetime_cast_date_sql(
        self, sql: str, params: Sequence[Any], tzname: Optional[str]
    ) -> Tuple[str, Tuple[Any, ...]]:
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"CAST({sql} AS date)", params

    def datetime_cast_time_sql(
        self, sql: str, params: Sequence[Any], tzname: Optional[str]
    ) -> Tuple[str, Tuple[Any, ...]]:
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"CAST({sql} AS time)", params

    def datetime_extract_sql(
        self, lookup_type: str, sql: str, params: Sequence[Any], tzname: Optional[str]
    ) -> Tuple[str, Tuple[Any, ...]]:
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        if lookup_type == "second":
            return f"FLOOR(EXTRACT(SECOND FROM {sql}))", params
        return self.date_extract_sql(lookup_type, sql, params)

    def datetime_trunc_sql(
        self, lookup_type: str, sql: str, params: Sequence[Any], tzname: Optional[str]
    ) -> Tuple[str, Tuple[Any, ...]]:
        sql, params = self._convert_sql_to_tz(sql, params, tzname)
        return f"DATE_TRUNC('{self._trunc_kind(lookup_type)}', {sql})", params

    def time_extract_sql(
        self, lookup_type: str, sql: str, params: Sequence[Any]
    ) -> Tuple[str, Tuple[Any, ...]]:
        if lookup_type == "second":
            return f"FLOOR(EXTRACT(SECOND FROM {sql}))", tuple(params)
        return self.date_extract_sql(lookup_type, sql, params)

    def time_trunc_sql(
        self, lookup_type: str, sql: str, params: Sequence[Any], tzname: Optional[str] = None
    ) -> Tuple[str, Tuple[Any, ...]]:
        if tzname and settings.USE_TZ:
            sql, params = self._convert_sql_to_tz(sql, params, tzname)
        # DATE_TRUNC takes a timestamp: truncate the time on a fixed day.
        return (
            f"CAST(DATE_TRUNC('{self._trunc_kind(lookup_type)}', "
            f"TIMESTAMP '2000-01-01 00:00:00' + (CAST({sql} AS time) - TIME '00:00:00')) AS time)",
            tuple(params),
        )

    def subtract_temporals(
        self, internal_type: str, lhs: Tuple[str, Sequence[Any]], rhs: Tuple[str, Sequence[Any]]
    ) -> Tuple[str, Tuple[Any, ...]]:
        lhs_sql, lhs_params = lhs
        rhs_sql, rhs_params = rhs
        params = (*lhs_params, *rhs_params)
        if internal_type == "DateField":
            # date - date is a number of days.
            return f"(INTERVAL '1 day' * ({lhs_sql} - {rhs_sql}))", params
        return f"({lhs_sql} - {rhs_sql})", params

    # --- statements ---

    def distinct_sql(self, fields: List[str], params: Any) -> Tuple[List[str], List[Any]]:
        if fields:
            params = [param for param_list in params for param in param_list]
            return ["DISTINCT ON (%s)" % ", ".join(fields)], params
        return ["DISTINCT"], []

    def return_insert_columns(self, fields: Sequence[Any]) -> Tuple[str, Tuple[Any, ...]]:
        if not fields:
            return "", ()
        columns = [
            "%s.%s"
            % (self.quote_name(field.model._meta.db_table), self.quote_name(field.column))
            for field in fields
        ]
        return "RETURNING %s" % ", ".join(columns), ()

    def fetch_returned_insert_rows(self, cursor: Any) -> List[Any]:
        return cursor.fetchall()

    def bulk_insert_sql(self, fields: Sequence[Any], placeholder_rows: Iterable[Any]) -> str:
        rows = (", ".join(row) for row in placeholder_rows)
        return "VALUES " + ", ".join("(%s)" % row for row in rows)

    def bulk_batch_size(self, fields: Sequence[Any], objs: Sequence[Any]) -> int:
        # The protocol counts a statement's parameters in 16 bits.
        if fields:
            return max(1, min(len(objs), 65535 // len(fields)))
        return len(objs)

    def on_conflict_suffix_sql(
        self,
        fields: Sequence[Any],
        on_conflict: Any,
        update_fields: Sequence[str],
        unique_fields: Sequence[str],
    ) -> str:
        if on_conflict == OnConflict.IGNORE:
            return "ON CONFLICT DO NOTHING"
        if on_conflict == OnConflict.UPDATE:
            return "ON CONFLICT(%s) DO UPDATE SET %s" % (
                ", ".join(map(self.quote_name, unique_fields)),
                ", ".join(
                    f"{field} = EXCLUDED.{field}" for field in map(self.quote_name, update_fields)
                ),
            )
        return super().on_conflict_suffix_sql(fields, on_conflict, update_fields, unique_fields)

    def explain_query_prefix(self, format: Optional[str] = None, **options: Any) -> str:
        extra: Dict[str, str] = {}
        if options:
            normalized = {name.upper(): value for name, value in options.items()}
            for name in self.explain_options:
                if name in normalized:
                    extra[name] = "true" if normalized.pop(name) else "false"
            options = normalized
        prefix = super().explain_query_prefix(format, **options)
        if format:
            extra["FORMAT"] = format
        if extra:
            prefix += " (%s)" % ", ".join("%s %s" % item for item in extra.items())
        return prefix

    def last_executed_query(self, cursor: Any, sql: str, params: Any) -> Optional[str]:
        if params is None:
            return sql
        try:
            if isinstance(params, dict):
                return sql % {key: self._literal(value) for key, value in params.items()}
            return sql % tuple(self._literal(value) for value in params)
        except (TypeError, ValueError):
            return sql

    def _literal(self, value: Any) -> str:
        from .schema import quote_literal

        return quote_literal(value)

    # --- flush and sequences ---

    def _foreign_key_edges(self) -> List[Tuple[str, str]]:
        """``(referencing table, referenced table)`` for every foreign key in the current schema."""
        with self.connection.cursor() as cursor:
            return self.connection.introspection.get_foreign_key_edges(cursor)

    def sql_flush(
        self,
        style: Any,
        tables: Sequence[str],
        *,
        reset_sequences: bool = False,
        allow_cascade: bool = False,
    ) -> List[str]:
        """One ``TRUNCATE`` per table (the server truncates one table per statement).

        Tables are emptied referencing ones first. A table referenced only by tables that are
        being flushed too is truncated with ``CASCADE``, which then reaches nothing else; one
        referenced from outside the list gets ``CASCADE`` only when ``allow_cascade`` is set.
        """
        if not tables:
            return []
        wanted = list(tables)
        names: Set[str] = set(wanted)
        referenced_by: Dict[str, Set[str]] = {name: set() for name in wanted}
        for child, parent in self._foreign_key_edges():
            if parent in referenced_by and child != parent:
                referenced_by[parent].add(child)
        ordered: List[str] = []
        placed: Set[str] = set()

        def place(name: str, path: Tuple[str, ...] = ()) -> None:
            if name in placed or name in path:
                return
            for child in sorted(referenced_by.get(name, ())):
                if child in names:
                    place(child, path + (name,))
            placed.add(name)
            ordered.append(name)

        for name in wanted:
            place(name)
        statements = []
        for name in ordered:
            parts = [style.SQL_KEYWORD("TRUNCATE"), style.SQL_FIELD(self.quote_name(name))]
            if reset_sequences:
                parts.append(style.SQL_KEYWORD("RESTART IDENTITY"))
            children = referenced_by.get(name, set()) - {name}
            if children and (allow_cascade or children <= names):
                parts.append(style.SQL_KEYWORD("CASCADE"))
            statements.append("%s;" % " ".join(parts))
        return statements

    def _sequence_name(self, table: str, column: Optional[str]) -> Optional[str]:
        with self.connection.cursor() as cursor:
            for sequence in self.connection.introspection.get_sequences(cursor, table):
                if column is None or sequence["column"] == column:
                    return sequence["name"]
        return None

    def sequence_reset_by_name_sql(self, style: Any, sequences: Sequence[Dict[str, Any]]) -> List[str]:
        sql = []
        for sequence_info in sequences:
            name = sequence_info.get("name") or self._sequence_name(
                sequence_info["table"], sequence_info.get("column")
            )
            if name is None:
                continue
            sql.append(
                "%s %s %s;"
                % (
                    style.SQL_KEYWORD("ALTER SEQUENCE"),
                    style.SQL_FIELD(self.quote_name(name)),
                    style.SQL_KEYWORD("RESTART WITH 1"),
                )
            )
        return sql

    def sequence_reset_sql(self, style: Any, model_list: Sequence[Any]) -> List[str]:
        from django.db import models

        output = []
        qn = self.quote_name
        for model in model_list:
            for field in model._meta.local_fields:
                if isinstance(field, models.AutoField):
                    name = self._sequence_name(model._meta.db_table, field.column)
                    if name is None:
                        break
                    table = qn(model._meta.db_table)
                    column = qn(field.column)
                    # setval runs once per statement, so the table is read in a subquery. On an
                    # empty table the sequence is set to 0, so the next id is 1.
                    output.append(
                        "%s setval('%s', COALESCE((SELECT MAX(%s) FROM %s), 0));"
                        % (
                            style.SQL_KEYWORD("SELECT"),
                            style.SQL_FIELD(name.replace("'", "''")),
                            style.SQL_FIELD(column),
                            style.SQL_TABLE(table),
                        )
                    )
                    break
        return output

    def tablespace_sql(self, tablespace: Optional[str], inline: bool = False) -> str:
        return ""
