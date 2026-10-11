"""Migrations on NusaDB: the ``DatabaseSchemaEditor``.

The server's DDL differs from what Django's base editor writes in a few places, handled here:

* ``ALTER TABLE ... DROP COLUMN`` takes no ``CASCADE`` and leaves the column's indexes and
  constraints in place, so they are dropped first.
* ``ALTER TABLE ... RENAME COLUMN`` is refused while an index or constraint names the column, so
  those are dropped, the column renamed, and each declared again under its own name.
* An identity column cannot be added to an existing column, so turning a field into an
  ``AutoField`` raises ``NotSupportedError``. Dropping the column's default removes its identity.
* DDL statements take no parameters, so values are written into the SQL as literals.
"""

from __future__ import annotations

import datetime
import decimal
import json
import re
import uuid
from typing import Any, List, Optional, Sequence, Tuple

from django.db import NotSupportedError
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.utils import strip_quotes

import nusadb

from .introspection import TableObject

_AUTO_FIELDS = {"AutoField", "BigAutoField", "SmallAutoField"}


def quote_literal(value: Any) -> str:
    """``value`` as an SQL literal the server reads back as the same value."""
    if value is None:
        return "NULL"
    if isinstance(value, nusadb.Json):
        return "'%s'::jsonb" % value.dumps(value.value).replace("'", "''")
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float, decimal.Decimal)):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "'\\x%s'::bytea" % bytes(value).hex()
    if isinstance(value, datetime.datetime):
        kind = "TIMESTAMPTZ" if value.tzinfo is not None else "TIMESTAMP"
        return "%s '%s'" % (kind, value.isoformat(sep=" "))
    if isinstance(value, datetime.date):
        return "DATE '%s'" % value.isoformat()
    if isinstance(value, datetime.time):
        return "TIME '%s'" % value.isoformat()
    if isinstance(value, datetime.timedelta):
        seconds = decimal.Decimal(value.days * 86400 + value.seconds) + decimal.Decimal(
            value.microseconds
        ).scaleb(-6)
        return "INTERVAL '%s seconds'" % seconds
    if isinstance(value, uuid.UUID):
        return "'%s'::uuid" % value
    if isinstance(value, (dict, list)):
        return "'%s'::jsonb" % json.dumps(value).replace("'", "''")
    return "'%s'" % str(value).replace("'", "''")


class _ColumnRename(str):
    """``ALTER TABLE ... RENAME COLUMN`` with the statements that must run around it: dropping
    the indexes and constraints that name the column, and declaring them again after."""

    before: List[str]
    after: List[str]


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    """Schema changes for NusaDB."""

    sql_delete_column = "ALTER TABLE %(table)s DROP COLUMN %(column)s"
    sql_create_column_inline_fk = (
        "CONSTRAINT %(name)s REFERENCES %(to_table)s(%(to_column)s)%(deferrable)s"
    )
    sql_delete_index = "DROP INDEX IF EXISTS %(name)s"
    sql_alter_column_type = "ALTER COLUMN %(column)s TYPE %(type)s%(collation)s"
    sql_drop_identity = "ALTER TABLE %(table)s ALTER COLUMN %(column)s DROP DEFAULT"
    sql_delete_sequence = "DROP SEQUENCE IF EXISTS %(sequence)s"

    def quote_value(self, value: Any) -> str:
        literal = quote_literal(value)
        # The base editor %-formats SQL after quoting values into it.
        return literal.replace("%", "%%")

    def execute(self, sql: Any, params: Optional[Sequence[Any]] = ()) -> None:
        if isinstance(sql, _ColumnRename):
            for statement in sql.before:
                self.execute(statement, None)
            self.execute(str(sql), None)
            for statement in sql.after:
                self.execute(statement, None)
            return
        if not params or self.collect_sql:
            return super().execute(sql, params)
        # DDL takes no parameters: write the values in as literals. Formatting also turns each
        # `%%` into `%`, as a driver substituting `%s` would.
        literal_sql = str(sql) % tuple(quote_literal(value) for value in params)
        return super().execute(literal_sql, None)

    # --- columns ---

    def _table_objects(self, table: str) -> List[TableObject]:
        with self.connection.cursor() as cursor:
            return self.connection.introspection.get_table_objects(cursor, table)

    def _incoming_foreign_keys(self, table: str, column: str) -> List[Tuple[str, TableObject]]:
        with self.connection.cursor() as cursor:
            return self.connection.introspection.get_incoming_foreign_keys(cursor, table, column)

    def _drop_object_sql(self, table: str, obj: TableObject) -> str:
        if obj.kind == "index":
            return self.sql_delete_index % {"name": self.quote_name(obj.name)}
        return self.sql_delete_constraint % {
            "table": self.quote_name(table),
            "name": self.quote_name(obj.name),
        }

    def _create_object_sql(self, table: str, obj: TableObject, renames: dict) -> str:
        """The statement that declares ``obj`` again, with columns renamed by ``renames``."""
        qn = self.quote_name
        columns = ", ".join(qn(renames.get(column, column)) for column in obj.columns)
        if obj.kind == "index":
            return "CREATE %sINDEX %s ON %s (%s)" % (
                "UNIQUE " if obj.unique else "",
                qn(obj.name),
                qn(table),
                columns,
            )
        if obj.kind == "primary key":
            body = "PRIMARY KEY (%s)" % columns
        elif obj.kind == "unique":
            body = "UNIQUE (%s)" % columns
        elif obj.kind == "check":
            body = "CHECK (%s)" % _rename_identifiers(obj.check_clause or "", renames)
        else:
            referred_renames = renames if obj.referred_table == table else {}
            body = "FOREIGN KEY (%s) REFERENCES %s (%s)" % (
                columns,
                qn(obj.referred_table or ""),
                ", ".join(qn(referred_renames.get(c, c)) for c in obj.referred_columns),
            )
            if obj.on_delete and obj.on_delete not in ("NO ACTION", "NONE"):
                body += " ON DELETE " + obj.on_delete
            if obj.on_update and obj.on_update not in ("NO ACTION", "NONE"):
                body += " ON UPDATE " + obj.on_update
        return "ALTER TABLE %s ADD CONSTRAINT %s %s" % (qn(table), qn(obj.name), body)

    def _rename_field_sql(self, table: str, old_field: Any, new_field: Any, new_type: str) -> Any:
        sql = super()._rename_field_sql(table, old_field, new_field, new_type)
        if self.collect_sql:
            return sql
        old_column, new_column = old_field.column, new_field.column
        renames = {old_column: new_column}
        rename = _ColumnRename(sql)
        rename.before, rename.after = [], []
        # Foreign keys elsewhere that refer to the column go first and come back last.
        incoming = [
            (other, obj)
            for other, obj in self._incoming_foreign_keys(table, old_column)
            if other != table
        ]
        for other, obj in incoming:
            rename.before.append(self._drop_object_sql(other, obj))
        order = {"foreign key": 0, "check": 1, "index": 2, "unique": 3, "primary key": 4}
        own = sorted(
            (
                obj
                for obj in self._table_objects(table)
                if old_column in obj.columns
                or (obj.referred_table == table and old_column in obj.referred_columns)
            ),
            key=lambda obj: order[obj.kind],
        )
        model = getattr(new_field, "model", None) or getattr(old_field, "model", None)
        model_sql = self._model_object_sql(model, old_column, new_column)
        for obj in own:
            rename.before.append(self._drop_object_sql(table, obj))
        for obj in reversed(own):
            declared = model_sql.get(obj.name)
            rename.after.append(
                declared if declared is not None else self._create_object_sql(table, obj, renames)
            )
        for other, obj in incoming:
            target = obj.referred_table
            obj.referred_columns = [renames.get(c, c) for c in obj.referred_columns]
            rename.after.append(self._create_object_sql(other, obj, {}))
            obj.referred_table = target
        return rename

    def _model_object_sql(self, model: Any, old_column: str, new_column: str) -> dict:
        """``{name: statement}`` for the indexes and constraints the model declares in its Meta,
        written for the renamed column. They carry what information_schema does not report: a
        condition, expressions, included columns."""
        if model is None:
            return {}
        out = {}
        old, new = self.quote_name(old_column), self.quote_name(new_column)
        declared = [*getattr(model._meta, "indexes", ()), *getattr(model._meta, "constraints", ())]
        for item in declared:
            try:
                statement = item.create_sql(model, self)
            except Exception:  # noqa: BLE001 - fall back to what introspection reports
                continue
            if statement is not None and item.name:
                out[item.name] = str(statement).replace(old, new)
        return out

    def _column_sequence(self, table: str, column: str) -> Optional[str]:
        with self.connection.cursor() as cursor:
            for found in self.connection.introspection.get_sequences(cursor, table):
                if found["column"] == column:
                    return found["name"]
        return None

    def remove_field(self, model: Any, field: Any) -> None:
        sequence = None
        if (
            not self.collect_sql
            and not (field.many_to_many and field.remote_field.through._meta.auto_created)
            and field.db_parameters(connection=self.connection)["type"] is not None
        ):
            table = model._meta.db_table
            # The server keeps a dropped column's indexes and constraints: drop them first.
            for other, obj in self._incoming_foreign_keys(table, field.column):
                if other != table:
                    self.execute(self._drop_object_sql(other, obj), None)
            order = {"foreign key": 0, "check": 1, "index": 2, "unique": 3, "primary key": 4}
            for obj in sorted(self._table_objects(table), key=lambda obj: order[obj.kind]):
                if field.column in obj.columns:
                    self.execute(self._drop_object_sql(table, obj), None)
            sequence = self._column_sequence(table, field.column)
        super().remove_field(model, field)
        # Nor does it drop the sequence behind a dropped identity column.
        if sequence is not None:
            self.execute(self.sql_delete_sequence % {"sequence": self.quote_name(sequence)}, None)

    def _alter_column_type_sql(
        self,
        model: Any,
        old_field: Any,
        new_field: Any,
        new_type: str,
        old_collation: Any,
        new_collation: Any,
    ) -> Any:
        old_auto = old_field.get_internal_type() in _AUTO_FIELDS
        new_auto = new_field.get_internal_type() in _AUTO_FIELDS
        if new_auto and not old_auto:
            raise NotSupportedError(
                "NusaDB cannot turn an existing column into an identity column; "
                "add a new AutoField column instead of altering %s.%s"
                % (model._meta.db_table, old_field.column)
            )
        if old_auto and not new_auto:
            # Called after a rename of the column, if any: the new name is current.
            table = strip_quotes(model._meta.db_table)
            column = strip_quotes(new_field.column)
            sequence = None if self.collect_sql else self._column_sequence(table, column)
            self.execute(
                self.sql_drop_identity
                % {"table": self.quote_name(table), "column": self.quote_name(column)},
                None,
            )
            # The server keeps the sequence of a column that loses its identity.
            if sequence is not None:
                self.execute(self.sql_delete_sequence % {"sequence": self.quote_name(sequence)}, None)
        return super()._alter_column_type_sql(
            model, old_field, new_field, new_type, old_collation, new_collation
        )


def _rename_identifiers(clause: str, renames: dict) -> str:
    """``clause`` with each column in ``renames`` replaced, quoted or bare, outside strings."""
    if not renames:
        return clause
    parts = re.split(r"('(?:[^']|'')*')", clause)
    for index in range(0, len(parts), 2):
        text = parts[index]
        for old, new in renames.items():
            quoted_new = '"%s"' % new.replace('"', '""')
            text = text.replace('"%s"' % old.replace('"', '""'), quoted_new)
            text = re.sub(r'(?<![\w"$])%s(?![\w"$])' % re.escape(old), quoted_new, text)
        parts[index] = text
    return "".join(parts)
