"""Reading tables, columns, constraints and indexes back from a NusaDB database.

Everything comes from ``information_schema``, in the current schema (``current_schema()``).
"""

from __future__ import annotations

import re
from collections import namedtuple
from typing import Any, Dict, List, Optional, Sequence, Tuple

from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.introspection import FieldInfo as BaseFieldInfo
from django.db.backends.base.introspection import TableInfo as BaseTableInfo
from django.db.models import Index

FieldInfo = namedtuple("FieldInfo", BaseFieldInfo._fields + ("is_autofield", "comment"))
TableInfo = namedtuple("TableInfo", BaseTableInfo._fields + ("comment",))

# A column default that draws from a sequence: an identity column.
_NEXTVAL = re.compile(r"^nextval\('(?P<name>(?:[^']|'')+)'(?:::\w+)?\)$", re.IGNORECASE)
# A word in a CHECK clause that may name a column, quoted or not.
_IDENTIFIER = re.compile(r'"((?:[^"]|"")+)"|\b([A-Za-z_][A-Za-z0-9_$]*)\b')
# Text inside single quotes, skipped when looking for column names.
_STRING = re.compile(r"'(?:[^']|'')*'")

_SCHEMA = "current_schema()"


class TableObject:
    """A constraint or index of a table, as the schema editor needs it to drop and declare it
    again: ``kind`` is ``"primary key"``, ``"unique"``, ``"foreign key"``, ``"check"`` or
    ``"index"``."""

    __slots__ = (
        "name",
        "kind",
        "columns",
        "unique",
        "check_clause",
        "referred_table",
        "referred_columns",
        "on_delete",
        "on_update",
    )

    def __init__(self, name: str, kind: str, columns: List[str]) -> None:
        self.name = name
        self.kind = kind
        self.columns = columns
        self.unique = kind in ("primary key", "unique")
        self.check_clause: Optional[str] = None
        self.referred_table: Optional[str] = None
        self.referred_columns: List[str] = []
        self.on_delete: Optional[str] = None
        self.on_update: Optional[str] = None


def check_clause_columns(clause: str, columns: Sequence[str]) -> List[str]:
    """The table columns a CHECK clause names, in table order."""
    text = _STRING.sub("''", clause)
    found = set()
    for match in _IDENTIFIER.finditer(text):
        quoted, bare = match.groups()
        if quoted is not None:
            found.add(quoted.replace('""', '"'))
        elif bare is not None:
            found.add(bare)
    lowered = {word.lower() for word in found}
    return [c for c in columns if c in found or (c == c.lower() and c in lowered)]


class DatabaseIntrospection(BaseDatabaseIntrospection):
    """Introspection over ``information_schema``."""

    # The data_type names information_schema.columns reports, to Django field types.
    data_types_reverse = {
        "boolean": "BooleanField",
        "bytea": "BinaryField",
        "bigint": "BigIntegerField",
        "smallint": "SmallIntegerField",
        "integer": "IntegerField",
        "text": "TextField",
        "real": "FloatField",
        "double precision": "FloatField",
        "inet": "GenericIPAddressField",
        "cidr": "GenericIPAddressField",
        "character": "CharField",
        "character varying": "CharField",
        "date": "DateField",
        "time without time zone": "TimeField",
        "time with time zone": "TimeField",
        "timestamp without time zone": "DateTimeField",
        "timestamp with time zone": "DateTimeField",
        "interval": "DurationField",
        "numeric": "DecimalField",
        "uuid": "UUIDField",
        "json": "JSONField",
        "jsonb": "JSONField",
    }

    def get_field_type(self, data_type: str, description: Any) -> str:
        field_type = super().get_field_type(data_type, description)
        if description.is_autofield:
            if field_type == "IntegerField":
                return "AutoField"
            if field_type == "BigIntegerField":
                return "BigAutoField"
            if field_type == "SmallIntegerField":
                return "SmallAutoField"
        return field_type

    def get_table_list(self, cursor: Any) -> List[TableInfo]:
        """Tables (``"t"``) and views (``"v"``) of the current schema."""
        cursor.execute(
            "SELECT table_name, table_type FROM information_schema.tables "
            f"WHERE table_schema = {_SCHEMA}"
        )
        tables = [
            TableInfo(name, "v" if str(kind).upper() == "VIEW" else "t", None)
            for name, kind in cursor.fetchall()
        ]
        cursor.execute(
            f"SELECT table_name FROM information_schema.views WHERE table_schema = {_SCHEMA}"
        )
        known = {table.name for table in tables}
        tables.extend(
            TableInfo(name, "v", None) for (name,) in cursor.fetchall() if name not in known
        )
        return sorted(tables, key=lambda table: table.name)

    def get_table_description(self, cursor: Any, table_name: str) -> List[FieldInfo]:
        """The columns of ``table_name``, in table order."""
        cursor.execute(
            "SELECT column_name, data_type, character_maximum_length, numeric_precision, "
            "numeric_scale, is_nullable, column_default, ordinal_position "
            "FROM information_schema.columns "
            f"WHERE table_schema = {_SCHEMA} AND table_name = %s",
            [table_name],
        )
        rows = sorted(cursor.fetchall(), key=lambda row: row[7])
        if not rows:
            # No columns: make a missing table raise, as reading it would.
            cursor.execute("SELECT * FROM %s LIMIT 1" % self.connection.ops.quote_name(table_name))
        fields = []
        for name, data_type, length, precision, scale, nullable, default, _position in rows:
            is_auto = default is not None and _NEXTVAL.match(str(default)) is not None
            fields.append(
                FieldInfo(
                    name,
                    str(data_type),
                    length,
                    length,
                    precision if data_type == "numeric" else None,
                    scale if data_type == "numeric" else None,
                    str(nullable).upper() in ("YES", "TRUE", "T", "1"),
                    None if is_auto else default,
                    None,
                    is_auto,
                    None,
                )
            )
        return fields

    def get_sequences(
        self, cursor: Any, table_name: str, table_fields: Sequence[Any] = ()
    ) -> List[Dict[str, Any]]:
        """The identity columns of ``table_name`` and the sequences behind them."""
        cursor.execute(
            "SELECT column_name, column_default FROM information_schema.columns "
            f"WHERE table_schema = {_SCHEMA} AND table_name = %s",
            [table_name],
        )
        sequences = []
        for column, default in cursor.fetchall():
            match = _NEXTVAL.match(str(default)) if default is not None else None
            if match is not None:
                sequences.append(
                    {
                        "name": match.group("name").replace("''", "'"),
                        "table": table_name,
                        "column": column,
                    }
                )
        return sequences

    def _key_columns(self, cursor: Any, table_name: Optional[str]) -> Dict[Tuple[str, str], List[str]]:
        """``{(table, constraint): [column, ...]}`` from key_column_usage, in key order."""
        sql = (
            "SELECT table_name, constraint_name, column_name, ordinal_position "
            f"FROM information_schema.key_column_usage WHERE table_schema = {_SCHEMA}"
        )
        params: List[Any] = []
        if table_name is not None:
            sql += " AND table_name = %s"
            params.append(table_name)
        cursor.execute(sql, params)
        rows = sorted(cursor.fetchall(), key=lambda row: (row[0], row[1], row[3]))
        out: Dict[Tuple[str, str], List[str]] = {}
        for table, constraint, column, _position in rows:
            out.setdefault((table, constraint), []).append(column)
        return out

    def _constraint_keys(self, cursor: Any) -> Dict[str, Tuple[str, List[str]]]:
        """``{constraint: (table, [column, ...])}`` for every key in the current schema, to find
        what a foreign key refers to."""
        return {
            constraint: (table, columns)
            for (table, constraint), columns in self._key_columns(cursor, None).items()
        }

    def _foreign_keys(self, cursor: Any) -> List[Tuple[str, str, str, str, str]]:
        """``(constraint, referenced key constraint, key schema, update rule, delete rule)``."""
        cursor.execute(
            "SELECT constraint_name, unique_constraint_name, unique_constraint_schema, "
            "update_rule, delete_rule FROM information_schema.referential_constraints "
            f"WHERE constraint_schema = {_SCHEMA}"
        )
        return [tuple(row) for row in cursor.fetchall()]

    def get_table_objects(self, cursor: Any, table_name: str) -> List[TableObject]:
        """Every constraint and index of ``table_name``.

        An index the server keeps for a constraint (it carries the constraint's name) is not
        listed separately.
        """
        cursor.execute(
            "SELECT constraint_name, constraint_type FROM information_schema.table_constraints "
            f"WHERE table_schema = {_SCHEMA} AND table_name = %s",
            [table_name],
        )
        kinds = {name: str(kind).lower() for name, kind in cursor.fetchall()}
        key_columns = {
            constraint: columns
            for (_table, constraint), columns in self._key_columns(cursor, table_name).items()
        }
        objects: Dict[str, TableObject] = {}
        for name, kind in kinds.items():
            if kind == "check":
                continue
            objects[name] = TableObject(name, kind, list(key_columns.get(name, [])))
        if "check" in kinds.values():
            columns = [info.name for info in self.get_table_description(cursor, table_name)]
            cursor.execute(
                "SELECT constraint_name, check_clause FROM information_schema.check_constraints "
                f"WHERE constraint_schema = {_SCHEMA}"
            )
            for name, clause in cursor.fetchall():
                if kinds.get(name) == "check":
                    obj = TableObject(name, "check", check_clause_columns(str(clause), columns))
                    obj.check_clause = str(clause)
                    objects[name] = obj
        if any(obj.kind == "foreign key" for obj in objects.values()):
            keys = self._constraint_keys(cursor)
            for name, key, _key_schema, on_update, on_delete in self._foreign_keys(cursor):
                obj = objects.get(name)
                if obj is None or obj.kind != "foreign key":
                    continue
                referred = keys.get(key)
                if referred is not None:
                    obj.referred_table, obj.referred_columns = referred[0], list(referred[1])
                obj.on_update = str(on_update).upper() if on_update else None
                obj.on_delete = str(on_delete).upper() if on_delete else None
        cursor.execute(
            "SELECT index_name, non_unique, column_name, seq_in_index "
            "FROM information_schema.statistics "
            f"WHERE table_schema = {_SCHEMA} AND table_name = %s",
            [table_name],
        )
        indexes: Dict[str, List[Tuple[int, str]]] = {}
        unique_index: Dict[str, bool] = {}
        for name, non_unique, column, position in cursor.fetchall():
            if name in kinds:
                continue
            indexes.setdefault(name, []).append((int(position), column))
            unique_index[name] = not bool(int(non_unique))
        for name, columns in indexes.items():
            obj = TableObject(name, "index", [column for _position, column in sorted(columns)])
            obj.unique = unique_index[name]
            objects[name] = obj
        return list(objects.values())

    def get_constraints(self, cursor: Any, table_name: str) -> Dict[str, Dict[str, Any]]:
        constraints: Dict[str, Dict[str, Any]] = {}
        for obj in self.get_table_objects(cursor, table_name):
            foreign_key = None
            if obj.kind == "foreign key" and obj.referred_table and obj.referred_columns:
                foreign_key = (obj.referred_table, obj.referred_columns[0])
            constraints[obj.name] = {
                "columns": obj.columns,
                "primary_key": obj.kind == "primary key",
                "unique": obj.unique,
                "foreign_key": foreign_key,
                "check": obj.kind == "check",
                "index": obj.kind in ("index", "primary key", "unique"),
                "definition": None,
                "options": None,
                "type": Index.suffix if obj.kind == "index" else None,
                "orders": ["ASC"] * len(obj.columns) if obj.kind == "index" else [],
            }
        return constraints

    def get_relations(self, cursor: Any, table_name: str) -> Dict[str, Tuple[str, str]]:
        """``{column: (referred column, referred table)}`` for the foreign keys of the table."""
        relations = {}
        for obj in self.get_table_objects(cursor, table_name):
            if obj.kind == "foreign key" and obj.referred_table:
                for column, referred in zip(obj.columns, obj.referred_columns):
                    relations[column] = (referred, obj.referred_table)
        return relations

    def get_incoming_foreign_keys(
        self, cursor: Any, table_name: str, column: str
    ) -> List[Tuple[str, TableObject]]:
        """``(table, foreign key)`` for each foreign key elsewhere that refers to ``column`` of
        ``table_name``."""
        keys = self._constraint_keys(cursor)
        found = []
        tables = set()
        for name, key, _key_schema, _on_update, _on_delete in self._foreign_keys(cursor):
            referred = keys.get(key)
            if referred is None or referred[0] != table_name or column not in referred[1]:
                continue
            owner = keys.get(name)
            if owner is not None and owner[0] != table_name:
                tables.add(owner[0])
        for table in sorted(tables):
            for obj in self.get_table_objects(cursor, table):
                if (
                    obj.kind == "foreign key"
                    and obj.referred_table == table_name
                    and column in obj.referred_columns
                ):
                    found.append((table, obj))
        return found

    def get_foreign_key_edges(self, cursor: Any) -> List[Tuple[str, str]]:
        """``(referencing table, referenced table)`` for every foreign key in the schema."""
        keys = self._constraint_keys(cursor)
        edges = []
        for name, key, _key_schema, _on_update, _on_delete in self._foreign_keys(cursor):
            owner, referred = keys.get(name), keys.get(key)
            if owner is not None and referred is not None:
                edges.append((owner[0], referred[0]))
        return edges
