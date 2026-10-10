"""PEP-249 type objects and constructors.

A column's ``type_code`` (``cursor.description[i][1]``) is the NusaDB type name the server reports.
Each type object compares equal to every type name in its group, so
``description[i][1] == nusadb.NUMBER`` works as PEP-249 describes.
"""

from __future__ import annotations

import datetime as _dt
import time as _time
from typing import FrozenSet


class DBAPITypeObject:
    """Equal to any of a group of type names."""

    __slots__ = ("names",)

    def __init__(self, *names: str) -> None:
        self.names: FrozenSet[str] = frozenset(names)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return other in self.names or (
                other.startswith("ARRAY") and "ARRAY" in self.names
            )
        if isinstance(other, DBAPITypeObject):
            return self.names == other.names
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        return result if result is NotImplemented else not result

    def __hash__(self) -> int:
        return hash(self.names)

    def __repr__(self) -> str:
        return f"DBAPITypeObject{tuple(sorted(self.names))}"


STRING = DBAPITypeObject("TEXT", "UUID", "JSON", "ARRAY", "VECTOR", "UNKNOWN")
BINARY = DBAPITypeObject("BYTES")
NUMBER = DBAPITypeObject("INT", "FLOAT", "NUMERIC", "BOOL")
DATETIME = DBAPITypeObject("DATE", "TIME", "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL")
# The server has no row-id column type.
ROWID = DBAPITypeObject()

Date = _dt.date
Time = _dt.time
Timestamp = _dt.datetime


def DateFromTicks(ticks: float) -> _dt.date:  # noqa: N802 - name mandated by PEP-249
    return Date(*_time.localtime(ticks)[:3])


def TimeFromTicks(ticks: float) -> _dt.time:  # noqa: N802 - name mandated by PEP-249
    return Time(*_time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks: float) -> _dt.datetime:  # noqa: N802 - name mandated by PEP-249
    return Timestamp(*_time.localtime(ticks)[:6])


def Binary(data: bytes) -> bytes:  # noqa: N802 - name mandated by PEP-249
    return bytes(data)
