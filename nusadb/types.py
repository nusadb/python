"""Python values for the server's range, network, vector and composite types.

A range column decodes to :class:`Range`, ``INET`` to an :mod:`ipaddress` interface (or address,
for a single host), ``CIDR`` to an :mod:`ipaddress` network and ``VECTOR`` to a list of floats. Each
binds back as a parameter: a :class:`Range` or an :mod:`ipaddress` value directly, a vector through
:class:`Vector` (a plain list binds as an array). A composite value arrives as its text form, which
:func:`parse_record` splits into its fields; :class:`CompositeInfo` turns it into a Python object
with typed fields, and :func:`register_composite` binds such objects back as records.
"""

from __future__ import annotations

import datetime as _dt
import ipaddress
import re
from decimal import Decimal
from collections import namedtuple
from typing import TYPE_CHECKING, Any, Callable, Iterable, List, Optional, Sequence, Tuple, Union

from .exceptions import DataError, ProgrammingError

if TYPE_CHECKING:  # pragma: no cover
    from .aio import AsyncConnection
    from .connection import Connection

__all__ = ["Range", "Vector", "parse_record", "CompositeInfo", "register_composite"]

_BOUNDS = ("[)", "(]", "()", "[]")


class Range:
    """A range value: ``lower`` and ``upper`` bounds (``None`` is unbounded on that side), which of
    them are included (``bounds``, one of ``"[)"``, ``"(]"``, ``"()"``, ``"[]"``), or ``empty``.

    ``x in r`` tests membership; two ranges compare equal when their bounds, inclusion and emptiness
    match (the server returns discrete ranges in their ``[)`` form, so compare with that form).
    """

    __slots__ = ("lower", "upper", "bounds", "empty")

    def __init__(
        self,
        lower: Any = None,
        upper: Any = None,
        bounds: str = "[)",
        empty: bool = False,
    ) -> None:
        if bounds not in _BOUNDS:
            raise ValueError(f"bounds must be one of {', '.join(_BOUNDS)} (got {bounds!r})")
        self.lower = None if empty else lower
        self.upper = None if empty else upper
        self.bounds = "()" if empty else bounds
        self.empty = empty

    @property
    def isempty(self) -> bool:
        return self.empty

    @property
    def lower_inf(self) -> bool:
        return not self.empty and self.lower is None

    @property
    def upper_inf(self) -> bool:
        return not self.empty and self.upper is None

    @property
    def lower_inc(self) -> bool:
        return not self.empty and self.lower is not None and self.bounds[0] == "["

    @property
    def upper_inc(self) -> bool:
        return not self.empty and self.upper is not None and self.bounds[1] == "]"

    def __contains__(self, value: Any) -> bool:
        if self.empty:
            return False
        if self.lower is not None:
            if value < self.lower or (value == self.lower and not self.lower_inc):
                return False
        if self.upper is not None:
            if value > self.upper or (value == self.upper and not self.upper_inc):
                return False
        return True

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Range):
            return NotImplemented
        if self.empty or other.empty:
            return self.empty == other.empty
        return (self.lower, self.upper, self.bounds) == (other.lower, other.upper, other.bounds)

    def __hash__(self) -> int:
        return hash((self.lower, self.upper, self.bounds, self.empty))

    def __bool__(self) -> bool:
        return not self.empty

    def __repr__(self) -> str:
        if self.empty:
            return "Range(empty=True)"
        return f"Range({self.lower!r}, {self.upper!r}, {self.bounds!r})"

    def __str__(self) -> str:
        """The range's text form, which the server reads back (``[1,10)``, ``empty``)."""
        if self.empty:
            return "empty"
        lower = "" if self.lower is None else _bound_text(self.lower)
        upper = "" if self.upper is None else _bound_text(self.upper)
        return f"{self.bounds[0]}{lower},{upper}{self.bounds[1]}"


def _bound_text(value: Any) -> str:
    text = value.isoformat(sep=" ") if isinstance(value, _dt.datetime) else str(value)
    # A bound with a separator or a quote in it is written quoted, quotes doubled.
    if any(c in text for c in ' ,()[]"\\') or text == "":
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


class Vector:
    """Bind ``values`` as a ``VECTOR`` (a plain ``list`` binds as an array)."""

    __slots__ = ("values",)

    def __init__(self, values: Iterable[float]) -> None:
        self.values = [float(v) for v in values]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Vector):
            return self.values == other.values
        return NotImplemented

    def __hash__(self) -> int:
        return hash(tuple(self.values))

    def __repr__(self) -> str:
        return f"Vector({self.values!r})"

    def __str__(self) -> str:
        return "[" + ",".join(repr(v) for v in self.values) + "]"


_INT = re.compile(r"[+-]?\d+")
_DECIMAL = re.compile(r"[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?")
_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
_OFFSET = re.compile(r"(?<=\d)([+-]\d{2}):?(\d{2})?$")
_FRACTION = re.compile(r"\.(\d{1,6})(?=[+-]|$)")
_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?([+-]\d{2}(:?\d{2})?|Z)?")


def _decode_bound(text: str) -> Any:
    """A range bound at the type its text has: an integer, a decimal, a date or a timestamp; any
    other text stays a string."""
    if _INT.fullmatch(text):
        return int(text)
    if _DECIMAL.fullmatch(text):
        return Decimal(text)
    if _DATE.fullmatch(text):
        return _dt.date.fromisoformat(text)
    if _TIMESTAMP.fullmatch(text):
        # An offset written `+05` or `+0530` is read as `+05:00` / `+05:30`, the form every
        # supported Python's `fromisoformat` accepts.
        normalized = _OFFSET.sub(
            lambda m: f"{m.group(1)}:{m.group(2) or '00'}", text.replace("Z", "+00:00")
        )
        # Fractional seconds padded to six digits: `fromisoformat` before Python 3.11 reads only
        # three or six.
        normalized = _FRACTION.sub(lambda m: "." + m.group(1).ljust(6, "0"), normalized)
        return _dt.datetime.fromisoformat(normalized)
    return text


def _split_quoted(body: str) -> List[Optional[str]]:
    """Split the inside of a ``(a,b)`` or range text at its top-level commas, unquoting quoted
    fields (``""`` and ``\\"`` escape a quote). An empty, unquoted field is ``None``."""
    fields: List[Optional[str]] = []
    current: List[str] = []
    quoted = False
    in_quotes = False
    i = 0
    while i < len(body):
        c = body[i]
        if in_quotes:
            if c == "\\" and i + 1 < len(body):
                current.append(body[i + 1])
                i += 2
                continue
            if c == '"':
                if i + 1 < len(body) and body[i + 1] == '"':
                    current.append('"')
                    i += 2
                    continue
                in_quotes = False
            else:
                current.append(c)
        elif c == '"':
            in_quotes = quoted = True
        elif c == ",":
            fields.append("".join(current) if current or quoted else None)
            current, quoted = [], False
        else:
            current.append(c)
        i += 1
    fields.append("".join(current) if current or quoted else None)
    return fields


def decode_range(text: str) -> Union[Range, str]:
    """A range's text (``[1,10)``, ``(,5]``, ``empty``) as a :class:`Range`."""
    if text.strip().lower() == "empty":
        return Range(empty=True)
    if len(text) < 3 or text[0] not in "[(" or text[-1] not in ")]":
        return text
    parts = _split_quoted(text[1:-1])
    if len(parts) != 2:
        return text
    lower, upper = (None if p is None else _decode_bound(p) for p in parts)
    return Range(lower, upper, text[0] + text[-1])


def decode_inet(text: str) -> Any:
    """``INET`` text as an :mod:`ipaddress` interface (``192.168.1.5/24``), or an address when it
    names a single host (``10.0.0.1``)."""
    if "/" in text:
        address, _, length = text.partition("/")
        max_length = 32 if ":" not in address else 128
        if int(length) == max_length:
            return ipaddress.ip_address(address)
        return ipaddress.ip_interface(text)
    return ipaddress.ip_address(text)


def decode_cidr(text: str) -> Any:
    """``CIDR`` text as an :mod:`ipaddress` network."""
    return ipaddress.ip_network(text)


def decode_vector(text: str) -> List[float]:
    """``VECTOR`` text (``[1,2,3]``) as a list of floats."""
    body = text.strip()
    if not (body.startswith("[") and body.endswith("]")):
        raise ValueError(f"not a vector: {text!r}")
    inner = body[1:-1].strip()
    return [float(item) for item in inner.split(",")] if inner else []


def parse_record(text: str) -> Tuple[Optional[str], ...]:
    """Split a composite (row) value's text, ``(1,"a b",)``, into its fields as strings, ``None``
    for a NULL field: ``("1", "a b", None)``. Convert each field to the type it has."""
    body = text.strip()
    if not (body.startswith("(") and body.endswith(")")):
        raise ValueError(f"not a record: {text!r}")
    return tuple(_split_quoted(body[1:-1]))


# The catalog of composite types: one row per type, its fields as alternating name and type text
# joined by the unit separator. Only a superuser may read it.
_COMPOSITE_QUERY = "SELECT def FROM nusadb_composites WHERE name = $1"
_FIELD_SEPARATOR = "\x1f"
_RECORD_SPECIALS = frozenset('(),"\\')


class CompositeInfo:
    """The fields of a composite type (``CREATE TYPE name AS (field type, ...)``), and how to turn
    its values into Python objects and back.

    The server reports a composite column as ``TEXT``, so its values arrive as record text such as
    ``(1,"a b",)``. :meth:`load` converts that text into an instance of :attr:`python_type` (a
    named tuple by default) with each field decoded at its type; :meth:`dump` writes an instance
    back as record text. :func:`register_composite` makes instances bind as that text.

    Build one with :meth:`fetch`, which reads the fields from the server's catalog (a superuser
    connection), or directly from the field names and SQL types::

        info = CompositeInfo("address", [("street", "TEXT"), ("number", "INT")])
    """

    def __init__(
        self,
        name: str,
        fields: Sequence[Tuple[str, str]],
        factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        if not fields:
            raise ProgrammingError(f"composite type {name!r} has no fields")
        self.name = name
        #: ``(field name, SQL type)`` pairs, in declared order.
        self.fields: List[Tuple[str, str]] = [(str(f), str(t)) for f, t in fields]
        self._wires = [_field_wire(t) for _f, t in self.fields]
        #: What :meth:`load` builds: ``factory`` if given, else a named tuple of the fields.
        self.python_type: Callable[..., Any] = factory or namedtuple(  # type: ignore[assignment]
            _class_name(name), self.field_names, rename=True
        )

    @property
    def field_names(self) -> List[str]:
        return [field for field, _type in self.fields]

    @property
    def field_types(self) -> List[str]:
        return [sql_type for _field, sql_type in self.fields]

    @classmethod
    def fetch(cls, conn: "Connection", name: str) -> Optional["CompositeInfo"]:
        """Read the fields of composite type ``name`` from the server, or ``None`` when there is
        no such type. The catalog is readable by a superuser only; another role gets a
        :class:`ProgrammingError` and builds the info from the field list instead."""
        cur = conn.cursor(binary=False)
        try:
            for candidate in _candidates(name):
                cur.execute(_COMPOSITE_QUERY, [candidate])
                row = cur.fetchone()
                if row is not None:
                    return cls(candidate, _split_definition(candidate, row[0]))
        except ProgrammingError as exc:
            return _catalog_error(exc, name)
        finally:
            cur.close()
        return None

    @classmethod
    async def fetch_async(cls, conn: "AsyncConnection", name: str) -> Optional["CompositeInfo"]:
        """:meth:`fetch` on an asyncio connection."""
        try:
            for candidate in _candidates(name):
                cur = await conn.execute(_COMPOSITE_QUERY, [candidate], binary=False)
                row = await cur.fetchone()
                if row is not None:
                    return cls(candidate, _split_definition(candidate, row[0]))
        except ProgrammingError as exc:
            return _catalog_error(exc, name)
        return None

    def load(self, text: Optional[str]) -> Any:
        """The record text ``text`` as a :attr:`python_type` instance, each field converted to the
        Python type its SQL type decodes to (``None`` stays ``None``)."""
        if text is None:
            return None
        if not isinstance(text, str):
            raise DataError(f"a {self.name} value is record text, not {type(text).__name__}")
        try:
            parts = parse_record(text)
        except ValueError as exc:
            raise DataError(str(exc)) from None
        if len(parts) != len(self.fields):
            raise DataError(
                f"a {self.name} value has {len(self.fields)} fields, got {len(parts)}: {text!r}"
            )
        from .connection import Cursor

        values = [
            None if part is None else Cursor._decode_cell(part.encode("utf-8"), wire)
            for part, wire in zip(parts, self._wires)
        ]
        return self.python_type(*values)

    def dump(self, value: Any) -> str:
        """``value`` (a tuple of the fields in order, or an object with an attribute per field)
        as record text, ``(f1,f2,...)``, each field written as it binds as a parameter."""
        if isinstance(value, tuple):
            items = tuple(value)
        else:
            try:
                items = tuple(getattr(value, field) for field in self.field_names)
            except AttributeError as exc:
                raise ProgrammingError(f"cannot write {value!r} as a {self.name}: {exc}") from None
        if len(items) != len(self.fields):
            raise ProgrammingError(
                f"a {self.name} has {len(self.fields)} fields, got {len(items)}"
            )
        return "(" + ",".join(_record_field(item) for item in items) + ")"

    def __repr__(self) -> str:
        return f"CompositeInfo({self.name!r}, {self.fields!r})"


def register_composite(
    info: CompositeInfo,
    conn: Union["Connection", "AsyncConnection", None] = None,
    factory: Optional[Callable[..., Any]] = None,
) -> CompositeInfo:
    """Make instances of ``info.python_type`` bind as ``info``'s record text, so they can be
    passed as parameters (into a composite column, or compared with ``$1::typename``).

    With ``factory`` (a class whose constructor takes the fields in order, such as a named tuple
    or a dataclass) it becomes ``info.python_type``, both what :meth:`CompositeInfo.load` builds and
    what binds. With a connection the binding applies to that connection only; with ``None``, to
    every connection. Returns ``info``."""
    if factory is not None:
        info.python_type = factory
    klass = info.python_type
    if not isinstance(klass, type):
        raise ProgrammingError("register_composite needs a class to bind; pass factory=")
    if conn is None:
        from .connection import register_adapter

        register_adapter(klass, info.dump)
    else:
        conn._adapters[klass] = info.dump
    return info


def _candidates(name: str) -> List[str]:
    return [name] if name == name.lower() else [name, name.lower()]


def _split_definition(name: str, definition: Optional[str]) -> List[Tuple[str, str]]:
    parts = (definition or "").split(_FIELD_SEPARATOR)
    if len(parts) < 2 or len(parts) % 2:
        raise DataError(f"could not read the fields of composite type {name!r}")
    return list(zip(parts[0::2], parts[1::2]))


def _catalog_error(exc: ProgrammingError, name: str) -> None:
    state = getattr(exc, "sqlstate", None)
    if state == "42P01":
        return None  # no composite type has been created yet
    if state == "42501":
        raise ProgrammingError(
            f"reading the fields of {name!r} needs a superuser; build "
            f"CompositeInfo({name!r}, [(field, type), ...]) instead"
        ) from exc
    raise exc


def _field_wire(sql_type: str) -> str:
    from ._binary import wire_type

    try:
        wire = wire_type(sql_type)
    except ProgrammingError:
        return "TEXT"  # an enum, a domain: kept as text
    return "RANGE" if wire.startswith("RANGE") else wire


def _class_name(name: str) -> str:
    cleaned = re.sub(r"\W", "_", name.rsplit(".", 1)[-1])
    return cleaned if cleaned.isidentifier() else "Record"


def _record_field(value: Any) -> str:
    """One field of record text: empty for NULL, quoted when it holds a separator, a quote, a
    backslash or space, or is empty."""
    from .connection import _adapt, _encode_param

    value = _adapt(value)
    if value is None:
        return ""
    encoded = _encode_param(value)
    text = "" if encoded is None else encoded.decode("utf-8")
    if "\x00" in text:
        raise ProgrammingError("a record field cannot hold a NUL character")
    if text == "" or any(c in _RECORD_SPECIALS or c.isspace() for c in text):
        return '"' + text.replace("\\", "\\\\").replace('"', '""') + '"'
    return text
