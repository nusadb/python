"""Python values for the server's range, network, vector and composite types.

A range column decodes to :class:`Range`, ``INET`` to an :mod:`ipaddress` interface (or address,
for a single host), ``CIDR`` to an :mod:`ipaddress` network and ``VECTOR`` to a list of floats. Each
binds back as a parameter: a :class:`Range` or an :mod:`ipaddress` value directly, a vector through
:class:`Vector` (a plain list binds as an array). A composite value arrives as its text form, which
:func:`parse_record` splits into its fields.
"""

from __future__ import annotations

import datetime as _dt
import ipaddress
import re
from decimal import Decimal
from typing import Any, Iterable, List, Optional, Tuple, Union

__all__ = ["Range", "Vector", "parse_record"]

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
