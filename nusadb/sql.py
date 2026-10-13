"""Building SQL text safely from pieces.

Parameters are the right way to pass values, but they cannot stand for a table or column name,
and some statements take no parameters at all. This module builds such SQL without pasting
strings together::

    from nusadb import sql

    query = sql.SQL("SELECT {fields} FROM {table} WHERE {key} = {value}").format(
        fields=sql.SQL(", ").join(sql.Identifier(name) for name in ["id", "name"]),
        table=sql.Identifier("app", "users"),
        key=sql.Identifier("id"),
        value=sql.Placeholder(),
    )
    cur.execute(query, [42])

:class:`SQL` is trusted text, written by the program. :class:`Identifier` quotes a name, and
:class:`Literal` writes a value as a quoted SQL literal, so neither can change the statement around
it whatever it holds. :class:`Placeholder` writes a ``%s`` or ``%(name)s`` marker for a value
passed with the statement. Every cursor and connection method that takes SQL text also takes one
of these objects.
"""

from __future__ import annotations

import re
import string
from typing import Any, Iterable, Iterator, List, Optional, Sequence, Union

from .exceptions import ProgrammingError

__all__ = ["Composable", "Composed", "SQL", "Identifier", "Literal", "Placeholder"]


class Composable:
    """A piece of SQL. :meth:`as_string` renders it; ``+`` joins two pieces into a
    :class:`Composed`, and ``* n`` repeats one."""

    def __init__(self, wrapped: Any) -> None:
        self._wrapped = wrapped

    def as_string(self, context: Any = None) -> str:
        """The SQL text. ``context`` is the connection (or cursor) the text is for; the rendering
        does not depend on it today, so ``None`` works as well."""
        raise NotImplementedError

    def __add__(self, other: Any) -> "Composed":
        if isinstance(other, Composed):
            return Composed([self, *other._wrapped])
        if isinstance(other, Composable):
            return Composed([self, other])
        return NotImplemented

    def __mul__(self, n: int) -> "Composed":
        return Composed([self] * n)

    def __eq__(self, other: object) -> bool:
        if type(self) is not type(other):
            return False
        return bool(self._wrapped == other._wrapped)  # type: ignore[attr-defined]

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __hash__(self) -> int:
        return hash((type(self), repr(self._wrapped)))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._wrapped!r})"


class Composed(Composable):
    """A sequence of pieces, rendered one after the other. :class:`SQL.format`, :meth:`SQL.join`
    and ``+`` build one."""

    def __init__(self, seq: Iterable[Composable]) -> None:
        items: List[Composable] = []
        for item in seq:
            if not isinstance(item, Composable):
                raise TypeError(f"a Composed holds Composable objects, not {type(item).__name__}")
            items.append(item)
        super().__init__(items)

    @property
    def seq(self) -> List[Composable]:
        """The pieces, in order."""
        return list(self._wrapped)

    def as_string(self, context: Any = None) -> str:
        return "".join(item.as_string(context) for item in self._wrapped)

    def __iter__(self) -> Iterator[Composable]:
        return iter(self._wrapped)

    def __add__(self, other: Any) -> "Composed":
        if isinstance(other, Composed):
            return Composed([*self._wrapped, *other._wrapped])
        if isinstance(other, Composable):
            return Composed([*self._wrapped, other])
        return NotImplemented

    def join(self, joiner: Union[str, "SQL"]) -> "Composed":
        """The pieces with ``joiner`` (a ``str`` or :class:`SQL`) between each two."""
        if isinstance(joiner, str):
            joiner = SQL(joiner)
        elif not isinstance(joiner, SQL):
            raise TypeError(f"join takes a str or an SQL, not {type(joiner).__name__}")
        return joiner.join(self._wrapped)


class SQL(Composable):
    """SQL text written by the program, rendered as it is. Never build one from outside input:
    use :class:`Identifier` for names and :class:`Literal` or parameters for values.

    :meth:`format` fills ``{}``, ``{0}`` and ``{name}`` fields with other pieces (``{{`` and ``}}``
    are literal braces); :meth:`join` puts this text between pieces."""

    def __init__(self, text: str) -> None:
        if not isinstance(text, str):
            raise TypeError(f"SQL takes a str, not {type(text).__name__}")
        super().__init__(text)

    @property
    def string(self) -> str:
        """The text."""
        return self._wrapped

    def as_string(self, context: Any = None) -> str:
        return self._wrapped

    def format(self, *args: Composable, **kwargs: Composable) -> Composed:
        """This text with each field replaced by a piece: ``{}`` takes the arguments in order,
        ``{0}`` by position and ``{name}`` by keyword. Every argument must be a
        :class:`Composable`; a field may not carry a conversion or a format spec, and automatic
        and numbered fields cannot be mixed."""
        out: List[Composable] = []
        auto: Optional[int] = None
        for literal, field, spec, conversion in string.Formatter().parse(self._wrapped):
            if literal:
                out.append(SQL(literal))
            if field is None:
                continue
            if spec or conversion:
                raise ValueError(f"a field takes no format spec or conversion: {{{field}}}")
            if field == "":
                if auto is None:
                    auto = 0
                elif auto < 0:
                    raise ValueError("cannot mix {} and numbered fields")
                if auto >= len(args):
                    raise IndexError(f"no argument for field {{}} number {auto + 1}")
                value: Any = args[auto]
                auto += 1
            elif field.isdigit():
                if auto is not None and auto > 0:
                    raise ValueError("cannot mix {} and numbered fields")
                auto = -1
                index = int(field)
                if index >= len(args):
                    raise IndexError(f"no argument for field {{{field}}}")
                value = args[index]
            elif field.isidentifier():
                if field not in kwargs:
                    raise KeyError(field)
                value = kwargs[field]
            else:
                raise ValueError(f"a field is empty, a number or a name, not {{{field}}}")
            if not isinstance(value, Composable):
                raise TypeError(
                    f"format takes Composable arguments (SQL, Identifier, Literal, ...), "
                    f"not {type(value).__name__}"
                )
            out.append(value)
        return Composed(out)

    def join(self, seq: Iterable[Composable]) -> Composed:
        """The pieces of ``seq`` with this text between each two."""
        out: List[Composable] = []
        for i, item in enumerate(seq):
            if i:
                out.append(self)
            out.append(item)
        return Composed(out)


class Identifier(Composable):
    """A name (a table, a column, a schema), quoted so it is read as exactly that name:
    ``Identifier("users")`` is ``"users"``, ``Identifier("app", "users")`` is ``"app"."users"``.
    A double quote inside a name is doubled. An empty name or one holding a NUL character is
    refused."""

    def __init__(self, *strings: str) -> None:
        if not strings:
            raise TypeError("Identifier takes at least one name")
        for part in strings:
            if not isinstance(part, str):
                raise TypeError(f"an identifier is a str, not {type(part).__name__}")
            if not part:
                raise ProgrammingError("an identifier cannot be empty")
            if "\x00" in part:
                raise ProgrammingError("an identifier cannot hold a NUL character")
        super().__init__(strings)

    @property
    def strings(self) -> Sequence[str]:
        """The name's parts."""
        return self._wrapped

    def as_string(self, context: Any = None) -> str:
        return ".".join('"' + part.replace('"', '""') + '"' for part in self._wrapped)

    def __repr__(self) -> str:
        return f"Identifier({', '.join(map(repr, self._wrapped))})"


# The cast written after a quoted literal, by the type the value binds as.
_CASTS = {
    "FLOAT": "FLOAT",
    "NUMERIC": "NUMERIC",
    "BYTES": "BYTEA",
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMETZ": "TIMETZ",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMPTZ",
    "INTERVAL": "INTERVAL",
    "UUID": "UUID",
    "JSON": "JSON",
    "INET": "INET",
    "CIDR": "CIDR",
    "VECTOR": "VECTOR",
}


class Literal(Composable):
    """A value written into the SQL as a literal, the way it would bind as a parameter.

    ``None`` is ``NULL``, a ``bool`` ``true`` or ``false`` and an ``int`` its digits (in
    parentheses when negative). A ``str`` is a quoted string. Other values are their parameter
    text as a quoted string with a cast to the type they bind as (``'2024-01-01'::DATE``,
    ``'1.5'::NUMERIC``); a list is an array literal and a type the driver does not know is its
    quoted ``str()``, both left for the server to type where they are used. Adapters from
    :func:`nusadb.register_adapter` apply. A quote is doubled, and text with a backslash is
    written as an escape string (``E'...'``) with the backslash doubled, so the literal means the
    same whatever the server's string settings. A NUL character is refused.
    """

    @property
    def wrapped(self) -> Any:
        """The value."""
        return self._wrapped

    def as_string(self, context: Any = None) -> str:
        from . import _protocol as proto
        from .connection import _adapt, _encode_param, _param_tag, _scoped_adapters

        connection = getattr(context, "_conn", context)  # a cursor's connection, or the connection
        with _scoped_adapters(getattr(connection, "_adapters", None) or {}):
            value = _adapt(self._wrapped)
            if value is None:
                return "NULL"
            if isinstance(value, bool):
                return "true" if value else "false"
            if isinstance(value, int) and _param_tag(value) == proto.TAG_BYTES["INT"]:
                text = int.__repr__(value)
                return f"({text})" if value < 0 else text
            encoded = _encode_param(value)
            tag = _param_tag(value)
        text = "" if encoded is None else encoded.decode("utf-8")
        quoted = quote_string(text)
        if isinstance(value, str):
            return quoted
        cast = _CASTS.get(proto.TYPE_TAGS.get(tag, ""))
        return f"{quoted}::{cast}" if cast else quoted


def quote_string(text: str) -> str:
    """``text`` as a SQL string literal: ``'...'`` with each quote doubled, or ``E'...'`` with
    backslashes doubled too when it holds one. A NUL character is refused."""
    if "\x00" in text:
        raise ProgrammingError("a string literal cannot hold a NUL character")
    body = text.replace("'", "''")
    if "\\" in text:
        return "E'" + body.replace("\\", "\\\\") + "'"
    return "'" + body + "'"


_NAME = re.compile(r"\w+")


class Placeholder(Composable):
    """A parameter marker: ``%s`` (by position), or ``%(name)s`` with a ``name`` (by name from a
    mapping of parameters). The values go with the statement, as with any parameter."""

    def __init__(self, name: Optional[str] = None) -> None:
        if name is not None:
            if not isinstance(name, str) or not _NAME.fullmatch(name):
                raise ProgrammingError(
                    f"a placeholder name is letters, digits and underscores, not {name!r}"
                )
        super().__init__(name)

    @property
    def name(self) -> Optional[str]:
        return self._wrapped

    def as_string(self, context: Any = None) -> str:
        return "%s" if self._wrapped is None else f"%({self._wrapped})s"

    def __repr__(self) -> str:
        return "Placeholder()" if self._wrapped is None else f"Placeholder({self._wrapped!r})"
