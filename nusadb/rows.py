"""Row factories: pass one as ``row_factory`` to :meth:`Connection.cursor`,
:meth:`Connection.stream`, or set it on a connection or cursor.

A row factory is called with the cursor once per result and returns the function that builds
each row from its values (a tuple)::

    cur = conn.cursor(row_factory=nusadb.rows.dict_row)
    cur.execute("SELECT id, name FROM users")
    cur.fetchone()  # {'id': 1, 'name': 'ann'}
"""

from __future__ import annotations

from collections import namedtuple
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Mapping, Optional, Tuple

if TYPE_CHECKING:
    from .connection import Cursor
    from .types import CompositeInfo

RowFactory = Callable[["Cursor"], Callable[[Tuple[Any, ...]], Any]]


def _names(cursor: "Cursor") -> List[str]:
    return [column[0] for column in cursor.description or []]


def tuple_row(cursor: "Cursor") -> Callable[[Tuple[Any, ...]], Tuple[Any, ...]]:
    """Rows as tuples (the default)."""
    return lambda values: values


def dict_row(cursor: "Cursor") -> Callable[[Tuple[Any, ...]], Dict[str, Any]]:
    """Rows as ``{column: value}`` dicts. A repeated column name keeps its last value."""
    names = _names(cursor)
    return lambda values: dict(zip(names, values))


def namedtuple_row(cursor: "Cursor") -> Callable[[Tuple[Any, ...]], Any]:
    """Rows as named tuples. A column name that is not a valid field name (or repeats) becomes
    ``_<position>``."""
    row_type = namedtuple("Row", _names(cursor), rename=True)  # type: ignore[misc]
    return lambda values: row_type(*values)


def composite_row(
    columns: Mapping[str, "CompositeInfo"], row_factory: Optional[RowFactory] = None
) -> RowFactory:
    """A row factory that loads composite columns: each column named in ``columns`` is turned
    from its record text into an object by its :class:`~nusadb.types.CompositeInfo`, then the
    row is built by ``row_factory`` (tuples by default). A named column missing from a result is
    left out::

        cur = conn.cursor(row_factory=composite_row({"home": address_info}, dict_row))
        cur.execute("SELECT id, home FROM people")
        cur.fetchone()  # {'id': 1, 'home': address(street='Main', number=5)}
    """
    inner = row_factory or tuple_row

    def factory(cursor: "Cursor") -> Callable[[Tuple[Any, ...]], Any]:
        loads = [(i, columns[name]) for i, name in enumerate(_names(cursor)) if name in columns]
        build = inner(cursor)
        if not loads:
            return build

        def make(values: Tuple[Any, ...]) -> Any:
            loaded = list(values)
            for i, info in loads:
                loaded[i] = info.load(loaded[i])
            return build(tuple(loaded))

        return make

    return factory
