"""PEP-249 (DB-API 2.0) exception hierarchy.

The standard tree::

    Exception
     +-- Warning
     +-- Error
          +-- InterfaceError
          +-- DatabaseError
               +-- DataError
               +-- OperationalError
               +-- IntegrityError
               +-- InternalError
               +-- ProgrammingError
               +-- NotSupportedError
"""

from __future__ import annotations


class Warning(Exception):  # noqa: N818 - name mandated by PEP-249
    """Important warnings such as data truncations."""


class Error(Exception):  # noqa: N818 - name mandated by PEP-249
    """Base class of all driver errors."""


class InterfaceError(Error):
    """An error related to the driver/interface rather than the database."""


class DatabaseError(Error):
    """An error related to the database."""


class DataError(DatabaseError):
    """A problem with the processed data (out of range, division by zero, ...)."""


class OperationalError(DatabaseError):
    """An operational problem (disconnect, transport failure, auth)."""


class IntegrityError(DatabaseError):
    """A relational-integrity violation (constraint, foreign key)."""


class InternalError(DatabaseError):
    """An internal database error (invalid cursor, transaction out of sync)."""


class ProgrammingError(DatabaseError):
    """A programming error (bad SQL, wrong parameter count, unknown table)."""


class NotSupportedError(DatabaseError):
    """A method or API the database does not support was used."""


def from_sqlstate(code: str, message: str) -> DatabaseError:
    """Map a server SQLSTATE (``docs/wire-protocol.md`` §14) to the closest
    PEP-249 exception. The current server emits ``XX000`` for most errors, so the
    default is :class:`ProgrammingError` (the typical cause from a client).
    """
    cls_class = code[:2]
    mapping = {
        "23": IntegrityError,
        "22": DataError,
        "42": ProgrammingError,
        "08": OperationalError,
        "28": OperationalError,
        "0A": NotSupportedError,
    }
    exc = mapping.get(cls_class, ProgrammingError)
    return exc(f"{code}: {message}")
