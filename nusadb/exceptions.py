"""PEP-249 (DB-API 2.0) exception hierarchy.

The standard tree::

    Exception
     +-- Warning
     +-- Error
          +-- InterfaceError
          +-- DatabaseError
               +-- DataError
               +-- OperationalError
               |    +-- TransactionRolledBack
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

    #: The server's five-character SQLSTATE, or ``None`` for an error raised by the driver itself.
    sqlstate: "str | None" = None


class InterfaceError(Error):
    """An error related to the driver/interface rather than the database."""


class DatabaseError(Error):
    """An error related to the database."""


class DataError(DatabaseError):
    """A problem with the processed data (out of range, division by zero, ...)."""


class OperationalError(DatabaseError):
    """An operational problem (disconnect, transport failure, auth)."""


class TransactionRolledBack(OperationalError):
    """``COMMIT`` found the transaction failed (a statement in it raised an error), so the server
    rolled it back: none of its work was saved."""


class IntegrityError(DatabaseError):
    """A relational-integrity violation (constraint, foreign key)."""


class InternalError(DatabaseError):
    """An internal database error (invalid cursor, transaction out of sync)."""


class ProgrammingError(DatabaseError):
    """A programming error (bad SQL, wrong parameter count, unknown table)."""


class NotSupportedError(DatabaseError):
    """A method or API the database does not support was used."""


def from_sqlstate(code: str, message: str) -> DatabaseError:
    """Map a server SQLSTATE (``docs/wire-protocol.md`` §14) to the closest PEP-249 exception.

    The mapping reads the two-character class, not the whole code, so a code the server refines
    within a class still maps the same way. An unknown class maps to :class:`ProgrammingError`.
    The five-character code itself is kept on the exception as ``sqlstate``.
    """
    mapping = {
        "23": IntegrityError,
        "22": DataError,
        "42": ProgrammingError,
        "08": OperationalError,
        "28": OperationalError,
        "0A": NotSupportedError,
        "XX": InternalError,
        # Transaction state, resources or an operator: retrying after a rollback or a backoff is
        # the way through, which is what OperationalError tells a caller.
        "25": OperationalError,
        "40": OperationalError,
        "53": OperationalError,
        "57": OperationalError,
        "58": OperationalError,
    }
    err = mapping.get(code[:2], ProgrammingError)(f"{code}: {message}")
    err.sqlstate = code
    return err
