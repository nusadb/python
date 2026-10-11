"""Test databases: each test run gets its own database, made with ``CREATE DATABASE``.

``NAME`` (or ``TEST["NAME"]``, default ``test_`` + ``NAME``) is created before the tests and
dropped after them. The statements run on a connection to the server's default database,
``nusadb``, as ``USER``, which must be allowed to create databases (the superuser, or a role
with ``CREATEDB``). With ``--keepdb`` an existing test database is reused.
"""

from __future__ import annotations

import sys
from typing import Any, Dict

from django.db.backends.base.creation import BaseDatabaseCreation

import nusadb

# SQLSTATE of CREATE DATABASE for a name that is taken.
_DUPLICATE_DATABASE = "42P04"
# SQLSTATE of a statement the server does not support.
_FEATURE_NOT_SUPPORTED = "0A000"


class DatabaseCreation(BaseDatabaseCreation):
    """Creates and drops the test database."""

    def sql_table_creation_suffix(self) -> str:
        return ""

    def _execute_create_test_db(
        self, cursor: Any, parameters: Dict[str, Any], keepdb: bool = False
    ) -> None:
        try:
            cursor.execute("CREATE DATABASE %(dbname)s" % parameters)
        except Exception as exc:
            cause = exc.__cause__ if isinstance(exc.__cause__, nusadb.Error) else exc
            sqlstate = getattr(cause, "sqlstate", None)
            if sqlstate == _DUPLICATE_DATABASE:
                if keepdb:
                    return
                raise
            if sqlstate == _FEATURE_NOT_SUPPORTED:
                self.log(
                    "This NusaDB server cannot create databases (CREATE DATABASE is not "
                    "supported). The test database needs a server that supports it."
                )
                sys.exit(2)
            raise
