"""``manage.py dbshell``: runs ``nusadb-cli``, the interactive shell shipped with the server."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    """Builds the ``nusadb-cli`` command line from the connection settings.

    The password is passed in the ``NUSADB_PASSWORD`` environment variable, never on the command
    line. ``nusadb-cli`` has no flags for a client certificate or the timeouts, so a connection
    whose ``OPTIONS`` set ``ssl`` cannot be opened this way.
    """

    executable_name = "nusadb-cli"

    @classmethod
    def settings_to_cmd_args_env(
        cls, settings_dict: Dict[str, Any], parameters: Sequence[str]
    ) -> Tuple[List[str], Optional[Dict[str, str]]]:
        options = settings_dict.get("OPTIONS") or {}
        if options.get("ssl") is not None:
            raise ImproperlyConfigured(
                "dbshell cannot pass an ssl.SSLContext to nusadb-cli; run nusadb-cli "
                "yourself with --tls-ca (and --tls-domain if needed)."
            )
        args = [cls.executable_name]
        host = settings_dict.get("HOST") or "127.0.0.1"
        port = settings_dict.get("PORT") or 5678
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        args += ["--host", f"{host}:{port}"]
        if settings_dict.get("USER"):
            args += ["--user", settings_dict["USER"]]
        args += ["--database", settings_dict.get("NAME") or "nusadb"]
        args.extend(parameters)
        env = None
        if settings_dict.get("PASSWORD"):
            env = {"NUSADB_PASSWORD": str(settings_dict["PASSWORD"])}
        return args, env
