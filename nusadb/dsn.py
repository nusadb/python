"""Connection URLs: ``nusadb://[user[:password]@]host[:port][/database][?key=value&...]``.

The ``nusadbs://`` scheme turns TLS on, verified against the system's trusted roots. An IPv6 host
goes in brackets (``nusadb://[::1]:5678/db``). Percent-encode reserved characters in the user,
password and database.

Query keys:

=====================================================  ===============================================
``connect_timeout``, ``statement_timeout``,            a duration: ``30`` (seconds), ``500ms``,
``keepalive``                                          ``30s``, ``5min``, ``1h``
``tls``                                                ``disable``, ``require`` (verify against the
                                                       system roots), ``insecure`` (no verification;
                                                       tests only)
``tls_ca``                                             a PEM file of CA certificates to trust instead
                                                       of the system roots
``tls_cert``, ``tls_key``                              the PEM client certificate and key, for a
                                                       server that requires one
=====================================================  ===============================================
"""

from __future__ import annotations

import ssl as ssl_module
from typing import Any, Dict, Optional, Tuple
from urllib.parse import unquote_to_bytes

from .exceptions import ProgrammingError

_UNITS = {"": 1.0, "s": 1.0, "ms": 0.001, "min": 60.0, "h": 3600.0}


def parse_dsn(dsn: str) -> Dict[str, Any]:
    """The :func:`nusadb.connect` keyword arguments a URL names. Only what the URL spells out is
    present, so the caller's own defaults fill the rest.

    Raises :class:`ProgrammingError` on a malformed URL, an unknown key or value, or a key that
    contradicts another."""
    out: Dict[str, Any] = {}
    if dsn.startswith("nusadbs://"):
        rest, tls_default = dsn[len("nusadbs://") :], "require"
    elif dsn.startswith("nusadb://"):
        rest, tls_default = dsn[len("nusadb://") :], None
    else:
        raise ProgrammingError("a connection URL starts with nusadb:// or nusadbs://")
    rest, _, query = rest.partition("?")
    # The host cannot hold an "@", so the last one ends the user and password.
    authority, at, hostpart = rest.rpartition("@")
    if at:
        user, colon, password = authority.partition(":")
        if user:
            out["user"] = _decode(user)
        if colon:
            out["password"] = _decode(password)
    host_port, _, database = hostpart.partition("/")
    host, port = _split_host_port(host_port)
    if host:
        out["host"] = host
    if port is not None:
        if not port.isdigit() or not 0 < int(port) < 65536:
            raise ProgrammingError(f"invalid port {port!r} in the connection URL")
        out["port"] = int(port)
    if database:
        out["database"] = _decode(database)
    out.update(_apply_query(query, tls_default))
    return out


def _apply_query(query: str, tls_default: Optional[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    tls_mode = tls_default
    ca = cert = key = None
    for pair in filter(None, query.split("&")):
        name, _, raw = pair.partition("=")
        value = _decode(raw)
        if name in ("connect_timeout", "keepalive"):
            seconds = parse_duration(name, value)
            if seconds <= 0:
                raise ProgrammingError(f"{name} must be more than zero")
            out[name] = seconds
        elif name == "statement_timeout":
            out[name] = parse_duration(name, value)
        elif name == "tls":
            if value not in ("disable", "require", "insecure"):
                raise ProgrammingError(f"tls must be disable, require or insecure, not {value!r}")
            tls_mode = value
        elif name == "tls_ca":
            ca = value
        elif name == "tls_cert":
            cert = value
        elif name == "tls_key":
            key = value
        else:
            raise ProgrammingError(f"unknown connection URL parameter {name!r}")
    if (cert is None) != (key is None):
        raise ProgrammingError("tls_cert and tls_key go together")
    wants_files = ca is not None or cert is not None
    if tls_mode == "disable":
        if wants_files:
            raise ProgrammingError("tls=disable contradicts tls_ca / tls_cert")
        out["ssl"] = None
        return out
    if tls_mode == "insecure" and ca is not None:
        raise ProgrammingError("tls=insecure skips verification, so tls_ca would be ignored")
    if tls_mode is None and not wants_files:
        return out
    try:
        if tls_mode == "insecure":
            context = ssl_module.SSLContext(ssl_module.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl_module.CERT_NONE
        else:
            context = ssl_module.create_default_context(cafile=ca)
        if cert is not None:
            context.load_cert_chain(cert, key)
    except (OSError, ssl_module.SSLError) as exc:
        raise ProgrammingError(f"could not load the TLS files named in the URL: {exc}") from exc
    out["ssl"] = context
    return out


def parse_duration(name: str, value: str) -> float:
    """A duration in seconds from ``30``, ``500ms``, ``30s``, ``5min`` or ``1h``."""
    digits = len(value) - len(value.lstrip("0123456789"))
    number, unit = value[:digits], value[digits:]
    if not number or unit not in _UNITS:
        raise ProgrammingError(f"{name}: invalid duration {value!r} (use ms, s, min or h)")
    return int(number) * _UNITS[unit]


def _split_host_port(host_port: str) -> Tuple[str, Optional[str]]:
    if host_port.startswith("["):
        host, bracket, after = host_port[1:].partition("]")
        if not bracket:
            raise ProgrammingError(f"unclosed [ in host {host_port!r}")
        if not after:
            return host, None
        if not after.startswith(":"):
            raise ProgrammingError(f"unexpected {after!r} after the host")
        return host, after[1:]
    host, colon, port = host_port.partition(":")
    if colon and ":" in port:
        raise ProgrammingError(
            f"an IPv6 host goes in brackets, as in [::1]:5678 (got {host_port!r})"
        )
    return host, (port if colon else None)


def _decode(text: str) -> str:
    try:
        return unquote_to_bytes(text).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ProgrammingError("percent-encoding in the connection URL is not UTF-8") from exc
