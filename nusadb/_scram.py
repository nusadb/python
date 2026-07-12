"""Client-side SCRAM-SHA-256 (RFC 5802 / RFC 7677) for the Nusa Wire Protocol.

Stdlib only (``hashlib``/``hmac``/``os``/``base64``). Implements the four-step
exchange of ``docs/wire-protocol.md`` §7.2 and verifies the server's signature
(mutual authentication).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
from typing import Tuple

_GS2_HEADER = "n,,"
# base64("n,,") — the channel-binding value `c=` in client-final.
_CHANNEL_BINDING = base64.b64encode(_GS2_HEADER.encode("ascii")).decode("ascii")


def _hmac(key: bytes, msg: bytes) -> bytes:
    return hmac.new(key, msg, hashlib.sha256).digest()


def _xor(a: bytes, b: bytes) -> bytes:
    return bytes(x ^ y for x, y in zip(a, b))


def client_first(user: str) -> Tuple[str, str]:
    """Return ``(client_first_bare, full_client_first)`` with a fresh nonce."""
    nonce = base64.b64encode(os.urandom(18)).decode("ascii")
    bare = f"n={user},r={nonce}"
    return bare, _GS2_HEADER + bare


def _parse_server_first(message: str) -> Tuple[str, bytes, int]:
    """Parse ``r=<nonce>,s=<base64 salt>,i=<iterations>``."""
    parts = dict(field.split("=", 1) for field in message.split(","))
    combined_nonce = parts["r"]
    salt = base64.b64decode(parts["s"])
    iterations = int(parts["i"])
    return combined_nonce, salt, iterations


def client_final(
    password: str,
    client_first_bare: str,
    server_first: str,
) -> Tuple[bytes, str]:
    """Build the client-final message and the cached state to verify the server.

    Returns ``(client_final_bytes, server_signature_b64_expected)``.
    """
    combined_nonce, salt, iterations = _parse_server_first(server_first)

    salted = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    client_key = _hmac(salted, b"Client Key")
    stored_key = hashlib.sha256(client_key).digest()

    without_proof = f"c={_CHANNEL_BINDING},r={combined_nonce}"
    auth_message = f"{client_first_bare},{server_first},{without_proof}".encode("utf-8")
    client_signature = _hmac(stored_key, auth_message)
    proof = _xor(client_key, client_signature)
    final = f"{without_proof},p={base64.b64encode(proof).decode('ascii')}"

    server_key = _hmac(salted, b"Server Key")
    server_signature = _hmac(server_key, auth_message)
    expected = base64.b64encode(server_signature).decode("ascii")
    return final.encode("utf-8"), expected


def verify_server_final(server_final: str, expected_signature_b64: str) -> bool:
    """Check the server-final ``v=<signature>`` against the expected value."""
    parts = dict(field.split("=", 1) for field in server_final.split(","))
    got = parts.get("v", "")
    return hmac.compare_digest(got, expected_signature_b64)
