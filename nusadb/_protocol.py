"""Low-level Nusa Wire Protocol codec.

Implements the framing and message layouts of ``docs/wire-protocol.md``
(``PROTOCOL_VERSION 1.1``): a frame is ``[type:u8][len:u32][payload]`` with
``len`` the *total* size including the 5-byte header, big-endian throughout.
Strings are length-prefixed UTF-8 (``[len:u32][bytes]``, **not** null-terminated).

This module is intentionally dependency-free (stdlib only).
"""

from __future__ import annotations

import struct
from typing import List, Optional, Tuple

# Protocol magic: ASCII "NUSA".
PROTOCOL_MAGIC = 0x4E555341
# Request minor 2 to receive the typed RowDescription (per-column type tags) AND the element type of
# an ARRAY column (protocol 1.2). An older server ignores the minor and answers with a form the driver
# still handles (1.1 = typed scalars only; 1.0 = untyped names).
PROTOCOL_VERSION = (1, 2)
HEADER_LEN = 5
MAX_FRAME_LEN = 256 * 1024 * 1024

# Column type tags carried by RowDescriptionTyped (protocol 1.1, wire-protocol.md §9.2). Maps the
# 1-byte tag to a canonical type name; an unknown/0x00 tag is UNKNOWN (treated as text).
TYPE_TAGS = {
    0x00: "UNKNOWN", 0x01: "BOOL", 0x02: "INT", 0x03: "FLOAT", 0x04: "NUMERIC",
    0x05: "TEXT", 0x06: "BYTES", 0x07: "DATE", 0x08: "TIME", 0x09: "TIMETZ",
    0x0A: "TIMESTAMP", 0x0B: "TIMESTAMPTZ", 0x0C: "INTERVAL", 0x0D: "UUID",
    0x0E: "JSON", 0x0F: "ARRAY", 0x10: "VECTOR",
}

# Protocol 1.2: an ARRAY column's tag is ``ARRAY_ELEMENT_FLAG | <element tag>`` (e.g. INT[] = 0x82),
# so the element type travels with the column and the row decoder casts the elements at their type.
ARRAY_ELEMENT_FLAG = 0x80


def type_name(tag: int) -> str:
    """Canonical type name for a RowDescriptionTyped type tag (UNKNOWN if unrecognised). An
    element-typed array tag (protocol 1.2, ``0x80 | element``) is named ``ARRAY:<element>`` —
    e.g. ``ARRAY:INT`` — so the row decoder can cast the elements at their real type."""
    if tag & ARRAY_ELEMENT_FLAG:
        return "ARRAY:" + TYPE_TAGS.get(tag & ~ARRAY_ELEMENT_FLAG, "UNKNOWN")
    return TYPE_TAGS.get(tag, "UNKNOWN")

# Frontend (client -> server) type bytes.
T_STARTUP = ord("S")
T_QUERY = ord("Q")
T_PARSE = ord("P")
T_BIND = ord("B")
T_DESCRIBE = ord("D")
T_EXECUTE = ord("E")
T_SYNC = ord("Y")
T_CLOSE = ord("C")
T_SASL_INITIAL = ord("p")
T_SASL_RESPONSE = ord("r")
T_COPY_DATA = ord("d")
T_COPY_DONE = ord("c")
T_COPY_FAIL = ord("f")
T_CANCEL_REQUEST = ord("K")
T_TERMINATE = ord("X")

# Backend (server -> client) type bytes.
B_AUTH = ord("R")
B_BACKEND_KEY = ord("K")
B_READY = ord("Z")
B_COMMAND_COMPLETE = ord("C")
B_ERROR = ord("E")
B_ROW_DESCRIPTION = ord("T")
B_ROW_DESCRIPTION_TYPED = ord("y")  # protocol 1.1 (typed columns)
B_DATA_ROW = ord("D")
B_PARSE_COMPLETE = ord("1")
B_BIND_COMPLETE = ord("2")
B_CLOSE_COMPLETE = ord("3")
B_PARAMETER_DESCRIPTION = ord("t")
B_NO_DATA = ord("n")
B_PORTAL_SUSPENDED = ord("z")
B_COPY_IN = ord("G")
B_COPY_OUT = ord("H")
B_COPY_DATA = ord("d")
B_COPY_DONE = ord("c")
B_NOTIFICATION_RESPONSE = ord("A")  # async LISTEN/NOTIFY: [pid:u32][channel:str][payload:str]

# Authentication sub-codes (the leading u32 of an `R` message).
AUTH_OK = 0
AUTH_SASL = 10
AUTH_SASL_CONTINUE = 11
AUTH_SASL_FINAL = 12


class Writer:
    """Accumulates a message payload with the protocol's primitive encodings."""

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def u8(self, value: int) -> "Writer":
        self._buf += struct.pack(">B", value)
        return self

    def u16(self, value: int) -> "Writer":
        self._buf += struct.pack(">H", value)
        return self

    def u32(self, value: int) -> "Writer":
        self._buf += struct.pack(">I", value)
        return self

    def raw(self, data: bytes) -> "Writer":
        self._buf += data
        return self

    def string(self, text: str) -> "Writer":
        encoded = text.encode("utf-8")
        self.u32(len(encoded))
        self._buf += encoded
        return self

    def fields(self, values: List[Optional[bytes]]) -> "Writer":
        """Encode a `Fields` list (DataRow / Bind params)."""
        self.u16(len(values))
        for value in values:
            if value is None:
                self.u8(0)
            else:
                self.u8(1)
                self.u32(len(value))
                self._buf += value
        return self

    def frame(self, message_type: int) -> bytes:
        """Wrap the payload in a frame header: ``[type][len][payload]``."""
        total = len(self._buf) + HEADER_LEN
        return struct.pack(">BI", message_type, total) + bytes(self._buf)


class Reader:
    """Reads the protocol's primitive encodings from a payload buffer."""

    __slots__ = ("_buf", "_pos")

    def __init__(self, payload: bytes) -> None:
        self._buf = payload
        self._pos = 0

    def remaining(self) -> int:
        return len(self._buf) - self._pos

    def _take(self, n: int) -> bytes:
        if self._pos + n > len(self._buf):
            raise ValueError("truncated payload")
        chunk = self._buf[self._pos : self._pos + n]
        self._pos += n
        return chunk

    def u8(self) -> int:
        return struct.unpack(">B", self._take(1))[0]

    def u16(self) -> int:
        return struct.unpack(">H", self._take(2))[0]

    def u32(self) -> int:
        return struct.unpack(">I", self._take(4))[0]

    def string(self) -> str:
        n = self.u32()
        return self._take(n).decode("utf-8")

    def rest(self) -> bytes:
        chunk = self._buf[self._pos :]
        self._pos = len(self._buf)
        return chunk

    def fields(self) -> List[Optional[bytes]]:
        count = self.u16()
        out: List[Optional[bytes]] = []
        for _ in range(count):
            present = self.u8()
            if present == 0:
                out.append(None)
            else:
                n = self.u32()
                out.append(self._take(n))
        return out


# --- frontend message builders (return wire-ready frame bytes) ---


def startup(user: str, database: str) -> bytes:
    return (
        Writer()
        .u32(PROTOCOL_MAGIC)
        .u16(PROTOCOL_VERSION[0])
        .u16(PROTOCOL_VERSION[1])
        .string(user)
        .string(database)
        .frame(T_STARTUP)
    )


def query(sql: str) -> bytes:
    return Writer().string(sql).frame(T_QUERY)


def parse(name: str, sql: str) -> bytes:
    return Writer().string(name).string(sql).u16(0).frame(T_PARSE)


def bind(portal: str, statement: str, params: List[Optional[bytes]]) -> bytes:
    return (
        Writer()
        .string(portal)
        .string(statement)
        .fields(params)
        .u16(0)  # result format count: empty => all text
        .frame(T_BIND)
    )


def describe_portal(name: str) -> bytes:
    return Writer().u8(ord("P")).string(name).frame(T_DESCRIBE)


def execute(portal: str, max_rows: int = 0) -> bytes:
    return Writer().string(portal).u32(max_rows).frame(T_EXECUTE)


def sync() -> bytes:
    return Writer().frame(T_SYNC)


def terminate() -> bytes:
    return Writer().frame(T_TERMINATE)


def sasl_initial(mechanism: str, data: bytes) -> bytes:
    return Writer().string(mechanism).u32(len(data)).raw(data).frame(T_SASL_INITIAL)


def sasl_response(data: bytes) -> bytes:
    return Writer().raw(data).frame(T_SASL_RESPONSE)


def cancel_request(pid: int, secret: int) -> bytes:
    return Writer().u32(pid).u32(secret).frame(T_CANCEL_REQUEST)


def copy_data(data: bytes) -> bytes:
    return Writer().raw(data).frame(T_COPY_DATA)


def copy_done() -> bytes:
    return Writer().frame(T_COPY_DONE)


def copy_fail(message: str) -> bytes:
    return Writer().string(message).frame(T_COPY_FAIL)


def decode_frame_header(header: bytes) -> Tuple[int, int]:
    """Return ``(message_type, payload_len)`` from a 5-byte frame header."""
    message_type, total = struct.unpack(">BI", header)
    if total < HEADER_LEN:
        raise ValueError(f"malformed frame length {total}")
    if total > MAX_FRAME_LEN:
        raise ValueError(f"frame too large: {total}")
    return message_type, total - HEADER_LEN
