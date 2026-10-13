"""The binary value layouts of wire-protocol §11.2 and the binary ``COPY`` stream of §12.4.

A binary result column and a field of a binary ``COPY`` carry the same bytes, so one set of
decoders serves both. Every multi-byte integer is big-endian, except the bit count of a ``BIT``
value and the bounds of a ``RANGE``, which are little-endian (the server's storage layout).

The types whose binary form is their canonical text (``NUMERIC``, ``TEXT``, ``JSON``, arrays,
``VECTOR``, the geometric and full-text types, ``XML``) are not decoded here: the caller hands them
to the text decoders, so both formats give the same Python value.

This module has no dependency on the rest of the driver beyond :mod:`nusadb.types`.
"""

from __future__ import annotations

import datetime as _dt
import ipaddress
import re
import struct
import uuid as _uuid
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from . import types as _types
from .exceptions import InterfaceError, ProgrammingError

_EPOCH = _dt.datetime(1970, 1, 1)
_EPOCH_UTC = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)
_EPOCH_ORDINAL = _dt.date(1970, 1, 1).toordinal()
_MAX_ORDINAL = _dt.date.max.toordinal()
_MICROS_PER_DAY = 86_400_000_000
_I64_MIN, _I64_MAX = -(2**63), 2**63 - 1

# The element kind byte that leads a binary RANGE value.
_RANGE_KINDS = {0: "INT", 1: "NUMERIC", 2: "DATE", 3: "TIMESTAMP", 4: "TIMESTAMPTZ"}
_RANGE_KIND_BYTES = {name: kind for kind, name in _RANGE_KINDS.items()}
_RANGE_BOUND_SIZE = {0: 8, 1: 17, 2: 4, 3: 8, 4: 8}
_RANGE_EMPTY, _RANGE_LOWER_INF, _RANGE_UPPER_INF = 0x01, 0x02, 0x04
_RANGE_LOWER_INC, _RANGE_UPPER_INC = 0x08, 0x10


def _malformed(type_name: str, detail: str) -> InterfaceError:
    return InterfaceError(f"malformed binary {type_name} value: {detail}")


def _fixed(data: bytes, width: int, type_name: str) -> bytes:
    if len(data) != width:
        raise _malformed(type_name, f"expected {width} byte(s), got {len(data)}")
    return data


# --- the server's text forms, for values Python cannot hold ---


def _civil_from_days(days: int) -> Tuple[int, int, int]:
    """Year, month and day of a day count since 1970-01-01 (proleptic Gregorian)."""
    z = days + 719_468
    era = z // 146_097
    doe = z - era * 146_097
    yoe = (doe - doe // 1460 + doe // 36_524 - doe // 146_096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    return (y + 1 if m <= 2 else y), m, d


def _fraction(micros: int) -> str:
    return "." + f"{micros:06d}".rstrip("0") if micros else ""


def format_date(days: int) -> str:
    y, m, d = _civil_from_days(days)
    return f"{y:04d}-{m:02d}-{d:02d}"


def format_time(micros: int) -> str:
    secs, frac = divmod(micros, 1_000_000)
    return f"{secs // 3600:02d}:{secs % 3600 // 60:02d}:{secs % 60:02d}{_fraction(frac)}"


def format_timestamp(micros: int) -> str:
    days, tod = divmod(micros, _MICROS_PER_DAY)
    return f"{format_date(days)} {format_time(tod)}"


def _format_offset(east: int) -> str:
    sign = "-" if east < 0 else "+"
    h, rest = divmod(abs(east), 3600)
    m, s = divmod(rest, 60)
    if s:
        return f"{sign}{h:02d}:{m:02d}:{s:02d}"
    if m:
        return f"{sign}{h:02d}:{m:02d}"
    return f"{sign}{h:02d}"


def format_interval(months: int, days: int, micros: int) -> str:
    """An interval's text, ``1 year 2 mons 3 days 04:05:06``, as the server renders it."""
    years = abs(months) // 12 * (1 if months >= 0 else -1)
    mons = months - years * 12
    parts = []
    if years:
        parts.append(f"{years} year{'' if years == 1 else 's'}")
    if mons:
        parts.append(f"{mons} mon{'' if mons == 1 else 's'}")
    if days:
        parts.append(f"{days} day{'' if days == 1 else 's'}")
    if micros or not parts:
        secs, frac = divmod(abs(micros), 1_000_000)
        sign = "-" if micros < 0 else ""
        parts.append(
            f"{sign}{secs // 3600:02d}:{secs % 3600 // 60:02d}:{secs % 60:02d}{_fraction(frac)}"
        )
    return " ".join(parts)


def format_numeric(mantissa: int, scale: int) -> str:
    """A decimal's canonical text from its mantissa and scale (``5`` at scale 2 is ``0.05``)."""
    if scale == 0:
        return str(mantissa)
    sign = "-" if mantissa < 0 else ""
    digits = str(abs(mantissa))
    if len(digits) > scale:
        return f"{sign}{digits[:-scale]}.{digits[-scale:]}"
    return f"{sign}0.{digits.rjust(scale, '0')}"


# --- decoders: bytes of one binary field to a Python value ---


def _decode_bool(data: bytes) -> bool:
    if data == b"\x01":
        return True
    if data == b"\x00":
        return False
    raise _malformed("BOOL", f"{data!r} is not 0x00 or 0x01")


def _decode_int(data: bytes) -> int:
    return struct.unpack(">q", _fixed(data, 8, "INT"))[0]


def _decode_float(data: bytes) -> float:
    return struct.unpack(">d", _fixed(data, 8, "FLOAT"))[0]


def date_from_days(days: int) -> Any:
    """A day count as a ``date``, or as the server's text when Python cannot hold it."""
    ordinal = _EPOCH_ORDINAL + days
    if 1 <= ordinal <= _MAX_ORDINAL:
        return _dt.date.fromordinal(ordinal)
    return format_date(days)


def _decode_date(data: bytes) -> Any:
    return date_from_days(struct.unpack(">i", _fixed(data, 4, "DATE"))[0])


def _time_of_day(micros: int, tz: Optional[_dt.tzinfo] = None) -> Optional[_dt.time]:
    if not 0 <= micros < _MICROS_PER_DAY:
        return None
    secs, frac = divmod(micros, 1_000_000)
    return _dt.time(secs // 3600, secs % 3600 // 60, secs % 60, frac, tzinfo=tz)


def _decode_time(data: bytes) -> Any:
    micros = struct.unpack(">q", _fixed(data, 8, "TIME"))[0]
    value = _time_of_day(micros)
    return format_time(micros) if value is None else value


def _decode_timetz(data: bytes) -> Any:
    micros, west = struct.unpack(">qi", _fixed(data, 12, "TIMETZ"))
    if abs(west) < 86_400:
        value = _time_of_day(micros, _dt.timezone(_dt.timedelta(seconds=-west)))
        if value is not None:
            return value
    return format_time(micros) + _format_offset(-west)


def timestamp_from_micros(micros: int, aware: bool) -> Any:
    """Microseconds since the epoch as a ``datetime`` (in UTC when ``aware``), or as the server's
    text when Python cannot hold it."""
    try:
        if aware:
            return _EPOCH_UTC + _dt.timedelta(microseconds=micros)
        return _EPOCH + _dt.timedelta(microseconds=micros)
    except OverflowError:
        text = format_timestamp(micros)
        return text + "+00" if aware else text


def _decode_timestamp(data: bytes) -> Any:
    return timestamp_from_micros(struct.unpack(">q", _fixed(data, 8, "TIMESTAMP"))[0], False)


def _decode_timestamptz(data: bytes) -> Any:
    return timestamp_from_micros(struct.unpack(">q", _fixed(data, 8, "TIMESTAMPTZ"))[0], True)


def _decode_interval(data: bytes) -> Any:
    months, days, micros = struct.unpack(">iiq", _fixed(data, 16, "INTERVAL"))
    if months:
        # A month is not a fixed number of days: kept as text, as the text format does.
        return format_interval(months, days, micros)
    return _dt.timedelta(days=days, microseconds=micros)


def _decode_uuid(data: bytes) -> _uuid.UUID:
    return _uuid.UUID(bytes=_fixed(data, 16, "UUID"))


def _decode_macaddr(data: bytes) -> str:
    return ":".join(f"{b:02x}" for b in _fixed(data, 6, "MACADDR"))


def _decode_macaddr8(data: bytes) -> str:
    return ":".join(f"{b:02x}" for b in _fixed(data, 8, "MACADDR8"))


def _network_parts(data: bytes, type_name: str) -> Tuple[Any, int]:
    if len(data) < 2:
        raise _malformed(type_name, "shorter than its flags and mask")
    flags, masklen = data[0], data[1]
    width = 16 if flags & 0x02 else 4
    if len(data) != 2 + width:
        raise _malformed(type_name, f"expected {2 + width} bytes, got {len(data)}")
    address = ipaddress.ip_address(bytes(data[2:]))
    if masklen > width * 8:
        raise _malformed(type_name, f"mask length {masklen} is too long")
    return address, masklen


def _decode_inet(data: bytes) -> Any:
    address, masklen = _network_parts(data, "INET")
    # The same values as the text form: a single host is an address, anything else an interface.
    if masklen == address.max_prefixlen:
        return address
    return ipaddress.ip_interface((address, masklen))


def _decode_cidr(data: bytes) -> Any:
    address, masklen = _network_parts(data, "CIDR")
    try:
        return ipaddress.ip_network((address, masklen))
    except ValueError as exc:
        raise _malformed("CIDR", str(exc)) from None


def _decode_bit(data: bytes) -> str:
    if len(data) < 4:
        raise _malformed("BIT", "shorter than its bit count")
    count = struct.unpack("<I", data[:4])[0]
    if len(data) != 4 + (count + 7) // 8:
        raise _malformed("BIT", f"{count} bits do not fit {len(data) - 4} byte(s)")
    packed = int.from_bytes(data[4:], "big") if count else 0
    width = (len(data) - 4) * 8
    return format(packed, f"0{width}b")[:count] if count else ""


def _range_bound_text(kind: int, data: bytes) -> str:
    """One range bound's canonical text, read from its little-endian storage layout."""
    if kind == 0:
        return str(struct.unpack("<q", data)[0])
    if kind == 1:
        mantissa = int.from_bytes(data[:16], "little", signed=True)
        if data[16] > 38:
            raise _malformed("RANGE", f"numeric bound scale {data[16]} is out of range")
        return format_numeric(mantissa, data[16])
    if kind == 2:
        return format_date(struct.unpack("<i", data)[0])
    text = format_timestamp(struct.unpack("<q", data)[0])
    return text + "+00" if kind == 4 else text


def range_text(data: bytes) -> str:
    """A binary RANGE value as its canonical text (``[1,5)``, ``empty``), checking the layout."""
    if len(data) < 2:
        raise _malformed("RANGE", "shorter than its kind and flags")
    kind, flags = data[0], data[1]
    if kind not in _RANGE_KINDS:
        raise _malformed("RANGE", f"unknown element kind {kind}")
    if flags & ~0x1F:
        raise _malformed("RANGE", f"unknown flag bits in 0x{flags:02x}")
    if flags & _RANGE_EMPTY:
        if len(data) != 2:
            raise _malformed("RANGE", "an empty range carries bounds")
        return "empty"
    size = _RANGE_BOUND_SIZE[kind]
    finite = [not flags & _RANGE_LOWER_INF, not flags & _RANGE_UPPER_INF]
    if len(data) != 2 + size * sum(finite):
        raise _malformed("RANGE", f"expected {2 + size * sum(finite)} bytes, got {len(data)}")
    pos, texts = 2, []
    for present in finite:
        if present:
            texts.append(_quote_bound(_range_bound_text(kind, data[pos : pos + size])))
            pos += size
        else:
            texts.append("")
    lower = "[" if finite[0] and flags & _RANGE_LOWER_INC else "("
    upper = "]" if finite[1] and flags & _RANGE_UPPER_INC else ")"
    return f"{lower}{texts[0]},{texts[1]}{upper}"


def _quote_bound(text: str) -> str:
    """A range bound as the server writes it: quoted when it holds a space or a delimiter."""
    if any(c.isspace() or c in '"\\()[],' for c in text):
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return text


def _decode_range(data: bytes) -> Any:
    # Through the range's text, so a bound decodes exactly as the text format decodes it (and a
    # range whose bound Python cannot hold stays that text, as it does in the text format).
    text = range_text(data)
    try:
        return _types.decode_range(text)
    except ValueError:
        return text


#: The decoder of each type whose binary form is not its text, by type name.
DECODERS: Dict[str, Callable[[bytes], Any]] = {
    "BOOL": _decode_bool,
    "INT": _decode_int,
    "FLOAT": _decode_float,
    "DATE": _decode_date,
    "TIME": _decode_time,
    "TIMETZ": _decode_timetz,
    "TIMESTAMP": _decode_timestamp,
    "TIMESTAMPTZ": _decode_timestamptz,
    "INTERVAL": _decode_interval,
    "UUID": _decode_uuid,
    "BYTES": bytes,
    "MACADDR": _decode_macaddr,
    "MACADDR8": _decode_macaddr8,
    "INET": _decode_inet,
    "CIDR": _decode_cidr,
    "BIT": _decode_bit,
    "VARBIT": _decode_bit,
    "RANGE": _decode_range,
}


# --- type names ---

_SPACES = re.compile(r"\s+")
_MODIFIERS = re.compile(r"\s*\([^)]*\)")

# A SQL type name to the wire type name its binary layout follows. A range keeps its element kind
# (``RANGE:INT``), which its layout carries.
_TYPE_NAMES = {
    "BOOL": "BOOL",
    "BOOLEAN": "BOOL",
    "INT": "INT",
    "INTEGER": "INT",
    "INT2": "INT",
    "INT4": "INT",
    "INT8": "INT",
    "SMALLINT": "INT",
    "BIGINT": "INT",
    "SERIAL": "INT",
    "BIGSERIAL": "INT",
    "SMALLSERIAL": "INT",
    "FLOAT": "FLOAT",
    "FLOAT4": "FLOAT",
    "FLOAT8": "FLOAT",
    "REAL": "FLOAT",
    "DOUBLE": "FLOAT",
    "DOUBLE PRECISION": "FLOAT",
    "NUMERIC": "NUMERIC",
    "DECIMAL": "NUMERIC",
    "TEXT": "TEXT",
    "VARCHAR": "TEXT",
    "CHAR": "TEXT",
    "CHARACTER": "TEXT",
    "CHARACTER VARYING": "TEXT",
    "BPCHAR": "TEXT",
    "STRING": "TEXT",
    "CITEXT": "TEXT",
    "BYTES": "BYTES",
    "BYTEA": "BYTES",
    "DATE": "DATE",
    "TIME": "TIME",
    "TIME WITHOUT TIME ZONE": "TIME",
    "TIMETZ": "TIMETZ",
    "TIME WITH TIME ZONE": "TIMETZ",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
    "DATETIME": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMPTZ",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
    "INTERVAL": "INTERVAL",
    "UUID": "UUID",
    "JSON": "JSON",
    "JSONB": "JSON",
    "VECTOR": "VECTOR",
    "MACADDR": "MACADDR",
    "MACADDR8": "MACADDR8",
    "INET": "INET",
    "CIDR": "CIDR",
    "BIT": "BIT",
    "VARBIT": "VARBIT",
    "BIT VARYING": "VARBIT",
    "RANGE": "RANGE",
    "INT4RANGE": "RANGE:INT",
    "INT8RANGE": "RANGE:INT",
    "NUMRANGE": "RANGE:NUMERIC",
    "DATERANGE": "RANGE:DATE",
    "TSRANGE": "RANGE:TIMESTAMP",
    "TSTZRANGE": "RANGE:TIMESTAMPTZ",
    "GEOMETRY": "GEOMETRY",
    "POINT": "GEOMETRY",
    "LINE": "GEOMETRY",
    "LSEG": "GEOMETRY",
    "BOX": "GEOMETRY",
    "PATH": "GEOMETRY",
    "POLYGON": "GEOMETRY",
    "CIRCLE": "GEOMETRY",
    "TSVECTOR": "TSVECTOR",
    "TSQUERY": "TSQUERY",
    "XML": "XML",
    "ARRAY": "ARRAY",
}


def wire_type(name: str) -> str:
    """The wire type name (``INT``, ``RANGE:DATE``, ``ARRAY:TEXT``, ...) of a SQL type name such
    as ``bigint``, ``varchar(20)``, ``numeric(10,2)``, ``daterange`` or ``text[]``. Raises
    :class:`ProgrammingError` for a name it does not know."""
    if not isinstance(name, str):
        raise ProgrammingError(f"a type name is a string, not {type(name).__name__}")
    text = _SPACES.sub(" ", _MODIFIERS.sub("", name)).strip().upper()
    if text.endswith("[]"):
        element = wire_type(text[:-2].rstrip("[] "))
        if element.startswith(("ARRAY", "RANGE:")):
            return "ARRAY"
        return "ARRAY:" + element
    if text.endswith(" ARRAY"):
        return "ARRAY:" + wire_type(text[: -len(" ARRAY")])
    if text.startswith("ARRAY:"):
        return "ARRAY:" + wire_type(text[len("ARRAY:") :])
    if text.startswith("RANGE:") and text[len("RANGE:") :] in _RANGE_KIND_BYTES:
        return text
    found = _TYPE_NAMES.get(text)
    if found is None:
        raise ProgrammingError(f"unknown type {name!r} for a binary value")
    return found


# --- encoders: a Python value to its binary field ---


def _refuse(value: Any, type_name: str, hint: str = "") -> ProgrammingError:
    message = f"cannot write a {type(value).__name__} as a binary {type_name} value"
    return ProgrammingError(message + (f" ({hint})" if hint else ""))


def _micros_since_epoch(value: _dt.datetime) -> int:
    if value.tzinfo is not None and value.utcoffset() is not None:
        delta = value - _EPOCH_UTC
    else:
        delta = value - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _time_micros(value: _dt.time) -> int:
    return ((value.hour * 60 + value.minute) * 60 + value.second) * 1_000_000 + value.microsecond


def _decimal_parts(value: Any, type_name: str) -> Tuple[int, int]:
    """The mantissa and scale of an ``int`` or finite ``Decimal`` (a numeric range bound)."""
    if isinstance(value, int) and not isinstance(value, bool):
        mantissa, scale = int(value), 0
    elif isinstance(value, Decimal) and value.is_finite():
        sign, digits, exponent = value.as_tuple()
        mantissa = int("".join(map(str, digits)) or "0") * (-1 if sign else 1)
        if exponent >= 0:
            mantissa, scale = mantissa * 10**exponent, 0
        else:
            scale = -exponent
    else:
        raise _refuse(value, type_name, "a numeric bound is an int or a finite Decimal")
    if scale > 38 or not -(2**127) <= mantissa < 2**127:
        raise ProgrammingError(f"{value!r} does not fit a binary numeric bound")
    return mantissa, scale


def _range_kind(value: _types.Range) -> Optional[str]:
    """The element kind a range's finite bounds have, ``None`` when it has none."""
    kinds = {_bound_kind(b) for b in (value.lower, value.upper) if b is not None}
    if len(kinds) > 1:
        raise ProgrammingError(f"the bounds of {value!r} are of different types")
    return kinds.pop() if kinds else None


def _bound_kind(bound: Any) -> str:
    if isinstance(bound, bool):
        raise _refuse(bound, "RANGE")
    if isinstance(bound, int):
        return "INT"
    if isinstance(bound, Decimal):
        return "NUMERIC"
    if isinstance(bound, _dt.datetime):
        return "TIMESTAMPTZ" if bound.utcoffset() is not None else "TIMESTAMP"
    if isinstance(bound, _dt.date):
        return "DATE"
    raise _refuse(bound, "RANGE", "a bound is an int, a Decimal, a date or a datetime")


def _encode_range(value: Any, kind_name: Optional[str]) -> bytes:
    if not isinstance(value, _types.Range):
        raise _refuse(value, "RANGE", "pass a nusadb.Range")
    found = _range_kind(value)
    kind_name = kind_name or found
    if kind_name is None:
        raise ProgrammingError(
            f"cannot tell the element type of {value!r}; name the range type (int4range, ...)"
        )
    if found is not None and found != kind_name and not (found == "INT" and kind_name == "NUMERIC"):
        raise ProgrammingError(f"{value!r} has {found} bounds, not {kind_name}")
    kind = _RANGE_KIND_BYTES[kind_name]
    if value.empty:
        return bytes([kind, _RANGE_EMPTY | _RANGE_LOWER_INF | _RANGE_UPPER_INF])
    flags = 0
    out = bytearray()
    for bound, inf_flag, inc_flag, inclusive in (
        (value.lower, _RANGE_LOWER_INF, _RANGE_LOWER_INC, value.bounds[0] == "["),
        (value.upper, _RANGE_UPPER_INF, _RANGE_UPPER_INC, value.bounds[1] == "]"),
    ):
        if bound is None:
            flags |= inf_flag
            continue
        if inclusive:
            flags |= inc_flag
        if kind_name == "INT":
            if not _I64_MIN <= bound <= _I64_MAX:
                raise ProgrammingError(f"range bound {bound} does not fit a 64-bit integer")
            out += struct.pack("<q", bound)
        elif kind_name == "NUMERIC":
            mantissa, scale = _decimal_parts(bound, "RANGE")
            out += mantissa.to_bytes(16, "little", signed=True) + bytes([scale])
        elif kind_name == "DATE":
            out += struct.pack("<i", bound.toordinal() - _EPOCH_ORDINAL)
        else:
            out += struct.pack("<q", _micros_since_epoch(bound))
    return bytes([kind, flags]) + bytes(out)


def _parse_mac(value: Any, width: int, type_name: str) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
    elif isinstance(value, str):
        digits = re.sub(r"[:.\-]", "", value.strip())
        try:
            raw = bytes.fromhex(digits)
        except ValueError:
            raise ProgrammingError(f"{value!r} is not a {type_name} address") from None
    else:
        raise _refuse(value, type_name)
    if len(raw) != width:
        raise ProgrammingError(f"a {type_name} address has {width} bytes, not {len(raw)}")
    return raw


def _encode_network(value: Any, cidr: bool) -> bytes:
    type_name = "CIDR" if cidr else "INET"
    try:
        if isinstance(value, str):
            value = ipaddress.ip_network(value) if cidr else ipaddress.ip_interface(value)
    except ValueError as exc:
        raise ProgrammingError(f"{value!r} is not a {type_name} value: {exc}") from None
    if isinstance(value, (ipaddress.IPv4Network, ipaddress.IPv6Network)):
        address, masklen = value.network_address, value.prefixlen
    elif isinstance(value, (ipaddress.IPv4Interface, ipaddress.IPv6Interface)) and not cidr:
        address, masklen = value.ip, value.network.prefixlen
    elif isinstance(value, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        address, masklen = value, value.max_prefixlen
    else:
        raise _refuse(value, type_name)
    flags = (1 if cidr else 0) | (2 if address.version == 6 else 0)
    return bytes([flags, masklen]) + address.packed


def _encode_bits(value: Any) -> bytes:
    if not isinstance(value, str) or any(c not in "01" for c in value):
        raise ProgrammingError(f"a BIT value is a string of 0 and 1, not {value!r}")
    count = len(value)
    packed = (int(value, 2) << (-count % 8)) if count else 0
    return struct.pack("<I", count) + packed.to_bytes((count + 7) // 8, "big")


def encode_value(value: Any, wire: str, text: Callable[[Any], Optional[bytes]]) -> bytes:
    """``value`` (not ``None``) in the binary layout of wire type ``wire``. ``text`` writes a value
    in its text form, for the types whose binary form is text. Raises :class:`ProgrammingError`
    when the value does not fit the type."""
    if wire == "BOOL":
        if not isinstance(value, bool):
            raise _refuse(value, wire)
        return b"\x01" if value else b"\x00"
    if wire == "INT":
        if isinstance(value, bool) or not isinstance(value, int):
            raise _refuse(value, wire)
        if not _I64_MIN <= value <= _I64_MAX:
            raise ProgrammingError(f"{value} does not fit a 64-bit INT; use NUMERIC")
        return struct.pack(">q", value)
    if wire == "FLOAT":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise _refuse(value, wire)
        return struct.pack(">d", float(value))
    if wire == "DATE":
        if isinstance(value, _dt.datetime) or not isinstance(value, _dt.date):
            raise _refuse(value, wire)
        return struct.pack(">i", value.toordinal() - _EPOCH_ORDINAL)
    if wire in ("TIME", "TIMETZ"):
        if not isinstance(value, _dt.time):
            raise _refuse(value, wire)
        offset = value.utcoffset()
        if wire == "TIME":
            if offset is not None:
                raise _refuse(value, wire, "a time with a tzinfo is a TIMETZ")
            return struct.pack(">q", _time_micros(value))
        if offset is None:
            raise _refuse(value, wire, "a TIMETZ needs a time with a tzinfo")
        west = -(offset.days * 86_400 + offset.seconds)
        return struct.pack(">qi", _time_micros(value), west)
    if wire in ("TIMESTAMP", "TIMESTAMPTZ"):
        if not isinstance(value, _dt.datetime):
            raise _refuse(value, wire)
        aware = value.utcoffset() is not None
        if aware != (wire == "TIMESTAMPTZ"):
            hint = (
                "a TIMESTAMPTZ needs a datetime with a tzinfo"
                if not aware
                else ("a datetime with a tzinfo is a TIMESTAMPTZ")
            )
            raise _refuse(value, wire, hint)
        return struct.pack(">q", _micros_since_epoch(value))
    if wire == "INTERVAL":
        if not isinstance(value, _dt.timedelta):
            raise _refuse(value, wire)
        micros = value.seconds * 1_000_000 + value.microseconds
        return struct.pack(">iiq", 0, value.days, micros)
    if wire == "UUID":
        if isinstance(value, str):
            try:
                value = _uuid.UUID(value)
            except ValueError:
                raise ProgrammingError(f"{value!r} is not a UUID") from None
        if not isinstance(value, _uuid.UUID):
            raise _refuse(value, wire)
        return value.bytes
    if wire == "BYTES":
        if not isinstance(value, (bytes, bytearray, memoryview)):
            raise _refuse(value, wire)
        return bytes(value)
    if wire == "MACADDR":
        return _parse_mac(value, 6, wire)
    if wire == "MACADDR8":
        return _parse_mac(value, 8, wire)
    if wire in ("INET", "CIDR"):
        return _encode_network(value, wire == "CIDR")
    if wire in ("BIT", "VARBIT"):
        return _encode_bits(value)
    if wire == "RANGE" or wire.startswith("RANGE:"):
        return _encode_range(value, wire.partition(":")[2] or None)
    if wire == "JSON" and not isinstance(value, str) and not hasattr(value, "dumps"):
        # A list is JSON here, not an array.
        import json

        return json.dumps(value, separators=(",", ":")).encode("utf-8")
    if wire == "VECTOR" and isinstance(value, (list, tuple)):
        value = _types.Vector(value)
    # The rest are sent as their text.
    encoded = text(value)
    if encoded is None:  # pragma: no cover - None is handled by the caller
        raise ProgrammingError("NULL has no binary form")
    return encoded


def infer_wire_type(value: Any) -> str:
    """The wire type a Python value is written as when no type is named. Raises
    :class:`ProgrammingError` for a value whose type is not clear from its Python type."""
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT" if _I64_MIN <= value <= _I64_MAX else "NUMERIC"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, Decimal):
        return "NUMERIC"
    if isinstance(value, str):
        return "TEXT"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "BYTES"
    if isinstance(value, _dt.datetime):
        return "TIMESTAMPTZ" if value.utcoffset() is not None else "TIMESTAMP"
    if isinstance(value, _dt.date):
        return "DATE"
    if isinstance(value, _dt.time):
        return "TIMETZ" if value.utcoffset() is not None else "TIME"
    if isinstance(value, _dt.timedelta):
        return "INTERVAL"
    if isinstance(value, _uuid.UUID):
        return "UUID"
    if isinstance(value, dict) or hasattr(value, "dumps"):
        return "JSON"
    if isinstance(value, (list, tuple)):
        return "ARRAY"
    if isinstance(value, _types.Vector):
        return "VECTOR"
    if isinstance(value, (ipaddress.IPv4Network, ipaddress.IPv6Network)):
        return "CIDR"
    if isinstance(value, (ipaddress.IPv4Address, ipaddress.IPv6Address)) or isinstance(
        value, (ipaddress.IPv4Interface, ipaddress.IPv6Interface)
    ):
        return "INET"
    if isinstance(value, _types.Range):
        kind = _range_kind(value)
        if kind is None:
            raise ProgrammingError(
                f"cannot tell the element type of {value!r}; pass types=[...] to name it"
            )
        return "RANGE:" + kind
    raise ProgrammingError(
        f"cannot tell the binary type of a {type(value).__name__}; pass types=[...] to name it"
    )


# --- the binary COPY stream (§12.4) ---

COPY_SIGNATURE = b"NUSACOPY\xff\r\n"
_COPY_CHUNK = 65536


class CopyWriter:
    """Writes rows as a binary COPY stream (§12.4): :attr:`HEADER`, then :meth:`row` for each row,
    then :attr:`TRAILER`. Each value is written at its column's type in ``types`` (wire type names,
    see :func:`wire_type`), or, without ``types``, at the type its Python type maps to; a column
    whose values map to different types is refused. Values pass through ``adapt`` first, and the
    types whose binary form is text are written by ``text``."""

    HEADER = COPY_SIGNATURE + struct.pack(">II", 0, 0)
    TRAILER = struct.pack(">h", -1)

    def __init__(
        self,
        types: Optional[Sequence[str]],
        adapt: Callable[[Any], Any],
        text: Callable[[Any], Optional[bytes]],
    ) -> None:
        self._named = types is not None
        self._columns: Optional[List[Optional[str]]] = (
            None if types is None else [wire_type(name) for name in types]
        )
        self._adapt = adapt
        self._text = text
        self._rows = 0

    def row(self, row: Sequence[Any]) -> bytes:
        """One row: its field count, then each field's length and bytes (``-1`` for NULL)."""
        self._rows += 1
        if isinstance(row, (str, bytes)) or not isinstance(row, Sequence):
            raise ProgrammingError(
                f"a COPY row must be a sequence of values, not {type(row).__name__}"
            )
        if self._columns is None:
            if len(row) > 32767:
                raise ProgrammingError("a binary COPY row holds at most 32767 fields")
            self._columns = [None] * len(row)
        columns = self._columns
        if len(row) != len(columns):
            raise ProgrammingError(
                f"row {self._rows} has {len(row)} values; the COPY has {len(columns)} columns"
            )
        out = bytearray(struct.pack(">h", len(columns)))
        for i, value in enumerate(row):
            value = self._adapt(value)
            if value is None:
                out += b"\xff\xff\xff\xff"
                continue
            wire = columns[i]
            if not self._named:
                inferred = infer_wire_type(value)
                if wire is None:
                    columns[i] = wire = inferred
                elif wire != inferred:
                    raise ProgrammingError(
                        f"column {i + 1} holds both {wire} and {inferred} values; "
                        "pass types=[...] to name the column types"
                    )
            assert wire is not None
            field = encode_value(value, wire, self._text)
            out += struct.pack(">i", len(field)) + field
        return bytes(out)


def copy_binary_chunks(rows: Iterable[Sequence[Any]], writer: CopyWriter) -> Iterator[bytes]:
    """``rows`` as a binary COPY stream written by ``writer``, in chunks of about 64 KiB."""
    buf = bytearray(writer.HEADER)
    for row in rows:
        buf += writer.row(row)
        if len(buf) >= _COPY_CHUNK:
            yield bytes(buf)
            buf = bytearray()
    buf += writer.TRAILER
    yield bytes(buf)


class CopyReader:
    """Reads a binary COPY stream as it arrives in chunks, checking it strictly: the signature, no
    flag set, whole rows of one width (``len(types)`` when types are named), lengths of ``-1`` or
    more, the trailer, and nothing after it. A fault is kept and raised by :meth:`finish`, once the
    COPY has ended and the connection is back in step."""

    def __init__(self, types: Optional[Sequence[str]], decode: Callable[[bytes, str], Any]) -> None:
        self._wires = None if types is None else [_base_wire(wire_type(t)) for t in types]
        self._decode = decode
        self._buf = bytearray()
        self._pos = 0
        self._header = False
        self._done = False
        self._width: Optional[int] = None if types is None else len(types)
        self._error: Optional[Exception] = None
        self.rows: List[Tuple[Any, ...]] = []

    def __call__(self, chunk: bytes) -> None:
        if self._error is not None:
            return
        try:
            if self._done:
                if chunk:
                    raise InterfaceError("binary COPY stream has bytes after its trailer")
                return
            self._buf += chunk
            self._parse()
        except (InterfaceError, ValueError) as exc:
            self._error = exc if isinstance(exc, InterfaceError) else InterfaceError(str(exc))

    write = __call__

    def _take(self, n: int) -> Optional[bytes]:
        if len(self._buf) - self._pos < n:
            return None
        chunk = bytes(self._buf[self._pos : self._pos + n])
        self._pos += n
        return chunk

    def _parse(self) -> None:
        while not self._done:
            start = self._pos
            if not self._header:
                fixed = self._take(len(COPY_SIGNATURE) + 8)
                if fixed is None:
                    if not COPY_SIGNATURE.startswith(bytes(self._buf[: len(COPY_SIGNATURE)])):
                        raise InterfaceError("binary COPY stream does not start with its signature")
                    break
                if fixed[: len(COPY_SIGNATURE)] != COPY_SIGNATURE:
                    raise InterfaceError("binary COPY stream does not start with its signature")
                flags, extension = struct.unpack(">II", fixed[len(COPY_SIGNATURE) :])
                if flags:
                    raise InterfaceError(f"binary COPY stream sets unknown flags 0x{flags:08x}")
                if self._take(extension) is None:
                    self._pos = start
                    break
                self._header = True
                continue
            row = self._read_row()
            if row is None:
                self._pos = start
                break
        del self._buf[: self._pos]
        self._pos = 0
        if self._done and self._buf:
            raise InterfaceError("binary COPY stream has bytes after its trailer")

    def _read_row(self) -> Optional[bool]:
        head = self._take(2)
        if head is None:
            return None
        count = struct.unpack(">h", head)[0]
        if count == -1:
            self._done = True
            return True
        if count < 0:
            raise InterfaceError(f"binary COPY row has a field count of {count}")
        if self._width is None:
            self._width = count
        elif count != self._width:
            raise InterfaceError(f"binary COPY row has {count} fields, expected {self._width}")
        fields: List[Optional[bytes]] = []
        for _ in range(count):
            size = self._take(4)
            if size is None:
                return None
            length = struct.unpack(">i", size)[0]
            if length == -1:
                fields.append(None)
                continue
            if length < -1:
                raise InterfaceError(f"binary COPY field has a length of {length}")
            data = self._take(length)
            if data is None:
                return None
            fields.append(data)
        if self._wires is None:
            self.rows.append(tuple(fields))
        else:
            self.rows.append(
                tuple(
                    None if field is None else self._decode(field, wire)
                    for field, wire in zip(fields, self._wires)
                )
            )
        return True

    def finish(self) -> List[Tuple[Any, ...]]:
        """The rows read; raises the stream's fault, or :class:`InterfaceError` for a stream that
        ended early."""
        if self._error is not None:
            raise self._error
        if not self._done:
            raise InterfaceError("binary COPY stream ended without its trailer")
        return self.rows


def _base_wire(wire: str) -> str:
    """The type name a decoder is looked up under (``RANGE:INT`` reads as ``RANGE``)."""
    return "RANGE" if wire.startswith("RANGE") else wire
