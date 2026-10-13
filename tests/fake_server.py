"""A scripted stand-in for ``nusadb-server`` that speaks just enough of the wire protocol to test
what a real server cannot be made to do on demand: notices in every kind of reply, two-phase
commit, and the protocol minor a client asks for."""

from __future__ import annotations

import socket
import struct
import threading
import time
from typing import Dict, List, Optional, Tuple

TEXT, TIMESTAMPTZ, INT = 0x05, 0x0B, 0x02


def frame(kind: str, payload: bytes = b"") -> bytes:
    return struct.pack(">BI", ord(kind), len(payload) + 5) + payload


def text(value: str) -> bytes:
    data = value.encode("utf-8")
    return struct.pack(">I", len(data)) + data


def notice(message: str, severity: str = "NOTICE", code: str = "00000") -> bytes:
    return frame("N", text(severity) + text(code) + text(message))


def error(code: str, message: str) -> bytes:
    return frame("E", text(code) + text(message))


def complete(tag: str) -> bytes:
    return frame("C", text(tag))


def columns(*described: Tuple[str, int]) -> bytes:
    body = struct.pack(">H", len(described))
    for name, tag in described:
        body += text(name) + bytes([tag])
    return frame("y", body)


def data_row(*values: Optional[str]) -> bytes:
    body = struct.pack(">H", len(values))
    for value in values:
        if value is None:
            body += b"\x00"
        else:
            raw = value.encode("utf-8")
            body += b"\x01" + struct.pack(">I", len(raw)) + raw
    return frame("D", body)


class FakeServer:
    """Listens on an ephemeral port; each connection gets its own session. ``queries`` records
    every SQL text received, ``minors`` the protocol minor each client asked for, and ``prepared``
    the transactions prepared and not yet finished (gid -> owner)."""

    def __init__(self) -> None:
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(8)
        self.port = self._listener.getsockname()[1]
        self.queries: List[str] = []
        self.minors: List[int] = []
        self.prepared: Dict[str, str] = {}
        self._lock = threading.Lock()
        self._stopped = False
        self._sockets: List[socket.socket] = []
        threading.Thread(target=self._accept, daemon=True).start()

    def stop(self) -> None:
        self._stopped = True
        self._listener.close()
        for sock in self._sockets:
            try:
                sock.close()
            except OSError:
                pass

    def _accept(self) -> None:
        while not self._stopped:
            try:
                sock, _ = self._listener.accept()
            except OSError:
                return
            self._sockets.append(sock)
            threading.Thread(target=self._serve, args=(sock,), daemon=True).start()

    @staticmethod
    def _read(sock: socket.socket, n: int) -> bytes:
        out = b""
        while len(out) < n:
            chunk = sock.recv(n - len(out))
            if not chunk:
                raise ConnectionError("client went away")
            out += chunk
        return out

    def _message(self, sock: socket.socket) -> Tuple[str, bytes]:
        kind, length = struct.unpack(">BI", self._read(sock, 5))
        return chr(kind), self._read(sock, length - 5)

    def _serve(self, sock: socket.socket) -> None:
        try:
            kind, payload = self._message(sock)
            assert kind == "S"
            self.minors.append(struct.unpack(">IHH", payload[:8])[2])
            user_len = struct.unpack(">I", payload[8:12])[0]
            user = payload[12 : 12 + user_len].decode()
            sock.sendall(
                frame("R", struct.pack(">I", 0))
                + frame("K", struct.pack(">II", 7, 99))
                + notice("welcome", "INFO")
                + frame("Z", b"I")
            )
            session = _Session(self, sock, user)
            while True:
                kind, payload = self._message(sock)
                if kind == "X":
                    return
                if kind == "Q":
                    sql = payload[4:].decode("utf-8")
                    with self._lock:
                        self.queries.append(sql)
                    session.query(sql)
                elif kind == "Y":
                    session.send(session.ready())
                else:
                    session.extended(kind)
        except (ConnectionError, OSError, AssertionError):
            pass
        finally:
            sock.close()


class _Session:
    def __init__(self, server: FakeServer, sock: socket.socket, user: str) -> None:
        self.server = server
        self.sock = sock
        self.user = user
        self.status = "I"

    def ready(self) -> bytes:
        return frame("Z", self.status.encode())

    def send(self, data: bytes) -> None:
        self.sock.sendall(data)

    def query(self, sql: str) -> None:
        upper = sql.upper()
        out = b""
        if upper == "BEGIN":
            if self.status != "I":
                out = notice("there is already a transaction in progress", "WARNING", "25001")
            self.status = "T"
            out += complete("BEGIN")
        elif upper in ("COMMIT", "ROLLBACK"):
            tag = "ROLLBACK" if self.status == "E" else upper
            self.status = "I"
            out = complete(tag)
        elif upper.startswith("PREPARE TRANSACTION "):
            gid = _literal(sql[len("PREPARE TRANSACTION ") :])
            if self.status == "I":
                out = error("25P01", "PREPARE TRANSACTION can only be used in a transaction block")
            else:
                with self.server._lock:
                    self.server.prepared[gid] = self.user
                self.status = "I"
                out = complete("PREPARE TRANSACTION")
        elif upper.startswith(("COMMIT PREPARED ", "ROLLBACK PREPARED ")):
            action, _, rest = sql.partition(" PREPARED ")
            gid = _literal(rest)
            if self.status != "I":
                out = error("25001", f"{action} PREPARED cannot run inside a transaction block")
                self.status = "E"
            elif self.server.prepared.pop(gid, None) is None:
                out = error("42704", f"prepared transaction with identifier {gid!r} does not exist")
            else:
                out = complete(f"{action.upper()} PREPARED")
        elif upper.startswith("SELECT GID, PREPARED, OWNER, DATABASE FROM NUSADB_PREPARED_XACTS"):
            out = columns(
                ("gid", TEXT), ("prepared", TIMESTAMPTZ), ("owner", TEXT), ("database", TEXT)
            )
            for gid, owner in sorted(self.server.prepared.items()):
                out += data_row(gid, "2024-01-02 03:04:05+00", owner, "nusadb")
            out += complete(f"SELECT {len(self.server.prepared)}")
        elif upper == "SELECT NOTICES":
            out = (
                notice("before the rows")
                + columns(("n", INT))
                + data_row("1")
                + notice("between the rows", "WARNING", "01000")
                + data_row("2")
                + notice("after the rows")
                + complete("SELECT 2")
            )
        elif upper == "FAIL WITH A NOTICE":
            out = notice("about to fail") + error("22012", "division by zero")
            if self.status == "T":
                self.status = "E"
        elif upper.startswith("COPY T TO STDOUT"):
            self.send(frame("H", b"\x00\x00\x00") + frame("d", b"1\n") + notice("mid copy"))
            out = frame("d", b"2\n") + frame("c") + notice("copy done") + complete("COPY 2")
        elif upper.startswith("COPY T FROM STDIN"):
            self.send(frame("G", b"\x00\x00\x00") + notice("send the rows"))
            rows = 0
            while True:
                kind, payload = self.server._message(self.sock)
                if kind == "d":
                    rows += payload.count(b"\n")
                elif kind in ("c", "f"):
                    break
            out = notice("rows loaded") + complete(f"COPY {rows}")
        elif upper.startswith("LISTEN "):
            self.send(complete("LISTEN") + self.ready())
            time.sleep(0.2)
            # A notice and then the notification, on an otherwise idle connection.
            self.send(
                notice("idle notice") + frame("A", struct.pack(">I", 7) + text("ch") + text("hi"))
            )
            return
        else:
            if self.status == "E":
                out = error("25P02", "current transaction is aborted")
            else:
                out = complete("SELECT 0")
        self.send(out + self.ready())

    def extended(self, kind: str) -> None:
        """Answer one extended-query message as it arrives (``Sync`` sends ReadyForQuery)."""
        if kind == "P":
            self.send(frame("1"))
        elif kind == "B":
            self.send(frame("2"))
        elif kind == "D":
            self.send(notice("described") + columns(("n", INT)))
        elif kind == "E":
            self.send(data_row("5") + notice("executed", "WARNING", "01000") + complete("SELECT 1"))
        elif kind == "C":
            self.send(frame("3"))


def _literal(text_: str) -> str:
    text_ = text_.strip()
    assert text_.startswith("'") and text_.endswith("'"), text_
    return text_[1:-1].replace("''", "'")
