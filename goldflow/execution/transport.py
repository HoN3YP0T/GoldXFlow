"""Phase 16: transports for the bridge.

The engine-facing interface is the same for every transport: ``send(line)`` and ``receive() -> list[line]``
(non-blocking), plus ``connected``.
- **TcpServer** listens on a loopback address. MQL5 sockets can only connect, never listen, so the **EA is the
  client**. One EA connection is accepted at a time; a new connection replaces the old one, as after an EA restart.
- **FileQueue** is the fallback. Messages are files in ``<dir>/to_ea/`` and ``<dir>/to_py/`` (the MT5 Common Files
  folder ``goldflow``), written to a temporary name and then renamed, so a reader never sees a partial message. Names
  are ``<seq:012d>.msg``. The reader deletes each file after reading it.
- **Pipe** joins the engine to the simulated EA in memory (tests, fault injection).
"""

from __future__ import annotations

import os
import selectors
import socket
from collections import deque
from pathlib import Path


class TcpServer:
    def __init__(self, host: str, port: int) -> None:
        self.sock = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.sock.listen(1)
        self.sock.setblocking(False)
        self.port = self.sock.getsockname()[1]
        self.conn: socket.socket | None = None
        self.buf = b""
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.sock, selectors.EVENT_READ)
        self.connections = 0

    @property
    def connected(self) -> bool:
        return self.conn is not None

    def _accept(self) -> None:
        for key, _ in self.sel.select(timeout=0):
            if key.fileobj is self.sock:
                c, _ = self.sock.accept()
                c.setblocking(False)
                if self.conn is not None:
                    self.sel.unregister(self.conn)
                    self.conn.close()
                self.conn, self.buf = c, b""
                self.sel.register(c, selectors.EVENT_READ)
                self.connections += 1

    def send(self, line: str) -> bool:
        self._accept()
        if self.conn is None:
            return False
        try:
            self.conn.sendall(line.encode("ascii"))
            return True
        except OSError:
            self._drop()
            return False

    def receive(self) -> list[str]:
        self._accept()
        if self.conn is None:
            return []
        try:
            while True:
                b = self.conn.recv(65536)
                if not b:
                    self._drop()
                    break
                self.buf += b
        except BlockingIOError:
            pass
        except OSError:
            self._drop()
        out = []
        while b"\n" in self.buf:
            ln, self.buf = self.buf.split(b"\n", 1)
            out.append(ln.decode("ascii", "replace") + "\n")
        return out

    def _drop(self) -> None:
        if self.conn is not None:
            try:
                self.sel.unregister(self.conn)
            except (KeyError, ValueError):
                pass
            self.conn.close()
        self.conn = None

    def close(self) -> None:
        self._drop()
        self.sel.close()
        self.sock.close()


class FileQueue:
    def __init__(self, root: Path, me: str = "py") -> None:
        self.root = Path(root)
        self.out_dir = self.root / ("to_ea" if me == "py" else "to_py")
        self.in_dir = self.root / ("to_py" if me == "py" else "to_ea")
        for d in (self.out_dir, self.in_dir):
            d.mkdir(parents=True, exist_ok=True)
        existing = sorted(self.out_dir.glob("*.msg"))
        self.n = int(existing[-1].stem) if existing else 0
        self.connected = True

    def send(self, line: str) -> bool:
        self.n += 1
        tmp = self.out_dir / f"{self.n:012d}.tmp"
        tmp.write_text(line, encoding="ascii")
        os.replace(tmp, self.out_dir / f"{self.n:012d}.msg")
        return True

    def receive(self) -> list[str]:
        out = []
        for p in sorted(self.in_dir.glob("*.msg")):
            out.append(p.read_text(encoding="ascii"))
            p.unlink()
        return out

    def close(self) -> None:
        pass


class Pipe:
    """In-memory duplex channel: ``a`` for the engine, ``b`` for the simulated EA."""

    class End:
        def __init__(self, inbox: deque[str], outbox: deque[str], pipe: "Pipe") -> None:
            self.inbox, self.outbox, self.pipe = inbox, outbox, pipe

        @property
        def connected(self) -> bool:
            return self.pipe.up

        def send(self, line: str) -> bool:
            if not self.pipe.up:
                return False
            self.outbox.append(line)
            return True

        @property
        def connections(self) -> int:
            return self.pipe.connections

        def receive(self) -> list[str]:
            if not self.pipe.up:
                return []
            out = list(self.inbox)
            self.inbox.clear()
            return out

        def close(self) -> None:
            pass

    def __init__(self) -> None:
        q1: deque[str] = deque()
        q2: deque[str] = deque()
        self.up = True
        self.connections = 1  # incremented by the simulated EA when it reconnects or restarts
        self.a, self.b = Pipe.End(q2, q1, self), Pipe.End(q1, q2, self)
