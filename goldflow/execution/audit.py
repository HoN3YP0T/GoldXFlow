"""Phase 16: the execution audit log. It is append-only and hash-chained.

Every record has the shape ``{"i", "at", "kind", "data", "prev", "hash"}``, where
``hash = sha256(prev + canonical(i, at, kind, data))``. A deleted, reordered or edited line breaks the chain, and
``verify()`` finds it.

Kinds:
- ``OUT`` / ``IN``: every message, raw;
- ``REFUSED``: a message that failed authentication or schema;
- ``SIGNAL``;
- ``RISK``: the decision, with every failure;
- ``ORDER``: state changes;
- ``RECONCILE``;
- ``ALERT``;
- ``EMERGENCY``;
- ``START`` / ``STOP``;
- ``GATE``.

The Phase 16 deployment gate (``exec-gate``) reads its evidence from this log only.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterator


class AuditBroken(RuntimeError):
    pass


def _h(prev: str, i: int, at: int, kind: str, data: Any) -> str:
    body = json.dumps({"i": i, "at": at, "kind": kind, "data": data}, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256((prev + body).encode()).hexdigest()


class AuditLog:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.i, self.prev = 0, "0" * 64
        if self.path.exists():
            for r in self.records():
                self.i, self.prev = int(r["i"]), str(r["hash"])
        self._fh = open(self.path, "a", encoding="utf-8")

    def write(self, at_ms: int, kind: str, data: Any) -> None:
        self.i += 1
        h = _h(self.prev, self.i, int(at_ms), kind, data)
        rec = {"i": self.i, "at": int(at_ms), "kind": kind, "data": data, "prev": self.prev, "hash": h}
        self._fh.write(json.dumps(rec, separators=(",", ":"), default=str) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self.prev = h

    def records(self) -> Iterator[dict[str, Any]]:
        if not self.path.exists():
            return
        with open(self.path, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    yield json.loads(line)

    def verify(self) -> int:
        prev, n = "0" * 64, 0
        for r in self.records():
            n += 1
            if (
                r["i"] != n
                or r["prev"] != prev
                or _h(prev, r["i"], r["at"], r["kind"], json.loads(json.dumps(r["data"], default=str))) != r["hash"]
            ):
                raise AuditBroken(f"{self.path}: chain broken at record {n}")
            prev = r["hash"]
        return n

    def close(self) -> None:
        self._fh.close()
