"""Phase 13: the research ledger and the test-set lock.

**Research ledger** (``data/research/ledger.jsonl``): append-only, one JSON object per line, each carrying the SHA-256
of the previous line (a hash chain, so an edited or deleted entry is detected). Entry kinds:
* ``HYPOTHESIS``: written **before** results are seen: id, statement, strategy, parameter grid, data windows, success
  criteria. A result can only refer to a hypothesis that was registered earlier.
* ``RESULT``: run ids and metrics for a registered hypothesis.
* ``REJECTED_VARIANT``: a variant that was tried and dropped, with the reason (kept to limit selection bias).
* ``DECISION``: what was decided and why.

**Test-set lock** (``data/research/test_lock.json``): the untouched test period is opened **once**, for one final
candidate (identified by the hash of its strategy, parameters and settings). Opening it again for the same candidate
(e.g. to regenerate a report) is allowed and recorded; opening it for any other candidate is refused
(``TestSetReused``). Walk-forward and optimisation refuse any range that overlaps the test period.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

KINDS = ("HYPOTHESIS", "RESULT", "REJECTED_VARIANT", "DECISION")


class LedgerError(RuntimeError):
    pass


class TestSetReused(RuntimeError):
    __test__ = False  # not a pytest test class


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class ResearchLedger:
    def __init__(self, root: Path) -> None:
        self.path = Path(root) / "ledger.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def entries(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        return [json.loads(x) for x in self.path.read_text().splitlines() if x.strip()]

    def verify(self) -> list[dict[str, Any]]:
        """Check the hash chain and the ordering rules; returns the entries."""
        prev = "0" * 64
        seen: set[str] = set()
        out = self.entries()
        for i, e in enumerate(out):
            body = {k: v for k, v in e.items() if k != "hash"}
            if e.get("prev") != prev or hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest() != e.get("hash"):
                raise LedgerError(f"research ledger entry {i} was modified or the chain is broken")
            if e["kind"] == "HYPOTHESIS":
                seen.add(e["id"])
            elif e["kind"] in ("RESULT", "REJECTED_VARIANT") and e.get("hypothesis") not in seen:
                raise LedgerError(
                    f"entry {i}: {e['kind']} for hypothesis {e.get('hypothesis')} that was not registered before it"
                )
            prev = e["hash"]
        return out

    def append(self, kind: str, **fields: Any) -> dict[str, Any]:
        if kind not in KINDS:
            raise LedgerError(f"unknown ledger kind {kind}")
        es = self.verify()
        if kind == "HYPOTHESIS":
            if any(e["kind"] == "HYPOTHESIS" and e["id"] == fields.get("id") for e in es):
                raise LedgerError(f"hypothesis {fields.get('id')} already registered")
            for k in ("id", "statement", "strategy", "success_criteria"):
                if not fields.get(k):
                    raise LedgerError(f"a hypothesis needs '{k}' (written before any result is seen)")
        elif kind in ("RESULT", "REJECTED_VARIANT") and not any(
            e["kind"] == "HYPOTHESIS" and e["id"] == fields.get("hypothesis") for e in es
        ):
            raise LedgerError(f"register hypothesis {fields.get('hypothesis')} before recording a {kind}")
        e = {"seq": len(es), "kind": kind, "time": _now(), "prev": es[-1]["hash"] if es else "0" * 64, **fields}
        e["hash"] = hashlib.sha256(json.dumps(e, sort_keys=True, default=str).encode()).hexdigest()
        with self.path.open("a") as f:
            f.write(json.dumps(e, sort_keys=True, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())
        return e


class TestSetLock:
    __test__ = False  # not a pytest test class

    def __init__(self, root: Path, test_range: tuple[int, int]) -> None:
        self.path = Path(root) / "test_lock.json"
        self.range = test_range

    def status(self) -> dict[str, Any] | None:
        return json.loads(self.path.read_text()) if self.path.exists() else None

    def guard(self, a: int, b: int, what: str) -> None:
        """Refuse any optimisation or walk-forward range that touches the test period."""
        if a < self.range[1] and b > self.range[0]:
            raise TestSetReused(f"{what} [{a}, {b}) overlaps the untouched test period; it may only be used through open_for()")

    def open_for(self, candidate_hash: str, reason: str) -> dict[str, Any]:
        st = self.status()
        if st is not None and st["candidate_hash"] != candidate_hash:
            raise TestSetReused(f"the test set was already opened on {st['opened_utc']} for candidate {st['candidate_hash'][:12]}; "
                                "it cannot be reused for another candidate")  # fmt: skip
        st = st or {
            "candidate_hash": candidate_hash,
            "opened_utc": _now(),
            "reason": reason,
            "range": list(self.range),
            "accesses": [],
        }
        st["accesses"].append({"time": _now(), "reason": reason})
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(json.dumps(st, indent=1))
        os.replace(tmp, self.path)
        return st
