"""Phase 7: event lifecycle shared by absorption, exhaustion and divergence.

An event is first emitted when its evidence is complete (``CANDIDATE`` for absorption and exhaustion; ``CONFIRMED``
for a divergence, whose swing is confirmed by its right-hand bars). It is then tracked on the processor's finalised
base-interval bars (default 1 s), strictly **after** the evidence window, until one of:

* ``CONFIRMED``: price reached the confirmation level (the expected response happened);
* ``INVALIDATED``: price reached the invalidation level first. If both levels are touched inside the same base bar,
  the order inside that bar is unknown, so the event is INVALIDATED with reason ``both_in_same_base_bar``. It is
  never claimed as confirmed on ambiguous evidence;
* ``EXPIRED``: neither happened before the deadline.

The resolution is stamped at the end of the base bar in which it became known (``event_time``) and is available at
``event_time + trade shift``. Since tracking starts at the evidence window end, ``event_time >= evidence_end`` always
holds: there is no back-dating. Each lifecycle step is its own row with the same ``event_id``.
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np
from numpy.typing import NDArray

DQ_ORDER = ("OK", "UNVERIFIED", "PARTIAL", "MISSING")


def worst_dq(xs: list[str]) -> str:
    return max(xs, key=lambda s: DQ_ORDER.index(s) if s in DQ_ORDER else 3) if xs else "OK"


def worst_cov(xs: list[str]) -> str:
    order = ("FULL", "FILTERED", "PARTIAL_START")
    return max(xs, key=lambda s: order.index(s) if s in order else 2) if xs else "FULL"


def event_row(
    event_type: str,
    status: str,
    side: str,
    interval: str,
    bin_ticks: int,
    evidence_start: int,
    evidence_end: int,
    detected_at: int,
    shift: int,
    level_low: int | None,
    level_high: int | None,
    reference: int | None,
    confirm: int | None,
    invalidate: int | None,
    deadline: int,
    lots: int | None,
    first_id: int | None,
    last_id: int | None,
    unknown_share: float,
    dq: str,
    coverage: str,
    detail: dict[str, Any],
) -> dict[str, Any]:
    eid = f"{event_type}:{side}:{interval}:{detected_at}"
    return {
        "event_id": eid,
        "event_type": event_type,
        "status": status,
        "side": side,
        "interval": interval,
        "bin_ticks": bin_ticks,
        "evidence_start": evidence_start,
        "evidence_end": evidence_end,
        "detected_at": detected_at,
        "event_time": detected_at,
        "available_at": detected_at + shift,
        "level_low_ticks": level_low,
        "level_high_ticks": level_high,
        "reference_ticks": reference,
        "confirm_ticks": confirm,
        "invalidate_ticks": invalidate,
        "deadline": deadline,
        "resolution_price_ticks": None,
        "reason": None,
        "lots": lots,
        "first_trade_id": first_id,
        "last_trade_id": last_id,
        "unknown_share": unknown_share,
        "dq_status": dq,
        "coverage": coverage,
        "detail": json.dumps(detail, sort_keys=True),
    }


class Tracker:
    """Follows one event after its evidence window. ``direction`` is the expected price response: -1 = down (buyers
    absorbed or exhausted, bearish divergence), +1 = up."""

    __slots__ = ("row", "direction", "confirm", "invalidate", "next_s", "deadline")

    def __init__(self, row: dict[str, Any], direction: int) -> None:
        self.row = row
        self.direction = direction
        self.confirm = row["confirm_ticks"]
        self.invalidate = row["invalidate_ticks"]
        self.next_s = int(row["evidence_end"])
        self.deadline = int(row["deadline"])

    def scan(self, s: NDArray[Any], hi: NDArray[Any], lo: NDArray[Any], base_ms: int, upto: int, shift: int
             ) -> dict[str, Any] | None:  # fmt: skip
        """Scan finalised base bars (starts ``s`` sorted) with next_s <= start < deadline and end <= upto."""
        a = int(np.searchsorted(s, self.next_s, "left"))
        b = int(np.searchsorted(s, min(self.deadline, upto - base_ms + 1), "left"))
        if b > a:
            h, lw = hi[a:b], lo[a:b]
            if self.direction < 0:
                c = lw <= self.confirm if self.confirm is not None else np.zeros(b - a, bool)
                v = h >= self.invalidate if self.invalidate is not None else np.zeros(b - a, bool)
            else:
                c = h >= self.confirm if self.confirm is not None else np.zeros(b - a, bool)
                v = lw <= self.invalidate if self.invalidate is not None else np.zeros(b - a, bool)
            hit = c | v
            if hit.any():
                i = int(np.argmax(hit))
                both = bool(c[i] and v[i])
                status = "CONFIRMED" if (c[i] and not v[i]) else "INVALIDATED"
                px = int(lw[i] if (self.direction < 0) == (status == "CONFIRMED") else h[i])
                return self._resolve(status, int(s[a + i]) + base_ms, shift, px, "both_in_same_base_bar" if both else None)
            self.next_s = int(s[b - 1]) + base_ms
        if upto >= self.deadline:
            return self._resolve("EXPIRED", self.deadline, shift, None, None)
        self.next_s = max(self.next_s, min(upto, self.deadline))
        return None

    def _resolve(self, status: str, t: int, shift: int, px: int | None, reason: str | None) -> dict[str, Any]:
        r = dict(self.row)
        r.update(status=status, event_time=t, available_at=t + shift, resolution_price_ticks=px, reason=reason)
        return r

    def state(self) -> list[Any]:
        return [self.row, self.direction, self.next_s]

    @classmethod
    def load(cls, s: list[Any]) -> "Tracker":
        t = cls(dict(s[0]), int(s[1]))
        t.next_s = int(s[2])
        return t
