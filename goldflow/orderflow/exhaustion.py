"""Phase 7: exhaustion candidates. A new extreme made on declining aggressive volume with a delta divergence.

On finalised flow bars of ``interval`` (default 1 m), for buyers (the mirror for sellers):
1. **Push.** A bar is an up-push when its high is above every high of the bars that started in the previous
   ``lookback_bars`` intervals (at least one such bar must exist).
2. **Sequence.** Take the last ``pushes`` up-pushes (k, default 3) that started within ``max_span_bars`` intervals of
   this one and lie in the same CVD reset instance (``cvd_reset``, default trading_day). Their highs must rise
   strictly (each push is a new extreme) and their aggressive **buy** lots must fall strictly (declining volume).
3. **Delta divergence at the extreme.** The CVD close of the last push is below the CVD close of the push before it:
   price made the higher high while cumulative delta made a lower one.
4. **Side quality.** No push may have an unknown-side share above ``max_unknown_share``.

The candidate (``EXHAUSTION`` / ``CANDIDATE``, side = the exhausted aggressor) is emitted at the last push's end and
then tracked for ``confirm_window``: CONFIRMED after a pullback of ``confirm_bins`` bins from the extreme, INVALIDATED if
price exceeds the extreme by ``inval_bins`` bins first. Trace: every trade of the push bars (the id range and the
evidence window are on the row; the pushes are in ``detail``).
"""

from __future__ import annotations

from collections import deque
from typing import Any

from .events import Tracker, event_row, worst_cov, worst_dq
from .flow_settings import ExhaustionParams


class ExhaustionDetector:
    def __init__(self, p: ExhaustionParams, interval_ms: int, max_unknown: Any, shift: int) -> None:
        self.p = p
        self.iv = p.interval
        self.I = interval_ms
        self.max_unknown = max_unknown
        self.shift = shift
        self.hist: deque[list[Any]] = deque()  # [start, high, low]
        self.pushes: dict[int, list[dict[str, Any]]] = {1: [], -1: []}
        self.stats = {"pushes": 0, "candidates": 0, "suppressed_unknown_side": 0}

    def discard(self) -> None:
        self.hist.clear()
        self.pushes = {1: [], -1: []}

    def evaluate(self, bar: dict[str, Any], cvd: int | None, cvd_key: str | None) -> list[Tracker]:
        p, I = self.p, self.I
        s = int(bar["bar_start"])
        while self.hist and self.hist[0][0] < s - p.lookback_bars * I:
            self.hist.popleft()
        prev = list(self.hist)
        self.hist.append([s, int(bar["high_ticks"]), int(bar["low_ticks"])])
        out: list[Tracker] = []
        if not prev:
            return out
        for side in (1, -1):
            ext = int(bar["high_ticks"]) if side > 0 else int(bar["low_ticks"])
            is_push = ext > max(h[1] for h in prev) if side > 0 else ext < min(h[2] for h in prev)
            ps = [x for x in self.pushes[side] if x["s"] >= s - (p.max_span_bars - 1) * I]
            if is_push:
                self.stats["pushes"] += 1
                ps.append({"s": s, "e": int(bar["bar_end"]), "ext": ext, "lots": int(bar["buy_lots"] if side > 0 else bar["sell_lots"]),
                           "cvd": cvd, "key": cvd_key, "unk": float(bar["unknown_share"]),
                           "unk_bad": bool(int(bar["volume_lots"]) and int(bar["unknown_lots"]) * self.max_unknown.denominator
                                           > self.max_unknown.numerator * int(bar["volume_lots"])),
                           "fid": int(bar["first_trade_id"]), "lid": int(bar["last_trade_id"]), "dq": bar["dq_status"],
                           "cov": bar["coverage"]})  # fmt: skip
            self.pushes[side] = ps[-(p.pushes + 4) :]
            if is_push and len(ps) >= p.pushes:
                t = self._test(ps[-p.pushes :], side)
                if t is not None:
                    out.append(t)
        return out

    def _test(self, P: list[dict[str, Any]], side: int) -> Tracker | None:
        p = self.p
        if any(x["cvd"] is None for x in P) or len({x["key"] for x in P}) != 1:
            return None
        rising = all((b["ext"] - a["ext"]) * side > 0 for a, b in zip(P, P[1:]))
        falling = all(b["lots"] < a["lots"] for a, b in zip(P, P[1:]))
        diverge = (P[-1]["cvd"] - P[-2]["cvd"]) * side < 0
        if not (rising and falling and diverge):
            return None
        if any(x["unk_bad"] for x in P):
            self.stats["suppressed_unknown_side"] += 1
            return None
        bt = p.bin_ticks
        ext = P[-1]["ext"]
        E = P[-1]["e"]
        conf = ext - side * p.confirm_bins * bt
        inval = ext + side * p.inval_bins * bt
        row = event_row(
            "EXHAUSTION", "CANDIDATE", "BUY" if side > 0 else "SELL", self.iv, bt, P[0]["s"], E, E, self.shift,
            ext if side < 0 else None, ext if side > 0 else None, ext, conf, inval, E + p.confirm_ms, P[-1]["lots"],
            min(x["fid"] for x in P), max(x["lid"] for x in P), max(x["unk"] for x in P),
            worst_dq([x["dq"] for x in P]), worst_cov([x["cov"] for x in P]),
            {"pushes": [{"bar_start": x["s"], "extreme_ticks": x["ext"], "aggressor_lots": x["lots"], "cvd": x["cvd"]} for x in P],
             "cvd_series": P[-1]["key"]},
        )  # fmt: skip
        self.stats["candidates"] += 1
        return Tracker(row, -side)

    def state(self) -> dict[str, Any]:
        return {"hist": list(self.hist), "pushes": {str(k): v for k, v in self.pushes.items()}, "stats": dict(self.stats)}

    def load(self, s: dict[str, Any]) -> None:
        self.hist = deque(list(x) for x in s["hist"])
        self.pushes = {int(k): [dict(x) for x in v] for k, v in s["pushes"].items()}
        self.stats = {**self.stats, **s["stats"]}
