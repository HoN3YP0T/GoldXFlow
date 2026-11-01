"""Phase 7: swing divergence between price and CVD (or bar delta). No repainting.

On finalised flow bars of ``interval`` (default 5 m), with R = ``swing_bars``:
* **Pivot high** at bar i: high(i) > every high of the R bars before it and >= every high of the R bars after it (the
  first bar of a flat top is the pivot). Pivot low is the mirror. The 2R + 1 bars must be contiguous in time (no
  interval without trades in between), otherwise there is no pivot.
* A pivot is only known once its R right-hand bars have closed: **confirmation time = end of bar i + R**. Nothing
  before that moment can use it.
* **Bearish divergence:** a confirmed pivot high H2 above the previous confirmed pivot high H1, at most
  ``max_gap_bars`` intervals earlier, while the measure is lower at H2 than at H1. Measure = CVD close of the pivot
  bar in the ``cvd_reset`` instance (both pivots must be in the same instance) or the pivot bar's delta. Bullish is
  the mirror on pivot lows.
* The event is emitted at the confirmation time with status CONFIRMED, side BEARISH or BULLISH. It is then tracked
  for ``track_bars`` intervals: INVALIDATED once price exceeds the H2 extreme by ``inval_bins`` bins, else EXPIRED.
Trace: the trades of the bars in [evidence_start, evidence_end) (the earlier pivot through the confirming bar).
"""

from __future__ import annotations

from collections import deque
from typing import Any

from .events import Tracker, event_row, worst_cov, worst_dq
from .flow_settings import DivergenceParams


class DivergenceDetector:
    def __init__(self, p: DivergenceParams, interval_ms: int, max_unknown: Any, shift: int) -> None:
        self.p = p
        self.I = interval_ms
        self.max_unknown = max_unknown
        self.shift = shift
        self.win: deque[dict[str, Any]] = deque(maxlen=2 * p.swing_bars + 1)
        self.last: dict[int, dict[str, Any] | None] = {1: None, -1: None}  # last confirmed pivot high (+1) / low (-1)
        self.stats = {"pivots": 0, "divergences": 0, "suppressed_unknown_side": 0}

    def discard(self) -> None:
        self.win.clear()
        self.last = {1: None, -1: None}

    def evaluate(self, bar: dict[str, Any], cvd: int | None, cvd_key: str | None) -> list[Tracker]:
        p, I, R = self.p, self.I, self.p.swing_bars
        v = int(bar["volume_lots"])
        self.win.append({"s": int(bar["bar_start"]), "e": int(bar["bar_end"]), "hi": int(bar["high_ticks"]),
                         "lo": int(bar["low_ticks"]), "cvd": cvd, "key": cvd_key, "delta": int(bar["delta_lots"]),
                         "unk": float(bar["unknown_share"]), "fid": int(bar["first_trade_id"]), "lid": int(bar["last_trade_id"]),
                         "unk_bad": bool(v and int(bar["unknown_lots"]) * self.max_unknown.denominator > self.max_unknown.numerator * v),
                         "dq": bar["dq_status"], "cov": bar["coverage"]})  # fmt: skip
        if len(self.win) < 2 * R + 1:
            return []
        w = list(self.win)
        if w[-1]["s"] - w[0]["s"] != 2 * R * I:
            return []
        c = w[R]
        out = []
        for kind in (1, -1):
            if kind > 0:
                piv = c["hi"] > max(x["hi"] for x in w[:R]) and c["hi"] >= max(x["hi"] for x in w[R + 1 :])
            else:
                piv = c["lo"] < min(x["lo"] for x in w[:R]) and c["lo"] <= min(x["lo"] for x in w[R + 1 :])
            if not piv:
                continue
            self.stats["pivots"] += 1
            m = c["cvd"] if p.measure == "cvd" else c["delta"]
            cur = {**c, "m": m, "ext": c["hi"] if kind > 0 else c["lo"], "window": w}
            prev = self.last[kind]
            self.last[kind] = {k: v for k, v in cur.items() if k != "window"}
            if prev is None or m is None or prev["m"] is None or c["s"] - prev["s"] > p.max_gap_bars * I:
                continue
            if p.measure == "cvd" and prev["key"] != c["key"]:
                continue
            if not ((cur["ext"] - prev["ext"]) * kind > 0 and (m - prev["m"]) * kind < 0):
                continue
            if prev["unk_bad"] or c["unk_bad"]:
                self.stats["suppressed_unknown_side"] += 1
                continue
            out.append(self._event(prev, cur, w[-1], kind))
        return out

    def _event(self, prev: dict[str, Any], cur: dict[str, Any], conf_bar: dict[str, Any], kind: int) -> Tracker:
        p = self.p
        bt = p.bin_ticks
        E = conf_bar["e"]
        inval = cur["ext"] + kind * p.inval_bins * bt
        side = "BEARISH" if kind > 0 else "BULLISH"
        bars = [prev] + cur["window"][p.swing_bars :]
        row = event_row(
            "DIVERGENCE", "CONFIRMED", side, p.interval, bt, prev["s"], E, E, self.shift,
            cur["ext"] if kind < 0 else None, cur["ext"] if kind > 0 else None, cur["ext"], None, inval,
            E + p.track_bars * self.I, None, prev["fid"], conf_bar["lid"], max(prev["unk"], cur["unk"]),
            worst_dq([b["dq"] for b in bars]), worst_cov([b["cov"] for b in bars]),
            {"measure": p.measure, "cvd_series": cur["key"] if p.measure == "cvd" else None,
             "pivot_1": {"bar_start": prev["s"], "extreme_ticks": prev["ext"], "measure": prev["m"]},
             "pivot_2": {"bar_start": cur["s"], "extreme_ticks": cur["ext"], "measure": cur["m"]},
             "confirmed_by_bar": conf_bar["s"]},
        )  # fmt: skip
        self.stats["divergences"] += 1
        return Tracker(row, -kind)

    def state(self) -> dict[str, Any]:
        return {"win": list(self.win), "last": {str(k): v for k, v in self.last.items()}, "stats": dict(self.stats)}

    def load(self, s: dict[str, Any]) -> None:
        self.win = deque((dict(x) for x in s["win"]), maxlen=2 * self.p.swing_bars + 1)
        self.last = {int(k): (dict(v) if v else None) for k, v in s["last"].items()}
        self.stats = {**self.stats, **s["stats"]}
