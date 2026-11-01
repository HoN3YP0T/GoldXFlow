"""Phase 7: absorption candidates. Never "proven": a candidate becomes CONFIRMED only after the price response.

For each finalised bar of ``interval`` (default 1 m) and each aggressor side, from its exact footprint at base
resolution (1 s x 1 tick), re-binned to ``bin_ticks``:

1. **Zone.** zone(L) = aggressive lots of that side in bins [L - tol, L + tol]. L* is the bin with the largest zone
   volume; ties go to the bin nearer the aggressor's extreme (highest for buyers, lowest for sellers).
2. **Volume.** zone(L*) >= ``min_zone_lots`` and/or >= the ``zone_quantile`` of the same side's per-bar maximum zone
   volume over the previous ``quantile_hours`` (bars that started before this one). Every criterion that is set
   must pass; an unavailable quantile (fewer than ``min_samples`` bars) fails.
3. **No progress.** The bar's extreme in the aggressor's direction is at most ``max_progress_bins`` bins beyond the
   zone (buyers: high bin - (L* + tol) <= max_progress).
4. **Not one order.** The zone's aggressive lots come from at least ``min_active_seconds`` distinct base bars, and no
   single base bar holds more than ``max_second_share`` of them. A taker order is one millisecond (cluster_ms = 0),
   so it can never satisfy this alone: a single large trade is never labelled absorption.
5. **Side quality.** The bar's unknown-side share <= ``max_unknown_share``; otherwise the bar is not evaluated
   (counted as ``suppressed_unknown_side``).

The candidate is emitted at the bar end (``ABSORPTION`` / ``CANDIDATE``, side = the aggressor that was absorbed) and
tracked from there for ``confirm_window``:
* buyers absorbed: CONFIRMED once price trades ``confirm_bins`` bins below the zone's low; INVALIDATED once it trades
  in the bin above the allowed progress (L* + tol + max_progress + 1).
* sellers absorbed: the mirror.
Trace: the trades with time in [evidence_start, evidence_end), price in [level_low, level_high] and the event's side.
"""

from __future__ import annotations

from typing import Any

import numpy as np
from numpy.typing import NDArray

from .big_trades import RollingQuantile
from .events import Tracker, event_row
from .flow_settings import AbsorptionParams

I64 = np.int64


class AbsorptionDetector:
    def __init__(self, p: AbsorptionParams, interval_ms: int, base_ms: int, max_unknown: Any, shift: int) -> None:
        self.p = p
        self.I = interval_ms
        self.base_ms = base_ms
        self.max_unknown = max_unknown
        self.shift = shift
        self.acc: dict[int, list[dict[str, NDArray[Any]]]] = {}  # developing bucket -> base level rows
        self.rq = {s: RollingQuantile(p.quantile_ms, p.zone_quantile, p.min_samples) for s in (1, -1)} if p.zone_quantile else {}
        self.stats = {
            "bars_evaluated": 0,
            "candidates": 0,
            "suppressed_unknown_side": 0,
            "quantile_unavailable": 0,
            "rejected_volume": 0,
            "rejected_progress": 0,
            "rejected_single_burst": 0,
        }

    # ------------------------------------------------------------------ input
    def add_base_levels(self, levels: dict[str, Any]) -> None:
        s = np.asarray(levels["bar_start"], I64)
        if not len(s):
            return
        bk = s - s % self.I
        lv = np.asarray(levels["level"], I64) // self.p.bin_ticks
        buy = np.asarray(levels["buy_lots"], I64)
        sell = np.asarray(levels["sell_lots"], I64)
        fid = np.asarray(levels["first_trade_id"], I64)
        lid = np.asarray(levels["last_trade_id"], I64)
        for b in np.unique(bk):
            m = bk == b
            self.acc.setdefault(int(b), []).append(
                {"s": s[m], "bin": lv[m], "buy": buy[m], "sell": sell[m], "fid": fid[m], "lid": lid[m]}
            )

    def discard(self) -> None:
        self.acc = {}

    # ------------------------------------------------------------------ evaluation
    def evaluate(self, bar: dict[str, Any]) -> list[Tracker]:
        """Evaluate one finalised flow bar of the detector's interval. Returns the new trackers (their rows are the
        CANDIDATE events)."""
        B = int(bar["bar_start"])
        parts = self.acc.pop(B, [])
        for k in [k for k in self.acc if k < B]:
            del self.acc[k]
        if not parts:
            return []
        a = {k: np.concatenate([x[k] for x in parts]) for k in parts[0]}
        vol = int(bar["volume_lots"])
        if vol and int(bar["unknown_lots"]) * self.max_unknown.denominator > self.max_unknown.numerator * vol:
            self.stats["suppressed_unknown_side"] += 1
            return []
        self.stats["bars_evaluated"] += 1
        out = []
        p = self.p
        for side, lots in ((1, a["buy"]), (-1, a["sell"])):
            th, n = self.rq[side].threshold(B) if self.rq else (None, 0)
            zone = self._zone(a, lots, side)
            if zone is None:
                continue
            Lstar, zlots, msk = zone
            if self.rq:
                self.rq[side].add(B, np.array([zlots], I64))
            if p.min_zone_lots is not None and zlots < p.min_zone_lots:
                self.stats["rejected_volume"] += 1
                continue
            if self.rq:
                if th is None:
                    self.stats["quantile_unavailable"] += 1
                    continue
                if zlots < th:
                    self.stats["rejected_volume"] += 1
                    continue
            hb, lb = int(bar["high_ticks"]) // p.bin_ticks, int(bar["low_ticks"]) // p.bin_ticks
            progress = max(0, hb - (Lstar + p.tol_bins)) if side > 0 else max(0, (Lstar - p.tol_bins) - lb)
            if progress > p.max_progress_bins:
                self.stats["rejected_progress"] += 1
                continue
            secs, sl = np.unique(a["s"][msk], return_inverse=True)
            per = np.zeros(len(secs), I64)
            np.add.at(per, sl, lots[msk])
            active = int((per > 0).sum())
            mx = int(per.max())
            if active < p.min_active_seconds or mx * p.max_second_share.denominator > p.max_second_share.numerator * zlots:
                self.stats["rejected_single_burst"] += 1
                continue
            bt = p.bin_ticks
            lo_px, hi_px = (Lstar - p.tol_bins) * bt, (Lstar + p.tol_bins + 1) * bt - 1
            if side > 0:  # buyers absorbed: expect down
                ref, conf = lo_px, lo_px - p.confirm_bins * bt
                inval = (Lstar + p.tol_bins + p.max_progress_bins + 1) * bt
            else:
                ref, conf = hi_px, hi_px + p.confirm_bins * bt
                inval = (Lstar - p.tol_bins - p.max_progress_bins) * bt - 1
            E = int(bar["bar_end"])
            row = event_row(
                "ABSORPTION", "CANDIDATE", "BUY" if side > 0 else "SELL", bar["interval"], bt, B, E, E, self.shift,
                lo_px, hi_px, ref, conf, inval, E + p.confirm_ms, zlots,
                int(a["fid"][msk].min()), int(a["lid"][msk].max()), float(bar["unknown_share"]), bar["dq_status"], bar["coverage"],
                {"zone_lots": zlots, "bar_volume_lots": vol, "side_lots": int(lots.sum()), "threshold_lots": th,
                 "quantile_samples": n, "min_zone_lots": p.min_zone_lots, "progress_bins": progress,
                 "active_base_bars": active, "max_base_bar_lots": mx},
            )  # fmt: skip
            self.stats["candidates"] += 1
            out.append(Tracker(row, -side))
        return out

    def _zone(self, a: dict[str, NDArray[Any]], lots: NDArray[Any], side: int) -> tuple[int, int, NDArray[Any]] | None:
        has = lots > 0
        if not has.any():
            return None
        bins = a["bin"]
        lo, hi = int(bins[has].min()), int(bins[has].max())
        t = self.p.tol_bins
        dense = np.zeros(hi - lo + 1 + 2 * t, I64)
        np.add.at(dense, bins[has] - lo + t, lots[has])
        cs = np.concatenate([[0], np.cumsum(dense)])
        w = 2 * t + 1
        zone = cs[w:] - cs[:-w]  # zone[j] = sum of dense[j .. j + 2t] -> centre bin lo + j
        best = int(zone.max())
        js = np.flatnonzero(zone == best)
        j = int(js[-1] if side > 0 else js[0])
        Lstar = lo + j
        msk = has & (bins >= Lstar - t) & (bins <= Lstar + t)
        return Lstar, best, msk

    # ------------------------------------------------------------------ state
    def state(self) -> dict[str, Any]:
        return {
            "acc": [[b, [{k: v for k, v in x.items()} for x in xs]] for b, xs in sorted(self.acc.items())],
            "rq": {str(s): q.state() for s, q in self.rq.items()},
            "stats": dict(self.stats),
        }

    def load(self, s: dict[str, Any]) -> None:
        self.acc = {int(b): [{k: np.asarray(v, I64) for k, v in x.items()} for x in xs] for b, xs in s["acc"]}
        for k, v in s["rq"].items():
            self.rq[int(k)].load(v)
        self.stats = {**self.stats, **s["stats"]}
