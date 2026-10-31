"""Absorption candidates and their confirmation (Phase 7). Definitions: docs/METRICS_ORDERFLOW_V1.md.

Evaluated at the end E of every finalised bar of `interval`, over the trailing window W = window_bars * interval,
i.e. [E - W, E), in price bins of `bin_ticks`. For aggressive BUYS (absorbed by resting sellers; SELL mirrors):
  1. zone(b) = Σ buy lots in bins [b - tol, b + tol] over W; L = the bin with the largest zone (lowest bin on ties)
  2. effort:   zone(L) >= abs_lots, or >= the trailing nearest-rank quantile of this per-bar maximum
  3. no result: highest traded bin in W - L <= max_progress_bins
  4. not one trade: >= min_orders reconstructed taker BUY orders touching the zone, completed within W, and the
     largest of them <= max_single_order_frac * zone(L). A single large trade is never absorption.
=> ABSORPTION CANDIDATE, event time E + shift, evidence [E - W, E).
Each later bar (start >= E), until E + confirm_window_ms:
  low bin <= L - confirm_bins                         => CONFIRMED (event time = that bar's end + shift)
  high bin > L + max_progress_bins + break_bins       => FAILED (the level gave way)
  both in the same bar (order inside the bar unknown) => AMBIGUOUS (not confirmed)
  nothing by the deadline                             => EXPIRED (event time = deadline + shift)
Confirmation is never back-dated: its time is always after the evidence window end.
"""

from __future__ import annotations

import json
from collections import deque
from fractions import Fraction
from typing import Any

import numpy as np
from numpy.typing import NDArray

from .delta import rebin_levels
from .rolling import RollingQuantile

SIDES = ((1, "BUY"), (-1, "SELL"))


class AbsorptionDetector:
    def __init__(self, cfg: dict[str, Any], interval: str, interval_ms: int, src_bin: int, shift_ms: int, cluster_ms: int):
        self.cfg, self.interval, self.I, self.src_bin, self.shift = cfg, interval, interval_ms, src_bin, shift_ms
        self.cluster_ms = cluster_ms
        self.bin = int(cfg["bin_ticks"])
        self.W = int(cfg["window_bars"]) * interval_ms
        self.tol = int(cfg["tol_bins"])
        self.maxfrac = Fraction(str(cfg["max_single_order_frac"]))
        qc = cfg["quantile"]
        self.rq = (
            {
                s: RollingQuantile(
                    Fraction(str(qc["q"])), int(qc["lookback_hours"]) * 3_600_000, int(qc["min_samples"]), int(qc["update_ms"])
                )
                for s, _ in SIDES
            }
            if qc["enabled"]
            else None
        )
        self.bars: deque[dict[str, Any]] = deque()
        self.orders: deque[tuple[int, int, int, int, int, int]] = deque()  # (end_t, first_t, side, min_bin, max_bin, size)
        self.cands: list[dict[str, Any]] = []
        self.prev_floor: int | None = None
        self.observe_from: int | None = None
        self.stats = {"evaluations": 0, "partial_window_skipped": 0, "threshold_not_ready": 0, "candidates": 0}

    # ---------------------------------------------------------------- inputs
    def add_orders(self, o: dict[str, NDArray[Any]]) -> None:
        for i in range(len(o["size"])):
            self.orders.append(
                (
                    int(o["last_t"][i]) + self.cluster_ms + 1,
                    int(o["first_t"][i]),
                    int(o["side"][i]),
                    int(o["min_p"][i]) // self.bin,
                    int(o["max_p"][i]) // self.bin,
                    int(o["size"][i]),
                )
            )

    def on_advance(self, floor: int) -> list[dict[str, Any]]:
        out = self._expire(self.prev_floor) if self.prev_floor is not None else []
        self.prev_floor = floor
        return out

    def on_bars(self, bars: dict[str, Any], levels: dict[str, Any], final_upto: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        r = rebin_levels(levels, self.src_bin, self.bin)
        bs = np.asarray(bars["bar_start"], np.int64)
        for k in range(len(bs)):
            s, e = int(bs[k]), int(bars["bar_end"][k])
            a, b = np.searchsorted(r["bar_start"], s, "left"), np.searchsorted(r["bar_start"], s, "right")
            bar = {
                "start": s,
                "end": e,
                "hi": int(bars["high_ticks"][k]) // self.bin,
                "lo": int(bars["low_ticks"][k]) // self.bin,
                **{c: r[c][a:b] for c in ("bin", "buy", "sell", "unk", "first_id", "last_id")},
            }
            out += self._update_candidates(bar)
            self.bars.append(bar)
            while self.bars and self.bars[0]["start"] < e - self.W:
                self.bars.popleft()
            while self.orders and self.orders[0][0] < e - self.W:
                self.orders.popleft()
            out += self._evaluate(e)
        out += self._expire(final_upto)
        return out

    # ---------------------------------------------------------------- detection
    def _evaluate(self, E: int) -> list[dict[str, Any]]:
        self.stats["evaluations"] += 1
        win = list(self.bars)
        bins = np.concatenate([x["bin"] for x in win])
        if len(bins) == 0:
            return []
        lo, hi = int(bins.min()), int(bins.max())
        span = hi - lo + 1
        rel = bins - lo
        # exact integer sums (bincount weights are float64; integer sums below 2**53 are exact)
        dense_by = {
            c: np.bincount(rel, weights=np.concatenate([x[c] for x in win]), minlength=span).astype(np.int64)
            for c in ("buy", "sell", "unk")
        }
        unk = dense_by["unk"]
        idx = np.arange(span)
        a_ = np.maximum(idx - self.tol, 0)
        b_ = np.minimum(idx + self.tol, span - 1) + 1
        out = []
        for sgn, name in SIDES:
            dense = dense_by["buy" if sgn > 0 else "sell"]
            cs = np.concatenate([[0], np.cumsum(dense)])
            zone = cs[b_] - cs[a_]  # zone(b) = Σ dense over [b - tol, b + tol] (clipped to the traded range)
            j = int(np.argmax(zone))
            L, vol = lo + j, int(zone[j])
            thr = self.cfg["abs_lots"]
            thr_q = None
            if self.rq is not None:
                thr_q, nq = self.rq[sgn].threshold(E)
                self.rq[sgn].add(E, vol)  # sample after evaluation
                if thr is None:
                    thr = thr_q
            if self.observe_from is not None and E - self.W < self.observe_from:
                self.stats["partial_window_skipped"] += 1
                continue
            if thr is None:
                self.stats["threshold_not_ready"] += 1
                continue
            if vol <= 0 or vol < thr:
                continue
            wh = max(x["hi"] for x in win)
            wl = min(x["lo"] for x in win)
            progress = (wh - L) if sgn > 0 else (L - wl)
            if progress > int(self.cfg["max_progress_bins"]):
                continue
            zlo, zhi = L - self.tol, L + self.tol
            sizes = [
                o[5]
                for o in self.orders
                if o[2] == sgn and o[0] <= E and o[0] - self.cluster_ms - 1 >= E - self.W and o[4] >= zlo and o[3] <= zhi
            ]
            if len(sizes) < int(self.cfg["min_orders"]) or max(sizes) * self.maxfrac.denominator > self.maxfrac.numerator * vol:
                continue
            if any(c["sgn"] == sgn and abs(c["L"] - L) <= self.tol for c in self.cands):
                continue
            ids_lo, ids_hi = [], []
            for x in win:
                m = (x["bin"] >= zlo) & (x["bin"] <= zhi)
                if m.any():
                    ids_lo.append(int(x["first_id"][m].min()))
                    ids_hi.append(int(x["last_id"][m].max()))
            zu = int(unk[max(0, zlo - lo) : zhi - lo + 1].sum())
            cid = f"ABSORPTION:{name}:{self.interval}:{E}:{L}"
            cand = {"id": cid, "sgn": sgn, "L": L, "E": E, "deadline": E + int(self.cfg["confirm_window_ms"])}
            self.cands.append(cand)
            self.stats["candidates"] += 1
            details = {
                "zone_bins": [zlo, zhi],
                "progress_bins": progress,
                "window_high_bin": wh,
                "window_low_bin": wl,
                "orders_in_zone": len(sizes),
                "largest_order_lots": max(sizes),
                "quantile_threshold": thr_q,
                "abs_lots": self.cfg["abs_lots"],
                "window_ms": self.W,
            }
            out.append(
                self._row(
                    cid,
                    "CANDIDATE",
                    None,
                    name,
                    E - self.W,
                    E,
                    E + self.shift,
                    L,
                    vol,
                    int(thr),
                    zu,
                    min(ids_lo) if ids_lo else None,
                    max(ids_hi) if ids_hi else None,
                    details,
                )
            )
        return out

    def _update_candidates(self, bar: dict[str, Any]) -> list[dict[str, Any]]:
        out, keep = [], []
        for c in self.cands:
            if bar["end"] > c["deadline"]:
                out.append(self._terminal(c, "EXPIRED", c["deadline"], {"reason": "confirm window passed"}))
                continue
            L, sgn = c["L"], c["sgn"]
            lim = L + int(self.cfg["max_progress_bins"]) + int(self.cfg["break_bins"])
            if sgn > 0:
                broken, conf = bar["hi"] > lim, bar["lo"] <= L - int(self.cfg["confirm_bins"])
            else:
                lim = L - int(self.cfg["max_progress_bins"]) - int(self.cfg["break_bins"])
                broken, conf = bar["lo"] < lim, bar["hi"] >= L + int(self.cfg["confirm_bins"])
            det = {"bar_high_bin": bar["hi"], "bar_low_bin": bar["lo"]}
            if broken and conf:
                out.append(self._terminal(c, "AMBIGUOUS", bar["end"], det))
            elif conf:
                out.append(self._terminal(c, "CONFIRMED", bar["end"], det))
            elif broken:
                out.append(self._terminal(c, "FAILED", bar["end"], det))
            else:
                keep.append(c)
        self.cands = keep
        return out

    def _expire(self, done_upto: int | None) -> list[dict[str, Any]]:
        if done_upto is None:
            return []
        out, keep = [], []
        for c in self.cands:
            if c["deadline"] <= done_upto:
                out.append(self._terminal(c, "EXPIRED", c["deadline"], {"reason": "confirm window passed"}))
            else:
                keep.append(c)
        self.cands = keep
        return out

    def _terminal(self, c: dict[str, Any], status: str, t: int, det: dict[str, Any]) -> dict[str, Any]:
        name = "BUY" if c["sgn"] > 0 else "SELL"
        return self._row(
            f"{c['id']}:{status}",
            status,
            c["id"],
            name,
            c["E"],
            t,
            t + self.shift,
            c["L"],
            None,
            None,
            0,
            None,
            None,
            {**det, "confirm_bins": self.cfg["confirm_bins"], "break_bins": self.cfg["break_bins"]},
        )

    def _row(
        self,
        eid: str,
        status: str,
        parent: str | None,
        side: str,
        ev_s: int,
        ev_e: int,
        t: int,
        L: int,
        vol: int | None,
        thr: int | None,
        unk: int,
        fid: int | None,
        lid: int | None,
        details: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "event_id": eid,
            "event_type": "ABSORPTION",
            "status": status,
            "parent_id": parent,
            "side": side,
            "interval": self.interval,
            "bin_ticks": self.bin,
            "evidence_start_ms": ev_s,
            "evidence_end_ms": ev_e,
            "event_time_ms": t,
            "ref_price_ticks": L * self.bin,
            "value_lots": vol,
            "threshold_lots": thr,
            "unknown_lots": unk,
            "first_trade_id": fid,
            "last_trade_id": lid,
            "n_trades": None,
            "trade_ids": None,
            "details": json.dumps(details, sort_keys=True, default=str),
        }

    # ---------------------------------------------------------------- state
    def reset(self) -> None:
        self.bars.clear()
        self.orders.clear()
        self.cands = []

    def get_state(self) -> dict[str, Any]:
        return {
            "bars": [{k: (v.tolist() if isinstance(v, np.ndarray) else v) for k, v in b.items()} for b in self.bars],
            "orders": [list(o) for o in self.orders],
            "cands": self.cands,
            "prev_floor": self.prev_floor,
            "observe_from": self.observe_from,
            "stats": self.stats,
            "rq": {str(k): v.get_state() for k, v in self.rq.items()} if self.rq else None,
        }

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        self.bars = deque({k: (np.asarray(v, np.int64) if isinstance(v, list) else v) for k, v in b.items()} for b in s["bars"])
        self.orders = deque(tuple(int(x) for x in o) for o in s["orders"])  # type: ignore[misc]
        self.cands, self.prev_floor, self.observe_from = list(s["cands"]), s["prev_floor"], s["observe_from"]
        self.stats = dict(s["stats"])
        if self.rq and s.get("rq"):
            for k, v in s["rq"].items():
                self.rq[int(k)].set_state(v)
