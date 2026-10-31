"""Exhaustion (Phase 7). Definitions: docs/METRICS_ORDERFLOW_V1.md.

On finalised bars of `interval` (high side; the low side mirrors with sell volume):
  push           a bar whose high exceeds every high of the previous `lookback_bars` bars (by time)
  exhaustion     the last k pushes (all within the lookback) have strictly declining aggressive BUY volume, AND the
                 newest push's delta is below the previous push's delta (higher price, weaker delta: divergence)
=> EXHAUSTION CANDIDATE at the newest push's bar end (+ shift), evidence [first push start, newest push end).
Later bars, until confirm_window_bars bars after it:
  low bin <= extreme bin - confirm_bins   => CONFIRMED (pullback), at that bar's end
  high > extreme (new high)               => INVALIDATED, at that bar's end
  both in one bar                         => AMBIGUOUS
  nothing by the deadline                 => EXPIRED
"""

from __future__ import annotations

import json
from collections import deque
from typing import Any

import numpy as np


class ExhaustionDetector:
    def __init__(self, cfg: dict[str, Any], interval: str, interval_ms: int, shift_ms: int):
        self.cfg, self.interval, self.I, self.shift = cfg, interval, interval_ms, shift_ms
        self.bin = int(cfg["bin_ticks"])
        self.look = int(cfg["lookback_bars"]) * interval_ms
        self.k = int(cfg["k_pushes"])
        self.hist: deque[dict[str, int]] = deque()
        self.pushes: dict[int, deque[dict[str, int]]] = {1: deque(), -1: deque()}
        self.cands: list[dict[str, Any]] = []
        self.observe_from: int | None = None
        self.prev_floor: int | None = None

    def on_advance(self, floor: int) -> list[dict[str, Any]]:
        out = self._expire(self.prev_floor) if self.prev_floor is not None else []
        self.prev_floor = floor
        return out

    def on_bars(self, bars: dict[str, Any], final_upto: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for k in range(len(bars["bar_start"])):
            b = {
                c: int(np.asarray(bars[c])[k])
                for c in (
                    "bar_start",
                    "bar_end",
                    "high_ticks",
                    "low_ticks",
                    "buy_lots",
                    "sell_lots",
                    "unknown_lots",
                    "first_trade_id",
                    "last_trade_id",
                )
            }
            out += self._update(b)
            s = b["bar_start"]
            while self.hist and self.hist[0]["bar_start"] < s - self.look:
                self.hist.popleft()
            full = self.observe_from is None or s - self.look >= self.observe_from
            if self.hist and full:
                for sgn in (1, -1):
                    if sgn > 0:
                        is_push = b["high_ticks"] > max(h["high_ticks"] for h in self.hist)
                    else:
                        is_push = b["low_ticks"] < min(h["low_ticks"] for h in self.hist)
                    if not is_push:
                        continue
                    P = self.pushes[sgn]
                    P.append(b)
                    while P and P[0]["bar_start"] < s - self.look:
                        P.popleft()
                    out += self._check(sgn)
            self.hist.append(b)
        out += self._expire(final_upto)
        return out

    def _check(self, sgn: int) -> list[dict[str, Any]]:
        P = list(self.pushes[sgn])
        if len(P) < self.k:
            return []
        last = P[-self.k :]
        vol = [p["buy_lots"] if sgn > 0 else p["sell_lots"] for p in last]
        declining = all(vol[i] > vol[i + 1] for i in range(len(vol) - 1))
        d = [p["buy_lots"] - p["sell_lots"] for p in last]
        diverging = d[-1] < d[-2] if sgn > 0 else d[-1] > d[-2]
        if not (declining and diverging):
            return []
        newest = last[-1]
        ext = newest["high_ticks"] if sgn > 0 else newest["low_ticks"]
        side = "BUY" if sgn > 0 else "SELL"
        E = newest["bar_end"]
        cid = f"EXHAUSTION:{side}:{self.interval}:{E}"
        self.cands = [c for c in self.cands if c["sgn"] != sgn]  # a newer extreme supersedes (the old one was invalidated)
        self.cands.append(
            {"id": cid, "sgn": sgn, "ext": ext, "E": E, "deadline": E + int(self.cfg["confirm_window_bars"]) * self.I}
        )
        details = {
            "push_bar_starts": [p["bar_start"] for p in last],
            "push_volumes": vol,
            "push_deltas": d,
            "push_extremes_ticks": [p["high_ticks"] if sgn > 0 else p["low_ticks"] for p in last],
            "push_unknown_lots": [p["unknown_lots"] for p in last],
            "lookback_ms": self.look,
        }
        return [
            self._row(
                cid,
                "CANDIDATE",
                None,
                side,
                last[0]["bar_start"],
                E,
                E + self.shift,
                ext,
                vol[-1],
                sum(p["unknown_lots"] for p in last),
                min(p["first_trade_id"] for p in last),
                max(p["last_trade_id"] for p in last),
                details,
            )
        ]

    def _update(self, b: dict[str, int]) -> list[dict[str, Any]]:
        out, keep = [], []
        for c in self.cands:
            if b["bar_end"] > c["deadline"]:
                out.append(self._terminal(c, "EXPIRED", c["deadline"], {}))
                continue
            cb = int(self.cfg["confirm_bins"])
            if c["sgn"] > 0:
                inval = b["high_ticks"] > c["ext"]
                conf = b["low_ticks"] // self.bin <= c["ext"] // self.bin - cb
            else:
                inval = b["low_ticks"] < c["ext"]
                conf = b["high_ticks"] // self.bin >= c["ext"] // self.bin + cb
            det = {"bar_high_ticks": b["high_ticks"], "bar_low_ticks": b["low_ticks"]}
            if inval and conf:
                out.append(self._terminal(c, "AMBIGUOUS", b["bar_end"], det))
            elif conf:
                out.append(self._terminal(c, "CONFIRMED", b["bar_end"], det))
            elif inval:
                out.append(self._terminal(c, "INVALIDATED", b["bar_end"], det))
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
                out.append(self._terminal(c, "EXPIRED", c["deadline"], {}))
            else:
                keep.append(c)
        self.cands = keep
        return out

    def _terminal(self, c: dict[str, Any], status: str, t: int, det: dict[str, Any]) -> dict[str, Any]:
        side = "BUY" if c["sgn"] > 0 else "SELL"
        return self._row(
            f"{c['id']}:{status}",
            status,
            c["id"],
            side,
            c["E"],
            t,
            t + self.shift,
            c["ext"],
            None,
            0,
            None,
            None,
            {**det, "confirm_bins": self.cfg["confirm_bins"]},
            parent=c["id"],
        )

    def _row(
        self,
        eid: str,
        status: str,
        _p: Any,
        side: str,
        ev_s: int,
        ev_e: int,
        t: int,
        ref: int,
        vol: int | None,
        unk: int,
        fid: int | None,
        lid: int | None,
        details: dict[str, Any],
        parent: str | None = None,
    ) -> dict[str, Any]:
        return {
            "event_id": eid,
            "event_type": "EXHAUSTION",
            "status": status,
            "parent_id": parent,
            "side": side,
            "interval": self.interval,
            "bin_ticks": self.bin,
            "evidence_start_ms": ev_s,
            "evidence_end_ms": ev_e,
            "event_time_ms": t,
            "ref_price_ticks": ref,
            "value_lots": vol,
            "threshold_lots": None,
            "unknown_lots": unk,
            "first_trade_id": fid,
            "last_trade_id": lid,
            "n_trades": None,
            "trade_ids": None,
            "details": json.dumps(details, sort_keys=True, default=str),
        }

    def reset(self) -> None:
        self.hist.clear()
        self.pushes = {1: deque(), -1: deque()}
        self.cands = []

    def get_state(self) -> dict[str, Any]:
        return {
            "hist": list(self.hist),
            "pushes": {str(k): list(v) for k, v in self.pushes.items()},
            "cands": self.cands,
            "observe_from": self.observe_from,
            "prev_floor": self.prev_floor,
        }

    def set_state(self, s: dict[str, Any]) -> None:
        if s:
            self.hist = deque(s["hist"])
            self.pushes = {int(k): deque(v) for k, v in s["pushes"].items()}
            self.cands, self.observe_from, self.prev_floor = list(s["cands"]), s["observe_from"], s["prev_floor"]
