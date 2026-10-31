"""Price / CVD and price / delta divergence on confirmed swings (Phase 7). Definitions: docs/METRICS_ORDERFLOW_V1.md.

Swing high at bar i (in the sequence of finalised bars of `interval`): high_i is strictly greater than the highs of
the `swing_left` bars before it and of the `swing_right` bars after it. It is CONFIRMED only when bar i+R has closed,
so the confirmation time is that bar's end (+ shift). A swing is never used before its confirmation (no repainting).

Bearish divergence (side BUY: buying effort weakening): a newly confirmed swing high P2 above the previous confirmed
swing high P1 (at most `max_pivot_gap_bars` bars earlier) while the oscillator is lower at P2:
  cvd    CVD at the pivot bar's end, same reset instance (`cvd_reset`) for both pivots, else not comparable
  delta  the pivot bar's delta
Bullish divergence (side SELL) mirrors on swing lows. Event: DETECTED at the confirmation time, evidence
[P1 bar start, confirmation bar end). INVALIDATED later if price exceeds the P2 extreme by more than `inval_bins`
bins (checked until max_pivot_gap_bars bars after confirmation).
"""

from __future__ import annotations

import json
from typing import Any

import numpy as np

from .cvd import ResetCVD


class DivergenceDetector:
    def __init__(self, cfg: dict[str, Any], interval: str, interval_ms: int, shift_ms: int, cal: Any):
        self.cfg, self.interval, self.I, self.shift = cfg, interval, interval_ms, shift_ms
        self.bin = int(cfg["bin_ticks"])
        self.Lb, self.Rb = int(cfg["swing_left"]), int(cfg["swing_right"])
        self.gap = int(cfg["max_pivot_gap_bars"])
        self.cvd = ResetCVD(interval, interval_ms, [cfg["cvd_reset"]], cal)
        self.series = cfg["cvd_reset"]
        self.bars: list[dict[str, Any]] = []  # recent bars with idx and cvd
        self.idx = 0
        self.pivots: dict[int, list[dict[str, Any]]] = {1: [], -1: []}
        self.active: list[dict[str, Any]] = []
        self.stats = {"pivots_high": 0, "pivots_low": 0, "cvd_not_comparable": 0}

    def on_bars(self, bars: dict[str, Any]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        blocks = self.cvd.on_bars(bars)
        cv: dict[int, tuple[int, str, str]] = {}
        for blk in blocks:
            for j in range(len(blk["bar_start"])):
                cv[int(blk["bar_start"][j])] = (int(blk["cvd_lots"][j]), str(blk["instance"][j]), str(blk["coverage"][j]))
        for k in range(len(bars["bar_start"])):
            b: dict[str, Any] = {
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
            c = cv.get(b["bar_start"])
            b.update(
                idx=self.idx,
                delta=b["buy_lots"] - b["sell_lots"],
                cvd=c[0] if c else None,
                inst=c[1] if c else None,
                cvd_cov=c[2] if c else None,
            )
            self.idx += 1
            out += self._invalidate(b)
            self.bars.append(b)
            keep = self.Lb + self.Rb + 1 + self.gap + 2
            if len(self.bars) > keep:
                self.bars = self.bars[-keep:]
            out += self._confirm_pivot()
        return out

    def _confirm_pivot(self) -> list[dict[str, Any]]:
        n = len(self.bars)
        ci = n - 1 - self.Rb  # candidate centre, confirmed now that R bars after it have closed
        if ci - self.Lb < 0:
            return []
        c = self.bars[ci]
        left, right = self.bars[ci - self.Lb : ci], self.bars[ci + 1 :]
        conf_bar = self.bars[-1]
        out = []
        for sgn in (1, -1):
            key = "high_ticks" if sgn > 0 else "low_ticks"
            if not all((c[key] > x[key]) if sgn > 0 else (c[key] < x[key]) for x in left + right):
                continue
            self.stats["pivots_high" if sgn > 0 else "pivots_low"] += 1
            piv = {
                "idx": c["idx"],
                "bar_start": c["bar_start"],
                "bar_end": c["bar_end"],
                "ext": c[key],
                "cvd": c["cvd"],
                "inst": c["inst"],
                "delta": c["delta"],
                "first_id": c["first_trade_id"],
                "confirm_end": conf_bar["bar_end"],
                "last_id": conf_bar["last_trade_id"],
                "unk": c["unknown_lots"],
            }
            prev = [p for p in self.pivots[sgn] if piv["idx"] - p["idx"] <= self.gap]
            if prev:
                out += self._divergences(sgn, prev[-1], piv)
            self.pivots[sgn] = prev + [piv]
        return out

    def _divergences(self, sgn: int, p1: dict[str, Any], p2: dict[str, Any]) -> list[dict[str, Any]]:
        if not (p2["ext"] > p1["ext"] if sgn > 0 else p2["ext"] < p1["ext"]):
            return []
        out = []
        side = "BUY" if sgn > 0 else "SELL"
        for osc in self.cfg["oscillators"]:
            if osc == "cvd":
                if p1["inst"] is None or p1["inst"] != p2["inst"] or p1["cvd"] is None or p2["cvd"] is None:
                    self.stats["cvd_not_comparable"] += 1
                    continue
                a, b = p1["cvd"], p2["cvd"]
            else:
                a, b = p1["delta"], p2["delta"]
            if not (b < a if sgn > 0 else b > a):
                continue
            E = p2["confirm_end"]
            eid = f"DIVERGENCE_{osc.upper()}:{side}:{self.interval}:{p2['bar_start']}"
            self.active.append(
                {"id": eid, "sgn": sgn, "ext": p2["ext"], "until_idx": self.idx - 1 + self.gap, "E": E, "osc": osc}
            )
            details = {
                "bias": "BEARISH" if sgn > 0 else "BULLISH",
                "oscillator": osc,
                "p1_bar_start": p1["bar_start"],
                "p2_bar_start": p2["bar_start"],
                "p1_extreme_ticks": p1["ext"],
                "p2_extreme_ticks": p2["ext"],
                "p1_osc": a,
                "p2_osc": b,
                "cvd_reset": self.series if osc == "cvd" else None,
                "cvd_instance": p2["inst"] if osc == "cvd" else None,
                "swing_left": self.Lb,
                "swing_right": self.Rb,
                "p2_unknown_lots": p2["unk"],
            }
            out.append(
                self._row(
                    eid,
                    "DETECTED",
                    None,
                    side,
                    osc,
                    p1["bar_start"],
                    E,
                    E + self.shift,
                    p2["ext"],
                    p2["unk"],
                    p1["first_id"],
                    p2["last_id"],
                    details,
                )
            )
        return out

    def _invalidate(self, b: dict[str, Any]) -> list[dict[str, Any]]:
        out, keep = [], []
        for a in self.active:
            if b["idx"] > a["until_idx"]:
                continue  # tracking window over; stays DETECTED
            lim = self.inval_ticks()
            hit = b["high_ticks"] > a["ext"] + lim if a["sgn"] > 0 else b["low_ticks"] < a["ext"] - lim
            if hit:
                side = "BUY" if a["sgn"] > 0 else "SELL"
                out.append(
                    self._row(
                        f"{a['id']}:INVALIDATED",
                        "INVALIDATED",
                        a["id"],
                        side,
                        a["osc"],
                        a["E"],
                        b["bar_end"],
                        b["bar_end"] + self.shift,
                        a["ext"],
                        0,
                        None,
                        None,
                        {
                            "bar_high_ticks": b["high_ticks"],
                            "bar_low_ticks": b["low_ticks"],
                            "inval_bins": self.cfg["inval_bins"],
                        },
                    )
                )
            else:
                keep.append(a)
        self.active = keep
        return out

    def inval_ticks(self) -> int:
        return int(self.cfg["inval_bins"]) * self.bin

    def _row(
        self,
        eid: str,
        status: str,
        parent: str | None,
        side: str,
        osc: str,
        ev_s: int,
        ev_e: int,
        t: int,
        ref: int,
        unk: int,
        fid: int | None,
        lid: int | None,
        details: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "event_id": eid,
            "event_type": f"DIVERGENCE_{osc.upper()}",
            "status": status,
            "parent_id": parent,
            "side": side,
            "interval": self.interval,
            "bin_ticks": self.bin,
            "evidence_start_ms": ev_s,
            "evidence_end_ms": ev_e,
            "event_time_ms": t,
            "ref_price_ticks": ref,
            "value_lots": None,
            "threshold_lots": None,
            "unknown_lots": unk,
            "first_trade_id": fid,
            "last_trade_id": lid,
            "n_trades": None,
            "trade_ids": None,
            "details": json.dumps(details, sort_keys=True, default=str),
        }

    def reset(self) -> None:
        self.bars, self.pivots, self.active = [], {1: [], -1: []}, []
        self.cvd.on_seek()

    def get_state(self) -> dict[str, Any]:
        return {
            "bars": self.bars,
            "idx": self.idx,
            "pivots": {str(k): v for k, v in self.pivots.items()},
            "active": self.active,
            "stats": self.stats,
            "cvd": self.cvd.get_state(),
        }

    def set_state(self, s: dict[str, Any]) -> None:
        if s:
            self.bars, self.idx, self.active, self.stats = list(s["bars"]), s["idx"], list(s["active"]), dict(s["stats"])
            self.pivots = {int(k): list(v) for k, v in s["pivots"].items()}
            self.cvd.set_state(s["cvd"])
