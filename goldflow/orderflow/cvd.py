"""Cumulative volume delta with resets (Phase 7). Definitions: docs/METRICS_ORDERFLOW_V1.md.

CVD(series, t) = Σ (buy_lots - sell_lots) over the finalised bars of the series' current instance that end by t.
Reset series:
  utc_day, trading_day, trading_week   one series each, reset at every instance start (Phase 5 calendar)
  session                              one series per configured session (asia, london, new_york), defined only
                                       inside the session
  anchor:<name>                        from an anchor time onward (`add_anchor`), never reset
Unknown-side lots accumulate separately (cum_unknown_lots) and are never assigned to buy or sell.

A bar belongs to an instance only if it lies wholly inside it. Bars are UTC-aligned, so with the default sessions
(and intervals that divide their boundaries) every bar of the interval is either fully inside or fully outside.
If processing started after an instance began, its CVD is labelled coverage=PARTIAL_START.
"""

from __future__ import annotations

from typing import Any

import numpy as np
from numpy.typing import NDArray

SERIES_KINDS = ("utc_day", "trading_day", "trading_week", "session")


class ResetCVD:
    """CVD for one bar interval and a set of reset kinds. Feed finalised bars in time order."""

    def __init__(self, interval: str, interval_ms: int, resets: list[str] | tuple[str, ...], cal: Any):
        for r in resets:
            if r not in SERIES_KINDS:
                raise ValueError(f"unknown CVD reset {r!r}")
        self.interval, self.interval_ms, self.resets, self.cal = interval, interval_ms, tuple(resets), cal
        self.anchors: dict[str, int] = {}
        # series -> [instance key, start_ms, cum_buy, cum_sell, cum_unk, partial_start]
        self.cur: dict[str, list[Any]] = {}
        self.observe_from: int | None = None
        self.filtered = False  # replay used date/session filters: CVD across the gaps is not meaningful

    def _cov(self, partial: bool) -> str:
        return "FILTERED" if self.filtered else ("PARTIAL_START" if partial else "FULL")

    def add_anchor(self, name: str, t_ms: int) -> None:
        if t_ms % self.interval_ms:
            raise ValueError(f"anchor must align to the {self.interval} interval")
        self.anchors[name] = int(t_ms)

    def value(self, series: str) -> dict[str, Any] | None:
        c = self.cur.get(series)
        if c is None:
            return None
        return {
            "instance": c[0],
            "cvd_lots": c[2] - c[3],
            "cum_buy_lots": c[2],
            "cum_sell_lots": c[3],
            "cum_unknown_lots": c[4],
            "coverage": self._cov(c[5]),
        }

    def on_bars(self, bars: dict[str, Any]) -> list[dict[str, NDArray[Any]]]:
        """Update every series with finalised bars; returns output column blocks (one per series instance)."""
        bs = np.asarray(bars["bar_start"], np.int64)
        if len(bs) == 0:
            return []
        be = np.asarray(bars["bar_end"], np.int64)
        buy, sell, unk = (np.asarray(bars[k], np.int64) for k in ("buy_lots", "sell_lots", "unknown_lots"))
        out: list[dict[str, NDArray[Any]]] = []
        insts = [i for i in self.cal.instances(int(bs.min()), int(be.max()) - 1) if i.kind in self.resets]
        spans: list[tuple[str, str, int, int]] = [
            (i.kind if i.kind != "session" else f"session:{i.name}", i.key, i.start_ms, i.end_ms) for i in insts
        ]
        spans += [(f"anchor:{n}", f"anchor:{n}", t, 2**62) for n, t in sorted(self.anchors.items())]
        for series, key, s, e in sorted(spans, key=lambda x: (x[2], x[0])):
            m = (bs >= s) & (be <= e)
            if not m.any():
                continue
            c = self.cur.get(series)
            if c is None or c[0] != key:
                partial = self.observe_from is not None and s < self.observe_from
                c = [key, s, 0, 0, 0, partial]
                self.cur[series] = c
            cb = c[2] + np.cumsum(buy[m])
            cs = c[3] + np.cumsum(sell[m])
            cu = c[4] + np.cumsum(unk[m])
            c[2], c[3], c[4] = int(cb[-1]), int(cs[-1]), int(cu[-1])
            n = int(m.sum())
            out.append(
                {
                    "interval": np.full(n, self.interval, object),
                    "series": np.full(n, series, object),
                    "instance": np.full(n, key, object),
                    "bar_start": bs[m],
                    "bar_end": be[m],
                    "available_at": np.asarray(bars["available_at"], np.int64)[m],
                    "bar_delta_lots": buy[m] - sell[m],
                    "bar_unknown_lots": unk[m],
                    "cvd_lots": cb - cs,
                    "cum_buy_lots": cb,
                    "cum_sell_lots": cs,
                    "cum_unknown_lots": cu,
                    "coverage": np.full(n, self._cov(c[5]), object),
                    "dq_status": np.asarray(bars["dq_status"], object)[m],
                    "weekend": np.asarray(bars["weekend"], bool)[m],
                }
            )
        return out

    def on_seek(self) -> None:
        self.cur = {}

    def get_state(self) -> dict[str, Any]:
        return {"cur": self.cur, "anchors": self.anchors, "observe_from": self.observe_from, "filtered": self.filtered}

    def set_state(self, s: dict[str, Any]) -> None:
        if s:
            self.cur = {k: list(v) for k, v in s["cur"].items()}
            self.anchors = dict(s["anchors"])
            self.observe_from = s["observe_from"]
            self.filtered = bool(s.get("filtered"))
