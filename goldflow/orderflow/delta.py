"""Delta helpers (Phase 7). Definitions: docs/METRICS_ORDERFLOW_V1.md.

Delta = buy_lots - sell_lots (exchange-reported aggressor sides). UNKNOWN-side lots are carried next to every delta
as `unknown_lots` and are never assigned to either side. Per-bar and per-level deltas come straight from the Phase 5
footprint; this module provides exact re-binning of footprint levels and the per-row delta columns.
"""

from __future__ import annotations

from typing import Any

import numpy as np
from numpy.typing import NDArray


def rebin_levels(levels: dict[str, Any], src_bin: int, bin_ticks: int) -> dict[str, NDArray[Any]]:
    """Re-aggregate Phase 5 footprint levels (sorted by bar_start, level) to price bins of `bin_ticks` ticks.

    Returns columns bar_start, bin, buy, sell, unk, cnt, first_id, last_id sorted by (bar_start, bin). Exact integers.
    The bin's lowest price is bin * bin_ticks ticks."""
    if bin_ticks % src_bin:
        raise ValueError(f"bin_ticks {bin_ticks} must be a multiple of the footprint bin {src_bin}")
    bs = np.asarray(levels["bar_start"], np.int64)
    if len(bs) == 0:
        z = np.empty(0, np.int64)
        return {k: z for k in ("bar_start", "bin", "buy", "sell", "unk", "cnt", "first_id", "last_id")}
    b = (np.asarray(levels["level"], np.int64) * src_bin) // bin_ticks
    order = np.lexsort((b, bs))
    bs, b = bs[order], b[order]
    start = np.ones(len(bs), bool)
    start[1:] = (bs[1:] != bs[:-1]) | (b[1:] != b[:-1])
    idx = np.flatnonzero(start)

    def s(k: str) -> NDArray[Any]:
        return np.add.reduceat(np.asarray(levels[k], np.int64)[order], idx)

    return {
        "bar_start": bs[idx],
        "bin": b[idx],
        "buy": s("buy_lots"),
        "sell": s("sell_lots"),
        "unk": s("unknown_lots"),
        "cnt": s("trade_count"),
        "first_id": np.minimum.reduceat(np.asarray(levels["first_trade_id"], np.int64)[order], idx),
        "last_id": np.maximum.reduceat(np.asarray(levels["last_trade_id"], np.int64)[order], idx),
    }


def bar_delta(bars: dict[str, Any]) -> dict[str, NDArray[Any]]:
    """Per-bar delta with unknown volume alongside (never assigned)."""
    buy, sell = np.asarray(bars["buy_lots"], np.int64), np.asarray(bars["sell_lots"], np.int64)
    return {"delta_lots": buy - sell, "unknown_lots": np.asarray(bars["unknown_lots"], np.int64)}


def level_delta(levels: dict[str, Any]) -> dict[str, NDArray[Any]]:
    """Per-level (footprint) delta with unknown volume alongside."""
    buy, sell = np.asarray(levels["buy_lots"], np.int64), np.asarray(levels["sell_lots"], np.int64)
    return {"delta_lots": buy - sell, "unknown_lots": np.asarray(levels["unknown_lots"], np.int64)}


def window_delta(summary: dict[str, Any]) -> dict[str, int]:
    """Delta of a finalised Phase 5 window (session, day, week, custom)."""
    return {"delta_lots": int(summary["buy_lots"]) - int(summary["sell_lots"]), "unknown_lots": int(summary["unknown_lots"])}
