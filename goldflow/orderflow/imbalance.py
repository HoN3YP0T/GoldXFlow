"""Footprint imbalance and stacked imbalance (Phase 7). Definitions: docs/METRICS_ORDERFLOW_V1.md.

Within one bar, at price bin p (bins of `bin_ticks`):
  diagonal (default)   buy imbalance  at p:  buy(p)  >= ratio * sell(p - 1)  and  buy(p)  >= min_lots
                       sell imbalance at p:  sell(p) >= ratio * buy(p + 1)   and  sell(p) >= min_lots
  horizontal           compares buy(p) with sell(p) (and sell(p) with buy(p))
  empty comparison bin zero_policy "none" (default): no imbalance; "imbalance": counts as one if the min_lots test passes
Stacked imbalance: >= stack_n consecutive bins (p, p+1, ...) with same-side imbalance in the same bar.

Only exchange-reported buy/sell volume enters the comparison. UNKNOWN lots at the level are reported alongside
(`unknown_lots`), so side uncertainty stays visible. The ratio is a Fraction: comparisons are exact integers.
"""

from __future__ import annotations

from fractions import Fraction
from typing import Any

import numpy as np
from numpy.typing import NDArray

from .delta import rebin_levels


def compute_imbalances(
    levels: dict[str, Any],
    src_bin: int,
    bin_ticks: int,
    ratio: Fraction,
    min_lots: int,
    mode: str = "diagonal",
    zero_policy: str = "none",
    stack_n: int = 3,
) -> tuple[dict[str, NDArray[Any]], list[dict[str, Any]]]:
    """Returns (imbalance rows, stacked runs). Rows: bar_start, bin, side (+1 buy / -1 sell), lots, opp_lots,
    unknown_lots, first_id, last_id, stacked (bool). Runs: bar_start, side, bin_lo, bin_hi, n, lots, opp_lots,
    unknown_lots, first_id, last_id."""
    r = rebin_levels(levels, src_bin, bin_ticks)
    n = len(r["bin"])
    empty = {
        k: np.empty(0, np.int64) for k in ("bar_start", "bin", "side", "lots", "opp_lots", "unknown_lots", "first_id", "last_id")
    }
    empty["stacked"] = np.empty(0, bool)
    if n == 0:
        return empty, []
    bs, b = r["bar_start"], r["bin"]
    _, bar_idx = np.unique(bs, return_inverse=True)
    bmin = int(b.min())
    width = int(b.max()) - bmin + 3
    key = bar_idx.astype(np.int64) * width + (b - bmin + 1)  # sorted, because rows are sorted by (bar_start, bin)

    def lookup(col: NDArray[Any], off: int) -> NDArray[Any]:
        k = key + off
        pos = np.searchsorted(key, k)
        pos_c = np.minimum(pos, n - 1)
        hit = key[pos_c] == k
        return np.where(hit, col[pos_c], 0)

    num, den = ratio.numerator, ratio.denominator
    buy, sell = r["buy"], r["sell"]
    opp_b = lookup(sell, -1) if mode == "diagonal" else sell
    opp_s = lookup(buy, +1) if mode == "diagonal" else buy

    def test(x: NDArray[Any], opp: NDArray[Any]) -> NDArray[Any]:
        strong = (opp > 0) & (x * den >= num * opp)
        if zero_policy == "imbalance":
            strong |= opp == 0
        return (x >= min_lots) & strong

    ib, is_ = test(buy, opp_b), test(sell, opp_s)
    rows = []
    runs: list[dict[str, Any]] = []
    for side, m, x, opp in ((1, ib, buy, opp_b), (-1, is_, sell, opp_s)):
        idx = np.flatnonzero(m)
        stacked = np.zeros(len(idx), bool)
        if len(idx):
            brk = np.ones(len(idx), bool)
            brk[1:] = (bar_idx[idx[1:]] != bar_idx[idx[:-1]]) | (b[idx[1:]] != b[idx[:-1]] + 1)
            starts = np.flatnonzero(brk)
            ends = np.append(starts[1:], len(idx))
            for s0, e0 in zip(starts, ends):
                if e0 - s0 >= stack_n:
                    j = idx[s0:e0]
                    stacked[s0:e0] = True
                    runs.append(
                        {
                            "bar_start": int(bs[j[0]]),
                            "side": side,
                            "bin_lo": int(b[j[0]]),
                            "bin_hi": int(b[j[-1]]),
                            "n": int(e0 - s0),
                            "lots": int(x[j].sum()),
                            "opp_lots": int(opp[j].sum()),
                            "unknown_lots": int(r["unk"][j].sum()),
                            "first_id": int(r["first_id"][j].min()),
                            "last_id": int(r["last_id"][j].max()),
                        }
                    )
        rows.append(
            {
                "bar_start": bs[idx],
                "bin": b[idx],
                "side": np.full(len(idx), side, np.int64),
                "lots": x[idx],
                "opp_lots": opp[idx],
                "unknown_lots": r["unk"][idx],
                "first_id": r["first_id"][idx],
                "last_id": r["last_id"][idx],
                "stacked": stacked,
            }
        )
    out = {k: np.concatenate([x[k] for x in rows]) for k in rows[0]}
    o = np.lexsort((out["side"], out["bin"], out["bar_start"]))
    out = {k: v[o] for k, v in out.items()}
    runs.sort(key=lambda d: (d["bar_start"], d["bin_lo"], d["side"]))
    return out, runs
