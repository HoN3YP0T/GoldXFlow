"""Phase 7: footprint imbalance (diagonal by default, horizontal as an option) and stacked imbalance.

Input: a finalised bar's exact 1-tick footprint (Phase 5 levels), re-binned to ``bin_ticks``. Only exchange-reported
buy and sell lots take part; unknown-side lots are reported next to every result and never assigned to a side.

Diagonal (default), for bin p of one bar:
* **buy imbalance** at p   if buy(p)  >= ratio * sell(p - 1) and buy(p)  >= min_lots
* **sell imbalance** at p  if sell(p) >= ratio * buy(p + 1)  and sell(p) >= min_lots
Horizontal compares buy(p) with sell(p) (and sell(p) with buy(p)).
A bin with no trades is an *empty* comparison bin (0 lots). With ``zero_policy = none`` (default) nothing is an
imbalance against an empty bin; with ``any`` the min_lots test alone decides. The ratio is an exact rational, so
the test is buy * den >= num * sell in integers.

Stacked imbalance: ``stack_n`` or more *adjacent* bins (p, p + 1, ...) with an imbalance on the same side in one bar.
Each run is reported once with its lowest and highest bin.
"""

from __future__ import annotations

from fractions import Fraction
from typing import Any

import numpy as np
from numpy.typing import NDArray

I64 = np.int64


def rebin_levels(levels: dict[str, Any], bin_ticks: int) -> dict[str, NDArray[Any]]:
    """Sum 1-tick level rows into (bar_start, bin) rows sorted by (bar_start, bin)."""
    b = np.asarray(levels["bar_start"], I64)
    lv = np.asarray(levels["level"], I64) // bin_ticks
    if not len(b):
        z = np.zeros(0, I64)
        return {k: z for k in ("bar_start", "bin", "buy", "sell", "unk", "first_id", "last_id")}
    o = np.lexsort((lv, b))
    b, lv = b[o], lv[o]
    new = np.ones(len(b), bool)
    new[1:] = (b[1:] != b[:-1]) | (lv[1:] != lv[:-1])
    st = np.flatnonzero(new)

    def s(k: str) -> NDArray[Any]:
        return np.add.reduceat(np.asarray(levels[k], I64)[o], st)

    return {
        "bar_start": b[st],
        "bin": lv[st],
        "buy": s("buy_lots"),
        "sell": s("sell_lots"),
        "unk": s("unknown_lots"),
        "first_id": np.minimum.reduceat(np.asarray(levels["first_trade_id"], I64)[o], st),
        "last_id": np.maximum.reduceat(np.asarray(levels["last_trade_id"], I64)[o], st),
    }


def _passes(x: NDArray[Any], other: NDArray[Any], ratio: Fraction, min_lots: int, zero_policy: str) -> NDArray[Any]:
    ok = (x >= min_lots) & (x * ratio.denominator >= other * ratio.numerator)
    if zero_policy == "none":
        ok &= other > 0
    return ok


def imbalances(
    r: dict[str, NDArray[Any]], method: str, ratio: Fraction, min_lots: int, zero_policy: str, stack_n: int
) -> tuple[dict[str, NDArray[Any]], dict[str, NDArray[Any]]]:
    """Imbalance rows and stacked runs for re-binned rows ``r`` (from ``rebin_levels``)."""
    b, p = r["bar_start"], r["bin"]
    n = len(b)
    if method == "diagonal":
        # comparison bin for buys: p - 1 in the same bar (row i-1 if it is exactly that bin, else empty)
        has_lo = np.zeros(n, bool)
        has_hi = np.zeros(n, bool)
        if n > 1:
            has_lo[1:] = (b[1:] == b[:-1]) & (p[1:] == p[:-1] + 1)
            has_hi[:-1] = has_lo[1:]
        sell_lo = np.where(has_lo, np.roll(r["sell"], 1), 0)
        buy_hi = np.where(has_hi, np.roll(r["buy"], -1), 0)
        idlo_f = np.where(has_lo, np.roll(r["first_id"], 1), r["first_id"])
        idlo_l = np.where(has_lo, np.roll(r["last_id"], 1), r["last_id"])
        idhi_f = np.where(has_hi, np.roll(r["first_id"], -1), r["first_id"])
        idhi_l = np.where(has_hi, np.roll(r["last_id"], -1), r["last_id"])
        opp_b, opp_s, cmp_b, cmp_s = sell_lo, buy_hi, p - 1, p + 1
        ids_b = (np.minimum(r["first_id"], idlo_f), np.maximum(r["last_id"], idlo_l))
        ids_s = (np.minimum(r["first_id"], idhi_f), np.maximum(r["last_id"], idhi_l))
    else:
        opp_b, opp_s, cmp_b, cmp_s = r["sell"], r["buy"], p, p
        ids_b = ids_s = (r["first_id"], r["last_id"])
    bi = _passes(r["buy"], opp_b, ratio, min_lots, zero_policy)
    si = _passes(r["sell"], opp_s, ratio, min_lots, zero_policy)
    parts = []
    for side, m, lots, opp, cmp_bin, ids in ((1, bi, r["buy"], opp_b, cmp_b, ids_b), (-1, si, r["sell"], opp_s, cmp_s, ids_s)):
        idx = np.flatnonzero(m)
        parts.append(
            {
                "bar_start": b[idx],
                "side": np.full(len(idx), side, np.int8),
                "bin": p[idx],
                "lots": lots[idx],
                "opposite_lots": opp[idx],
                "compare_bin": cmp_bin[idx],
                "unknown_lots": r["unk"][idx],
                "first_trade_id": ids[0][idx],
                "last_trade_id": ids[1][idx],
            }
        )
    imb = {k: np.concatenate([parts[0][k], parts[1][k]]) for k in parts[0]}
    o = np.lexsort((imb["bin"], -imb["side"], imb["bar_start"]))
    imb = {k: v[o] for k, v in imb.items()}
    # stacked runs: consecutive rows of the same bar and side whose bins are adjacent
    sb, ss, sp = imb["bar_start"], imb["side"], imb["bin"]
    k = len(sb)
    brk = np.ones(k, bool)
    if k > 1:
        brk[1:] = (sb[1:] != sb[:-1]) | (ss[1:] != ss[:-1]) | (sp[1:] != sp[:-1] + 1)
    st = np.flatnonzero(brk)
    ln = np.diff(np.append(st, k))
    keep = ln >= stack_n
    s0, ln = st[keep], ln[keep]
    e0 = s0 + ln - 1
    lots = np.add.reduceat(imb["lots"], st)[keep] if k else np.zeros(0, I64)
    stacked = {
        "bar_start": sb[s0],
        "side": ss[s0],
        "low_bin": sp[s0],
        "high_bin": sp[e0],
        "n_bins": ln.astype(I64),
        "lots": lots.astype(I64),
        "first_trade_id": np.minimum.reduceat(imb["first_trade_id"], st)[keep] if k else np.zeros(0, I64),
        "last_trade_id": np.maximum.reduceat(imb["last_trade_id"], st)[keep] if k else np.zeros(0, I64),
    }
    return imb, stacked
