"""Phase 9: Auction Market Theory rules. Reference levels, acceptance and rejection, activity, profile shape.

Everything is decided from measurable evidence with exact integer / rational arithmetic, and every output carries the
time it became known. See docs/METRICS_AUCTION_V1.md for the full definitions.

* **Reference levels** come from the **final** profile of the previous instance of each source window (Phase 6:
  high, low, POC, VAH, VAL, VWAP, VWAP bands, HVN/LVN) plus user levels. A level is usable from its ``available_at``
  (the window end) until the next instance of the same source window becomes available.
* **Episodes.** Per level and side (ABOVE / BELOW), an episode starts with the first bar that trades beyond the
  level and ends with the first bar that **closes** back inside it. While it runs:
  - ``ACCEPTANCE``: ``accept_time / interval`` consecutive closes beyond the level, **or** a close beyond with volume
    traded beyond >= ``accept_vol_frac`` x the source window's volume. Emitted at that bar's end.
  - ``RETURN``: an accepted episode that closes back inside (acceptance lost).
  - ``REJECTION``: not accepted, probed >= ``probe_bins`` bins beyond, and closed back inside within ``reject_time``.
* **Activity** per bar: initiative (away from value) or responsive (back toward value) from the bar's delta sign and
  its location versus the previous value area, or versus the developing VWAP when inside value.
* **Profile shape** of a final window: excess (a thin, fast tail), poor high/low (a flat extreme without excess) and
  low-volume single-print **proxies** (there is no TPO time grid; the proxy is labelled as such).
"""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from typing import Any

import numpy as np
from numpy.typing import NDArray

I64 = np.int64


@dataclass
class Level:
    id: str
    series: str
    window_key: str
    ltype: str
    price: Fraction  # ticks, exact
    available_at: int
    ref_lots: int | None  # volume of the source window (None for user levels)
    active_until: int | None = None

    def tree(self) -> list[Any]:
        return [self.id, self.series, self.window_key, self.ltype, [self.price.numerator, self.price.denominator],
                self.available_at, self.ref_lots, self.active_until]  # fmt: skip

    @classmethod
    def load(cls, x: list[Any]) -> "Level":
        return cls(x[0], x[1], x[2], x[3], Fraction(int(x[4][0]), int(x[4][1])), int(x[5]), x[6], x[7])


def levels_from_snapshot(rec: dict[str, Any], nodes: list[dict[str, Any]], types: tuple[str, ...], band_k: tuple[int, ...]
                         ) -> list[tuple[str, Fraction]]:  # fmt: skip
    """(type, price in ticks) for a final Phase 6 snapshot record. VAH is the top tick of the value area's highest bin
    and VAL the bottom tick of its lowest; POC, HVN and LVN are bin centres; VWAP is the exact rational sum_qp / sum_q."""
    bt = int(rec["bin_ticks"])
    out: list[tuple[str, Fraction]] = []
    if rec.get("total_lots", 0) <= 0:
        return out
    half = Fraction(bt - 1, 2)
    for t in types:
        if t == "HIGH" and rec.get("high_ticks") is not None:
            out.append((t, Fraction(int(rec["high_ticks"]))))
        elif t == "LOW" and rec.get("low_ticks") is not None:
            out.append((t, Fraction(int(rec["low_ticks"]))))
        elif t == "POC" and rec.get("poc_ticks") is not None:
            out.append((t, int(rec["poc_ticks"]) + half))
        elif t == "VAH" and rec.get("vah_ticks") is not None:
            out.append((t, Fraction(int(rec["vah_ticks"]) + bt - 1)))
        elif t == "VAL" and rec.get("val_ticks") is not None:
            out.append((t, Fraction(int(rec["val_ticks"]))))
        elif t == "VWAP" and rec.get("sum_q"):
            out.append((t, Fraction(int(rec["sum_qp"]), int(rec["sum_q"]))))
        elif t == "VWAP_BAND":
            for k in band_k:
                for d in ("up", "dn"):
                    v = rec.get(f"band_{d}_{k}_ticks")
                    if v is not None:
                        out.append((f"VWAP_{'+' if d == 'up' else '-'}{k}SD", Fraction(float(v))))
        elif t in ("HVN", "LVN"):
            for i, n in enumerate(sorted((n for n in nodes if n["node_type"] == t), key=lambda n: n["price_ticks"])):
                out.append((f"{t}{i + 1}", int(n["price_ticks"]) + half))
    return out


def beyond(x: int, L: Fraction, side: int) -> bool:
    """x (ticks) is strictly beyond L on ``side`` (+1 above, -1 below)."""
    return x * L.denominator > L.numerator if side > 0 else x * L.denominator < L.numerator


def beyond_mask(x: NDArray[Any], L: Fraction, side: int) -> NDArray[Any]:
    return x * L.denominator > L.numerator if side > 0 else x * L.denominator < L.numerator


def bar_beyond(lv: dict[str, NDArray[Any]], L: Fraction, side: int) -> tuple[int, int]:
    """(lots traded beyond L, buy - sell lots beyond L) from a bar's 1-tick footprint."""
    m = beyond_mask(lv["level"], L, side)
    if not m.any():
        return 0, 0
    return int((lv["buy"][m] + lv["sell"][m] + lv["unk"][m]).sum()), int((lv["buy"][m] - lv["sell"][m]).sum())


# ============================================================================ episodes
def new_episode(s: int) -> dict[str, Any]:
    return {"start": s, "ext": None, "vol_beyond": 0, "delta_beyond": 0, "vol_total": 0, "run": 0, "accepted_at": None,
            "bars": 0, "first_id": None, "last_id": None, "dq": "OK", "cov": "FULL"}  # fmt: skip


def step_episode(ep: dict[str, Any] | None, bar: dict[str, Any], lv: dict[str, NDArray[Any]], L: Fraction, side: int,
                 ref_lots: int | None, st: Any) -> tuple[dict[str, Any] | None, list[tuple[str, dict[str, Any]]]]:  # fmt: skip
    """Advance one level/side episode by one finalised bar. Returns the new episode state and the events:
    ("ACCEPTANCE" | "RETURN" | "REJECTION" | "CLOSED", episode snapshot)."""
    from .events import worst_cov, worst_dq

    ext = int(bar["high_ticks"]) if side > 0 else int(bar["low_ticks"])
    if ep is None:
        if not beyond(ext, L, side):
            return None, []
        ep = new_episode(int(bar["bar_start"]))
    vb, db = bar_beyond(lv, L, side)
    ep["vol_beyond"] += vb
    ep["delta_beyond"] += db
    ep["vol_total"] += int(bar["volume_lots"])
    ep["bars"] += 1
    if beyond(ext, L, side):
        ep["ext"] = ext if ep["ext"] is None else (max(ep["ext"], ext) if side > 0 else min(ep["ext"], ext))
    ep["first_id"] = int(bar["first_trade_id"]) if ep["first_id"] is None else ep["first_id"]
    ep["last_id"] = int(bar["last_trade_id"])
    ep["dq"] = worst_dq([ep["dq"], bar["dq_status"]])
    ep["cov"] = worst_cov([ep["cov"], bar["coverage"]])
    close_beyond = beyond(int(bar["close_ticks"]), L, side)
    ep["run"] = ep["run"] + 1 if close_beyond else 0
    e = int(bar["bar_end"])
    out: list[tuple[str, dict[str, Any]]] = []
    if ep["accepted_at"] is None and close_beyond:
        by_time = ep["run"] * st.interval_ms >= st.accept_ms
        by_vol = (
            ref_lots is not None and ep["vol_beyond"] * st.accept_vol_frac.denominator >= st.accept_vol_frac.numerator * ref_lots
        )
        if by_time or by_vol:
            ep["accepted_at"] = e
            out.append(("ACCEPTANCE", {**ep, "end": e, "by_time": by_time, "by_volume": by_vol}))
    if not close_beyond:
        snap = {**ep, "end": e}
        if ep["accepted_at"] is not None:
            out.append(("RETURN", snap))
        else:
            probe = (
                ep["ext"] is not None
                and (ep["ext"] * L.denominator - L.numerator) * side >= st.probe_bins * st.bin_ticks * L.denominator
            )
            if probe and e - ep["start"] <= st.reject_ms:
                out.append(("REJECTION", snap))
            out.append(("CLOSED", snap))
        return None, out
    return ep, out


# ============================================================================ activity
def activity(
    close: int, delta: int, prev_val: Fraction | None, prev_vah: Fraction | None, vwap: Fraction | None
) -> tuple[str, str, str]:
    """(location, vwap_side, activity) of one bar. Initiative = delta pointing away from value (or from VWAP when
    inside value), responsive = pointing back toward it."""
    if prev_val is None or prev_vah is None:
        loc = "UNKNOWN"
    elif beyond(close, prev_vah, 1):
        loc = "ABOVE_VALUE"
    elif beyond(close, prev_val, -1):
        loc = "BELOW_VALUE"
    else:
        loc = "INSIDE_VALUE"
    vs = "UNKNOWN" if vwap is None else ("ABOVE" if beyond(close, vwap, 1) else "BELOW" if beyond(close, vwap, -1) else "AT")
    if delta == 0:
        return loc, vs, "NEUTRAL"
    up = delta > 0
    if loc == "ABOVE_VALUE":
        return loc, vs, "INITIATIVE_BUY" if up else "RESPONSIVE_SELL"
    if loc == "BELOW_VALUE":
        return loc, vs, "RESPONSIVE_BUY" if up else "INITIATIVE_SELL"
    if loc == "INSIDE_VALUE" and vs in ("ABOVE", "BELOW"):
        if vs == "ABOVE":
            return loc, vs, "INITIATIVE_BUY" if up else "RESPONSIVE_SELL"
        return loc, vs, "RESPONSIVE_BUY" if up else "INITIATIVE_SELL"
    return loc, vs, "UNDETERMINED"


# ============================================================================ profile shape
def binned_window(levels: dict[str, Any], bin_ticks: int) -> dict[str, NDArray[Any]]:
    """Dense binned profile of a final window from its 1-tick levels: lots, first and last trade time per bin."""
    lv = np.asarray(levels["level"], I64)
    if not len(lv):
        z = np.zeros(0, I64)
        return {"lo": z, "vol": z, "first": z, "last": z}
    b = lv // bin_ticks
    lo, hi = int(b.min()), int(b.max())
    n = hi - lo + 1
    vol = np.zeros(n, I64)
    first = np.full(n, np.iinfo(I64).max, I64)
    last = np.full(n, np.iinfo(I64).min, I64)
    i = b - lo
    np.add.at(vol, i, np.asarray(levels["buy"], I64) + np.asarray(levels["sell"], I64) + np.asarray(levels["unk"], I64))
    np.minimum.at(first, i, np.asarray(levels["first_ts"], I64))
    np.maximum.at(last, i, np.asarray(levels["last_ts"], I64))
    return {"lo": np.array([lo], I64), "vol": vol, "first": first, "last": last}


def profile_shape(p: dict[str, NDArray[Any]], bin_ticks: int, st: Any) -> list[dict[str, Any]]:
    """Excess / poor extremes and single-print proxies of one dense binned profile (exact comparisons)."""
    v = p["vol"]
    n = len(v)
    if n < 3:
        return []
    lo = int(p["lo"][0])
    total = int(v.sum())
    out: list[dict[str, Any]] = []
    # tail test: bin volume <= tail_frac * mean  <=>  vol * n * den <= num * total
    thin = v * n * st.tail_frac.denominator <= st.tail_frac.numerator * total
    for side in ("HIGH", "LOW"):
        order = list(range(n - 1, -1, -1)) if side == "HIGH" else list(range(n))
        tail = []
        for j in order:
            if not thin[j]:
                break
            tail.append(j)
        traded = [j for j in tail if v[j] > 0]
        span = int(max(p["last"][j] for j in traded) - min(p["first"][j] for j in traded)) if traded else None
        ext = order[0]
        price = (lo + ext) * bin_ticks
        if len(tail) >= st.excess_bins and len(tail) < n and span is not None and span <= st.excess_ms:
            out.append({"type": f"EXCESS_{side}", "price_ticks": price, "tail_bins": len(tail), "tail_lots": int(v[tail].sum()),
                        "tail_span_ms": span, "edge_price_ticks": (lo + tail[-1]) * bin_ticks})  # fmt: skip
            continue
        top3 = [order[0], order[1], order[2]]
        mean3 = Fraction(int(v[top3].sum()), 3)
        if Fraction(int(v[ext])) >= st.poor_frac * mean3 and v[ext] > 0:
            out.append({"type": f"POOR_{side}", "price_ticks": price, "extreme_lots": int(v[ext]), "top3_mean_lots": float(mean3),
                        "tail_bins": len(tail)})  # fmt: skip
    # single-print proxy: interior runs of thin bins (not touching either extreme)
    sp = v * n * st.sp_frac.denominator <= st.sp_frac.numerator * total
    j = 1
    while j < n - 1:
        if sp[j]:
            k = j
            while k + 1 < n - 1 and sp[k + 1]:
                k += 1
            if k - j + 1 >= st.sp_min_bins:
                out.append({"type": "SINGLE_PRINT_PROXY", "price_ticks": (lo + j) * bin_ticks, "high_price_ticks": (lo + k) * bin_ticks,
                            "bins": k - j + 1, "lots": int(v[j : k + 1].sum()), "label": "LOW_VOLUME_PROXY (no TPO time grid)"})  # fmt: skip
            j = k + 1
        else:
            j += 1
    return out
