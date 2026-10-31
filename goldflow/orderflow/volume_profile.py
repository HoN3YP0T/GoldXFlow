"""Phase 6: Volume Profile, POC, Value Area and HVN/LVN (definitions in docs/METRICS_PROFILE_V1.md).

Everything here is a pure function of a binned profile. Volumes are integer lots (0.001 oz). Every comparison that
decides a result (POC, value-area steps, node prominence and volume filters) is done in exact integer or rational
arithmetic, so results are reproducible bit for bit on any machine. Floats appear only in reported magnitudes
(smoothed volume, prominence, node widths), never in a decision.

Price convention: bin ``b`` holds price ticks ``[b * bin_ticks, (b + 1) * bin_ticks - 1]``, and is reported at its
lowest price, ``b * bin_ticks`` ticks. The dense profile covers every bin from the lowest traded bin to the highest
one, including empty bins inside that range (they count as rows of zero volume, as in the CBOT convention).
"""

from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from typing import Any

import numpy as np
from numpy.typing import NDArray

from .settings import NodeParams

I64 = np.int64
_LIMIT = 2**62


@dataclass(frozen=True)
class BinnedProfile:
    """A dense profile ``vol[i]`` = lots traded in bin ``lo_bin + i``. ``sum_q``/``sum_qp`` are the exact 1-tick
    totals (lots, tick*lots) used for tie-breaks against the window's volume-weighted mean price."""

    bin_ticks: int
    lo_bin: int
    vol: NDArray[Any]
    buy: NDArray[Any]
    sell: NDArray[Any]
    sum_q: int
    sum_qp: int

    @property
    def n(self) -> int:
        return len(self.vol)

    def price(self, i: int) -> int:
        return (self.lo_bin + i) * self.bin_ticks


def bin_dense(
    lo_level: int, vol: NDArray[Any], buy: NDArray[Any], sell: NDArray[Any], bin_ticks: int, sum_q: int, sum_qp: int
) -> BinnedProfile:
    """Bin a dense 1-tick profile (index ``i`` = price level ``lo_level + i``; the first and last entries traded)."""
    n = len(vol)
    if n == 0:
        z = np.zeros(0, I64)
        return BinnedProfile(bin_ticks, 0, z, z, z, 0, 0)
    if bin_ticks == 1:
        return BinnedProfile(1, lo_level, vol, buy, sell, sum_q, sum_qp)
    lo_bin = lo_level // bin_ticks
    pad_l = lo_level - lo_bin * bin_ticks
    hi_level = lo_level + n - 1
    nb = hi_level // bin_ticks - lo_bin + 1
    pad_r = nb * bin_ticks - pad_l - n
    out = []
    for a in (vol, buy, sell):
        x = np.concatenate([np.zeros(pad_l, I64), np.asarray(a, I64), np.zeros(pad_r, I64)])
        out.append(x.reshape(nb, bin_ticks).sum(axis=1))
    return BinnedProfile(bin_ticks, lo_bin, out[0], out[1], out[2], sum_q, sum_qp)


def bin_sparse(levels: NDArray[Any], vol: NDArray[Any], buy: NDArray[Any], sell: NDArray[Any], bin_ticks: int) -> BinnedProfile:
    """Bin sparse 1-tick levels (any order, repeats allowed). Exact integer sums."""
    levels = np.asarray(levels, I64)
    vol = np.asarray(vol, I64)
    if len(levels) == 0:
        z = np.zeros(0, I64)
        return BinnedProfile(bin_ticks, 0, z, z, z, 0, 0)
    b = levels // bin_ticks
    lo = int(b.min())
    nb = int(b.max()) - lo + 1
    outs = []
    for a in (vol, buy, sell):
        d = np.zeros(nb, I64)
        np.add.at(d, b - lo, np.asarray(a, I64))
        outs.append(d)
    sq = int(vol.sum())
    sqp = exact_dot(levels, vol)
    return BinnedProfile(bin_ticks, lo, outs[0], outs[1], outs[2], sq, sqp)


def exact_dot(a: NDArray[Any], b: NDArray[Any]) -> int:
    """Exact sum(a * b) for int64 arrays: numpy when the result provably fits in int64, Python integers otherwise."""
    if len(a) == 0:
        return 0
    bound = int(np.abs(a).max()) * int(np.abs(b).sum())
    if bound < _LIMIT:
        return int(np.dot(np.asarray(a, I64), np.asarray(b, I64)))
    return sum(int(x) * int(y) for x, y in zip(a.tolist(), b.tolist()))


# ---------------------------------------------------------------------------------------------------- POC
def _mean_dist2(p: BinnedProfile, i: int) -> int:
    """2 * sum_q * |centre(bin i) - mean price| in exact integers (centre = low + (bin_ticks - 1) / 2 ticks)."""
    b = p.bin_ticks
    return abs(2 * p.sum_qp - (2 * (p.lo_bin + i) * b + b - 1) * p.sum_q)


def poc_index(p: BinnedProfile) -> int | None:
    """Bin with the maximum volume. Ties: the bin whose centre is closest to the window's volume-weighted mean price;
    if still tied, the lower price. None for an empty profile."""
    if p.n == 0 or int(p.vol.max()) <= 0:
        return None
    cand = np.flatnonzero(p.vol == p.vol.max())
    if len(cand) == 1:
        return int(cand[0])
    return min((_mean_dist2(p, int(i)), int(i)) for i in cand)[1]


def mean_side(p: BinnedProfile, i: int) -> int:
    """+1 if the window's volume-weighted mean price is above the centre of bin i, -1 if below, 0 if equal."""
    b = p.bin_ticks
    d = 2 * p.sum_qp - (2 * (p.lo_bin + i) * b + b - 1) * p.sum_q
    return (d > 0) - (d < 0)


# ---------------------------------------------------------------------------------------------------- value area
def value_area(p: BinnedProfile, poc: int, pct: Fraction, method: str = "pair") -> tuple[int, int, int, int]:
    """Value area by the expansion rule (docs/METRICS_PROFILE_V1.md section 4). Returns (val_index, vah_index,
    va_lots, steps). ``pair``: compare the next two bins above with the next two below and add the larger pair; near
    an edge use what remains. Equal pairs: add the side on which the window's volume-weighted mean price lies
    (relative to the POC centre); if the mean is exactly at the POC centre, add both. Stop as soon as the
    accumulated volume >= pct * total (whole steps are added, so the area may exceed the target)."""
    v = p.vol.tolist()
    n = len(v)
    total = sum(v)
    num, den = pct.numerator, pct.denominator
    need = num * total
    lo = hi = poc
    acc = v[poc]
    k = 2 if method == "pair" else 1
    side = mean_side(p, poc)
    steps = 0
    while acc * den < need:
        nu = min(k, n - 1 - hi)
        nd = min(k, lo)
        up = sum(v[hi + 1 : hi + 1 + nu]) if nu else -1
        dn = sum(v[lo - nd : lo]) if nd else -1
        if up > dn or (up == dn and side > 0):
            hi += nu
            acc += up
        elif dn > up or side < 0:
            lo -= nd
            acc += dn
        else:
            hi += nu
            lo -= nd
            acc += up + dn
        steps += 1
    return lo, hi, acc, steps


# ---------------------------------------------------------------------------------------------------- HVN / LVN
def gaussian_kernel(sigma_bins: int, truncate: int, bits: int) -> NDArray[Any]:
    """Integer Gaussian kernel: w(x) = round(2**bits * exp(-x^2 / (2 sigma^2))) for |x| <= truncate * sigma."""
    r = truncate * sigma_bins
    x = np.arange(-r, r + 1, dtype=np.float64)
    return np.floor(np.exp(-0.5 * (x / sigma_bins) ** 2) * (1 << bits) + 0.5).astype(I64)


def smooth(vol: NDArray[Any], kernel: NDArray[Any]) -> NDArray[Any]:
    """Exact integer convolution, zero outside the traded range, same length as ``vol`` (units: lots * sum(kernel))."""
    n = len(vol)
    if n == 0:
        return np.zeros(0, I64)
    if int(vol.max()) * int(kernel.sum()) >= _LIMIT:
        raise OverflowError("profile bin volume too large for the integer smoothing kernel; lower nodes.kernel_scale_bits")
    r = (len(kernel) - 1) // 2
    return np.convolve(np.asarray(vol, I64), kernel, mode="full")[r : r + n]


def _extrema(s: NDArray[Any]) -> list[tuple[int, int, int]]:
    """(kind, pos, value) for edges (0), strict local maxima (1) and minima (-1) of ``s``. A plateau is one extremum,
    positioned at its middle (lower middle for even widths). Plateaus touching an edge are never extrema."""
    n = len(s)
    ch = np.flatnonzero(s[1:] != s[:-1])
    starts = np.concatenate(([0], ch + 1))
    ends = np.concatenate((ch, [n - 1]))
    rv = s[starts]
    m = len(rv)
    out: list[tuple[int, int, int]] = [(0, 0, int(s[0]))]
    if m >= 3:
        mid = rv[1:-1]
        pk = (mid > rv[:-2]) & (mid > rv[2:])
        vl = (mid < rv[:-2]) & (mid < rv[2:])
        for j in np.flatnonzero(pk | vl):
            run = j + 1
            out.append((1 if pk[j] else -1, int((starts[run] + ends[run]) // 2), int(rv[run])))
    out.append((0, n - 1, int(s[-1])))
    return out


def _bases(ext: list[tuple[int, int, int]], want: int) -> dict[int, tuple[int, int]]:
    """For each extremum of kind ``want`` (+1 peaks on s, or -1 valleys handled by the caller negating values), the
    lowest point between it and the nearest strictly higher peak in the iteration direction (or the edge).
    Returns pos -> (min value, min pos); among equal minima the one nearest the peak is kept."""
    out: dict[int, tuple[int, int]] = {}
    stack: list[tuple[int, int, int]] = []  # (height, min value since previous stack entry, its pos)
    cur: tuple[int, int] | None = None
    for kind, pos, val in ext:
        if kind != want:
            if cur is None or val <= cur[0]:
                cur = (val, pos)
            continue
        assert cur is not None
        mv, mp = cur
        while stack and stack[-1][0] <= val:
            _, bv, bp = stack.pop()
            if bv < mv:
                mv, mp = bv, bp
        out[pos] = (mv, mp)
        stack.append((val, mv, mp))
        cur = None
    return out


def _prominences(s: NDArray[Any], sign: int) -> list[tuple[int, int, int, int]]:
    """(pos, prominence, left_base_pos, right_base_pos) of peaks (sign=+1) or valleys (sign=-1) of ``s``."""
    ext = _extrema(s)
    e = [(k * sign, p, v * sign) for k, p, v in ext]
    left = _bases(e, 1)
    right = _bases(list(reversed(e)), 1)
    out = []
    for k, p, v in e:
        if k != 1:
            continue
        lv, lp = left[p]
        rv, rp = right[p]
        out.append((p, v - max(lv, rv), lp, rp))
    return out


def _crossing(x: NDArray[Any], pos: int, base: int, line: float, step: int) -> float:
    """Fractional index where x first drops to ``line`` walking from ``pos`` towards ``base`` (linear interpolation,
    as in scipy.signal.peak_widths). The base is the lowest point, so the walk always stops at or before it."""
    i = pos
    while i != base and line < x[i]:
        i += step
    if x[i] < line:
        j = i - step  # the previous point is above the line
        return j + step * (float(x[j]) - line) / (float(x[j]) - float(x[i]))
    return float(i)


def find_nodes(p: BinnedProfile, prm: NodeParams, kernel: NDArray[Any]) -> list[dict[str, Any]]:
    """HVN and LVN on the integer-smoothed profile (docs/METRICS_PROFILE_V1.md section 5). Sorted by price."""
    n = p.n
    if n < 3:
        return []
    s = smooth(p.vol, kernel)
    w = int(kernel.sum())
    smax = int(s.max())
    total = int(p.vol.sum())
    if smax <= 0:
        return []
    pf = prm.prom_frac
    out: list[dict[str, Any]] = []
    for kind, sign in (("HVN", 1), ("LVN", -1)):
        frac = prm.hvn_min_vol_frac if sign > 0 else prm.lvn_max_vol_frac
        cands = []
        for pos, prom, lb, rb in _prominences(s, sign):
            if prom * pf.denominator < pf.numerator * smax:
                continue
            # volume filter against the mean bin volume total / n (exact): s/w >= frac * total / n  (HVN), <= (LVN)
            lhs = int(s[pos]) * n * frac.denominator
            rhs = frac.numerator * total * w
            if (sign > 0 and lhs < rhs) or (sign < 0 and lhs > rhs):
                continue
            cands.append((prom, pos, lb, rb))
        cands.sort(key=lambda c: (-c[0], c[1]))
        kept: list[tuple[int, int, int, int]] = []
        for c in cands:
            if all(abs(c[1] - k[1]) >= prm.min_separation_bins for k in kept):
                kept.append(c)
        hrel = float(prm.width_rel_height)
        x = s.astype(np.float64) * sign
        for prom, pos, lb, rb in kept:
            line = float(x[pos]) - prom * hrel
            li = _crossing(x, pos, lb, line, -1)
            ri = _crossing(x, pos, rb, line, 1)
            bt = p.bin_ticks
            out.append(
                {
                    "node_type": kind,
                    "bin": p.lo_bin + pos,
                    "price_ticks": p.price(pos),
                    "lots": int(p.vol[pos]),
                    "smoothed_lots": float(s[pos]) / w,
                    "prominence_lots": prom / w,
                    "left_ticks": (p.lo_bin + li) * bt,
                    "right_ticks": (p.lo_bin + ri) * bt,
                    "width_ticks": (ri - li) * bt,
                }
            )
    out.sort(key=lambda d: (d["price_ticks"], d["node_type"]))
    return out


# ---------------------------------------------------------------------------------------------------- all metrics
def profile_metrics(
    p: BinnedProfile, pct: Fraction, method: str, prm: NodeParams, kernel: NDArray[Any]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """POC, value area and nodes of one binned profile. Empty profile: counts are 0 and prices are None."""
    total = int(p.vol.sum()) if p.n else 0
    m: dict[str, Any] = {
        "bin_ticks": p.bin_ticks,
        "total_lots": total,
        "buy_lots": int(p.buy.sum()) if p.n else 0,
        "sell_lots": int(p.sell.sum()) if p.n else 0,
        "n_bins": p.n,
        "low_bin_ticks": p.price(0) if p.n else None,
        "high_bin_ticks": p.price(p.n - 1) if p.n else None,
        "poc_ticks": None,
        "poc_lots": None,
        "val_ticks": None,
        "vah_ticks": None,
        "va_lots": None,
        "va_bins": None,
        "va_steps": None,
        "va_target": str(pct),
    }
    poc = poc_index(p)
    nodes: list[dict[str, Any]] = []
    if poc is not None:
        lo, hi, acc, steps = value_area(p, poc, pct, method)
        m.update(
            poc_ticks=p.price(poc),
            poc_lots=int(p.vol[poc]),
            val_ticks=p.price(lo),
            vah_ticks=p.price(hi),
            va_lots=acc,
            va_bins=hi - lo + 1,
            va_steps=steps,
        )
        nodes = find_nodes(p, prm, kernel)
    m["hvn_count"] = sum(1 for x in nodes if x["node_type"] == "HVN")
    m["lvn_count"] = sum(1 for x in nodes if x["node_type"] == "LVN")
    return m, nodes
