"""Phase 6: VWAP and volume-weighted standard-deviation bands from exact integer sums.

For trades i in a window (price p_i in ticks, quantity q_i in lots):

    Q = sum q_i,   P1 = sum q_i p_i,   P2 = sum q_i p_i^2           (Python integers: exact, no overflow)
    VWAP = P1 / Q                                                    (ticks)
    var  = P2 / Q - VWAP^2 = (Q * P2 - P1^2) / Q^2                   (volume-weighted population variance, ticks^2)
    band_k = VWAP +/- k * sqrt(var)

At 1-tick resolution every trade in a price level has the same price, so the sums can be built from per-level
volumes: P1 = sum_level level * vol(level), P2 = sum_level level^2 * vol(level). That is exact, which lets VWAP be
built incrementally from finalised footprint rows and checked against a from-scratch recomputation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

import numpy as np
from numpy.typing import NDArray

_LIMIT = 2**62


@dataclass
class VwapSums:
    q: int = 0
    qp: int = 0
    qp2: int = 0

    def add(self, o: "VwapSums") -> None:
        self.q += o.q
        self.qp += o.qp
        self.qp2 += o.qp2

    def as_list(self) -> list[int]:
        return [self.q, self.qp, self.qp2]

    @property
    def vwap(self) -> Fraction | None:
        return Fraction(self.qp, self.q) if self.q else None

    @property
    def variance(self) -> Fraction | None:
        return Fraction(self.q * self.qp2 - self.qp * self.qp, self.q * self.q) if self.q else None


def sums_from_levels(level: NDArray[Any], vol: NDArray[Any]) -> VwapSums:
    """Exact sums from 1-tick levels and their lots. numpy is used only when every partial sum provably fits."""
    if len(level) == 0:
        return VwapSums()
    lv = np.asarray(level, np.int64)
    q = np.asarray(vol, np.int64)
    sq = int(q.sum())
    ref = int(lv.min())
    d = lv - ref  # centred on the lowest level so the numpy path rarely needs the fallback
    dmax = int(d.max())
    if dmax * dmax * sq < _LIMIT:
        s1, s2 = int(np.dot(d, q)), int(np.dot(d * d, q))
    else:
        dl, ql = d.tolist(), q.tolist()
        s1, s2 = sum(a * b for a, b in zip(dl, ql)), sum(a * a * b for a, b in zip(dl, ql))
    return VwapSums(sq, ref * sq + s1, ref * ref * sq + 2 * ref * s1 + s2)


def vwap_stats(s: VwapSums, band_k: tuple[int, ...] | list[int]) -> dict[str, Any]:
    """VWAP, sigma and bands in ticks (floats correctly rounded from the exact rationals). None when Q = 0."""
    out: dict[str, Any] = {"sum_q": s.q, "sum_qp": s.qp, "sum_qp2": s.qp2, "vwap_ticks": None, "sigma_ticks": None}
    for k in band_k:
        out[f"band_up_{k}_ticks"] = None
        out[f"band_dn_{k}_ticks"] = None
    vw, var = s.vwap, s.variance
    if vw is None or var is None:
        return out
    v = float(vw)
    sd = math.sqrt(float(var)) if var > 0 else 0.0
    out["vwap_ticks"] = v
    out["sigma_ticks"] = sd
    for k in band_k:
        out[f"band_up_{k}_ticks"] = v + k * sd
        out[f"band_dn_{k}_ticks"] = v - k * sd
    return out
