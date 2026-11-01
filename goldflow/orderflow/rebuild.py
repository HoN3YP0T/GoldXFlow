"""Phase 6: profile state rebuilt from scratch from the Phase 3 trades partitions (no replay, no processor).

Two independent paths, used by ``vprofile-verify``, the tests and ``vprofile-export``:
* ``state_from_trades``: exact 1-tick per-level lots and VWAP sums for trades with time in [start, end), read
  straight from Parquet. Profile metrics computed from it must equal the engine's snapshot exactly (parity).
* ``float_vwap``: VWAP and sigma recomputed in float64 from the *decimal* ``price`` and ``quantity`` columns (not
  ticks/lots), a numerically independent check of the integer VWAP (acceptance: 1e-9 relative).
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import date
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from numpy.typing import NDArray

from ..normalize import part_path
from ..processing.settings import ProcessingSettings
from ..timeutil import DAY_MS
from .settings import ProfileSettings
from .volume_profile import BinnedProfile, bin_dense, gaussian_kernel, profile_metrics
from .vwap import VwapSums, sums_from_levels, vwap_stats

I64 = np.int64
RECOVERED = "RECOVERED_FROM_AGGTRADES"


@dataclass
class Levels:
    level: NDArray[Any]
    vol: NDArray[Any]
    buy: NDArray[Any]
    sell: NDArray[Any]
    sums: VwapSums

    def binned(self, bin_ticks: int) -> BinnedProfile:
        if len(self.level) == 0:
            return bin_dense(0, np.zeros(0, I64), np.zeros(0, I64), np.zeros(0, I64), bin_ticks, 0, 0)
        lo, hi = int(self.level[0]), int(self.level[-1])
        d = {k: np.zeros(hi - lo + 1, I64) for k in ("v", "b", "s")}
        i = self.level - lo
        d["v"][i], d["b"][i], d["s"][i] = self.vol, self.buy, self.sell
        return bin_dense(lo, d["v"], d["b"], d["s"], bin_ticks, self.sums.q, self.sums.qp)


class TradeSource:
    """Day arrays from the trades partitions with a small LRU cache (windows reach back up to a trading week)."""

    def __init__(self, cfg: Any, cache_days: int = 10) -> None:
        self.cfg = cfg
        st = ProcessingSettings.from_config(cfg)
        self.time_col = st.trade_time_col
        self.include_recovered = st.include_recovered
        self.cache: OrderedDict[int, dict[str, NDArray[Any]]] = OrderedDict()
        self.cache_days = cache_days

    def day(self, dn: int) -> dict[str, NDArray[Any]]:
        if dn in self.cache:
            self.cache.move_to_end(dn)
            return self.cache[dn]
        d = date.fromordinal(date(1970, 1, 1).toordinal() + dn)
        p = part_path(self.cfg.data_root, self.cfg.symbol, "trades", str(d))
        out: dict[str, NDArray[Any]]
        if not p.exists():
            out = {k: np.zeros(0, I64) for k in ("t", "p", "q", "side")}
            out.update(price=np.zeros(0), qty=np.zeros(0))
        else:
            cols = [self.time_col, "price_ticks", "quantity_lots", "buyer_maker_flag", "price", "quantity", "record_origin"]
            t = pq.read_table(p, columns=cols)
            if not self.include_recovered:
                t = t.filter(pc.invert(pc.fill_null(pc.equal(t.column("record_origin").cast(pa.string()), RECOVERED), False)))
            bm = t.column("buyer_maker_flag")
            side = np.where(
                pc.is_null(bm).to_numpy(zero_copy_only=False),
                0,
                np.where(pc.fill_null(bm, False).to_numpy(zero_copy_only=False), -1, 1),
            ).astype(np.int8)
            out = {
                "t": t.column(self.time_col).cast(pa.int64()).to_numpy(),
                "p": t.column("price_ticks").to_numpy().astype(I64),
                "q": t.column("quantity_lots").to_numpy().astype(I64),
                "side": side,
                "price": t.column("price").to_numpy(),
                "qty": t.column("quantity").to_numpy(),
            }
        self.cache[dn] = out
        if len(self.cache) > self.cache_days:
            self.cache.popitem(last=False)
        return out

    def between(self, start_ms: int, end_ms: int) -> dict[str, NDArray[Any]]:
        parts = []
        for dn in range(start_ms // DAY_MS, (end_ms - 1) // DAY_MS + 1):
            a = self.day(dn)
            m = (a["t"] >= start_ms) & (a["t"] < end_ms)
            parts.append({k: v[m] for k, v in a.items()})
        return {k: np.concatenate([p[k] for p in parts]) for k in parts[0]} if parts else {}


def state_from_trades(src: TradeSource, start_ms: int, end_ms: int) -> Levels:
    a = src.between(start_ms, end_ms)
    if not a or not len(a["p"]):
        z = np.zeros(0, I64)
        return Levels(z, z, z, z, VwapSums())
    u, inv = np.unique(a["p"], return_inverse=True)
    out = []
    for w in (a["q"], np.where(a["side"] == 1, a["q"], 0), np.where(a["side"] == -1, a["q"], 0)):
        s = np.zeros(len(u), I64)
        np.add.at(s, inv, w)
        out.append(s)
    return Levels(u.astype(I64), out[0], out[1], out[2], sums_from_levels(u, out[0]))


def float_vwap(src: TradeSource, start_ms: int, end_ms: int) -> tuple[float | None, float | None]:
    """Two-pass float64 VWAP and volume-weighted population sigma from the decimal price/quantity columns."""
    a = src.between(start_ms, end_ms)
    if not a or not len(a["price"]):
        return None, None
    p, q = a["price"].astype(np.float64), a["qty"].astype(np.float64)
    vw = float(np.dot(p, q) / q.sum())
    sd = float(np.sqrt(np.dot(q, (p - vw) ** 2) / q.sum()))
    return vw, sd


def snapshot_from_trades(
    cfg: Any, src: TradeSource, start_ms: int, as_of: int, ps: ProfileSettings | None = None
) -> dict[int, dict[str, Any]]:
    """Profile metrics, nodes and VWAP for trades in [start_ms, as_of), per configured bin size, from scratch."""
    ps = ps or ProfileSettings.from_config(cfg)
    n = ps.nodes
    kernel = gaussian_kernel(n.smooth_bins, n.truncate_sigmas, n.kernel_scale_bits)
    lv = state_from_trades(src, start_ms, as_of)
    vw = vwap_stats(lv.sums, ps.band_k)
    out = {}
    for bt in ps.bin_ticks:
        p = lv.binned(bt)
        m, nodes = profile_metrics(p, ps.va_pct, ps.va_method, ps.nodes, kernel)
        out[bt] = {
            "metrics": {
                **m,
                **vw,
                "low_ticks": int(lv.level[0]) if len(lv.level) else None,
                "high_ticks": int(lv.level[-1]) if len(lv.level) else None,
            },
            "nodes": nodes,
            "profile": p,
        }
    return out
