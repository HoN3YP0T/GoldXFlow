"""Phase 7: Big Trades. Taker-order reconstruction from individual fills, and size thresholds without look-ahead.

Binance splits one aggressive (taker) order into many fills, one per resting maker order it hits. Phase 7 first
rebuilds the **taker order** and only then asks whether it was big:

* fills are taken in delivery order (Phase 3 order: exchange time, then trade id);
* a fill extends the current taker order when it has the same aggressor side, its time is within ``cluster_ms`` of
  the previous fill (default 0 = same millisecond), and its price moves in the aggressor's direction (buy: not down,
  sell: not up) by at most ``max_step_ticks``. Anything else starts a new taker order.

On real XAUUSDT days about 0.1 % of same-millisecond, same-side fill pairs step *against* the aggressor's direction:
those are two different takers in one millisecond, and the direction rule separates them. Two takers on the same side
in the same millisecond whose fills interleave monotonically cannot be told apart from one order; this is the stated
limit of trade-only data. aggTrades are not used: they are incomplete on 39 days and merge by price, not by taker.

A taker order is **complete** once no later fill could extend it: when every trade with time <= last fill time +
``cluster_ms`` has been delivered. Its ``available_at`` is therefore last_time + cluster_ms + 1 ms + trade shift, and
it is evaluated and emitted only when the clock reaches that point.

Thresholds (each may be enabled; ``combine`` = any | all of the enabled ones):
* absolute size ``min_qty_lots`` and notional ``min_notional_usdt`` (exact integers);
* rolling quantile: size >= the q-quantile of the taker orders that **ended** in the ``hours`` before the minute in
  which this order ended (orders ending inside that minute are not used yet);
* time-of-day baseline: size >= the q-quantile of the orders in the same UTC time-of-day bucket (default 30 min) on
  the previous ``lookback_days`` days that had orders in that bucket and the same weekend/regular status.
A quantile needs ``min_samples`` orders; below that it is *unavailable* (null), never guessed.

Quantiles are taken on an exact integer histogram: bins of one lot below 1,024 lots (1.024 oz), then 64 bins per
doubling (each bin's width is 1.09 % of its lower edge). The threshold is the **lower edge** of the bin holding the
nearest-rank quantile, ceil(q * n). Everything is integer, so results are identical on every machine.
"""

from __future__ import annotations

from fractions import Fraction
from typing import Any

import numpy as np
from numpy.typing import NDArray

I64 = np.int64
EXACT_BINS = 1024
PER_DOUBLING = 64
MAX_DOUBLINGS = 34  # up to 1024 * 2**34 lots (17.6 M oz): far beyond any order


def _iroot_ceil(x: int, n: int) -> int:
    """Smallest integer e with e**n >= x (exact integer binary search)."""
    lo, hi = 0, 1 << (x.bit_length() // n + 1)  # hi**n >= x
    while lo < hi:
        mid = (lo + hi) // 2
        if mid**n >= x:
            hi = mid
        else:
            lo = mid + 1
    return lo


def _edges() -> NDArray[Any]:
    out = list(range(EXACT_BINS))
    for i in range(MAX_DOUBLINGS * PER_DOUBLING + 1):
        out.append(_iroot_ceil(EXACT_BINS**PER_DOUBLING * 2**i, PER_DOUBLING))
    e = np.array(out, I64)
    assert (np.diff(e) > 0).all()
    return e


EDGES = _edges()
N_BINS = len(EDGES)


def size_bin(lots: NDArray[Any]) -> NDArray[Any]:
    """Histogram bin of each size (lots >= 1). Exact below 1,024 lots."""
    s = np.asarray(lots, I64)
    return np.clip(np.searchsorted(EDGES, s, side="right") - 1, 0, N_BINS - 1).astype(np.int16)


def merge_sparse(a: tuple[NDArray[Any], NDArray[Any]], b: tuple[NDArray[Any], NDArray[Any]]) -> tuple[NDArray[Any], NDArray[Any]]:
    bins = np.concatenate([a[0], b[0]])
    cnt = np.concatenate([a[1], b[1]])
    if not len(bins):
        return np.zeros(0, np.int16), np.zeros(0, I64)
    u, inv = np.unique(bins, return_inverse=True)
    c = np.zeros(len(u), I64)
    np.add.at(c, inv, cnt)
    return u.astype(np.int16), c


def sparse_of(bins: NDArray[Any]) -> tuple[NDArray[Any], NDArray[Any]]:
    u, c = np.unique(np.asarray(bins, np.int16), return_counts=True)
    return u.astype(np.int16), c.astype(I64)


def quantile_edge(hist: NDArray[Any], q: Fraction, min_samples: int) -> tuple[int | None, int]:
    """(lower edge in lots of the bin holding the nearest-rank q-quantile, sample count). None below min_samples."""
    n = int(hist.sum())
    if n < min_samples or n == 0:
        return None, n
    r = -(-q.numerator * n // q.denominator)  # ceil(q * n), exact
    idx = int(np.searchsorted(np.cumsum(hist), r, side="left"))
    return int(EDGES[min(idx, N_BINS - 1)]), n


class RollingQuantile:
    """q-quantile of samples keyed by time, over [R - window, R) for minute-aligned query points R.

    Queries must come in non-decreasing order of R, and a sample must never be added to a minute that a query has
    already passed (that would be look-ahead in reverse). Both are asserted."""

    def __init__(self, window_ms: int, q: Fraction, min_samples: int) -> None:
        self.wm = window_ms // 60_000
        self.q = q
        self.min_samples = min_samples
        self.minutes: dict[int, tuple[NDArray[Any], NDArray[Any]]] = {}
        self.hist = np.zeros(N_BINS, I64)
        self.lo: int | None = None  # window [lo, hi) in minutes
        self.hi: int | None = None
        self.last: tuple[int, int | None, int] | None = None

    def add(self, t_ms: int, lots: NDArray[Any]) -> None:
        m = int(t_ms) // 60_000
        if self.hi is not None and m < self.hi:
            raise AssertionError(f"rolling quantile: sample for minute {m} added after the window passed {self.hi}")
        self.minutes[m] = merge_sparse(self.minutes.get(m, (np.zeros(0, np.int16), np.zeros(0, I64))), sparse_of(size_bin(lots)))

    def threshold(self, t_ms: int) -> tuple[int | None, int]:
        R = int(t_ms) // 60_000
        if self.last is not None and self.last[0] == R:
            return self.last[1], self.last[2]
        if self.hi is not None and R < self.hi:
            raise AssertionError("rolling quantile queried out of order")
        lo_new = R - self.wm
        if self.hi is None or lo_new >= self.hi:  # the old window is entirely out of range: rebuild
            self.hist[:] = 0
            for m in sorted(k for k in self.minutes if lo_new <= k < R):
                b, c = self.minutes[m]
                np.add.at(self.hist, b.astype(I64), c)
        else:
            for m in sorted(k for k in self.minutes if self.hi <= k < R):
                b, c = self.minutes[m]
                np.add.at(self.hist, b.astype(I64), c)
            assert self.lo is not None
            for m in sorted(k for k in self.minutes if self.lo <= k < lo_new):
                b, c = self.minutes[m]
                np.subtract.at(self.hist, b.astype(I64), c)
        for m in [k for k in self.minutes if k < lo_new]:
            del self.minutes[m]
        self.lo, self.hi = lo_new, R
        th, n = quantile_edge(self.hist, self.q, self.min_samples)
        self.last = (R, th, n)
        return th, n

    def state(self) -> dict[str, Any]:
        ks = sorted(self.minutes)
        return {
            "minutes": [[k, self.minutes[k][0], self.minutes[k][1]] for k in ks],
            "lo": self.lo,
            "hi": self.hi,
            "last": list(self.last) if self.last else None,
        }

    def load(self, s: dict[str, Any]) -> None:
        self.minutes = {int(k): (np.asarray(b, np.int16), np.asarray(c, I64)) for k, b, c in s["minutes"]}
        self.lo, self.hi = s["lo"], s["hi"]
        x = s["last"]
        self.last = (int(x[0]), None if x[1] is None else int(x[1]), int(x[2])) if x else None
        self.hist = np.zeros(N_BINS, I64)
        if self.hi is not None:
            for m, (b, c) in self.minutes.items():
                if self.lo <= m < self.hi:
                    np.add.at(self.hist, b.astype(I64), c)


class TimeOfDayQuantile:
    """q-quantile per (UTC time-of-day bucket, weekend flag) over the previous ``lookback_days`` days with samples."""

    def __init__(self, bucket_ms: int, lookback_days: int, q: Fraction, min_samples: int) -> None:
        self.bucket_ms = bucket_ms
        self.lookback = lookback_days
        self.q = q
        self.min_samples = min_samples
        self.days: dict[tuple[int, int], dict[int, tuple[NDArray[Any], NDArray[Any]]]] = {}
        self.cache: dict[tuple[int, int, int], tuple[int | None, int]] = {}

    def key(self, t_ms: int, weekend: bool) -> tuple[int, int, int]:
        t = int(t_ms)
        return t // 86_400_000, (t % 86_400_000) // self.bucket_ms, int(bool(weekend))

    def add(self, t_ms: int, weekend: bool, lots: NDArray[Any]) -> None:
        d, k, w = self.key(t_ms, weekend)
        per = self.days.setdefault((k, w), {})
        if any((d2, k, w) in self.cache for d2 in range(d + 1, d + self.lookback + 2)):
            raise AssertionError("time-of-day quantile: sample added after a later day was queried")
        per[d] = merge_sparse(per.get(d, (np.zeros(0, np.int16), np.zeros(0, I64))), sparse_of(size_bin(lots)))
        for old in [x for x in per if x < d - self.lookback]:
            del per[old]

    def threshold(self, t_ms: int, weekend: bool) -> tuple[int | None, int]:
        d, k, w = self.key(t_ms, weekend)
        ck = (d, k, w)
        if ck in self.cache:
            return self.cache[ck]
        per = self.days.get((k, w), {})
        prev = sorted(x for x in per if x < d)[-self.lookback :]
        h = np.zeros(N_BINS, I64)
        for x in prev:
            b, c = per[x]
            np.add.at(h, b.astype(I64), c)
        r = quantile_edge(h, self.q, self.min_samples)
        self.cache[ck] = r
        for old in [c for c in self.cache if c[0] < d - 1]:
            del self.cache[old]
        return r

    def state(self) -> dict[str, Any]:
        return {
            "days": [[k, w, d, b, c] for (k, w), per in sorted(self.days.items()) for d, (b, c) in sorted(per.items())],
            "cache": [[d, k, w, th, n] for (d, k, w), (th, n) in sorted(self.cache.items(), key=lambda x: x[0])],
        }

    def load(self, s: dict[str, Any]) -> None:
        self.days = {}
        for k, w, d, b, c in s["days"]:
            self.days.setdefault((int(k), int(w)), {})[int(d)] = (np.asarray(b, np.int16), np.asarray(c, I64))
        self.cache = {(int(d), int(k), int(w)): (th, int(n)) for d, k, w, th, n in s["cache"]}


# ============================================================================ taker-order reconstruction
ORDER_COLS = ("side", "first_time", "last_time", "first_id", "last_id", "n_fills", "lots", "notional",
              "first_price", "last_price", "min_price", "max_price")  # fmt: skip


def taker_breaks(t: NDArray[Any], p: NDArray[Any], side: NDArray[Any], cluster_ms: int, max_step: int) -> NDArray[Any]:
    """new[i] is True where fill i cannot extend the taker order of fill i-1 (index 0 is left to the caller)."""
    n = len(t)
    new = np.ones(n, bool)
    if n > 1:
        dp = (p[1:] - p[:-1]) * side[1:].astype(I64)  # > 0: moving in the aggressor's direction
        new[1:] = (side[1:] != side[:-1]) | (t[1:] - t[:-1] > cluster_ms) | (dp < 0) | (np.abs(p[1:] - p[:-1]) > max_step)
        same_unknown = (side[1:] == 0) & (side[:-1] == 0)  # unknown side: no direction rule, same timestamp only
        new[1:] = np.where(same_unknown, (t[1:] - t[:-1] > cluster_ms) | (np.abs(p[1:] - p[:-1]) > max_step), new[1:])
    return new


def group_orders(
    t: NDArray[Any], ids: NDArray[Any], p: NDArray[Any], q: NDArray[Any], side: NDArray[Any], new: NDArray[Any]
) -> dict[str, NDArray[Any]]:
    """Aggregate fills into orders, given the break flags (new[0] must be True)."""
    st = np.flatnonzero(new)
    en = np.append(st[1:], len(t)) - 1
    q = q.astype(I64)
    p = p.astype(I64)
    return {
        "side": side[st].astype(np.int8),
        "first_time": t[st].astype(I64),
        "last_time": t[en].astype(I64),
        "first_id": ids[st].astype(I64),
        "last_id": ids[en].astype(I64),
        "n_fills": (en - st + 1).astype(I64),
        "lots": np.add.reduceat(q, st),
        "notional": np.add.reduceat(p * q, st),
        "first_price": p[st],
        "last_price": p[en],
        "min_price": np.minimum.reduceat(p, st),
        "max_price": np.maximum.reduceat(p, st),
        "start_index": st.astype(I64),
    }


class TakerOrderBuilder:
    """Incremental taker-order reconstruction across chunks. The last order stays open until a breaking fill
    arrives or the clock proves no later fill can extend it."""

    def __init__(self, cluster_ms: int, max_step: int) -> None:
        self.cluster_ms = cluster_ms
        self.max_step = max_step
        self.open: dict[str, Any] | None = None  # fills of the open order: t, ids, p, q, side arrays

    def ingest(self, t: NDArray[Any], ids: NDArray[Any], p: NDArray[Any], q: NDArray[Any], side: NDArray[Any]
               ) -> tuple[dict[str, NDArray[Any]], NDArray[Any], NDArray[Any]] | None:  # fmt: skip
        """Returns closed orders (columns), the flat fill ids of those orders and their offsets."""
        if not len(t):
            return None
        o = self.open
        if o is not None:
            t, ids, p, q, side = (
                np.concatenate([o[k], v]) for k, v in (("t", t), ("ids", ids), ("p", p), ("q", q), ("side", side))
            )
        new = taker_breaks(t, p, side, self.cluster_ms, self.max_step)
        st = np.flatnonzero(new)
        last = int(st[-1])
        self.open = {"t": t[last:].copy(), "ids": ids[last:].copy(), "p": p[last:].copy(), "q": q[last:].copy(),
                     "side": side[last:].copy()}  # fmt: skip
        if last == 0:
            return None
        return self._close(t[:last], ids[:last], p[:last], q[:last], side[:last], new[:last])

    def _close(self, t: NDArray[Any], ids: NDArray[Any], p: NDArray[Any], q: NDArray[Any], side: NDArray[Any], new: NDArray[Any]
               ) -> tuple[dict[str, NDArray[Any]], NDArray[Any], NDArray[Any]]:  # fmt: skip
        g = group_orders(t, ids, p, q, side, new)
        return g, ids.astype(I64), g.pop("start_index")

    def flush(self, time_floor: int) -> tuple[dict[str, NDArray[Any]], NDArray[Any], NDArray[Any]] | None:
        """Close the open order if every trade that could extend it has been delivered (time < time_floor)."""
        o = self.open
        if o is None or int(o["t"][-1]) + self.cluster_ms >= time_floor:
            return None
        self.open = None
        new = np.zeros(len(o["t"]), bool)
        new[0] = True
        return self._close(o["t"], o["ids"], o["p"], o["q"], o["side"], new)

    def state(self) -> dict[str, Any]:
        return {} if self.open is None else {k: np.asarray(v, I64 if k != "side" else np.int8) for k, v in self.open.items()}

    def load(self, s: dict[str, Any]) -> None:
        self.open = {k: np.asarray(v) for k, v in s.items()} if s else None
