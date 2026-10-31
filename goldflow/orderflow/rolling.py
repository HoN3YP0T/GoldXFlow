"""Exact trailing thresholds for Phase 7 (no look-ahead, deterministic, chunk-independent).

Quantile definition (nearest rank): for N samples sorted ascending, the q-quantile is the value at 1-based rank
ceil(q * N). It is always an observed value, computed in exact integer arithmetic (q is a Fraction).

RollingQuantile: a threshold for an observation at time t is computed at the boundary B = floor(t / update_ms) *
update_ms from the samples with time in [B - lookback, B). Samples at or after B are never used, so the threshold
for t depends only on the past and not on how the input was chunked.

DailyBucketBaseline: time-of-day baseline. Per bucket of the UTC day (e.g. 30 minutes), the threshold for day D uses
the same bucket on the previous `days` calendar days (never day D itself). Each day keeps its count and only its
`keep_top` largest values. The result is exact whenever the needed rank from the top is <= keep_top; otherwise
the baseline reports itself as not ready instead of approximating.
"""

from __future__ import annotations

import math
from collections import deque
from fractions import Fraction
from typing import Any

import numpy as np

DAY_MS = 86_400_000


def nearest_rank(sorted_vals: np.ndarray, q: Fraction) -> int:
    n = len(sorted_vals)
    rank = max(1, math.ceil(q * n))
    return int(sorted_vals[rank - 1])


class RollingQuantile:
    """Window kept as a sorted array, updated in batches at each boundary (insert new samples, delete evicted ones)."""

    def __init__(self, q: Fraction, lookback_ms: int, min_samples: int, update_ms: int):
        self.q, self.lookback_ms, self.min_samples, self.update_ms = q, lookback_ms, min_samples, update_ms
        self.fifo: deque[tuple[int, int]] = deque()  # samples inside the window, time order
        self.pending: deque[tuple[int, int]] = deque()  # samples at or after the current boundary
        self.sorted = np.empty(0, np.int64)
        self.b: int | None = None
        self._cache: tuple[int | None, int] = (None, 0)
        self._last_t: int | None = None

    def add(self, t_ms: int, value: int) -> None:
        if self._last_t is not None and t_ms < self._last_t:
            raise ValueError("RollingQuantile samples must be added in time order")
        self._last_t = int(t_ms)
        self.pending.append((int(t_ms), int(value)))

    def _advance(self, b: int) -> None:
        new: list[int] = []
        while self.pending and self.pending[0][0] < b:
            tv = self.pending.popleft()
            self.fifo.append(tv)
            new.append(tv[1])
        lo = b - self.lookback_ms
        rem: list[int] = []
        while self.fifo and self.fifo[0][0] < lo:
            rem.append(self.fifo.popleft()[1])
        if new:
            nv = np.sort(np.asarray(new, np.int64))
            self.sorted = np.insert(self.sorted, np.searchsorted(self.sorted, nv), nv)
        if rem:
            rv = np.sort(np.asarray(rem, np.int64))
            first = np.searchsorted(rv, rv, side="left")  # index of the first equal value in rv
            idx = np.searchsorted(self.sorted, rv, side="left") + (np.arange(len(rv)) - first)
            self.sorted = np.delete(self.sorted, idx)
        self.b = b

    def threshold(self, t_ms: int) -> tuple[int | None, int]:
        """(threshold or None if fewer than min_samples, sample count) for an observation at t_ms."""
        b = (t_ms // self.update_ms) * self.update_ms
        if b != self.b:
            if self.b is not None and b < self.b:
                raise ValueError("RollingQuantile queried backwards in time")
            self._advance(b)
            n = len(self.sorted)
            self._cache = (None, n) if n < self.min_samples else (nearest_rank(self.sorted, self.q), n)
        return self._cache

    def get_state(self) -> dict[str, Any]:
        return {
            "fifo": [list(x) for x in self.fifo],
            "pending": [list(x) for x in self.pending],
            "b": self.b,
            "last_t": self._last_t,
        }

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        self.fifo = deque((int(a), int(b)) for a, b in s["fifo"])
        self.pending = deque((int(a), int(b)) for a, b in s["pending"])
        self.sorted = np.sort(np.asarray([v for _, v in self.fifo], np.int64))
        self.b, self._last_t = s["b"], s["last_t"]
        n = len(self.sorted)
        self._cache = (None, n) if n < self.min_samples else (nearest_rank(self.sorted, self.q), n)


class DailyBucketBaseline:
    def __init__(self, q: Fraction, bucket_minutes: int, days: int, min_days: int, keep_top: int):
        self.q, self.bucket_ms, self.days, self.min_days, self.keep_top = q, bucket_minutes * 60_000, days, min_days, keep_top
        self.hist: dict[int, deque[tuple[int, int, list[int]]]] = {}  # bucket -> (day, count, top values desc)
        self.cur_day: int | None = None
        self.cur: dict[int, list[int]] = {}  # today's values per bucket

    def _bucket(self, t_ms: int) -> tuple[int, int]:
        return t_ms // DAY_MS, (t_ms % DAY_MS) // self.bucket_ms

    def _roll(self, day: int) -> None:
        if self.cur_day is None:
            self.cur_day = day
            return
        if day < self.cur_day:
            raise ValueError("DailyBucketBaseline used backwards in time")
        if day == self.cur_day:
            return
        for b, vals in self.cur.items():
            top = sorted(vals, reverse=True)[: self.keep_top]
            self.hist.setdefault(b, deque()).append((self.cur_day, len(vals), top))
        self.cur = {}
        self.cur_day = day
        for b in list(self.hist):
            dq = self.hist[b]
            while dq and dq[0][0] < day - self.days:
                dq.popleft()

    def threshold(self, t_ms: int) -> tuple[int | None, int, str]:
        """(threshold or None, days used, status) for an observation at t_ms. Uses only earlier days."""
        day, b = self._bucket(t_ms)
        self._roll(day)
        entries = [e for e in self.hist.get(b, ()) if day - self.days <= e[0] < day]
        if len(entries) < self.min_days:
            return None, len(entries), "WARMUP"
        n = sum(e[1] for e in entries)
        if n == 0:
            return None, len(entries), "EMPTY"
        r_top = n - max(1, math.ceil(self.q * n)) + 1  # rank from the top of the nearest-rank quantile
        if r_top > self.keep_top:
            return None, len(entries), "INEXACT"
        merged = sorted((v for e in entries for v in e[2]), reverse=True)
        return merged[r_top - 1], len(entries), "OK"

    def add(self, t_ms: int, value: int) -> None:
        day, b = self._bucket(t_ms)
        self._roll(day)
        self.cur.setdefault(b, []).append(int(value))

    def get_state(self) -> dict[str, Any]:
        return {
            "hist": {str(b): [list(e[:2]) + [e[2]] for e in dq] for b, dq in self.hist.items()},
            "cur_day": self.cur_day,
            "cur": {str(b): v for b, v in self.cur.items()},
        }

    def set_state(self, s: dict[str, Any]) -> None:
        if s:
            self.hist = {int(b): deque((int(e[0]), int(e[1]), [int(x) for x in e[2]]) for e in dq) for b, dq in s["hist"].items()}
            self.cur_day = s["cur_day"]
            self.cur = {int(b): [int(x) for x in v] for b, v in s["cur"].items()}
