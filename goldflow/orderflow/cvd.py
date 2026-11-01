"""Phase 7: running-delta paths. Intra-bar delta extremes (``delta.py``) and CVD with resets both come from here.

Both are built from the processor's finalised base-interval bars (default 1 s), in time order:
* the path value after each base bar is the running sum of ``delta = buy_lots - sell_lots`` (exact integers);
* for an output bar, ``open`` is the value before its first base bar, ``close`` the value after its last one, and
  ``high``/``low`` the extremes of the path *including the open*. Their resolution is the base interval: two
  opposite moves inside the same second are not seen. Unknown-side lots never enter delta.

``PathAgg`` keeps the one bar that may still receive base bars (``carry``) and the bars already complete
(``done``) until the caller takes them when the processor finalises that bar.
"""

from __future__ import annotations

from typing import Any

import numpy as np
from numpy.typing import NDArray

I64 = np.int64
Row = list[int]  # [bucket, open, high, low, close]


class PathAgg:
    """Path rows per bucket of ``interval_ms``. ``reset`` = True restarts the path at 0 at every bucket (intra-bar
    delta); False keeps one continuous running value (CVD inside one reset instance)."""

    __slots__ = ("I", "reset", "v", "carry", "done")

    def __init__(self, interval_ms: int, reset: bool) -> None:
        self.I = interval_ms
        self.reset = reset
        self.v = 0
        self.carry: Row | None = None
        self.done: dict[int, Row] = {}

    def add(self, s: NDArray[Any], d: NDArray[Any]) -> None:
        """Base bars in time order: starts ``s`` and deltas ``d``."""
        if not len(s):
            return
        I = self.I
        bk = s - s % I
        new = np.ones(len(bk), bool)
        new[1:] = bk[1:] != bk[:-1]
        st = np.flatnonzero(new)
        c = self.carry
        merge = c is not None and c[0] == int(bk[0])
        if c is not None and not merge:
            self.done[c[0]] = c
        cs = np.cumsum(d.astype(I64))
        if self.reset:
            before = (cs - d)[st]  # global cumsum before each group's first bar
            start_val = np.zeros(len(st), I64)
            if merge:
                assert c is not None
                start_val[0] = c[4]
            path = cs - np.repeat(before, np.diff(np.append(st, len(cs)))) + np.repeat(start_val, np.diff(np.append(st, len(cs))))
            opens = start_val.copy()
        else:
            path = self.v + cs
            prev = np.concatenate([[self.v], path[:-1]])
            opens = prev[st]
        hi = np.maximum(np.maximum.reduceat(path, st), opens)
        lo = np.minimum(np.minimum.reduceat(path, st), opens)
        en = np.append(st[1:], len(path)) - 1
        close = path[en]
        rows = [[int(bk[a]), int(o), int(h), int(lw), int(cl)] for a, o, h, lw, cl in zip(st, opens, hi, lo, close)]
        if merge:
            assert c is not None
            rows[0] = [c[0], c[1], max(c[2], rows[0][2]), min(c[3], rows[0][3]), rows[0][4]]
        for r in rows[:-1]:
            self.done[r[0]] = r
        self.carry = rows[-1]
        if not self.reset:
            self.v = int(path[-1])

    def pop(self, bucket: int) -> Row | None:
        r = self.done.pop(bucket, None)
        if r is None and self.carry is not None and self.carry[0] == bucket:
            r, self.carry = self.carry, None
        return r

    def drop_before(self, bucket: int) -> None:
        for k in [k for k in self.done if k < bucket]:
            del self.done[k]

    def empty(self) -> bool:
        return self.carry is None and not self.done

    def state(self) -> list[Any]:
        return [self.v, self.carry, [self.done[k] for k in sorted(self.done)]]

    @classmethod
    def load(cls, interval_ms: int, reset: bool, s: list[Any]) -> "PathAgg":
        a = cls(interval_ms, reset)
        a.v, a.carry = int(s[0]), (list(s[1]) if s[1] is not None else None)
        a.done = {int(r[0]): list(r) for r in s[2]}
        return a


class CvdSeries:
    """CVD of one reset instance (a UTC day, trading day, session, custom range or anchor), per output interval."""

    __slots__ = ("inst", "aggs")

    def __init__(self, inst: Any, intervals_ms: dict[str, int]) -> None:
        self.inst = inst
        self.aggs = {n: PathAgg(ms, reset=False) for n, ms in intervals_ms.items()}

    def add(self, s: NDArray[Any], d: NDArray[Any]) -> None:
        m = (s >= self.inst.start_ms) & (s < self.inst.end_ms)
        if m.any():
            ss, dd = s[m], d[m]
            for a in self.aggs.values():
                a.add(ss, dd)

    def empty(self) -> bool:
        return all(a.empty() for a in self.aggs.values())
