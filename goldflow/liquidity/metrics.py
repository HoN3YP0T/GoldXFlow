"""Book state and liquidity metrics (Phase 8). Definitions: docs/METRICS_LIQUIDITY_V1.md.

All metrics are computed only from a VALID book (`DepthUnavailable` otherwise) and in integer ticks/lots. A distance
band that reaches beyond the depth the snapshot actually covered is reported as incomplete (value None), never
extrapolated.
"""

from __future__ import annotations

import heapq
import math
from bisect import bisect_left, bisect_right
from collections import deque
from itertools import accumulate
from dataclasses import dataclass, field
from enum import Enum
from fractions import Fraction
from typing import Any

from .book import LevelChange, LocalOrderBook
from .coverage import DepthUnavailable


@dataclass(frozen=True)
class BandSide:
    lots: int | None  # None: band reaches beyond the known depth on this side
    levels: int | None
    topk_share: float | None  # share of the band's depth held by its k largest levels


@dataclass(frozen=True)
class BandMetrics:
    band: str  # e.g. "ticks:50" or "pct:0.25"
    bid: BandSide
    ask: BandSide
    imbalance: float | None  # (bid - ask) / (bid + ask); None if either side is incomplete or both are empty


@dataclass(frozen=True)
class BookMetrics:
    book: str
    book_time_ms: int
    last_update_id: int
    best_bid_ticks: int
    best_bid_lots: int
    best_ask_ticks: int
    best_ask_lots: int
    spread_ticks: int
    mid_x2_ticks: int  # 2 x mid, exact
    bid_levels: int
    ask_levels: int
    touch_imbalance: float  # (best_bid_lots - best_ask_lots) / (sum)
    bands: list[BandMetrics] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        d = dict(self.__dict__)
        d["bands"] = [
            {"band": b.band, "bid": b.bid.__dict__, "ask": b.ask.__dict__, "imbalance": b.imbalance} for b in self.bands
        ]
        return d


def _ratio(a: int, b: int) -> float | None:
    return None if a + b == 0 else (a - b) / (a + b)


def _topk(qs: list[int], k: int, tot: int) -> float | None:
    return None if tot == 0 else sum(heapq.nlargest(k, qs)) / tot


def book_metrics(
    book: LocalOrderBook,
    tick_bands: tuple[int, ...] = (10, 50, 100, 500),
    pct_bands: tuple[str, ...] = ("0.1", "0.25", "0.5", "1.0"),
    top_k: int = 5,
) -> BookMetrics:
    """Snapshot metrics of a VALID book. Raises DepthUnavailable for an invalid, syncing or one-sided book."""
    if not book.is_valid:
        raise DepthUnavailable(f"book {book.key.label} is {book.status.value} ({book.invalid_reason}); no depth metrics")
    bb, ba = book.best_bid(), book.best_ask()
    if bb is None or ba is None or book.last_event_ms is None or book.last_update_id is None:
        raise DepthUnavailable(f"book {book.key.label} has an empty side; no depth metrics")
    mid_x2 = bb[0] + ba[0]
    floor, cap = book.known_bid_floor, book.known_ask_cap
    # one sort per side, prefix sums for band totals (exact integers)
    bp = sorted(book.bids)  # ascending
    bq = [book.bids[p] for p in bp]
    ap = sorted(book.asks)
    aq = [book.asks[p] for p in ap]
    bcum = list(accumulate(bq, initial=0))
    acum = list(accumulate(aq, initial=0))
    specs: list[tuple[str, Fraction, Fraction]] = []  # (label, lower bound for 2p on bids, upper bound for 2p on asks)
    for n in tick_bands:
        specs.append((f"ticks:{n}", Fraction(mid_x2 - 2 * n), Fraction(mid_x2 + 2 * n)))
    for x in pct_bands:
        f = Fraction(x) / 100
        specs.append((f"pct:{x}", mid_x2 * (1 - f), mid_x2 * (1 + f)))
    bands: list[BandMetrics] = []
    for label, lo2, hi2 in specs:
        p_lo, p_hi = math.ceil(lo2 / 2), math.floor(hi2 / 2)  # bids: p >= p_lo; asks: p <= p_hi (exact)
        bid_complete = floor is None or floor <= p_lo
        ask_complete = cap is None or cap >= p_hi
        if bid_complete:
            i = bisect_left(bp, p_lo)
            btot = bcum[-1] - bcum[i]
            bside = BandSide(btot, len(bp) - i, _topk(bq[i:], top_k, btot))
        else:
            btot, bside = 0, BandSide(None, None, None)
        if ask_complete:
            j = bisect_right(ap, p_hi)
            atot = acum[j]
            aside = BandSide(atot, j, _topk(aq[:j], top_k, atot))
        else:
            atot, aside = 0, BandSide(None, None, None)
        imb = _ratio(btot, atot) if bid_complete and ask_complete else None
        bands.append(BandMetrics(label, bside, aside, imb))
    touch = _ratio(bb[1], ba[1])
    assert touch is not None
    return BookMetrics(
        book=book.key.label,
        book_time_ms=book.last_event_ms,
        last_update_id=book.last_update_id,
        best_bid_ticks=bb[0],
        best_bid_lots=bb[1],
        best_ask_ticks=ba[0],
        best_ask_lots=ba[1],
        spread_ticks=ba[0] - bb[0],
        mid_x2_ticks=mid_x2,
        bid_levels=len(book.bids),
        ask_levels=len(book.asks),
        touch_imbalance=touch,
        bands=bands,
    )


# ------------------------------------------------------------------------------------------------- level changes
class ChangeKind(str, Enum):
    ADDITION = "ADDITION"
    # The reduction is matched by passive volume traded at that price on that side in the same interval. Consistent
    # with a fill; still not proof (a cancel and a trade can coincide).
    REDUCTION_WITH_TRADE = "REDUCTION_WITH_TRADE"
    # No matching trade volume: "reduction (cancel or hidden fill unknown)". L2 deltas cannot tell which.
    REDUCTION_UNATTRIBUTED = "REDUCTION_UNATTRIBUTED"


@dataclass(frozen=True)
class ClassifiedChange:
    side: str
    price_ticks: int
    kind: ChangeKind
    lots: int


def classify_changes(
    changes: list[LevelChange], traded_passive: dict[tuple[str, int], int] | None = None
) -> list[ClassifiedChange]:
    """Label each level change. `traded_passive[(side, price_ticks)]` = lots executed against resting orders on that
    side at that price during the same interval (aggressive SELL trades consume BID, aggressive BUY trades consume ASK).
    Without trade input every reduction is REDUCTION_UNATTRIBUTED."""
    left = dict(traded_passive or {})
    out: list[ClassifiedChange] = []
    for c in changes:
        d = c.delta_lots
        if d > 0:
            out.append(ClassifiedChange(c.side, c.price_ticks, ChangeKind.ADDITION, d))
        elif d < 0:
            red = -d
            m = min(red, left.get((c.side, c.price_ticks), 0))
            if m:
                left[(c.side, c.price_ticks)] -= m
                out.append(ClassifiedChange(c.side, c.price_ticks, ChangeKind.REDUCTION_WITH_TRADE, m))
            if red - m:
                out.append(ClassifiedChange(c.side, c.price_ticks, ChangeKind.REDUCTION_UNATTRIBUTED, red - m))
    return out


class NearTouchFlow:
    """Rolling additions and reductions within `ticks` of the touch (the best price on that side *before* each update)
    over the trailing `window_ms` of exchange time. Depletion = reductions near the touch.

    A value is returned only when the whole window lies inside one continuous VALID run; after a reset (book
    invalidated) the window must refill before values are produced again."""

    FIELDS = ("bid_add", "bid_red", "ask_add", "ask_red")

    def __init__(self, window_ms: int = 60_000, ticks: int = 10):
        if window_ms <= 0 or ticks < 0:
            raise ValueError("window_ms must be > 0 and ticks >= 0")
        self.window_ms, self.ticks = window_ms, ticks
        self.events: deque[tuple[int, int, int, int, int]] = deque()
        self.sums = [0, 0, 0, 0]
        self.valid_since: int | None = None

    def reset(self) -> None:
        self.events.clear()
        self.sums = [0, 0, 0, 0]
        self.valid_since = None

    def update(self, t_ms: int, changes: list[ClassifiedChange], best_bid: int | None, best_ask: int | None) -> None:
        if self.valid_since is None:
            self.valid_since = t_ms
        v = [0, 0, 0, 0]
        for c in changes:
            if c.side == "BID":
                if best_bid is None or c.price_ticks < best_bid - self.ticks:
                    continue
                v[0 if c.kind is ChangeKind.ADDITION else 1] += c.lots
            else:
                if best_ask is None or c.price_ticks > best_ask + self.ticks:
                    continue
                v[2 if c.kind is ChangeKind.ADDITION else 3] += c.lots
        self.events.append((t_ms, v[0], v[1], v[2], v[3]))
        for i in range(4):
            self.sums[i] += v[i]
        self._evict(t_ms)

    def _evict(self, now_ms: int) -> None:
        while self.events and self.events[0][0] <= now_ms - self.window_ms:
            e = self.events.popleft()
            for i in range(4):
                self.sums[i] -= e[i + 1]

    def value(self, now_ms: int) -> dict[str, int] | None:
        if self.valid_since is None or now_ms - self.window_ms < self.valid_since:
            return None
        self._evict(now_ms)
        return dict(zip(self.FIELDS, self.sums))

    def get_state(self) -> dict[str, Any]:
        return {"events": [list(e) for e in self.events], "sums": list(self.sums), "valid_since": self.valid_since}

    def set_state(self, s: dict[str, Any]) -> None:
        if s:
            self.events = deque((int(a), int(b), int(c), int(d), int(e)) for a, b, c, d, e in s["events"])
            self.sums = [int(x) for x in s["sums"]]
            self.valid_since = s["valid_since"]
