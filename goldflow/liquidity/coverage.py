"""Per-interval depth capability (Phase 8).

`depth_capability(start, end)` is VALID only when one continuous VALID book interval covers the whole half-open window
[start, end). Every depth-based metric and signal must check it (`require_valid_depth`) and produce nothing otherwise.
There is no historical price-level L2 for 2026-04-30 .. 2026-10-30, so every window in that period is ABSENT.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from ..providers.base import CapabilityUnavailable
from .book import BookKey, LocalOrderBook, ValidityInterval


class DepthCapability(str, Enum):
    VALID = "VALID"  # one continuous, sequence-checked book covers the whole window
    PARTIAL = "PARTIAL"  # a valid book covers only part of the window (a gap or resync inside it)
    ABSENT = "ABSENT"  # no valid book at any time in the window (not recorded, or invalid throughout)


class DepthUnavailable(CapabilityUnavailable):
    """A depth-based calculation was requested for a window without a continuous valid book."""


@dataclass(frozen=True)
class Interval:
    start_ms: int
    end_ms: int  # exclusive


class DepthCoverage:
    """Closed VALID intervals (from a recording or replay) plus, optionally, a live book's open interval."""

    def __init__(self, key: BookKey, intervals: Iterable[Interval] = (), live_book: LocalOrderBook | None = None):
        self.key = key
        self._closed = sorted(intervals, key=lambda x: x.start_ms)
        self._live = live_book

    @classmethod
    def from_book(cls, book: LocalOrderBook, live: bool = True) -> "DepthCoverage":
        closed = [Interval(iv.start_ms, iv.end_ms) for iv in book.intervals if iv.end_ms is not None]
        return cls(book.key, closed, book if live else None)

    @classmethod
    def from_records(cls, key: BookKey, rows: Iterable[dict[str, Any]]) -> "DepthCoverage":
        return cls(key, [Interval(int(r["start_ms"]), int(r["end_ms"])) for r in rows if r.get("end_ms") is not None])

    def intervals(self) -> list[Interval]:
        out = list(self._closed)
        if self._live is not None and self._live.is_valid and self._live.intervals:
            iv: ValidityInterval = self._live.intervals[-1]
            until = self._live.known_valid_until()
            if iv.end_ms is None and until is not None:
                out.append(Interval(iv.start_ms, until))
        return out

    def capability(self, start_ms: int, end_ms: int) -> DepthCapability:
        if end_ms <= start_ms:
            raise ValueError("empty window")
        overlap = False
        for iv in self.intervals():
            if iv.start_ms <= start_ms and end_ms <= iv.end_ms:
                return DepthCapability.VALID
            if iv.start_ms < end_ms and start_ms < iv.end_ms:
                overlap = True
        return DepthCapability.PARTIAL if overlap else DepthCapability.ABSENT

    def require_valid(self, start_ms: int, end_ms: int, what: str = "depth metric") -> None:
        cap = self.capability(start_ms, end_ms)
        if cap is not DepthCapability.VALID:
            raise DepthUnavailable(f"{what} refused: depth capability {cap.value} for {self.key.label} in [{start_ms}, {end_ms})")

    def summary(self) -> dict[str, Any]:
        ivs = self.intervals()
        return {
            "book": self.key.label,
            "valid_intervals": len(ivs),
            "valid_ms": sum(i.end_ms - i.start_ms for i in ivs),
            "first_ms": ivs[0].start_ms if ivs else None,
            "last_ms": ivs[-1].end_ms if ivs else None,
        }


def require_valid_depth(coverage: DepthCoverage, start_ms: int, end_ms: int, what: str = "depth metric") -> None:
    coverage.require_valid(start_ms, end_ms, what)
