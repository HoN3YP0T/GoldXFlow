"""LiquidityTracker: a BookDriver listener that keeps near-touch flow and samples book metrics (Phase 8).

Samples are stamped with the book time (E of the last applied event) and emitted only from a VALID book. When a book
is invalid the tracker emits nothing for it and its rolling flow restarts from empty after the next sync.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .book import BookKey, LevelChange, LocalOrderBook
from .capture import CaptureRecord
from .metrics import NearTouchFlow, book_metrics, classify_changes

Level = tuple[int, int]


class LiquidityTracker:
    def __init__(
        self,
        cadence_ms: int = 1_000,
        flow_window_ms: int = 60_000,
        touch_ticks: int = 10,
        tick_bands: tuple[int, ...] = (10, 50, 100, 500),
        pct_bands: tuple[str, ...] = ("0.1", "0.25", "0.5", "1.0"),
        top_k: int = 5,
        sink: Callable[[dict[str, Any]], None] | None = None,
    ):
        if cadence_ms <= 0:
            raise ValueError("cadence_ms must be > 0")
        self.cadence_ms, self.flow_window_ms, self.touch_ticks = cadence_ms, flow_window_ms, touch_ticks
        self.tick_bands, self.pct_bands, self.top_k = tick_bands, pct_bands, top_k
        self.sink = sink
        self.flows: dict[BookKey, NearTouchFlow] = {}
        self.next_sample: dict[BookKey, int] = {}
        self.last: dict[BookKey, dict[str, Any]] = {}
        self.samples = 0

    def __call__(
        self, book: LocalOrderBook, changes: list[LevelChange], bb: Level | None, ba: Level | None, rec: CaptureRecord
    ) -> None:
        flow = self.flows.get(book.key)
        if flow is None:
            flow = self.flows[book.key] = NearTouchFlow(self.flow_window_ms, self.touch_ticks)
        if not book.is_valid or book.last_event_ms is None:
            flow.reset()
            self.next_sample.pop(book.key, None)
            self.last.pop(book.key, None)
            return
        t = book.last_event_ms
        if changes and bb is not None and ba is not None:
            flow.update(t, classify_changes(changes), bb[0], ba[0])
        elif flow.valid_since is None:
            flow.update(t, [], None, None)  # start the window at the first valid moment
        nxt = self.next_sample.get(book.key)
        if nxt is None or t >= nxt:
            m = book_metrics(book, self.tick_bands, self.pct_bands, self.top_k).as_dict()
            m["near_touch_flow"] = flow.value(t)
            m["flow_window_ms"], m["touch_ticks"] = self.flow_window_ms, self.touch_ticks
            self.last[book.key] = m
            self.samples += 1
            if self.sink is not None:
                self.sink(m)
            self.next_sample[book.key] = (t // self.cadence_ms + 1) * self.cadence_ms
