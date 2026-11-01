"""BookDriver: capture records -> books. The single code path shared by the live recorder and every replay (Phase 8).

The live recorder writes each payload to the capture first and then feeds the same `CaptureRecord` to this driver;
`replay_capture` reads the capture and feeds the identical records. The book state digest of a replay therefore
equals the live one by construction, and the tests prove it.

Routing: the combined-stream envelope {"stream": "...", "data": {...}} names the stream; `streams` maps stream names
to book kinds ("regular", "rpi"). A bare `depthUpdate` goes to the regular book. Subscription acks are ignored.
Connection records (OPEN, CLOSE, ERROR) and reader markers (SESSION_BOUNDARY, CAPTURE_TRUNCATED, CAPTURE_SEQ_GAP)
invalidate every book, because continuity across them cannot be proven.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from .book import (
    BookKey,
    LevelChange,
    LocalOrderBook,
    Outcome,
    OutcomeRecord,
    parse_depth_update,
    parse_snapshot,
)
from .capture import CaptureRecord
from .registry import BookRegistry
from .units import UnitError, UnitScale

Level = tuple[int, int]
# listener(book, changes, best_bid_before, best_ask_before, record)
BookListener = Callable[[LocalOrderBook, list[LevelChange], Level | None, Level | None, CaptureRecord], None]

INVALIDATING_CONN_EVENTS = {"CLOSE", "ERROR", "SESSION_BOUNDARY", "CAPTURE_TRUNCATED", "CAPTURE_SEQ_GAP"}


@dataclass(frozen=True)
class DriverConfig:
    symbol: str
    tick_size: str
    step_size: str
    streams: dict[str, str]  # stream name -> book kind
    provider_id: str = "binance"
    venue_id: str = "binance-futures-usdm"
    market_type: str = "usdm_perpetual"
    max_buffer: int = 20_000
    invalidate_on_cross: bool = True

    def key(self, kind: str) -> BookKey:
        return BookKey(self.provider_id, self.venue_id, self.market_type, self.symbol, kind)


@dataclass
class DriverStats:
    records: int = 0
    ws_depth: int = 0
    ws_ignored: int = 0  # subscription acks and other non-depth control messages
    ws_unknown_stream: int = 0
    ws_unparseable: int = 0
    snapshots: int = 0
    snapshot_http_errors: int = 0
    conn_events: int = 0
    markers: dict[str, int] = field(default_factory=dict)


class BookDriver:
    def __init__(self, cfg: DriverConfig, listeners: Iterable[BookListener] = (), collect_outcomes: bool = False):
        self.cfg = cfg
        self.prices, self.qtys = UnitScale(cfg.tick_size), UnitScale(cfg.step_size)
        self.registry = BookRegistry(cfg.max_buffer, cfg.invalidate_on_cross)
        self.books: dict[str, LocalOrderBook] = {k: self.registry.book(cfg.key(k)) for k in sorted(set(cfg.streams.values()))}
        if "regular" not in self.books:
            self.books["regular"] = self.registry.book(cfg.key("regular"))
        self.listeners = list(listeners)
        self.collect = collect_outcomes
        self.outcomes: dict[tuple[str, str], OutcomeRecord] = {}  # (book kind, record ref) -> final outcome
        self.stats = DriverStats()

    # ---------------------------------------------------------------- feeding
    def feed(self, rec: CaptureRecord) -> None:
        self.stats.records += 1
        if rec.kind == "ws":
            self._on_ws(rec)
        elif rec.kind == "snapshot":
            self._on_snapshot(rec)
        elif rec.kind == "conn":
            self._on_conn(rec)
        else:
            raise ValueError(f"unknown capture record kind {rec.kind!r}")
        self._drain()

    def feed_all(self, records: Iterable[CaptureRecord]) -> None:
        for r in records:
            self.feed(r)

    def finish(self, reason: str = "END_OF_CAPTURE") -> None:
        for b in self.books.values():
            b.finish(reason)
        self._drain()

    # ---------------------------------------------------------------- queries
    def needs_snapshot(self, kind: str = "regular") -> bool:
        """A snapshot should be requested: no usable book and at least one event buffered (Binance step 2)."""
        b = self.books[kind]
        return b.needs_snapshot and len(b.buffer) > 0

    def digests(self) -> dict[str, str]:
        return {k: b.state_digest() for k, b in sorted(self.books.items())}

    def get_state(self) -> dict[str, Any]:
        return {k: b.get_state() for k, b in self.books.items()}

    def set_state(self, s: dict[str, Any]) -> None:
        for k, st in (s or {}).items():
            if k in self.books:
                self.books[k].set_state(st)

    # ---------------------------------------------------------------- internals
    def _drain(self) -> None:
        for kind, b in self.books.items():
            for o in b.drain_outcomes():
                if self.collect:
                    self.outcomes[(kind, str(o.ref))] = o

    def _notify(
        self, book: LocalOrderBook, changes: list[LevelChange], bb: Level | None, ba: Level | None, rec: CaptureRecord
    ) -> None:
        for fn in self.listeners:
            fn(book, changes, bb, ba, rec)

    def _on_ws(self, rec: CaptureRecord) -> None:
        try:
            msg = json.loads(rec.raw)
        except ValueError:
            self.stats.ws_unparseable += 1
            for b in self.books.values():  # cannot tell which book lost an update: invalidate all
                b.on_invalid_payload("unparseable WebSocket message", rec.ref)
                self._notify(b, [], None, None, rec)
            return
        if not isinstance(msg, dict):
            self.stats.ws_ignored += 1
            return
        if "stream" in msg:
            kind = self.cfg.streams.get(str(msg["stream"]))
            if kind is None:
                self.stats.ws_unknown_stream += 1
                return
        elif msg.get("e") == "depthUpdate":
            kind = "regular"
        else:
            self.stats.ws_ignored += 1  # e.g. {"result": null, "id": 1}
            return
        book = self.books[kind]
        bb, ba = book.best_bid(), book.best_ask()
        try:
            ev = parse_depth_update(msg, self.prices, self.qtys, ref=rec.ref)
        except UnitError as e:
            self.stats.ws_unparseable += 1
            book.on_invalid_payload(str(e), rec.ref)
            self._notify(book, [], bb, ba, rec)
            return
        self.stats.ws_depth += 1
        changes = book.on_update(ev)
        self._notify(book, changes, bb, ba, rec)

    def _on_snapshot(self, rec: CaptureRecord) -> None:
        self.stats.snapshots += 1
        kind = str(rec.meta.get("book", "regular"))
        book = self.books.get(kind)
        if book is None:
            return
        if int(rec.meta.get("http_status", 200)) != 200:
            self.stats.snapshot_http_errors += 1
            return
        try:
            snap = parse_snapshot(json.loads(rec.raw), self.prices, self.qtys, ref=rec.ref, limit=rec.meta.get("limit"))
        except (UnitError, ValueError) as e:
            # A bad snapshot does not invalidate a book (it was never applied); it is recorded and another is requested.
            book.outcomes.append(OutcomeRecord(rec.ref, Outcome.INVALID_PAYLOAD, f"snapshot: {e}"))
            return
        bb, ba = book.best_bid(), book.best_ask()
        changes = book.on_snapshot(snap)
        self._notify(book, changes, bb, ba, rec)

    def _on_conn(self, rec: CaptureRecord) -> None:
        ev = str(rec.meta.get("event", ""))
        if rec.synthetic_marker:
            self.stats.markers[ev] = self.stats.markers.get(ev, 0) + 1
        else:
            self.stats.conn_events += 1
        if ev == "OPEN":
            reason = "NEW_CONNECTION"
        elif ev in INVALIDATING_CONN_EVENTS:
            reason = f"{ev} {rec.meta.get('reason', '')}".strip()
        else:
            return
        for b in self.books.values():
            if ev == "OPEN" and b.needs_snapshot and not b.buffer:
                continue  # nothing to invalidate at the start of a connection
            bb, ba = b.best_bid(), b.best_ask()
            b.on_disconnect(reason, rec.ref)
            self._notify(b, [], bb, ba, rec)


def replay_capture(
    cfg: DriverConfig, records: Iterable[CaptureRecord], listeners: Iterable[BookListener] = (), collect_outcomes: bool = False
) -> BookDriver:
    """Rebuild books from a capture with the same driver the live recorder uses."""
    drv = BookDriver(cfg, listeners, collect_outcomes)
    drv.feed_all(records)
    drv.finish()
    return drv
