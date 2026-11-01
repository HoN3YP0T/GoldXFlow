"""Local price-level order book for Binance USD-M futures (Phase 8).

Synchronisation follows Binance's documented procedure for `<symbol>@depth@100ms` + `GET /fapi/v1/depth`:

  1. Subscribe to the diff stream and buffer events.
  2. Fetch a REST snapshot (`lastUpdateId` = L). Request it only after at least one event is buffered.
  3. Drop buffered events with u < L.
  4. The first applied event must satisfy U <= L <= u. If the first remaining event has U > L, updates between the
     snapshot and the stream are missing: the snapshot is rejected and a new one is needed.
  5. Every later event must have pu == the previous event's u. Otherwise the book is INVALID: it is cleared,
     the validity interval is closed, and a new snapshot is needed (the breaking event is kept in the buffer because
     it may bridge the next snapshot).
  6. A quantity of 0 removes the level.

The book never guesses. A malformed price/quantity, a crossed book (configurable) or a lost connection invalidates it
exactly like a sequence gap. Every input event receives one final outcome (APPLIED, DROPPED_BEFORE_SNAPSHOT,
DUPLICATE, DISCARDED) and the book records continuous VALID intervals on the exchange clock, so every downstream
metric can prove the book was valid at the time it used it.

Time base: the exchange event time `E` (ms). A VALID interval starts at the E of the event that bridged the snapshot
and ends at (E of the last event applied before the break) + 1 ms, because a missed update could have happened at any
time after that. An open interval extends as linked events arrive.

State is held in integer ticks (price / tick) and lots (quantity / step), so the state digest is exact.
"""

from __future__ import annotations

import hashlib
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .units import UnitError, UnitScale


class MergedBookError(ValueError):
    """An event for a different provider/venue/instrument/book kind was routed to this book. Books are never merged."""


class BookStatus(str, Enum):
    NO_BOOK = "NO_BOOK"  # no usable snapshot: buffering, a new snapshot is required
    SYNCING = "SYNCING"  # snapshot applied, waiting for the event that bridges it (U <= L <= u)
    VALID = "VALID"  # continuous, sequence-checked book


class Outcome(str, Enum):
    """Final fate of each input record (stored as DepthEvent.sequence_validity)."""

    APPLIED = "APPLIED"
    DROPPED_BEFORE_SNAPSHOT = "DROPPED_BEFORE_SNAPSHOT"  # u < lastUpdateId: already contained in the snapshot
    DUPLICATE = "DUPLICATE"  # u <= last applied u while VALID
    DISCARDED = "DISCARDED"  # never applied: buffer overflow, connection loss or end of capture before a sync
    SNAPSHOT_APPLIED = "SNAPSHOT_APPLIED"
    SNAPSHOT_REJECTED = "SNAPSHOT_REJECTED"  # does not connect to the buffered stream (or arrived while VALID)
    INVALID_PAYLOAD = "INVALID_PAYLOAD"  # malformed; invalidates the book
    CONNECTION_LOST = "CONNECTION_LOST"


@dataclass(frozen=True)
class BookKey:
    """Identity of one book. One provider + venue + market + instrument + book kind = one book; never merged."""

    provider_id: str
    venue_id: str
    market_type: str
    symbol: str
    book_kind: str = "regular"  # "regular" or "rpi" (Retail Price Improvement liquidity, kept separate)

    @property
    def instrument_id(self) -> str:
        return f"{self.provider_id}:{self.market_type}:{self.symbol}"

    @property
    def label(self) -> str:
        return f"{self.instrument_id}:{self.book_kind}"


Level = tuple[int, int]  # (price_ticks, quantity_lots)


@dataclass(frozen=True)
class DepthUpdate:
    """One diff event, already converted to integer units."""

    event_time_ms: int  # E
    transaction_time_ms: int | None  # T
    first_update_id: int  # U
    final_update_id: int  # u
    prev_final_update_id: int | None  # pu
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    symbol: str | None = None
    ref: Any = None  # caller reference (capture seq) used to report the outcome


@dataclass(frozen=True)
class DepthSnapshot:
    last_update_id: int
    event_time_ms: int | None
    transaction_time_ms: int | None
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    ref: Any = None
    limit: int | None = None  # levels requested per side; a side with this many levels is truncated (deeper unknown)


@dataclass(frozen=True)
class LevelChange:
    side: str  # "BID" | "ASK"
    price_ticks: int
    old_lots: int
    new_lots: int

    @property
    def delta_lots(self) -> int:
        return self.new_lots - self.old_lots


@dataclass
class ValidityInterval:
    start_ms: int
    end_ms: int | None  # exclusive; None while the book is still VALID (see LocalOrderBook.known_valid_until)
    snapshot_update_id: int
    start_update_id: int
    last_update_id: int
    events_applied: int = 0
    end_reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass(frozen=True)
class OutcomeRecord:
    ref: Any
    outcome: Outcome
    detail: str = ""


# ------------------------------------------------------------------------------------------------- parsing
def _levels(raw: Any, prices: UnitScale, qtys: UnitScale) -> tuple[Level, ...]:
    if not isinstance(raw, list):
        raise UnitError("levels must be a list")
    out = []
    for lv in raw:
        if not isinstance(lv, (list, tuple)) or len(lv) < 2:
            raise UnitError(f"malformed level {lv!r}")
        out.append((prices.to_units(lv[0]), qtys.to_units(lv[1])))
    return tuple(out)


def parse_depth_update(msg: dict[str, Any], prices: UnitScale, qtys: UnitScale, ref: Any = None) -> DepthUpdate:
    """Binance futures `depthUpdate` (bare or wrapped in a combined-stream {"stream", "data"} envelope)."""
    d = msg.get("data", msg) if "stream" in msg else msg
    if d.get("e") != "depthUpdate":
        raise UnitError(f"not a depthUpdate event: e={d.get('e')!r}")
    try:
        return DepthUpdate(
            event_time_ms=int(d["E"]),
            transaction_time_ms=int(d["T"]) if d.get("T") is not None else None,
            first_update_id=int(d["U"]),
            final_update_id=int(d["u"]),
            prev_final_update_id=int(d["pu"]) if d.get("pu") is not None else None,
            bids=_levels(d.get("b", []), prices, qtys),
            asks=_levels(d.get("a", []), prices, qtys),
            symbol=d.get("s"),
            ref=ref,
        )
    except (KeyError, TypeError, ValueError) as e:
        raise UnitError(f"malformed depthUpdate: {e}") from e


def parse_snapshot(
    msg: dict[str, Any], prices: UnitScale, qtys: UnitScale, ref: Any = None, limit: int | None = None
) -> DepthSnapshot:
    """REST `GET /fapi/v1/depth` response."""
    try:
        return DepthSnapshot(
            last_update_id=int(msg["lastUpdateId"]),
            event_time_ms=int(msg["E"]) if msg.get("E") is not None else None,
            transaction_time_ms=int(msg["T"]) if msg.get("T") is not None else None,
            bids=_levels(msg["bids"], prices, qtys),
            asks=_levels(msg["asks"], prices, qtys),
            ref=ref,
            limit=limit,
        )
    except (KeyError, TypeError, ValueError) as e:
        raise UnitError(f"malformed depth snapshot: {e}") from e


# ------------------------------------------------------------------------------------------------- book
@dataclass
class BookCounters:
    applied: int = 0
    dropped_before_snapshot: int = 0
    duplicates: int = 0
    discarded: int = 0
    snapshots_applied: int = 0
    snapshots_rejected: int = 0
    invalidations: int = 0
    invalid_payloads: int = 0
    connection_losses: int = 0


@dataclass
class LocalOrderBook:
    key: BookKey
    max_buffer: int = 20_000
    invalidate_on_cross: bool = True
    status: BookStatus = BookStatus.NO_BOOK
    bids: dict[int, int] = field(default_factory=dict)
    asks: dict[int, int] = field(default_factory=dict)
    snapshot_id: int | None = None
    last_update_id: int | None = None  # u of the last applied event (or L after a snapshot)
    last_event_ms: int | None = None  # E of the last applied event
    buffer: deque[DepthUpdate] = field(default_factory=deque)
    intervals: list[ValidityInterval] = field(default_factory=list)
    outcomes: list[OutcomeRecord] = field(default_factory=list)
    counters: BookCounters = field(default_factory=BookCounters)
    last_changes: list[LevelChange] = field(default_factory=list)
    invalid_reason: str | None = "NO_SNAPSHOT"
    pending_snapshot_ref: Any = None  # outcome of a snapshot is decided when it bridges (or fails to)
    # Depth beyond the snapshot's deepest level is not known (only levels updated since then are). None = whole side known.
    known_bid_floor: int | None = None
    known_ask_cap: int | None = None

    # ---------------------------------------------------------------- public
    @property
    def needs_snapshot(self) -> bool:
        return self.status is BookStatus.NO_BOOK

    @property
    def is_valid(self) -> bool:
        return self.status is BookStatus.VALID

    def known_valid_until(self) -> int | None:
        """Exclusive end of the time the book is proven valid (last applied E + 1), or None if not VALID."""
        return self.last_event_ms + 1 if self.is_valid and self.last_event_ms is not None else None

    def drain_outcomes(self) -> list[OutcomeRecord]:
        out, self.outcomes = self.outcomes, []
        return out

    def on_update(self, ev: DepthUpdate) -> list[LevelChange]:
        """Feed one diff event. Returns the level changes it applied (empty if it was buffered or dropped)."""
        self.last_changes = []
        if ev.symbol is not None and ev.symbol != self.key.symbol:
            raise MergedBookError(f"event for {ev.symbol} routed to book {self.key.label}")
        if self.status is BookStatus.VALID:
            assert self.last_update_id is not None
            if ev.final_update_id <= self.last_update_id:
                self._outcome(ev.ref, Outcome.DUPLICATE, f"u={ev.final_update_id} <= {self.last_update_id}")
                self.counters.duplicates += 1
                return []
            if ev.prev_final_update_id != self.last_update_id:
                self._invalidate(f"SEQUENCE_GAP pu={ev.prev_final_update_id} expected {self.last_update_id}")
                self._buffer(ev)
                return []
            return self._apply(ev)
        if self.status is BookStatus.SYNCING:
            return self._try_bridge(ev)
        self._buffer(ev)
        return []

    def on_snapshot(self, snap: DepthSnapshot) -> list[LevelChange]:
        """Apply a REST snapshot if a new one is needed; buffered events are then processed in arrival order."""
        self.last_changes = []
        if self.status is not BookStatus.NO_BOOK:
            self._outcome(snap.ref, Outcome.SNAPSHOT_REJECTED, f"book is {self.status.value}; snapshot not needed")
            self.counters.snapshots_rejected += 1
            return []
        lid = snap.last_update_id
        while self.buffer and self.buffer[0].final_update_id < lid:
            old = self.buffer.popleft()
            self._outcome(old.ref, Outcome.DROPPED_BEFORE_SNAPSHOT, f"u={old.final_update_id} < L={lid}")
            self.counters.dropped_before_snapshot += 1
        if self.buffer and self.buffer[0].first_update_id > lid:
            self._outcome(
                snap.ref, Outcome.SNAPSHOT_REJECTED, f"L={lid} older than first buffered U={self.buffer[0].first_update_id}"
            )
            self.counters.snapshots_rejected += 1
            return []
        self.bids = {p: q for p, q in snap.bids if q > 0}
        self.asks = {p: q for p, q in snap.asks if q > 0}
        self.snapshot_id = lid
        self.last_update_id = lid
        lim = snap.limit
        self.known_bid_floor = min(self.bids) if lim is not None and len(snap.bids) >= lim and self.bids else None
        self.known_ask_cap = max(self.asks) if lim is not None and len(snap.asks) >= lim and self.asks else None
        self.status = BookStatus.SYNCING
        self.invalid_reason = "AWAITING_BRIDGE_EVENT"
        self.pending_snapshot_ref = snap.ref
        changes: list[LevelChange] = []
        pending = list(self.buffer)
        self.buffer.clear()
        for ev in pending:
            changes.extend(self.on_update(ev))
        self.last_changes = changes
        return changes

    def on_disconnect(self, reason: str, ref: Any = None) -> None:
        """Connection lost (or capture truncated): continuity can no longer be proven."""
        self.counters.connection_losses += 1
        self._outcome(ref, Outcome.CONNECTION_LOST, reason)
        if self.status is not BookStatus.NO_BOOK:
            self._invalidate(f"CONNECTION_LOST {reason}")
        while self.buffer:
            ev = self.buffer.popleft()
            self._outcome(ev.ref, Outcome.DISCARDED, "connection lost before sync")
            self.counters.discarded += 1

    def on_invalid_payload(self, detail: str, ref: Any = None) -> None:
        """A message for this book could not be parsed: an update may be lost, so the book is invalidated."""
        self.counters.invalid_payloads += 1
        self._outcome(ref, Outcome.INVALID_PAYLOAD, detail)
        if self.status is not BookStatus.NO_BOOK:
            self._invalidate(f"INVALID_PAYLOAD {detail}")

    def finish(self, reason: str = "END_OF_CAPTURE") -> None:
        """End of input: discard events that never synced and close the open interval (end = known_valid_until)."""
        while self.buffer:
            ev = self.buffer.popleft()
            self._outcome(ev.ref, Outcome.DISCARDED, reason)
            self.counters.discarded += 1
        if self.status is BookStatus.SYNCING:
            self._clear(reason)
        if self.intervals and self.intervals[-1].end_ms is None:
            iv = self.intervals[-1]
            iv.end_ms = (self.last_event_ms or iv.start_ms) + 1
            iv.end_reason = reason

    # ---------------------------------------------------------------- views
    def best_bid(self) -> Level | None:
        if not self.bids:
            return None
        p = max(self.bids)
        return p, self.bids[p]

    def best_ask(self) -> Level | None:
        if not self.asks:
            return None
        p = min(self.asks)
        return p, self.asks[p]

    def sorted_bids(self) -> list[Level]:
        return sorted(self.bids.items(), key=lambda x: -x[0])

    def sorted_asks(self) -> list[Level]:
        return sorted(self.asks.items())

    def state_digest(self) -> str:
        """Exact digest of the book: status, last update id, every level (integer units)."""
        h = hashlib.sha256(f"goldflow-book-v1|{self.key.label}|{self.status.value}|{self.last_update_id}".encode())
        for side, levels in (("B", sorted(self.bids.items())), ("A", sorted(self.asks.items()))):
            h.update(side.encode())
            for p, q in levels:
                h.update(f"{p}:{q};".encode())
        return h.hexdigest()

    # ---------------------------------------------------------------- checkpoint
    def get_state(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "bids": sorted(self.bids.items()),
            "asks": sorted(self.asks.items()),
            "snapshot_id": self.snapshot_id,
            "last_update_id": self.last_update_id,
            "last_event_ms": self.last_event_ms,
            "invalid_reason": self.invalid_reason,
            "pending_snapshot_ref": self.pending_snapshot_ref,
            "known_bid_floor": self.known_bid_floor,
            "known_ask_cap": self.known_ask_cap,
            "buffer": [_update_to_dict(e) for e in self.buffer],
            "intervals": [iv.as_dict() for iv in self.intervals],
            "counters": dict(self.counters.__dict__),
        }

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        self.status = BookStatus(s["status"])
        self.bids = {int(p): int(q) for p, q in s["bids"]}
        self.asks = {int(p): int(q) for p, q in s["asks"]}
        self.snapshot_id, self.last_update_id = s["snapshot_id"], s["last_update_id"]
        self.last_event_ms, self.invalid_reason = s["last_event_ms"], s["invalid_reason"]
        self.pending_snapshot_ref = s.get("pending_snapshot_ref")
        self.known_bid_floor, self.known_ask_cap = s.get("known_bid_floor"), s.get("known_ask_cap")
        self.buffer = deque(_update_from_dict(e) for e in s["buffer"])
        self.intervals = [ValidityInterval(**iv) for iv in s["intervals"]]
        self.counters = BookCounters(**s["counters"])

    # ---------------------------------------------------------------- internals
    def _outcome(self, ref: Any, outcome: Outcome, detail: str = "") -> None:
        if ref is not None:
            self.outcomes.append(OutcomeRecord(ref, outcome, detail))

    def _buffer(self, ev: DepthUpdate) -> None:
        self.buffer.append(ev)
        while len(self.buffer) > self.max_buffer:
            old = self.buffer.popleft()
            self._outcome(old.ref, Outcome.DISCARDED, "buffer overflow while waiting for a snapshot")
            self.counters.discarded += 1

    def _try_bridge(self, ev: DepthUpdate) -> list[LevelChange]:
        lid = self.snapshot_id
        assert lid is not None
        if ev.final_update_id < lid:
            self._outcome(ev.ref, Outcome.DROPPED_BEFORE_SNAPSHOT, f"u={ev.final_update_id} < L={lid}")
            self.counters.dropped_before_snapshot += 1
            return []
        if ev.first_update_id <= lid <= ev.final_update_id:
            changes = self._apply(ev, bridging=True)
            return changes
        # U > L: updates between the snapshot and this event are missing
        self._clear(f"SNAPSHOT_GAP L={lid} < U={ev.first_update_id}")
        self._buffer(ev)
        return []

    def _apply(self, ev: DepthUpdate, bridging: bool = False) -> list[LevelChange]:
        changes: list[LevelChange] = []
        for side, book, levels in (("BID", self.bids, ev.bids), ("ASK", self.asks, ev.asks)):
            for p, q in levels:
                old = book.get(p, 0)
                if q == 0:
                    book.pop(p, None)
                else:
                    book[p] = q
                if old != q:
                    changes.append(LevelChange(side, p, old, q))
        self.last_update_id = ev.final_update_id
        self.last_event_ms = ev.event_time_ms
        self.counters.applied += 1
        self._outcome(ev.ref, Outcome.APPLIED)
        if bridging:
            self._outcome(self.pending_snapshot_ref, Outcome.SNAPSHOT_APPLIED, f"L={self.snapshot_id}")
            self.pending_snapshot_ref = None
            self.counters.snapshots_applied += 1
            self.status = BookStatus.VALID
            self.invalid_reason = None
            assert self.snapshot_id is not None
            self.intervals.append(
                ValidityInterval(ev.event_time_ms, None, self.snapshot_id, ev.first_update_id, ev.final_update_id)
            )
        iv = self.intervals[-1]
        iv.last_update_id = ev.final_update_id
        iv.events_applied += 1
        if self.invalidate_on_cross and self.bids and self.asks and max(self.bids) >= min(self.asks):
            self._invalidate(f"CROSSED_BOOK bid={max(self.bids)} ask={min(self.asks)} at u={ev.final_update_id}")
            return changes
        self.last_changes = changes
        return changes

    def _invalidate(self, reason: str) -> None:
        if self.status is BookStatus.VALID and self.intervals and self.intervals[-1].end_ms is None:
            iv = self.intervals[-1]
            iv.end_ms = (self.last_event_ms if self.last_event_ms is not None else iv.start_ms) + 1
            iv.end_reason = reason
        if self.status is not BookStatus.NO_BOOK:
            self.counters.invalidations += 1
        self._clear(reason)

    def _clear(self, reason: str) -> None:
        if self.status is BookStatus.SYNCING:
            self._outcome(self.pending_snapshot_ref, Outcome.SNAPSHOT_REJECTED, f"never bridged: {reason}")
            self.counters.snapshots_rejected += 1
            self.pending_snapshot_ref = None
        self.status = BookStatus.NO_BOOK
        self.bids, self.asks = {}, {}
        self.snapshot_id = None
        self.invalid_reason = reason


def _update_to_dict(e: DepthUpdate) -> dict[str, Any]:
    return {
        "E": e.event_time_ms,
        "T": e.transaction_time_ms,
        "U": e.first_update_id,
        "u": e.final_update_id,
        "pu": e.prev_final_update_id,
        "b": [list(x) for x in e.bids],
        "a": [list(x) for x in e.asks],
        "s": e.symbol,
        "ref": e.ref,
    }


def _update_from_dict(d: dict[str, Any]) -> DepthUpdate:
    return DepthUpdate(
        d["E"],
        d["T"],
        d["U"],
        d["u"],
        d["pu"],
        tuple((int(p), int(q)) for p, q in d["b"]),
        tuple((int(p), int(q)) for p, q in d["a"]),
        d["s"],
        d["ref"],
    )
