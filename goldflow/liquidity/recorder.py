"""Live depth recorder for Binance USD-M futures (Phase 8).

Connects to the combined WebSocket stream (`<symbol>@depth@100ms`, plus the RPI stream when enabled), persists every
message to the raw capture *before* feeding it to the book, fetches REST snapshots only when a book needs one and
has buffered at least one event, and reconnects with bounded exponential backoff. Connections are rotated before
Binance's 24-hour limit. Every connection change is written to the capture, so a replay invalidates the book at the
same places the live run did.

Host: Binance answers HTTP 451 to US hosts. Run this on the trading PC or a non-US VPS; a geo-block stops the
recorder (it is never worked around).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from ..observability import metrics
from .book import BookStatus
from .capture import CaptureWriter
from .driver import BookDriver

log = logging.getLogger("goldflow.depth")


class WsConnection(Protocol):
    async def recv(self) -> str | bytes: ...


Connector = Callable[[str], contextlib.AbstractAsyncContextManager[WsConnection]]
SnapshotFetcher = Callable[[str], tuple[int, str]]  # book kind -> (http status, response text)


class SnapshotRefused(RuntimeError):
    """The REST snapshot endpoint refuses this host (e.g. HTTP 451). The recorder stops."""


@dataclass
class RecorderConfig:
    url: str
    snapshot_min_interval_s: float = 2.0
    reconnect_base_s: float = 1.0
    reconnect_cap_s: float = 60.0
    max_connection_s: float = 23 * 3600 + 50 * 60
    recv_timeout_s: float = 1.0
    status_every_s: float = 60.0
    snapshot_kinds: tuple[str, ...] = ("regular",)
    snapshot_limit: int = 1000


@dataclass
class RecorderSummary:
    status: str = "created"
    connections: int = 0
    messages: int = 0
    snapshots_requested: int = 0
    snapshot_errors: int = 0
    reconnects: int = 0
    last_error: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def default_connector(url: str) -> contextlib.AbstractAsyncContextManager[WsConnection]:
    try:
        import websockets
    except ImportError as e:  # pragma: no cover - depends on the optional extra
        raise RuntimeError("the live recorder needs the 'websockets' package: pip install -e .[live]") from e
    cm: contextlib.AbstractAsyncContextManager[WsConnection] = websockets.connect(
        url, ping_interval=20, ping_timeout=60, max_size=2**24, open_timeout=30
    )
    return cm


class DepthRecorder:
    def __init__(
        self,
        cfg: RecorderConfig,
        driver: BookDriver,
        writer: CaptureWriter,
        fetch_snapshot: SnapshotFetcher,
        connect: Connector = default_connector,
        now_ms: Callable[[], int] = lambda: int(time.time() * 1000),
    ):
        self.cfg, self.driver, self.writer = cfg, driver, writer
        self.fetch, self.connect, self.now_ms = fetch_snapshot, connect, now_ms
        self.summary = RecorderSummary()
        self._stop = asyncio.Event()
        self._snap_tasks: dict[str, asyncio.Task[None]] = {}
        self._last_snap: dict[str, float] = {}
        self._fatal: BaseException | None = None

    def stop(self) -> None:
        self._stop.set()

    def _record(self, kind: str, raw: str, meta: dict[str, Any] | None = None) -> None:
        rec = self.writer.write(kind, self.now_ms(), raw, meta)  # durable capture first ...
        self.driver.feed(rec)  # ... then the book
        if kind == "ws":
            self.summary.messages += 1
            metrics.inc("depth_ws_messages", provider="binance")

    def _maybe_snapshot(self) -> None:
        for kind in self.cfg.snapshot_kinds:
            if kind not in self.driver.books or not self.driver.needs_snapshot(kind):
                continue
            t = self._snap_tasks.get(kind)
            if t is not None and not t.done():
                continue
            if time.monotonic() - self._last_snap.get(kind, -1e9) < self.cfg.snapshot_min_interval_s:
                continue
            self._last_snap[kind] = time.monotonic()
            self._snap_tasks[kind] = asyncio.get_running_loop().create_task(self._snapshot(kind))

    async def _snapshot(self, kind: str) -> None:
        self.summary.snapshots_requested += 1
        try:
            status, text = await asyncio.get_running_loop().run_in_executor(None, self.fetch, kind)
        except SnapshotRefused as e:
            self._fatal = e
            self.summary.last_error = str(e)
            self._stop.set()
            return
        except Exception as e:  # network trouble: recorded, retried after snapshot_min_interval_s
            self.summary.snapshot_errors += 1
            self.summary.last_error = repr(e)
            log.warning("depth snapshot (%s) failed: %r", kind, e)
            return
        self._record("snapshot", text, {"book": kind, "limit": self.cfg.snapshot_limit, "http_status": status})
        if status != 200:
            self.summary.snapshot_errors += 1

    async def run(self, duration_s: float | None = None) -> RecorderSummary:
        deadline = time.monotonic() + duration_s if duration_s else None
        attempt = 0
        last_status = time.monotonic()
        self.summary.status = "running"
        while not self._stop.is_set():
            reason = "server closed"
            try:
                async with self.connect(self.cfg.url) as ws:
                    self.summary.connections += 1
                    attempt = 0
                    started = time.monotonic()
                    self._record("conn", "", {"event": "OPEN", "url": self.cfg.url})
                    while not self._stop.is_set():
                        if deadline is not None and time.monotonic() >= deadline:
                            reason = "duration reached"
                            self._stop.set()
                            break
                        if time.monotonic() - started >= self.cfg.max_connection_s:
                            reason = "planned rotation before the 24 h limit"
                            break
                        try:
                            msg = await asyncio.wait_for(ws.recv(), self.cfg.recv_timeout_s)
                        except asyncio.TimeoutError:
                            continue
                        self._record("ws", msg.decode() if isinstance(msg, bytes) else msg)
                        self._maybe_snapshot()
                        if time.monotonic() - last_status >= self.cfg.status_every_s:
                            last_status = time.monotonic()
                            self._status_log()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                reason = f"{type(e).__name__}: {e}"
                self.summary.last_error = reason
            if self._stop.is_set() and reason == "server closed":
                reason = f"stop requested ({self._fatal})" if self._fatal is not None else "stop requested"
            self._record("conn", "", {"event": "CLOSE", "reason": reason})
            if self._stop.is_set():
                break
            self.summary.reconnects += 1
            attempt += 1
            delay = min(self.cfg.reconnect_cap_s, self.cfg.reconnect_base_s * 2 ** min(attempt - 1, 10))
            delay *= 0.5 + random.random() / 2
            log.warning("depth stream closed (%s); reconnecting in %.1fs", reason, delay)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), delay)
        for t in self._snap_tasks.values():
            if not t.done():
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t
        self.writer.flush()
        self.driver.finish("RECORDER_STOPPED")
        self.summary.status = "geo_blocked" if self._fatal is not None else "stopped"
        self.summary.extra = {
            "session": self.writer.session,
            "files": [str(p) for p in self.writer.files],
            "bytes_written": self.writer.bytes_written,
            "digests": self.driver.digests(),
            "books": {k: _book_status(b) for k, b in self.driver.books.items()},
        }
        return self.summary

    def _status_log(self) -> None:
        for k, b in self.driver.books.items():
            metrics.gauge("depth_book_valid", 1.0 if b.status is BookStatus.VALID else 0.0, book=k)
            log.info("depth book %s %s", k, _book_status(b))


def _book_status(b: Any) -> dict[str, Any]:
    bb, ba = b.best_bid(), b.best_ask()
    return {
        "status": b.status.value,
        "reason": b.invalid_reason,
        "last_update_id": b.last_update_id,
        "last_event_ms": b.last_event_ms,
        "bid_levels": len(b.bids),
        "ask_levels": len(b.asks),
        "best_bid_ticks": bb[0] if bb else None,
        "best_ask_ticks": ba[0] if ba else None,
        "valid_intervals": len(b.intervals),
        "counters": dict(b.counters.__dict__),
    }
