"""Phase 14: the live event bus and feed health.

**LiveBus** delivers live data to the **same** consumers as the Phase 4 replay, through the same ``EventChunk``
interface (streams ``quality`` and ``trades``), so the Phase 5–12 code runs unchanged.

The bus clock is ``now - grace_ms``. A replay may finalise a bar ending at e once its clock reaches e because every
trade with time < e has already been delivered; live, a trade reaches us only after the exchange published it and the
poller fetched it. ``grace_ms`` (default 3 s: poll interval plus network latency, with margin) keeps the same
guarantee. A trade that still arrives after its bar was finalised is not added to it and is counted by the processor
(``late_after_bar_final``) and opens a ``LATE`` data-quality interval over it.

**FeedHealth**: each stream has a staleness limit. When a stream has been silent longer than its limit, a ``STALE``
data-quality interval opens at its last message time with an open end (dataset ``trades`` for the trade feed, so the
processor labels bars and the strategy engine suppresses signals: ``DQ_OPEN``). When the stream speaks again, the
interval is closed: the same (start, status) is sent again with the real end, which replaces the open end
(``merge_dq``).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pyarrow as pa

from ..replay import KEY_DTYPE, EventChunk

OPEN_END = 2**62


class LiveBus:
    def __init__(self, consumers: list[Any], grace_ms: int = 3000) -> None:
        self.consumers = consumers
        self.grace = grace_ms
        self.clock: int | None = None
        self.seq = 0
        self.pending: list[pa.Table] = []
        self.quality: list[dict[str, Any]] = []
        self.started = False

    def start(self, start_ms: int, resumed: bool = False, quality_before_start: list[dict[str, Any]] | None = None) -> None:
        for c in self.consumers:
            c.on_start({"start_ms": start_ms, "end_ms": None, "filtered": False, "resumed": resumed, "dataset_version": "live",
                        "quality_before_start": list(quality_before_start or [])})  # fmt: skip
        self.started = True

    def push_trades(self, table: pa.Table) -> None:
        if table.num_rows:
            self.pending.append(table)

    def push_quality(self, dataset: str, status: str, start_ms: int, end_ms: int) -> None:
        self.quality.append({"type": "DQ_BEGIN", "dataset": dataset, "status": status, "start_ms": int(start_ms), "end_ms": int(end_ms),
                             "avail_ms": int(start_ms)})  # fmt: skip

    def advance(self, now_ms: int) -> EventChunk | None:
        """Deliver everything pending and move the consumers' clock to now - grace (never backwards)."""
        clock = now_ms - self.grace if self.clock is None else max(self.clock, now_ms - self.grace)
        tbl = pa.concat_tables(self.pending).combine_chunks() if self.pending else None
        self.pending = []
        nq, nt = len(self.quality), (tbl.num_rows if tbl is not None else 0)
        keys = np.empty(nq + nt, KEY_DTYPE)
        keys["avail"] = clock
        keys["rank"] = 0
        keys["seq"] = np.arange(self.seq, self.seq + nq + nt)
        keys["stream"] = np.array([0] * nq + [1] * nt, np.int8)
        rows = np.concatenate([np.arange(nq), np.arange(nt)]).astype(np.int64)
        self.seq += nq + nt
        chunk = EventChunk(
            keys, rows, {"trades": tbl} if tbl is not None else {}, ["quality", "trades"], clock, quality=list(self.quality)
        )
        self.quality = []
        self.clock = clock
        for c in self.consumers:
            c.on_chunk(chunk)
        return chunk

    def stop(self, status: str = "stopped") -> None:
        for c in self.consumers:
            c.on_stop({"status": status, "end_ms": self.clock})

    def state(self) -> dict[str, Any]:
        return {"clock": self.clock, "seq": self.seq}

    def load(self, s: dict[str, Any]) -> None:
        self.clock, self.seq = s["clock"], int(s["seq"])


class FeedHealth:
    """Per-stream staleness. ``check(now)`` returns quality intervals to open or close."""

    def __init__(self, limits_ms: dict[str, int], datasets: dict[str, str]) -> None:
        self.limits = limits_ms
        self.datasets = datasets  # stream -> dataset whose DQ the staleness affects ("trades" blocks signals)
        self.last: dict[str, int | None] = {k: None for k in limits_ms}
        self.open: dict[str, int] = {}  # stream -> start of the open STALE interval
        self.events: list[dict[str, Any]] = []

    def seen(self, stream: str, t_ms: int) -> list[tuple[str, str, int, int]]:
        out = []
        if stream in self.open:
            s = self.open.pop(stream)
            out.append((self.datasets[stream], "STALE", s, t_ms))  # closes the interval: same start, real end
            self.events.append({"stream": stream, "event": "RECOVERED", "start_ms": s, "end_ms": t_ms})
        self.last[stream] = t_ms if self.last.get(stream) is None else max(int(self.last[stream] or 0), t_ms)
        return out

    def check(self, now_ms: int) -> list[tuple[str, str, int, int]]:
        out = []
        for k, lim in self.limits.items():
            last = self.last.get(k)
            if last is not None and k not in self.open and now_ms - last > lim:
                self.open[k] = last
                out.append((self.datasets[k], "STALE", last, OPEN_END))
                self.events.append({"stream": k, "event": "STALE", "start_ms": last, "detected_ms": now_ms})
        return out

    def status(self, now_ms: int) -> dict[str, Any]:
        return {
            k: {"last_ms": v, "age_ms": None if v is None else now_ms - v, "stale": k in self.open} for k, v in self.last.items()
        }

    def state(self) -> dict[str, Any]:
        return {"last": self.last, "open": self.open, "events": self.events[-200:]}

    def load(self, s: dict[str, Any]) -> None:
        self.last = {k: s["last"].get(k) for k in self.limits}
        self.open = {k: int(v) for k, v in s["open"].items()}
        self.events = list(s["events"])
