"""Raw depth capture: every payload persisted exactly as received, before any transformation (Phase 8).

Layout (UTC date and hour of the receive time):
  <data_root>/raw/binance/futures_um/<SYMBOL>/depth_l2/live/date=YYYY-MM-DD/<session>-<YYYYMMDDHH>.jsonl.gz

One JSON object per line:
  v        capture format version (1)
  session  recorder session id (sortable: start time + random suffix); a new process = a new session
  seq      strictly increasing per session; a jump means lines are missing
  kind     "ws" (WebSocket message) | "snapshot" (REST depth response) | "conn" (connection lifecycle)
           | "rest_trades" (Phase 14 live trade poll response; meta {"path", "params"})
  recv_ms  local receive time (ms, UTC)
  raw      the payload text exactly as received ("" for conn records)
  meta     small dict: snapshot {"book", "limit", "http_status"}; conn {"event", "reason"}

Files are gzip streams flushed with Z_SYNC_FLUSH, so a crash loses at most the unflushed tail. The reader yields
every complete line, then a synthetic `conn` record with event CAPTURE_TRUNCATED (or CAPTURE_SEQ_GAP /
SESSION_BOUNDARY), so whoever replays the capture invalidates the book exactly where continuity is unproven.
"""

from __future__ import annotations

import gzip
import json
import os
import secrets
import time
import zlib
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any

CAPTURE_VERSION = 1
# "rest_trades" records are written by the Phase 14 live runner into its own capture root, never the depth root.
CAPTURE_KINDS = ("ws", "snapshot", "conn", "rest_trades")


@dataclass(frozen=True)
class CaptureRecord:
    session: str
    seq: int
    kind: str
    recv_ms: int
    raw: str
    meta: dict[str, Any] = field(default_factory=dict)
    synthetic_marker: bool = False  # True for reader-generated boundary/truncation markers (never written to disk)

    @property
    def ref(self) -> str:
        if self.synthetic_marker:
            return f"{self.session}:{self.seq}:{self.meta.get('event')}"
        return f"{self.session}:{self.seq}"


def new_session_id(now_ms: int | None = None) -> str:
    t = datetime.fromtimestamp((now_ms if now_ms is not None else time.time() * 1000) / 1000, timezone.utc)
    return t.strftime("%Y%m%dT%H%M%S") + f"{t.microsecond // 1000:03d}Z-" + secrets.token_hex(3)


def capture_root(data_root: Path, market: str, symbol: str) -> Path:
    return Path(data_root) / "raw" / "binance" / market / symbol / "depth_l2" / "live"


class CaptureWriter:
    """Append-only gzip JSONL writer with hourly rotation and periodic sync flushes."""

    def __init__(self, root: Path, session: str | None = None, flush_every_s: float = 1.0):
        self.root = Path(root)
        self.session = session or new_session_id()
        self.flush_every_s = flush_every_s
        self.seq = 0
        self._fh: IO[bytes] | None = None
        self._gz: gzip.GzipFile | None = None
        self._hour: str | None = None
        self._last_flush = time.monotonic()
        self.files: list[Path] = []
        self.bytes_written = 0

    def _path(self, recv_ms: int) -> tuple[str, Path]:
        t = datetime.fromtimestamp(recv_ms / 1000, timezone.utc)
        hour = t.strftime("%Y%m%d%H")
        return hour, self.root / f"date={t.strftime('%Y-%m-%d')}" / f"{self.session}-{hour}.jsonl.gz"

    def write(self, kind: str, recv_ms: int, raw: str, meta: dict[str, Any] | None = None) -> CaptureRecord:
        if kind not in CAPTURE_KINDS:
            raise ValueError(f"unknown capture kind {kind!r}")
        hour, path = self._path(recv_ms)
        if hour != self._hour:
            self._close_file()
            path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(path, "ab")
            self._gz = gzip.GzipFile(fileobj=self._fh, mode="ab", compresslevel=6)
            self._hour = hour
            self.files.append(path)
        self.seq += 1
        rec = CaptureRecord(self.session, self.seq, kind, recv_ms, raw, dict(meta or {}))
        line = json.dumps(
            {
                "v": CAPTURE_VERSION,
                "session": rec.session,
                "seq": rec.seq,
                "kind": kind,
                "recv_ms": recv_ms,
                "raw": raw,
                "meta": rec.meta,
            },
            separators=(",", ":"),
        )
        data = (line + "\n").encode()
        assert self._gz is not None
        self._gz.write(data)
        self.bytes_written += len(data)
        if time.monotonic() - self._last_flush >= self.flush_every_s:
            self.flush()
        return rec

    def flush(self) -> None:
        if self._gz is not None and self._fh is not None:
            self._gz.flush(zlib.Z_SYNC_FLUSH)
            self._fh.flush()
            os.fsync(self._fh.fileno())
        self._last_flush = time.monotonic()

    def _close_file(self) -> None:
        if self._gz is not None and self._fh is not None:
            self._gz.close()
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._fh.close()
        self._gz, self._fh, self._hour = None, None, None

    def close(self) -> None:
        self._close_file()


def capture_files(root: Path, dates: list[str] | None = None) -> list[Path]:
    """All capture files, ordered by (session, hour)."""
    root = Path(root)
    if not root.exists():
        return []
    out: list[Path] = []
    for d in sorted(root.glob("date=*")):
        if dates is not None and d.name.split("=", 1)[1] not in dates:
            continue
        out.extend(d.glob("*.jsonl.gz"))
    return sorted(out, key=lambda p: p.name)


def _session_of(p: Path) -> str:
    return p.name.rsplit("-", 1)[0]


def _marker(session: str, seq: int, recv_ms: int, event: str, reason: str) -> CaptureRecord:
    return CaptureRecord(session, seq, "conn", recv_ms, "", {"event": event, "reason": reason}, synthetic_marker=True)


def iter_capture(files: list[Path]) -> Iterator[CaptureRecord]:
    """Yield records in capture order, with synthetic markers at session boundaries, sequence gaps and truncation."""
    session: str | None = None
    last_seq = 0
    last_ms = 0
    for path in files:
        s = _session_of(path)
        if s != session:
            if session is not None:
                yield _marker(session, last_seq, last_ms, "SESSION_BOUNDARY", f"next session {s}")
            session, last_seq = s, 0
        truncated = None
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                for line in fh:
                    if not line.endswith("\n"):
                        truncated = "partial last line"
                        break
                    try:
                        o = json.loads(line)
                    except ValueError:
                        truncated = "unparseable line"
                        break
                    seq = int(o["seq"])
                    if seq != last_seq + 1:
                        yield _marker(s, last_seq, last_ms, "CAPTURE_SEQ_GAP", f"seq {last_seq} -> {seq} in {path.name}")
                    last_seq, last_ms = seq, int(o["recv_ms"])
                    yield CaptureRecord(o["session"], seq, o["kind"], last_ms, o["raw"], o.get("meta") or {})
        except (EOFError, OSError, zlib.error) as e:
            truncated = f"{type(e).__name__}: {e}"
        if truncated:
            yield _marker(s, last_seq, last_ms, "CAPTURE_TRUNCATED", f"{path.name}: {truncated}")
