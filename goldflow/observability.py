"""Structured logging and metrics (Phase 1).

Logs: every record written to the JSON-lines file carries
  ts (UTC ISO-8601 ms), severity, component (logger name), message, event_id (unique per record),
  correlation_id (run id), profile, provider, instrument, plus any `extra=` fields.
The console keeps a compact human-readable format (the same messages), so existing grep-based workflows still work.

Metrics: an in-process registry of counters and gauges (connection state, event rates, queue depth, latency,
errors). `metrics.snapshot()` returns a plain dict; Phase 17 exports it to the dashboard.
"""

from __future__ import annotations

import contextvars
import itertools
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_correlation: contextvars.ContextVar[str] = contextvars.ContextVar("goldflow_correlation_id", default="-")
_static: dict[str, str] = {}
_seq = itertools.count(1)
_RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


def new_run_id(prefix: str = "run") -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"


def set_correlation_id(value: str) -> None:
    _correlation.set(value)


def get_correlation_id() -> str:
    return _correlation.get()


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "severity": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
            "event_id": f"{_correlation.get()}:{next(_seq)}",
            "correlation_id": _correlation.get(),
            **_static,
        }
        for k, v in record.__dict__.items():
            if k not in _RESERVED and not k.startswith("_"):
                out[k] = v if isinstance(v, (str, int, float, bool, type(None), list, dict)) else repr(v)
        if record.exc_info:
            out["exception"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def setup_logging(
    log_dir: Path,
    run_id: str,
    profile: str,
    provider: str = "-",
    instrument: str = "-",
    level: int = logging.INFO,
    console: bool = True,
) -> Path:
    """Configure root logging: JSON lines to <log_dir>/goldflow.jsonl plus a human console stream."""
    log_dir.mkdir(parents=True, exist_ok=True)
    set_correlation_id(run_id)
    _static.update({"profile": profile, "provider": provider, "instrument": instrument, "pid": str(os.getpid())})
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(level)
    path = log_dir / "goldflow.jsonl"
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(JsonFormatter())
    root.addHandler(fh)
    if console:
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        root.addHandler(ch)
    return path


class Metrics:
    """Thread-safe counters/gauges/timings with labels. Cheap enough for per-chunk (not per-event) use."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._timings: dict[tuple[str, tuple[tuple[str, str], ...]], list[float]] = {}
        self.started = time.time()

    @staticmethod
    def _key(name: str, labels: dict[str, str]) -> tuple[str, tuple[tuple[str, str], ...]]:
        return name, tuple(sorted(labels.items()))

    def inc(self, name: str, value: float = 1.0, **labels: str) -> None:
        k = self._key(name, labels)
        with self._lock:
            self._counters[k] = self._counters.get(k, 0.0) + value

    def gauge(self, name: str, value: float, **labels: str) -> None:
        with self._lock:
            self._gauges[self._key(name, labels)] = value

    def observe(self, name: str, seconds: float, **labels: str) -> None:
        """Latency-style observation (count, sum, max retained)."""
        k = self._key(name, labels)
        with self._lock:
            c = self._timings.setdefault(k, [0.0, 0.0, 0.0])
            c[0] += 1
            c[1] += seconds
            c[2] = max(c[2], seconds)

    def snapshot(self) -> dict[str, Any]:
        def fmt(d: dict[Any, Any]) -> list[dict[str, Any]]:
            return [{"name": n, "labels": dict(lb), "value": v} for (n, lb), v in sorted(d.items())]

        with self._lock:
            return {
                "uptime_s": round(time.time() - self.started, 1),
                "counters": fmt(self._counters),
                "gauges": fmt(self._gauges),
                "timings": [
                    {"name": n, "labels": dict(lb), "count": c[0], "sum_s": c[1], "max_s": c[2]}
                    for (n, lb), c in sorted(self._timings.items())
                ],
            }

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timings.clear()


metrics = Metrics()
