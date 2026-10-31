"""CLI commands for Phase 8: depth-record, depth-replay, depth-normalize."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from pathlib import Path
from typing import Any

from .capture import CaptureWriter, capture_files, capture_root, iter_capture
from .coverage import DepthCoverage, Interval
from .dataset import normalize_capture, write_validity
from .driver import BookDriver, DriverConfig, replay_capture
from .recorder import DepthRecorder, RecorderConfig, SnapshotRefused
from .tracker import LiquidityTracker

log = logging.getLogger("goldflow.depth")


def driver_config(cfg: Any) -> DriverConfig:
    s = cfg.settings
    d = s.depth
    return DriverConfig(
        symbol=cfg.symbol,
        tick_size=s.instrument.tick_size,
        step_size=s.instrument.step_size,
        streams=d.streams(cfg.symbol),
        max_buffer=d.max_buffer_events,
        invalidate_on_cross=d.invalidate_on_crossed_book,
    )


def make_tracker(cfg: Any, sink: Any = None) -> LiquidityTracker:
    d = cfg.settings.depth
    return LiquidityTracker(
        d.metrics_cadence_ms, d.flow_window_ms, d.touch_ticks, tuple(d.tick_bands), tuple(d.pct_bands), d.top_k, sink
    )


def _root(cfg: Any) -> Path:
    return capture_root(cfg.data_root, cfg.market, cfg.symbol)


def record(cfg: Any, duration_s: float | None) -> int:
    """Record the live depth stream. Exit 0 when stopped normally, 4 when the host is geo-blocked."""
    from ..acquire import make_rest
    from ..rest_client import GeoBlocked
    from ..retry import NonRetryable

    d = cfg.settings.depth
    rest = make_rest(cfg)

    def fetch(kind: str) -> tuple[int, str]:
        path = d.snapshot_path if kind == "regular" else d.rpi_snapshot_path
        try:
            return 200, str(rest.depth_snapshot_text(cfg.symbol, d.snapshot_limit, path))
        except GeoBlocked as e:
            raise SnapshotRefused(str(e)) from e
        except NonRetryable as e:
            return int(e.status or 0), str(e)

    dcfg = driver_config(cfg)
    last: dict[str, Any] = {}
    tracker = make_tracker(cfg, sink=lambda m: last.__setitem__(m["book"], m))
    driver = BookDriver(dcfg, listeners=[tracker])
    writer = CaptureWriter(_root(cfg), flush_every_s=d.flush_every_s)
    rcfg = RecorderConfig(
        url=d.url(cfg.symbol),
        snapshot_min_interval_s=d.snapshot_min_interval_s,
        reconnect_base_s=d.reconnect_base_s,
        reconnect_cap_s=d.reconnect_cap_s,
        max_connection_s=d.max_connection_s,
        status_every_s=d.status_every_s,
        snapshot_kinds=tuple(sorted(set(dcfg.streams.values()))),
        snapshot_limit=d.snapshot_limit,
    )
    rec = DepthRecorder(rcfg, driver, writer, fetch)
    log.info("depth recorder session=%s url=%s capture=%s", writer.session, rcfg.url, _root(cfg))

    async def main() -> Any:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, rec.stop)
            except (NotImplementedError, RuntimeError):  # pragma: no cover - Windows
                pass
        return await rec.run(duration_s)

    try:
        summary = asyncio.run(main())
    finally:
        writer.close()
    out = dict(summary.__dict__)
    out["last_metrics"] = last
    print(json.dumps(out, indent=1, default=str))
    return 4 if summary.status == "geo_blocked" else 0


def replay(cfg: Any, dates: list[str] | None) -> int:
    """Rebuild the books from the raw capture (same driver as live) and print digests and validity."""
    files = capture_files(_root(cfg), dates)
    if not files:
        print(json.dumps({"error": f"no capture files under {_root(cfg)}"}))
        return 1
    tracker = make_tracker(cfg)
    drv = replay_capture(driver_config(cfg), iter_capture(files), listeners=[tracker])
    out: dict[str, Any] = {"files": len(files), "digests": drv.digests(), "stats": dict(drv.stats.__dict__), "books": {}}
    for k, b in drv.books.items():
        cov = DepthCoverage(b.key, [Interval(i.start_ms, i.end_ms) for i in b.intervals if i.end_ms is not None])
        out["books"][k] = {
            "coverage": cov.summary(),
            "counters": dict(b.counters.__dict__),
            "intervals": [i.as_dict() for i in b.intervals],
        }
    out["metric_samples"] = tracker.samples
    print(json.dumps(out, indent=1, default=str))
    return 0


def normalize(cfg: Any) -> int:
    """Capture -> DepthEvent v1 Parquet partitions, validity intervals and data/reports/depth_coverage.{json,md}."""
    files = capture_files(_root(cfg))
    if not files:
        print(json.dumps({"error": f"no capture files under {_root(cfg)}"}))
        return 1
    dcfg = driver_config(cfg)
    norm = cfg.data_root / "normalized" / "provider=binance" / "market=usdm_perpetual" / f"instrument={cfg.symbol}"
    res = normalize_capture(dcfg, lambda: iter_capture(files), norm)
    derived = cfg.data_root / "derived" / "provider=binance" / "market=usdm_perpetual" / f"instrument={cfg.symbol}"
    vpaths = write_validity(res["intervals"], dcfg, derived)
    rep = {
        "capture_files": len(files),
        "digests": res["digests"],
        "partitions": res["partitions"],
        "outcome_counts": res["outcome_counts"],
        "counters": res["counters"],
        "stats": res["stats"],
        "validity_files": [str(p) for p in vpaths],
        "coverage": {
            k: DepthCoverage(
                dcfg.key(k), [Interval(i["start_ms"], i["end_ms"]) for i in ivs if i["end_ms"] is not None]
            ).summary()
            for k, ivs in res["intervals"].items()
        },
        "historical_period": "2026-04-30..2026-10-30: depth capability ABSENT (no historical L2 exists)",
    }
    rdir = cfg.data_root / "reports"
    rdir.mkdir(parents=True, exist_ok=True)
    (rdir / "depth_coverage.json").write_text(json.dumps(rep, indent=1, default=str))
    lines = ["# Depth (L2) coverage report", "", f"Capture files: {len(files)}", ""]
    for k, c in rep["coverage"].items():
        lines += [
            f"## Book `{k}`",
            "",
            f"- valid intervals: {c['valid_intervals']}",
            f"- valid time: {c['valid_ms'] / 3_600_000:.2f} h",
            f"- outcomes: {res['outcome_counts'].get(k, {})}",
            "",
        ]
    lines += ["Historical period 2026-04-30..2026-10-30: depth capability **ABSENT** (no historical price-level L2).", ""]
    (rdir / "depth_coverage.md").write_text("\n".join(lines))
    print(json.dumps({k: v for k, v in rep.items() if k != "partitions"}, indent=1, default=str))
    return 0


def run_depth_command(cfg: Any, a: Any) -> int:
    if a.cmd == "depth-record":
        return record(cfg, a.duration)
    if a.cmd == "depth-replay":
        return replay(cfg, a.dates)
    if a.cmd == "depth-normalize":
        return normalize(cfg)
    raise ValueError(a.cmd)
