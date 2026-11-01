"""Phase 14: the live runner (paper mode: signals only; orders are Phase 16).

One loop, run where Binance is reachable (the trading PC or a non-US VPS; US hosts get HTTP 451):
* **REST trade poller** every ``poll_ms``: raw response captured first (gzip JSONL with receive time), then the new
  trades are normalised by the Phase 3 validator and pushed onto the bus. Suspected gaps are backfilled with
  ``historicalTrades`` (API key) or recorded as ``GAP`` intervals.
* **WebSocket monitor** (``aggTrade``, ``markPrice``): captured raw, used for feed health and cross-checks only.
* **Bus** advance (``EventChunk``s to the same TradeProcessor and Phase 6–12 sinks as the replay) to the last successful
  poll time minus ``grace_ms`` (never the wall clock: a failed poll holds the clock), feed-health checks,
  late-trade detection, and a checkpoint every ``checkpoint_s`` (processor + every sink + poller + bus + health).
* **Restart recovery**: the checkpoint is loaded, a ``RECOVERY`` interval is opened (signals blocked), trades since the
  last durable trade id are backfilled, then the interval is closed and signals resume.

The core is ``step(now_ms)``, a synchronous unit (poll, push, advance, health, checkpoint) that tests drive with a
fake clock and a fake REST function; ``run()`` wraps it in an asyncio loop with the WebSocket monitor.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Callable

from ..liquidity.capture import CaptureWriter
from ..rest_client import GeoBlocked
from .bus import OPEN_END, FeedHealth, LiveBus
from .trades import GapTracker, normalize_rest_trades

log = logging.getLogger("goldflow.live")
DEFAULT_LIVE: dict[str, Any] = {
    "trade_source": "rest_poll",
    "poll_ms": 750,
    "rest_limit": 1000,
    "grace_ms": 3000,
    "stale_ms": {"trades": 15000, "aggTrade": 30000, "markPrice": 10000},
    "ws_streams": ["aggTrade", "markPrice"],
    "ws_base_url": "wss://fstream.binance.com/market/stream",  # aggTrade/markPrice (verified 2026-10-31)
    "checkpoint_s": 60,
    "backfill_max_pages": 50,
}


def live_settings(cfg: Any) -> dict[str, Any]:
    user = cfg.raw.get("live") or {}
    raw = {**DEFAULT_LIVE, **{k: v for k, v in user.items() if not k.startswith("_")}}
    raw["stale_ms"] = {**DEFAULT_LIVE["stale_ms"], **(user.get("stale_ms") or {})}
    bad = sorted(set(raw) - set(DEFAULT_LIVE))
    if bad:
        raise ValueError(f"live: unknown keys {bad}")
    if raw["trade_source"] != "rest_poll":
        raise ValueError(
            "live.trade_source: only 'rest_poll' (decision D3, option a: individual trades incl. RPI, as in the history)"
        )
    if (
        not (100 <= int(raw["poll_ms"]) <= 5000)
        or int(raw["rest_limit"]) not in (500, 1000)
        or int(raw["grace_ms"]) < int(raw["poll_ms"])
    ):
        raise ValueError("live: poll_ms in [100, 5000], rest_limit 500 or 1000, grace_ms >= poll_ms")
    return raw


class LiveRunner:
    def __init__(self, cfg: Any, consumers: list[Any], name: str, rest_get: Callable[..., Any] | None = None,
                 now: Callable[[], int] | None = None, capture: bool = True) -> None:  # fmt: skip
        self.cfg, self.name = cfg, name
        self.ls = live_settings(cfg)
        self.consumers = consumers
        self.bus = LiveBus(consumers, int(self.ls["grace_ms"]))
        self.tracker = GapTracker(int(self.ls["rest_limit"]))
        self.health = FeedHealth(
            {k: int(v) for k, v in self.ls["stale_ms"].items()},
            {"trades": "trades", "aggTrade": "monitor", "markPrice": "monitor"},
        )
        self.now = now or (lambda: int(time.time() * 1000))
        if rest_get is None:
            from ..acquire import make_rest

            rest = make_rest(cfg)
            self.rest_get = rest.get
            self.has_key = rest.has_key
        else:
            self.rest_get, self.has_key = rest_get, True
        root = Path(cfg.data_root) / "raw" / "binance" / cfg.market / cfg.symbol / "live"
        self.capture = CaptureWriter(root) if capture else None
        self.state_path = Path(cfg.data_root) / "live" / f"{name}.checkpoint.json"
        self.counters: dict[str, int] = {"polls": 0, "poll_errors": 0, "trades": 0, "rpi_trades": 0, "lots": 0, "rpi_lots": 0, "rejected": 0,
                                         "backfill_pages": 0, "gaps": 0, "late_trades": 0, "ws_messages": 0, "checkpoints": 0}  # fmt: skip
        self.last_checkpoint = 0
        self.hooks: list[Callable[[int], None]] = []  # called after each step (Phase 16: the execution bridge tick)
        self.recovery_open: int | None = None
        self.last_ok: int | None = None  # receive time of the last successful trade poll
        self.status = "created"

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        now = self.now()
        if self.state_path.exists():
            s = json.loads(self.state_path.read_text())
            for c in self.consumers:
                c.set_state(s["consumers"].get(c.name, {}))
            self.tracker = GapTracker.load(s["tracker"])
            self.bus.load(s["bus"])
            self.health.load(s["health"])
            self.counters.update(s["counters"])
            self.bus.start(int(s["start_ms"]), resumed=True)
            self.start_ms = int(s["start_ms"])
            # block signals until the trades since the last durable id are back
            self.recovery_open = int(self.tracker.last_time or now)
            self.bus.push_quality("trades", "RECOVERY", self.recovery_open, OPEN_END)
            self.status = "recovering"
        else:
            self.start_ms = now
            self.bus.start(now)
            self.status = "running"

    def checkpoint(self) -> None:
        st = {"name": self.name, "status": self.status, "start_ms": self.start_ms, "saved_ms": self.now(),
              "consumers": {c.name: c.get_state() for c in self.consumers},
              "tracker": self.tracker.state(), "bus": self.bus.state(), "health": self.health.state(), "counters": self.counters}  # fmt: skip
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_name(self.state_path.name + ".tmp")
        tmp.write_text(json.dumps(st, default=str))
        os.replace(tmp, self.state_path)
        self.counters["checkpoints"] += 1

    # ------------------------------------------------------------------ one step
    def _fetch(self, params: dict[str, Any], path: str = "/fapi/v1/trades") -> tuple[list[dict[str, Any]], int]:
        txt = self.rest_get(path, params, raw=True)
        recv = self.now()
        if self.capture is not None:
            self.capture.write("rest_trades", recv, txt, {"path": path, "params": params})
        return json.loads(txt), recv

    def _backfill(self, first_fresh: dict[str, Any]) -> list[dict[str, Any]]:
        """historicalTrades from the last seen id until the fresh page is reached; [] if impossible."""
        if not self.has_key or self.tracker.last_id is None:
            return []
        got: list[dict[str, Any]] = []
        nxt = self.tracker.last_id + 1
        for _ in range(int(self.ls["backfill_max_pages"])):
            rows, _ = self._fetch({"symbol": self.cfg.symbol, "fromId": nxt, "limit": 500}, "/fapi/v1/historicalTrades")
            self.counters["backfill_pages"] += 1
            if not rows:
                break
            got += rows
            nxt = max(int(r["id"]) for r in rows) + 1
            if nxt >= int(first_fresh["id"]):
                return got
        return []

    def poll(self) -> None:
        try:
            rows, recv = self._fetch({"symbol": self.cfg.symbol, "limit": int(self.ls["rest_limit"])})
        except GeoBlocked:
            raise
        except Exception as e:  # network or HTTP error: the feed may go stale; never invent data
            self.counters["poll_errors"] += 1
            log.warning("trade poll failed: %s", e)
            return
        self.counters["polls"] += 1
        self.last_ok = recv
        for d, st, a, b in self.health.seen("trades", recv):
            self.bus.push_quality(d, st, a, b)
        fresh, gap = self.tracker.new_rows(rows)
        if gap:
            back = self._backfill(fresh[0])
            if back:
                fresh = sorted(
                    {int(r["id"]): r for r in back + fresh if int(r["id"]) > (self.tracker.last_id or -1)}.values(),
                    key=lambda r: int(r["id"]),
                )
            else:
                a, b = int(self.tracker.last_time or 0), int(fresh[0]["time"])
                self.tracker.suspected_gaps.append([a, b])
                self.counters["gaps"] += 1
                self.bus.push_quality("trades", "GAP", a, b + 1)
        if fresh:
            tbl, rej, cnt = normalize_rest_trades(fresh, recv, self.cfg)
            for k in ("trades", "rpi_trades", "lots", "rpi_lots"):
                self.counters[k] += cnt[k]
            self.counters["rejected"] += sum(rej.values())
            base_final = self._final_upto()
            if base_final is not None and tbl.num_rows:
                ts = tbl.column("exchange_event_time_utc").cast("int64").to_numpy()
                late = ts < base_final
                if late.any():
                    self.counters["late_trades"] += int(late.sum())
                    self.bus.push_quality("trades", "LATE", int(ts[late].min()), base_final)
            self.bus.push_trades(tbl)
            self.tracker.advance(fresh)
        if self.recovery_open is not None:
            # first successful poll after a restart: the trades since the last durable id were backfilled (or the
            # unrecoverable part is now a GAP interval), so the RECOVERY interval ends here
            self.bus.push_quality("trades", "RECOVERY", self.recovery_open, recv)
            self.recovery_open = None
            self.status = "running"

    def _final_upto(self) -> int | None:
        for c in self.consumers:
            fu = getattr(c, "final_upto", None)
            if isinstance(fu, dict) and fu:
                vals = [v for v in fu.values() if v is not None]
                return max(vals) if vals else None  # the 1 s bars: a trade before this missed a final bar
        return None

    def ws_message(self, stream: str, recv_ms: int, raw: str) -> None:
        self.counters["ws_messages"] += 1
        if self.capture is not None:
            self.capture.write("ws", recv_ms, raw, {"stream": stream})
        for d, st, a, b in self.health.seen(stream, recv_ms):
            self.bus.push_quality(d, st, a, b)

    def step(self, now_ms: int | None = None) -> None:
        now = now_ms if now_ms is not None else self.now()
        self.poll()
        for d, st, a, b in self.health.check(now):
            self.bus.push_quality(d, st, a, b)
        # The clock follows the last successful poll, not the wall clock: while polls fail, no bar is finalised before
        # its trades could be fetched (they are added when the feed answers again; STALE flags a long outage).
        g = int(self.ls["grace_ms"])
        hold = self.last_ok if self.last_ok is not None else (self.bus.clock + g if self.bus.clock is not None else self.start_ms)
        self.bus.advance(min(now, hold))
        if now - self.last_checkpoint >= int(self.ls["checkpoint_s"]) * 1000:
            try:
                self.checkpoint()
            except OSError as e:  # Phase 17: a full or failing disk must not stop ingestion; retried next interval
                self.counters["checkpoint_errors"] = self.counters.get("checkpoint_errors", 0) + 1
                log.error("live checkpoint failed (%s); retrying in %ss", e, self.ls["checkpoint_s"])
            self.last_checkpoint = now
        for h in self.hooks:
            h(now)

    def stop(self) -> None:
        self.status = "stopped"  # recorded in the final checkpoint: a deliberately stopped runner is not a stale feed
        self.checkpoint()
        if self.capture is not None:
            self.capture.close()
        self.bus.stop("stopped")

    # ------------------------------------------------------------------ asyncio wrapper
    async def _ws(self, stop: asyncio.Event) -> None:
        from ..liquidity.recorder import default_connector

        streams = "/".join(f"{self.cfg.symbol.lower()}@{s}" for s in self.ls["ws_streams"])
        url = f"{self.ls['ws_base_url']}?streams={streams}"
        backoff = 1.0
        while not stop.is_set():
            try:
                async with default_connector(url) as ws:
                    backoff = 1.0
                    opened = time.monotonic()
                    while not stop.is_set() and time.monotonic() - opened < 23 * 3600:  # rotate before Binance's 24 h limit
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        except asyncio.TimeoutError:
                            continue  # silence (e.g. market closed) is reported by the staleness timers; pings detect a dead link
                        txt = msg if isinstance(msg, str) else msg.decode()
                        st = str(json.loads(txt).get("stream", "")).split("@")[-1]
                        self.ws_message(st, self.now(), txt)
            except Exception as e:  # reconnect with bounded backoff; staleness timers report the outage
                log.warning("live websocket: %r; reconnecting in %.0fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def run(self, duration_s: float | None = None) -> dict[str, Any]:
        self.start()
        stop = asyncio.Event()
        ws = asyncio.create_task(self._ws(stop))
        t0 = time.monotonic()
        try:
            while duration_s is None or time.monotonic() - t0 < duration_s:
                await asyncio.get_running_loop().run_in_executor(None, self.step)
                await asyncio.sleep(int(self.ls["poll_ms"]) / 1000)
        except GeoBlocked as e:
            self.status = "geo_blocked"
            log.error("REST geo-blocked (%s): run the live pipeline on the trading PC or a non-US VPS", e)
        finally:
            stop.set()
            ws.cancel()
            self.stop() if self.status != "geo_blocked" else (self.capture.close() if self.capture else None)
        return {"status": self.status, "counters": self.counters, "health": self.health.status(self.now()), "health_events": self.health.events[-20:],
                "rpi_share_of_lots": (self.counters["rpi_lots"] / self.counters["lots"]) if self.counters["lots"] else None}  # fmt: skip
