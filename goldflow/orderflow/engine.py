"""Phase 7 engine: one Phase 5 ProcessorSink that runs every orderflow analytic on the shared trade processor.

Inputs come only through the Phase 5 sink interface (finalised bars and footprints, accepted trades, the time floor),
so replay and the Phase 14 live bus run identical code. Every event is labelled with its sessions, the weekend flag,
the worst data-quality status and the coverage, and the rule "event time >= evidence window end" is checked as it is
recorded (violations are counted and must be 0).
"""

from __future__ import annotations

import json
from fractions import Fraction
from pathlib import Path
from typing import Any

import numpy as np
from numpy.typing import NDArray

from ..processing.trade_processor import DQ_NAMES, DQ_RANK, ProcessorSink
from .absorption import AbsorptionDetector
from .big_trades import BigTradeDetector, TakerOrderBuilder
from .cvd import ResetCVD
from .divergence import DivergenceDetector
from .exhaustion import ExhaustionDetector
from .imbalance import compute_imbalances
from .settings import OrderflowSettings
from .store import OrderflowStore, orderflow_root, rows_to_cols

DAY_MS = 86_400_000
SIDE = {1: "BUY", -1: "SELL"}


class OrderflowAnalytics(ProcessorSink):
    name = "orderflow"

    def __init__(self, cfg: Any, root: Path | None | str = "default", replace: bool = True):
        self.cfg = cfg
        self.root = orderflow_root(cfg.data_root, cfg.symbol) if root == "default" else (Path(root) if root else None)
        self.store = OrderflowStore(self.root)
        self.replace = replace
        self.of: OrderflowSettings | None = None
        self.proc: Any = None
        self.counters = {
            "events": 0,
            "confirm_before_evidence_end": 0,
            "imbalances": 0,
            "stacked": 0,
            "cvd_rows": 0,
            "seeks": 0,
            "big_trades": 0,
        }
        self._sess_cache: dict[int, list[tuple[int, int, str]]] = {}
        self.dq: list[list[Any]] = []
        self.ctx: dict[str, Any] = {}
        self.ready = False
        self._pending_state: dict[str, Any] | None = None

    # ---------------------------------------------------------------- lifecycle
    def _build(self, proc: Any) -> None:
        self.proc = proc
        st = proc.st
        self.of = OrderflowSettings.from_config(self.cfg, tuple(st.intervals))
        o = self.of
        sh = st.trade_shift_ms
        self.shift = sh
        self.cvds = {iv: ResetCVD(iv, st.interval_ms[iv], o["cvd"]["resets"], proc.cal) for iv in o["cvd"]["intervals"]}
        bt = o["big_trades"]
        self.builder = TakerOrderBuilder(int(bt["cluster_ms"]), int(bt["sweep_bin_ticks"]), int(bt["store_trade_ids_max"]))
        self.big = BigTradeDetector(bt, sh)
        ab = o["absorption"]
        self.absorb = AbsorptionDetector(
            ab, ab["interval"], st.interval_ms[ab["interval"]], st.footprint_bin_ticks, sh, int(bt["cluster_ms"])
        )
        ex = o["exhaustion"]
        self.exh = ExhaustionDetector(ex, ex["interval"], st.interval_ms[ex["interval"]], sh)
        dv = o["divergence"]
        self.div = DivergenceDetector(dv, dv["interval"], st.interval_ms[dv["interval"]], sh, proc.cal)
        im = o["imbalance"]
        self.imb_cfg = im
        self.imb_ratio = Fraction(str(im["ratio"]))
        self.ready = True
        if self._pending_state is not None:
            self._restore(self._pending_state)
            self._pending_state = None

    def on_start(self, ctx: dict[str, Any], proc: Any) -> None:
        if not self.ready:
            self._build(proc)
        self.ctx = {"dataset_version": ctx.get("dataset_version"), "config_hash": ctx.get("config_hash")}
        self._sync_observe(proc)
        self.store.start(bool(ctx.get("resumed")))

    def _sync_observe(self, proc: Any) -> None:
        for c in self.cvds.values():
            c.observe_from = proc.observe_from
            c.filtered = bool(proc.filtered)
        self.div.cvd.observe_from, self.div.cvd.filtered = proc.observe_from, bool(proc.filtered)
        self.absorb.observe_from = proc.observe_from
        self.exh.observe_from = proc.observe_from

    def _ensure(self) -> None:
        if not self.ready:
            raise RuntimeError("OrderflowAnalytics used before on_start (pass it to TradeProcessor(sinks=[...]))")

    # ---------------------------------------------------------------- inputs from the processor
    def on_trades(
        self, t: NDArray[Any], ids: NDArray[Any], p: NDArray[Any], q: NDArray[Any], side: NDArray[Any], rec: NDArray[Any]
    ) -> None:
        self._ensure()
        orders, oids = self.builder.on_trades(t, ids, p, q, side)
        self._orders(orders, oids)

    def _orders(self, orders: dict[str, NDArray[Any]], oids: Any) -> None:
        if len(orders["size"]) == 0:
            return
        ev = self.big.process(orders, oids)
        self.counters["big_trades"] += len(ev)
        self._emit(ev)
        self.absorb.add_orders(orders)

    def on_advance(self, time_floor_ms: int) -> None:
        self._ensure()
        self._sync_dq()
        orders, oids = self.builder.close_upto(time_floor_ms)
        self._orders(orders, oids)
        self._emit(self.absorb.on_advance(time_floor_ms))
        self._emit(self.exh.on_advance(time_floor_ms))

    def on_bars(self, interval: str, bars: dict[str, Any], levels: dict[str, Any], final_upto_ms: int) -> None:
        self._ensure()
        o = self.of
        assert o is not None
        if interval in self.cvds:
            for blk in self.cvds[interval].on_bars(bars):
                self.counters["cvd_rows"] += len(blk["bar_start"])
                self.store.add("cvd", blk)
        if interval == self.imb_cfg["interval"]:
            self._imbalances(interval, bars, levels)
        if interval == o["absorption"]["interval"]:
            self._emit(self.absorb.on_bars(bars, levels, final_upto_ms))
        if interval == o["exhaustion"]["interval"]:
            self._emit(self.exh.on_bars(bars, final_upto_ms))
        if interval == o["divergence"]["interval"]:
            self._emit(self.div.on_bars(bars))

    def on_seek(self, t_ms: int) -> None:
        self._ensure()
        self.counters["seeks"] += 1
        self.builder.reset()
        for c in self.cvds.values():
            c.on_seek()
        self.absorb.reset()
        self.exh.reset()
        self.div.reset()
        if self.proc is not None:
            self._sync_observe(self.proc)

    def on_stop(self, ctx: dict[str, Any], proc: Any) -> None:
        if ctx.get("status") == "completed":
            self.finalize()

    # ---------------------------------------------------------------- imbalances
    def _imbalances(self, interval: str, bars: dict[str, Any], levels: dict[str, Any]) -> None:
        im = self.imb_cfg
        rows, runs = compute_imbalances(
            levels,
            self.proc.st.footprint_bin_ticks,
            int(im["bin_ticks"]),
            self.imb_ratio,
            int(im["min_lots"]),
            im["mode"],
            im["zero_policy"],
            int(im["stack_n"]),
        )
        bs = np.asarray(bars["bar_start"], np.int64)
        I = self.proc.st.interval_ms[interval]
        n = len(rows["bin"])
        if n:
            j = np.searchsorted(bs, rows["bar_start"])
            self.store.add(
                "imbalances",
                {
                    "bar_start": rows["bar_start"],
                    "bar_end": rows["bar_start"] + I,
                    "available_at": np.asarray(bars["available_at"])[j],
                    "interval": np.full(n, interval, object),
                    "mode": np.full(n, im["mode"], object),
                    "bin_ticks": np.full(n, int(im["bin_ticks"]), np.int64),
                    "bin": rows["bin"],
                    "price_ticks": rows["bin"] * int(im["bin_ticks"]),
                    "side": np.where(rows["side"] > 0, "BUY", "SELL").astype(object),
                    "lots": rows["lots"],
                    "opp_lots": rows["opp_lots"],
                    "unknown_lots": rows["unknown_lots"],
                    "first_trade_id": rows["first_id"],
                    "last_trade_id": rows["last_id"],
                    "stacked": rows["stacked"],
                    "weekend": np.asarray(bars["weekend"], bool)[j],
                    "dq_status": np.asarray(bars["dq_status"], object)[j],
                    "coverage": np.asarray(bars["coverage"], object)[j],
                },
            )
            self.counters["imbalances"] += n
        ev = []
        for r in runs:
            k = int(np.searchsorted(bs, r["bar_start"]))
            side = SIDE[r["side"]]
            av = int(bars["available_at"][k])
            ev.append(
                {
                    "event_id": f"STACKED_IMBALANCE:{side}:{interval}:{r['bar_start']}:{r['bin_lo']}",
                    "event_type": "STACKED_IMBALANCE",
                    "status": "DETECTED",
                    "parent_id": None,
                    "side": side,
                    "interval": interval,
                    "bin_ticks": int(im["bin_ticks"]),
                    "evidence_start_ms": r["bar_start"],
                    "evidence_end_ms": r["bar_start"] + I,
                    "event_time_ms": av,
                    "ref_price_ticks": r["bin_lo"] * int(im["bin_ticks"]),
                    "value_lots": r["lots"],
                    "threshold_lots": None,
                    "unknown_lots": r["unknown_lots"],
                    "first_trade_id": r["first_id"],
                    "last_trade_id": r["last_id"],
                    "n_trades": None,
                    "trade_ids": None,
                    "details": json.dumps(
                        {
                            "bins": [r["bin_lo"], r["bin_hi"]],
                            "n_bins": r["n"],
                            "opp_lots": r["opp_lots"],
                            "mode": im["mode"],
                            "ratio": str(im["ratio"]),
                            "min_lots": im["min_lots"],
                        },
                        sort_keys=True,
                    ),
                }
            )
        self.counters["stacked"] += len(ev)
        self._emit(ev)

    # ---------------------------------------------------------------- labelling and recording
    def _sync_dq(self) -> None:
        seen = {tuple(x) for x in self.dq}
        for x in self.proc.dq:
            if tuple(x) not in seen:
                self.dq.append(list(x))
        floor = (self.proc.time_floor or 0) - 8 * DAY_MS
        self.dq = [x for x in self.dq if x[1] > floor]

    def _sessions_at(self, t: int) -> str:
        d = t // DAY_MS
        lst = self._sess_cache.get(d)
        if lst is None:
            lst = [
                (i.start_ms, i.end_ms, i.name)
                for i in self.proc.cal.instances(d * DAY_MS, d * DAY_MS + DAY_MS - 1)
                if i.kind == "session"
            ]
            if len(self._sess_cache) > 64:
                self._sess_cache.clear()
            self._sess_cache[d] = lst
        names = sorted({n for s, e, n in lst if s <= t < e})
        return "+".join(names) if names else "none"

    def _emit(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        p = self.proc
        for r in rows:
            s, e = int(r["evidence_start_ms"]), int(r["evidence_end_ms"])
            if r["event_time_ms"] < e:
                self.counters["confirm_before_evidence_end"] += 1
            r["sessions"] = self._sessions_at(e - 1)
            r["weekend"] = bool(p.cal.is_weekend(np.array([e - 1]))[0])
            code = 0
            for a, b, status in self.dq:
                if s < b and e > a:
                    code = max(code, DQ_RANK.get(status, 3))
            r["dq_status"] = str(DQ_NAMES[code])
            r["coverage"] = "FILTERED" if p.filtered else ("PARTIAL_START" if s < (p.observe_from or 0) else "FULL")
        self.counters["events"] += len(rows)
        self.store.add("events", rows_to_cols(rows))

    # ---------------------------------------------------------------- outputs
    def events(self) -> Any:
        return self.store.table("events")

    def finalize(self) -> dict[str, Any]:
        assert self.of is not None
        meta = {
            "orderflow_settings_hash": self.of.hash,
            "processing_settings_hash": self.proc.st.hash,
            "source_dataset_version": self.ctx.get("dataset_version"),
            "counters": dict(self.counters),
            "detector_stats": {
                "absorption": self.absorb.stats,
                "divergence": self.div.stats,
                "big_trades": {"orders_seen": self.big.orders_seen, "orders_with_unready_criteria": self.big.not_ready},
            },
            "open_at_end": {
                "absorption_candidates": len(self.absorb.cands),
                "exhaustion_candidates": len(self.exh.cands),
                "taker_order_open": self.builder.open is not None,
            },
            "settings": self.of.raw,
        }
        self.manifest = self.store.finalize(meta)
        return self.manifest

    # ---------------------------------------------------------------- state
    def get_state(self) -> dict[str, Any]:
        if not self.ready:
            return self._pending_state or {}
        assert self.of is not None
        return {
            "settings_hash": self.of.hash,
            "store": self.store.get_state(),
            "counters": dict(self.counters),
            "dq": self.dq,
            "cvds": {k: v.get_state() for k, v in self.cvds.items()},
            "builder": self.builder.get_state(),
            "big": self.big.get_state(),
            "absorb": self.absorb.get_state(),
            "exh": self.exh.get_state(),
            "div": self.div.get_state(),
        }

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        if not self.ready:
            self._pending_state = s  # restored once the processor settings are known (on_start)
            self.store.set_state(s["store"])
            return
        self._restore(s)

    def _restore(self, s: dict[str, Any]) -> None:
        from ..replay import ReplayGuardError

        assert self.of is not None
        if s.get("settings_hash") != self.of.hash:
            raise ReplayGuardError("orderflow checkpoint was taken with different orderflow settings; start a new run name")
        self.store.set_state(s["store"])
        self.counters = dict(s["counters"])
        self.dq = [list(x) for x in s["dq"]]
        for k, v in s["cvds"].items():
            self.cvds[k].set_state(v)
        self.builder.set_state(s["builder"])
        self.big.set_state(s["big"])
        self.absorb.set_state(s["absorb"])
        self.exh.set_state(s["exh"])
        self.div.set_state(s["div"])
