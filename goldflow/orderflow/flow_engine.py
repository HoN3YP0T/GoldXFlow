"""Phase 7: the orderflow engine. Delta, CVD, imbalance, Big Trades, absorption, exhaustion and divergence.

``OrderflowEngine`` is a Phase 5 ``ProcessorSink``. It runs inside the shared ``TradeProcessor``, so the same code
runs on historical replay and on the Phase 14 live bus. Inputs and their no-look-ahead guarantees:

* ``on_trades``: accepted fills in delivery order -> taker orders -> Big Trades. An order is evaluated only when the
  clock proves it complete (``available_at = last fill + cluster_ms + 1 ms + shift``).
* ``on_bars`` of the base interval (1 s): exact running-delta paths, CVD per reset instance, absorption footprints
  and the price path used to track events.
* ``on_bars`` of every flow interval: flow bars (Phase 5 bar + delta path), CVD rows, imbalances, detector input.
  A bar [s, e) reaches the engine only after the processor finalised it at e + shift.
* ``on_advance``: detectors run on the bars finalised in this advance (in time order), then every open event is
  tracked on the finalised base bars after its evidence window, then complete taker orders are released.

All decisions are made in integers or exact rationals. State is fully captured by ``get_state`` (inside the replay
checkpoint) and refused if the orderflow settings changed, so restore + continue equals an uninterrupted run
(``state_digest``).
"""

from __future__ import annotations

import base64
import logging
import zlib
from typing import Any, Callable

import numpy as np
from numpy.typing import NDArray

from ..processing.aggregation import digest_tree
from ..processing.sessions import Instance
from ..processing.settings import ProcessingSettings
from ..processing.trade_processor import DQ_NAMES, DQ_RANK, ProcessorSink
from ..replay import ReplayGuardError
from ..timeutil import DAY_MS, iso
from .absorption import AbsorptionDetector
from .big_trades import ORDER_COLS, RollingQuantile, TakerOrderBuilder, TimeOfDayQuantile
from .cvd import CvdSeries, PathAgg
from .delta import flow_bars
from .divergence import DivergenceDetector
from .events import Tracker
from .exhaustion import ExhaustionDetector
from .flow_settings import OrderflowSettings
from .flow_store import OrderflowStore
from .imbalance import imbalances, rebin_levels

log = logging.getLogger("goldflow.orderflow")
I64 = np.int64
OPEN_END = 2**62
SIDE = {1: "BUY", -1: "SELL", 0: "UNKNOWN"}
Listener = Callable[[str, list[dict[str, Any]]], None]
STAT_KEYS = ("base_bars", "flow_bars", "cvd_rows", "imbalances", "stacked", "imbalance_bars_suppressed_unknown_side",
             "taker_orders", "taker_orders_unknown_side", "big_trades", "big_trades_unknown_side", "events_emitted",
             "candidates_open", "seeks")  # fmt: skip


# ---------------------------------------------------------------------------- checkpoint encoding of numpy arrays
def pack(o: Any) -> Any:
    if isinstance(o, np.ndarray):
        a = np.ascontiguousarray(o)
        return {"__nd__": a.dtype.str, "shape": list(a.shape), "z": base64.b64encode(zlib.compress(a.tobytes(), 1)).decode()}
    if isinstance(o, dict):
        return {k: pack(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [pack(v) for v in o]
    if isinstance(o, np.integer):
        return int(o)
    return o


def unpack(o: Any) -> Any:
    if isinstance(o, dict):
        if "__nd__" in o:
            return (
                np.frombuffer(zlib.decompress(base64.b64decode(o["z"])), dtype=np.dtype(o["__nd__"])).reshape(o["shape"]).copy()
            )
        return {k: unpack(v) for k, v in o.items()}
    if isinstance(o, list):
        return [unpack(v) for v in o]
    return o


class OrderflowEngine(ProcessorSink):
    name = "orderflow"

    def __init__(
        self, cfg: Any, store: OrderflowStore | None = None, listeners: list[Listener] | tuple[Listener, ...] = ()
    ) -> None:
        self.fs = OrderflowSettings.from_config(cfg)
        self.pst = ProcessingSettings.from_config(cfg)
        self.store = store
        self.listeners = list(listeners)
        self.proc: Any = None
        self.base = self.pst.intervals[0]
        self.base_ms = self.pst.base_ms
        self.shift = self.pst.trade_shift_ms
        self.ims = {n: self.pst.interval_ms[n] for n in self.fs.all_intervals}
        self._cache: dict[tuple[int, int], list[Instance]] = {}
        self._reset()

    def _reset(self) -> None:
        fs = self.fs
        mu = fs.max_unknown_share
        self.paths = {n: PathAgg(ms, reset=True) for n, ms in self.ims.items()}
        self.cvd: dict[str, CvdSeries] = {}
        self.cvd_done: dict[str, int] = {}
        b = fs.big
        self.builder = TakerOrderBuilder(b.cluster_ms, b.max_step_ticks)
        self.rq = RollingQuantile(b.rolling.window_ms, b.rolling.q, b.rolling.min_samples) if b.rolling.enabled else None
        self.tod = TimeOfDayQuantile(b.tod.window_ms, b.tod.lookback_days, b.tod.q, b.tod.min_samples) if b.tod.enabled else None
        self.queue: dict[str, NDArray[Any]] | None = None  # closed taker orders awaiting their available_at
        self.qids = np.zeros(0, I64)
        self.ab = (
            AbsorptionDetector(fs.absorption, self.ims.get(fs.absorption.interval, 1), self.base_ms, mu, self.shift)
            if fs.absorption.enabled
            else None
        )
        self.ex = (
            ExhaustionDetector(fs.exhaustion, self.ims.get(fs.exhaustion.interval, 1), mu, self.shift)
            if fs.exhaustion.enabled
            else None
        )
        self.dv = DivergenceDetector(fs.divergence, self.ims.get(fs.divergence.interval, 1), mu, self.shift) if fs.divergence.enabled else None  # fmt: skip
        self.trackers: list[Tracker] = []
        self.pending: list[tuple[int, int, str, dict[str, Any], tuple[int | None, str | None]]] = []
        self.buf_s = np.zeros(0, I64)
        self.buf_hi = np.zeros(0, I64)
        self.buf_lo = np.zeros(0, I64)
        self.dq: list[list[Any]] = []
        self.stats = {k: 0 for k in STAT_KEYS}
        self.counts: dict[str, int] = {}

    # ================================================================== sink interface
    def on_start(self, ctx: dict[str, Any], proc: Any) -> None:
        self.proc = proc
        if proc.st.hash != self.pst.hash:
            raise ValueError("OrderflowEngine was built for different processing settings than its processor")
        if self.store is not None:
            self.store.start(
                {
                    "orderflow_settings_hash": self.fs.hash,
                    "processing_settings_hash": proc.st.hash,
                    "source_dataset_version": ctx.get("dataset_version"),
                    "bar_intervals": list(self.fs.bar_intervals),
                    "tick_size": proc.st.tick_size,
                    "step_size": proc.st.step_size,
                    "instrument_id": proc.st.instrument_id,
                    "trade_shift_ms": proc.st.trade_shift_ms,
                    "observe_from": proc.observe_from,
                    "filtered": bool(proc.filtered),
                },
                bool(ctx.get("resumed")),
            )

    def on_dq(self, start_ms: int, end_ms: int, status: str) -> None:
        x = [int(start_ms), int(end_ms), str(status)]
        if x not in self.dq:
            self.dq.append(x)

    def on_trades(self, t: NDArray[Any], ids: NDArray[Any], p: NDArray[Any], q: NDArray[Any], side: NDArray[Any]) -> None:
        res = self.builder.ingest(t.astype(I64), ids.astype(I64), p.astype(I64), q.astype(I64), side.astype(np.int8))
        if res is not None:
            self._enqueue(*res)

    def on_bars(self, interval: str, bars: dict[str, Any], levels: dict[str, Any], final_upto_ms: int) -> None:
        n = len(bars["bar_start"])
        if not n:
            return
        if interval == self.base:
            s = np.asarray(bars["bar_start"], I64)
            d = np.asarray(bars["delta_lots"], I64)
            for pa_ in self.paths.values():
                pa_.add(s, d)
            self._cvd_add(s, d)
            if self.ab is not None:
                self.ab.add_base_levels(levels)
            self.buf_s = np.concatenate([self.buf_s, s])
            self.buf_hi = np.concatenate([self.buf_hi, np.asarray(bars["high_ticks"], I64)])
            self.buf_lo = np.concatenate([self.buf_lo, np.asarray(bars["low_ticks"], I64)])
            self.stats["base_bars"] += n
        if interval not in self.ims:
            return
        rows = flow_bars(interval, bars, self.paths[interval])
        cvd_rows, cvd_for = self._cvd_pop(interval, rows)
        if interval in self.fs.bar_intervals:
            self._emit("bars", rows)
            self._emit("cvd", cvd_rows)
            self.stats["flow_bars"] += len(rows)
            self.stats["cvd_rows"] += len(cvd_rows)
        if interval == self.fs.imbalance.interval:
            self._imbalances(interval, rows, levels)
        ms = self.ims[interval]
        for r in rows:
            self.pending.append((int(r["bar_end"]), ms, interval, r, cvd_for.get(int(r["bar_start"]), (None, None))))

    def on_advance(self, proc: Any) -> None:
        fu = proc.final_upto.get(self.base)
        if fu is None:
            return
        fs = self.fs
        for _, _, interval, row, (cv, key) in sorted(self.pending, key=lambda x: (x[0], x[1], x[2])):
            new: list[Tracker] = []
            if self.ab is not None and interval == fs.absorption.interval:
                new += self.ab.evaluate(row)
            if self.ex is not None and interval == fs.exhaustion.interval:
                new += self.ex.evaluate(row, cv, key)
            if self.dv is not None and interval == fs.divergence.interval:
                new += self.dv.evaluate(row, cv, key)
            if new:
                self._emit("events", [t.row for t in new])
                self.trackers += new
        self.pending = []
        keep = []
        for tr in self.trackers:
            res = tr.scan(self.buf_s, self.buf_hi, self.buf_lo, self.base_ms, int(fu), self.shift)
            if res is None:
                keep.append(tr)
            else:
                self._emit("events", [res])
        self.trackers = keep
        self.stats["candidates_open"] = len(keep)
        self._release_orders(proc)
        floors = [tr.next_s for tr in keep] + [int(proc.final_upto.get(n) or fu) for n in self.ims]
        floor = min(floors) if floors else int(fu)
        j = int(np.searchsorted(self.buf_s, floor, "left"))
        if j:
            self.buf_s, self.buf_hi, self.buf_lo = self.buf_s[j:], self.buf_hi[j:], self.buf_lo[j:]
        self._cvd_prune(proc)
        self._prune_dq()
        if self.store is not None and proc.clock is not None:
            self.store.flush_complete(int(proc.clock))

    def on_seek(self, t_ms: int) -> None:
        """Skipped events are never delivered: discard every developing aggregate, open order and open event."""
        seeks = self.stats["seeks"] + 1
        stats, counts = dict(self.stats), dict(self.counts)
        self._reset()
        self.stats, self.counts = stats, counts
        self.stats["seeks"] = seeks
        if self.store is not None:
            self.store.discard_unwritten()

    def on_stop(self, ctx: dict[str, Any], proc: Any) -> None:
        if ctx.get("status") == "completed" and self.store is not None:
            self.store.flush_complete(int(proc.clock or 0), force=True)

    # ================================================================== emission
    def _emit(self, table: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if table == "events":
            for r in rows:
                k = f"{r['event_type']}:{r['status']}"
                self.counts[k] = self.counts.get(k, 0) + 1
            self.stats["events_emitted"] += len(rows)
        if self.store is not None and self.fs.persist:
            self.store.add(table, rows)
        for f in self.listeners:
            f(table, rows)

    def _cov(self, start_ms: int) -> str:
        of = self.proc.observe_from if self.proc is not None else None
        if of is not None and start_ms < of:
            return "PARTIAL_START"
        return "FILTERED" if (self.proc is not None and self.proc.filtered) else "FULL"

    def _dq_status(self, a: int, b: int) -> str:
        code = 0
        for s, e, st in self.dq:
            if s < b and e > a:
                code = max(code, DQ_RANK.get(st, 3))
        return str(DQ_NAMES[code])

    def _prune_dq(self) -> None:
        if not self.dq or self.proc is None:
            return
        f = min([int(self.buf_s[0])] if len(self.buf_s) else [int(self.proc.time_floor or 0)])
        if self.queue is not None and len(self.queue["first_time"]):
            f = min(f, int(self.queue["first_time"][0]))
        if self.builder.open is not None:
            f = min(f, int(self.builder.open["t"][0]))
        f -= DAY_MS  # flow bars up to 1 h and their windows stay well inside one day
        self.dq = [x for x in self.dq if x[1] > f]

    # ================================================================== imbalance
    def _imbalances(self, interval: str, rows: list[dict[str, Any]], levels: dict[str, Any]) -> None:
        ip = self.fs.imbalance
        mu = self.fs.max_unknown_share
        info = {int(r["bar_start"]): r for r in rows}
        bad = {
            b
            for b, r in info.items()
            if int(r["volume_lots"]) and int(r["unknown_lots"]) * mu.denominator > mu.numerator * int(r["volume_lots"])
        }
        self.stats["imbalance_bars_suppressed_unknown_side"] += len(bad)
        r = rebin_levels(levels, ip.bin_ticks)
        imb, stk = imbalances(r, ip.method, ip.ratio, ip.min_lots, ip.zero_policy, ip.stack_n)
        bt = ip.bin_ticks
        out = []
        for i in range(len(imb["bar_start"])):
            b = int(imb["bar_start"][i])
            if b in bad:
                continue
            bar = info[b]
            lots, opp = int(imb["lots"][i]), int(imb["opposite_lots"][i])
            out.append({"interval": interval, "bar_start": b, "available_at": int(bar["available_at"]), "method": ip.method,
                        "side": SIDE[int(imb["side"][i])], "bin_ticks": bt, "price_ticks": int(imb["bin"][i]) * bt,
                        "compare_price_ticks": int(imb["compare_bin"][i]) * bt, "lots": lots, "opposite_lots": opp,
                        "unknown_lots": int(imb["unknown_lots"][i]), "ratio": (lots / opp) if opp else None,
                        "first_trade_id": int(imb["first_trade_id"][i]), "last_trade_id": int(imb["last_trade_id"][i]),
                        "bar_unknown_share": float(bar["unknown_share"]), "dq_status": bar["dq_status"], "coverage": bar["coverage"]})  # fmt: skip
        st = []
        for i in range(len(stk["bar_start"])):
            b = int(stk["bar_start"][i])
            if b in bad:
                continue
            bar = info[b]
            st.append({"interval": interval, "bar_start": b, "available_at": int(bar["available_at"]), "method": ip.method,
                       "side": SIDE[int(stk["side"][i])], "bin_ticks": bt, "low_price_ticks": int(stk["low_bin"][i]) * bt,
                       "high_price_ticks": int(stk["high_bin"][i]) * bt, "n_bins": int(stk["n_bins"][i]), "lots": int(stk["lots"][i]),
                       "first_trade_id": int(stk["first_trade_id"][i]), "last_trade_id": int(stk["last_trade_id"][i]),
                       "dq_status": bar["dq_status"], "coverage": bar["coverage"]})  # fmt: skip
        self.stats["imbalances"] += len(out)
        self.stats["stacked"] += len(st)
        self._emit("imbalances", out)
        self._emit("stacked", st)

    # ================================================================== CVD
    def _instances(self, a: int, b: int) -> list[Instance]:
        kinds = set(self.fs.cvd_resets) - {"anchor"}
        out: dict[str, Instance] = {}
        custom = list(self.proc.custom) if self.proc is not None else []
        for day in range(a // DAY_MS, b // DAY_MS + 1):
            ck = (day, len(custom))
            if ck not in self._cache:
                s = day * DAY_MS
                self._cache[ck] = [i for i in self.proc.cal.instances(s, s + DAY_MS - 1, custom) if i.kind in kinds]
                if len(self._cache) > 64:
                    self._cache.pop(next(iter(self._cache)))
            for i in self._cache[ck]:
                if i.start_ms <= b and i.end_ms > a:
                    out[i.key] = i
        if "anchor" in self.fs.cvd_resets:
            for name, start in self.fs.cvd_anchors:
                if start <= b:
                    i = Instance("anchor", name, iso(start) or "", start, OPEN_END)
                    out[i.key] = i
        return sorted(out.values(), key=lambda i: (i.start_ms, i.key))

    def _cvd_add(self, s: NDArray[Any], d: NDArray[Any]) -> None:
        for inst in self._instances(int(s[0]), int(s[-1])):
            if inst.key not in self.cvd and inst.end_ms > self.cvd_done.get(inst.series, -1):
                self.cvd[inst.key] = CvdSeries(inst, self.ims)
        for c in self.cvd.values():
            c.add(s, d)

    def _cvd_pop(
        self, interval: str, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], dict[int, tuple[int | None, str | None]]]:
        out = []
        want = {self.fs.exhaustion.cvd_reset if self.ex else None, self.fs.divergence.cvd_reset if self.dv else None} - {None}
        det: dict[int, tuple[int | None, str | None]] = {}
        for r in rows:
            b, e = int(r["bar_start"]), int(r["bar_end"])
            for key in sorted(self.cvd):
                c = self.cvd[key]
                x = c.aggs[interval].pop(b)
                if x is None:
                    continue
                i = c.inst
                out.append({"interval": interval, "bar_start": b, "bar_end": e, "available_at": int(r["available_at"]),
                            "series": i.series, "series_key": i.key, "series_start": i.start_ms,
                            "series_end": None if i.end_ms >= OPEN_END else i.end_ms,
                            "straddles": b < i.start_ms or e > i.end_ms, "cvd_open": x[1], "cvd_high": x[2], "cvd_low": x[3],
                            "cvd_close": x[4], "coverage": self._cov(i.start_ms)})  # fmt: skip
                if i.kind in want and i.start_ms <= e - 1 < i.end_ms:
                    det[b] = (x[4], i.key)
        return out, det

    def _cvd_prune(self, proc: Any) -> None:
        fl = min(int(proc.final_upto.get(n) or 0) for n in self.ims)
        for k in [k for k, c in self.cvd.items() if c.inst.end_ms <= fl]:
            c = self.cvd.pop(k)
            self.cvd_done[c.inst.series] = max(self.cvd_done.get(c.inst.series, c.inst.end_ms), c.inst.end_ms)
        for c in self.cvd.values():
            for n, a in c.aggs.items():
                a.drop_before(int(proc.final_upto.get(n) or 0))

    # ================================================================== Big Trades
    def _enqueue(self, orders: dict[str, NDArray[Any]], ids: NDArray[Any], offs: NDArray[Any]) -> None:
        orders = dict(orders)
        n = len(orders["first_time"])
        self.stats["taker_orders"] += n
        self.stats["taker_orders_unknown_side"] += int((orders["side"] == 0).sum())
        if self.queue is None or not len(self.queue["first_time"]):
            self.queue = {**orders, "off": offs.astype(I64)}
            self.qids = ids
        else:
            base = len(self.qids)
            self.queue = {
                k: np.concatenate([self.queue[k], orders[k] if k != "off" else offs.astype(I64) + base]) for k in self.queue
            }
            self.qids = np.concatenate([self.qids, ids])

    def _release_orders(self, proc: Any) -> None:
        if proc.clock is None:
            return
        X = int(proc.clock) - self.shift  # every trade with time < X has been delivered
        res = self.builder.flush(X)
        if res is not None:
            self._enqueue(*res)
        Q = self.queue
        if Q is None or not len(Q["first_time"]):
            return
        avail = Q["last_time"] + self.fs.big.cluster_ms + 1
        k = int(np.searchsorted(avail, X, "right"))  # avail <= X  <=>  available_at (avail + shift) <= clock
        if not k:
            return
        R = {c: v[:k] for c, v in Q.items()}
        id_end = int(Q["off"][k]) if k < len(Q["off"]) else len(self.qids)
        ids = self.qids[:id_end]
        self.queue = {c: v[k:] for c, v in Q.items()}
        self.queue["off"] = self.queue["off"] - id_end
        self.qids = self.qids[id_end:]
        self._evaluate_orders(R, ids)

    def _evaluate_orders(self, R: dict[str, NDArray[Any]], ids: NDArray[Any]) -> None:
        b = self.fs.big
        n = len(R["first_time"])
        lots, lt = R["lots"], R["last_time"]
        flags: dict[str, NDArray[Any] | None] = {"qty": None, "notional": None, "rolling": None, "tod": None}
        th_r = np.full(n, -1, I64)
        n_r = np.zeros(n, I64)
        th_t = np.full(n, -1, I64)
        n_t = np.zeros(n, I64)
        if b.min_qty_lots is not None:
            flags["qty"] = lots >= b.min_qty_lots
        if b.min_notional_tick_lots is not None:
            flags["notional"] = R["notional"] >= b.min_notional_tick_lots
        minute = lt // 60_000
        if self.rq is not None:
            for m in np.unique(minute):
                sel = minute == m
                th, ns = self.rq.threshold(int(m) * 60_000)
                th_r[sel] = -1 if th is None else th
                n_r[sel] = ns
                self.rq.add(int(m) * 60_000, lots[sel])
            flags["rolling"] = np.where(th_r >= 0, lots >= th_r, False)
        wk = self.proc.cal.is_weekend(lt) if self.proc is not None else np.zeros(n, bool)
        if self.tod is not None:
            keys = np.stack([lt // DAY_MS, (lt % DAY_MS) // b.tod.window_ms, wk.astype(I64)], axis=1)
            uk, inv = np.unique(keys, axis=0, return_inverse=True)
            inv = inv.reshape(-1)
            for j in np.argsort(uk[:, 0] * 10_000 + uk[:, 1], kind="stable"):
                sel = inv == j
                t0 = int(uk[j, 0]) * DAY_MS + int(uk[j, 1]) * b.tod.window_ms
                th, ns = self.tod.threshold(t0, bool(uk[j, 2]))
                th_t[sel] = -1 if th is None else th
                n_t[sel] = ns
                self.tod.add(t0, bool(uk[j, 2]), lots[sel])
            flags["tod"] = np.where(th_t >= 0, lots >= th_t, False)
        enabled = [v for v in flags.values() if v is not None]
        if b.combine == "any":
            big = np.logical_or.reduce(enabled)
        else:
            avail = [
                th_r >= 0 if k == "rolling" else th_t >= 0 if k == "tod" else np.ones(n, bool)
                for k, v in flags.items()
                if v is not None
            ]
            big = np.logical_and.reduce(enabled) & np.logical_and.reduce(avail)
        idx = np.flatnonzero(big)
        if not len(idx):
            return
        offs = np.append(R["off"], len(ids))
        rows = []
        for i in idx.tolist():
            sd = int(R["side"][i])
            ft, la = int(R["first_time"][i]), int(R["last_time"][i])
            rows.append({
                "first_time": ft, "last_time": la, "available_at": la + b.cluster_ms + 1 + self.shift, "side": SIDE[sd],
                "n_fills": int(R["n_fills"][i]), "lots": int(lots[i]), "notional_tick_lots": int(R["notional"][i]),
                "vwap_ticks": int(R["notional"][i]) / int(lots[i]), "first_price_ticks": int(R["first_price"][i]),
                "last_price_ticks": int(R["last_price"][i]), "min_price_ticks": int(R["min_price"][i]),
                "max_price_ticks": int(R["max_price"][i]), "first_trade_id": int(R["first_id"][i]),
                "last_trade_id": int(R["last_id"][i]), "trade_ids": ids[int(offs[i]) : int(offs[i + 1])].tolist(),
                **{f"pass_{k if k != 'tod' else 'time_of_day'}": (None if v is None else bool(v[i])) for k, v in flags.items()},
                "rolling_threshold_lots": int(th_r[i]) if th_r[i] >= 0 else None, "rolling_samples": int(n_r[i]),
                "tod_threshold_lots": int(th_t[i]) if th_t[i] >= 0 else None, "tod_samples": int(n_t[i]),
                "weekend": bool(wk[i]), "dq_status": self._dq_status(ft, la + 1), "coverage": self._cov(ft),
            })  # fmt: skip
            if sd == 0:
                self.stats["big_trades_unknown_side"] += 1
        self.stats["big_trades"] += len(rows)
        self._emit("big_trades", rows)

    # ================================================================== state
    def _tree(self) -> dict[str, Any]:
        q = self.queue if self.queue is not None else {}
        return {
            "settings_hash": self.fs.hash,
            "paths": {n: a.state() for n, a in sorted(self.paths.items())},
            "cvd": {
                k: {
                    "inst": [c.inst.kind, c.inst.name, c.inst.label, c.inst.start_ms, c.inst.end_ms],
                    "aggs": {n: a.state() for n, a in sorted(c.aggs.items())},
                }
                for k, c in sorted(self.cvd.items())
            },  # fmt: skip
            "cvd_done": dict(sorted(self.cvd_done.items())),
            "builder": self.builder.state(),
            "rq": self.rq.state() if self.rq is not None else None,
            "tod": self.tod.state() if self.tod is not None else None,
            "queue": {k: q[k] for k in sorted(q)},
            "qids": self.qids,
            "ab": self.ab.state() if self.ab is not None else None,
            "ex": self.ex.state() if self.ex is not None else None,
            "dv": self.dv.state() if self.dv is not None else None,
            "trackers": [t.state() for t in self.trackers],
            "pending": [list(x) for x in self.pending],
            "buf": {"s": self.buf_s, "hi": self.buf_hi, "lo": self.buf_lo},
            "dq": [list(x) for x in self.dq],
            "stats": dict(self.stats),
            "counts": dict(sorted(self.counts.items())),
        }

    def state_digest(self) -> str:
        return digest_tree(self._tree())

    def get_state(self) -> dict[str, Any]:
        t = pack(self._tree())
        t["store"] = self.store.get_state() if self.store is not None else {}
        return t  # type: ignore[no-any-return]

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        if s.get("settings_hash") != self.fs.hash:
            raise ReplayGuardError(
                f"orderflow engine checkpoint was taken with orderflow settings {str(s.get('settings_hash'))[:12]}, "
                f"current settings are {self.fs.hash[:12]}; start a new run name"
            )
        self._reset()
        store = s.get("store") or {}
        s = unpack({k: v for k, v in s.items() if k != "store"})
        self.paths = {n: PathAgg.load(self.ims[n], True, v) for n, v in s["paths"].items()}
        for k, c in s["cvd"].items():
            cs = CvdSeries(Instance(*c["inst"]), self.ims)
            cs.aggs = {n: PathAgg.load(self.ims[n], False, v) for n, v in c["aggs"].items()}
            self.cvd[k] = cs
        self.cvd_done = {k: int(v) for k, v in s["cvd_done"].items()}
        self.builder.load(s["builder"])
        if self.rq is not None and s["rq"] is not None:
            self.rq.load(s["rq"])
        if self.tod is not None and s["tod"] is not None:
            self.tod.load(s["tod"])
        self.queue = {k: np.asarray(v, I64 if k != "side" else np.int8) for k, v in s["queue"].items()} if s["queue"] else None
        self.qids = np.asarray(s["qids"], I64)
        for name, det in (("ab", self.ab), ("ex", self.ex), ("dv", self.dv)):
            if det is not None and s[name] is not None:
                det.load(s[name])
        self.trackers = [Tracker.load(x) for x in s["trackers"]]
        self.pending = [(int(a), int(b), str(c), dict(d), (e[0], e[1])) for a, b, c, d, e in s["pending"]]
        self.buf_s, self.buf_hi, self.buf_lo = (np.asarray(s["buf"][k], I64) for k in ("s", "hi", "lo"))
        self.dq = [list(x) for x in s["dq"]]
        self.stats = {**self.stats, **s["stats"]}
        self.counts = dict(s["counts"])
        if self.store is not None:
            self.store.set_state(store)

    def summary(self) -> dict[str, Any]:
        det = {
            n: d.stats for n, d in (("absorption", self.ab), ("exhaustion", self.ex), ("divergence", self.dv)) if d is not None
        }
        return {
            "stats": dict(self.stats),
            "events": dict(sorted(self.counts.items())),
            "detectors": det,
            "open_taker_order": self.builder.open is not None,
            "settings_hash": self.fs.hash[:16],
            "state_digest": self.state_digest(),
        }


__all__ = ["OrderflowEngine", "ORDER_COLS", "pack", "unpack"]
