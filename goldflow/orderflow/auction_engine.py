"""Phase 9: the auction engine. Reference levels, acceptance / rejection, failed auctions, auction state, activity
and profile shape, as a Phase 5 ``ProcessorSink``.

Wiring: ``AuctionEngine(cfg, profile_engine)`` registers itself as a listener of the Phase 6 ``ProfileEngine`` (final
snapshots give the reference levels, developing snapshots give current value and VWAP), and must be placed **after**
it in the processor's sink list. It also receives the processor's finalised bars (with their 1-tick footprint) and
finalised windows directly.

Time order and no hindsight: bars are processed in ``on_advance`` strictly in time order, interleaved with the state
cadence. A level is used for a bar only if it was available at the bar's start. Every row carries ``available_at`` and
is emitted at or after it (tested with a clock check on replay).
"""

from __future__ import annotations

import json
from fractions import Fraction
from typing import Any, Callable

import numpy as np
import pyarrow as pa
from numpy.typing import NDArray

from .. import schemas as S
from ..processing.aggregation import digest_tree
from ..processing.trade_processor import ProcessorSink
from ..replay import ReplayGuardError
from .auction import Level, activity, beyond, binned_window, levels_from_snapshot, profile_shape, step_episode
from .auction_settings import AuctionSettings
from .events import worst_dq
from .failed_auction import FailedAuctionHold, Outcome, failed_auction_evidence
from .flow_engine import pack, unpack
from .flow_store import DerivedStore

I64 = np.int64
_T, _I, _F, _B, _X = S.TS, pa.int64(), pa.float64(), pa.bool_(), pa.string()
Listener = Callable[[str, list[dict[str, Any]]], None]
SIDE = {1: "ABOVE", -1: "BELOW"}
STAT_KEYS = ("bars", "levels_registered", "episodes_started", "acceptances", "returns", "rejections", "fa_candidates",
             "failed_auctions", "fa_dropped", "states", "activity_rows", "shape_events", "outcomes",
             "episodes_dropped_level_expired", "partial_windows_not_used", "seeks")  # fmt: skip


def _fx(f: Fraction) -> list[int]:
    return [f.numerator, f.denominator]


class AuctionStore(DerivedStore):
    """Phase 9 dataset: levels, events, outcomes, states, activity."""

    NAME = "auction"
    SCHEMA = "Auction"
    FIELDS = {
        "levels": [("level_id", _X), ("series", _X), ("window_key", _X), ("level_type", _X), ("price_ticks", _F),
                   ("price_exact", _X), ("available_at", _T), ("ref_lots", _I)],
        "events": [("event_id", _X), ("event_type", _X), ("side", _X), ("level_id", _X), ("level_type", _X), ("level_series", _X),
                   ("level_price_ticks", _F), ("episode_start", _T), ("event_time", _T), ("confirmed_at", _T),
                   ("available_at", _T), ("probe_extreme_ticks", _I), ("invalidation_ticks", _F), ("vol_beyond", _I),
                   ("vol_total", _I), ("delta_beyond", _I), ("bars", _I), ("closes_beyond", _I), ("first_trade_id", _I),
                   ("last_trade_id", _I), ("dq_status", _X), ("coverage", _X), ("detail", _X)],
        "outcomes": [("event_id", _X), ("horizon_minutes", _I), ("ref_price_ticks", _I), ("close_price_ticks", _I),
                     ("move_ticks", _I), ("mfe_ticks", _I), ("mae_ticks", _I), ("invalidated", _B), ("available_at", _T)],
        "states": [("series", _X), ("window_key", _X), ("as_of", _T), ("available_at", _T), ("state", _X), ("rotations", _I),
                   ("overlap", _F), ("poc_shift_bins", _I), ("accepted_above", _B), ("accepted_below", _B),
                   ("prev_window_key", _X), ("prev_val_ticks", _F), ("prev_vah_ticks", _F), ("prev_poc_ticks", _F),
                   ("cur_val_ticks", _F), ("cur_vah_ticks", _F), ("cur_poc_ticks", _F)],
        "activity": [("interval", _X), ("bar_start", _T), ("available_at", _T), ("close_ticks", _I), ("delta_lots", _I),
                     ("location", _X), ("vwap_side", _X), ("activity", _X), ("value_ref", _X)],
    }  # fmt: skip
    SORT = {
        "levels": [("available_at", "ascending"), ("level_id", "ascending")],
        "events": [("available_at", "ascending"), ("event_id", "ascending")],
        "outcomes": [("available_at", "ascending"), ("event_id", "ascending"), ("horizon_minutes", "ascending")],
        "states": [("as_of", "ascending"), ("series", "ascending")],
        "activity": [("bar_start", "ascending")],
    }
    DOC = {"spec": "docs/METRICS_AUCTION_V1.md", "available_at": "earliest simulated time the row may be used",
           "prices": "ticks; level prices are exact rationals (price_exact = num/den), price_ticks is their float value"}  # fmt: skip
    MATCH_KEYS = ("auction_settings_hash", "processing_settings_hash", "source_dataset_version")


class AuctionEngine(ProcessorSink):
    name = "auction"

    def __init__(self, cfg: Any, profile_engine: Any, store: AuctionStore | None = None,
                 listeners: list[Listener] | tuple[Listener, ...] = (), ib_engine: Any = None) -> None:  # fmt: skip
        self.st = AuctionSettings.from_config(cfg)
        self.profile = profile_engine
        if self.st.bin_ticks not in profile_engine.ps.bin_ticks:
            raise ValueError("auction.bin_ticks must be one of the profile engine's bin sizes")
        profile_engine.listeners.append(self._on_profile)
        self.ib = ib_engine
        if any(x.startswith("ib") for x in self.st.sources):
            if ib_engine is None:
                raise ValueError("auction.levels.sources includes IB levels: pass the IBEngine (ib_engine=...)")
            ib_engine.listeners.append(self._on_ib)
        self.store = store
        self.listeners = list(listeners)
        self.proc: Any = None
        self.shift = 0
        self._reset()

    def _reset(self) -> None:
        self.levels: dict[str, Level] = {}
        self.eps: dict[str, dict[str, Any]] = {}
        self.holds: list[FailedAuctionHold] = []
        self.outcomes: list[Outcome] = []
        self.finals: dict[str, list[dict[str, Any]]] = {}
        self.devs: dict[str, dict[int, dict[str, Any]]] = {}
        self.rot: dict[str, list[int]] = {}
        self.accepted: dict[str, bool] = {}
        self.pending: list[tuple[dict[str, Any], dict[str, NDArray[Any]]]] = []
        self.next_state: int | None = None
        self.stats = {k: 0 for k in STAT_KEYS}

    # ================================================================== helpers
    def _match(self, kind: str, name: str, wanted: tuple[str, ...]) -> bool:
        return kind in wanted or f"{kind}:{name}" in wanted

    def _emit(self, table: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if self.store is not None and self.st.persist:
            self.store.add(table, rows)
        for f in self.listeners:
            f(table, rows)

    # ================================================================== profile listener
    def _on_profile(self, recs: list[dict[str, Any]], nodes: list[dict[str, Any]]) -> None:
        st = self.st
        for r in recs:
            if int(r["bin_ticks"]) != st.bin_ticks or r["kind"] == "anchor":
                continue
            series = f"{r['kind']}:{r['name']}"
            if r["is_final"] and r["coverage"] != "FULL":
                self.stats["partial_windows_not_used"] += 1  # a partly observed window never supplies reference levels
                continue
            if r["is_final"]:
                if self._match(r["kind"], r["name"], st.sources):
                    self._register(r, [n for n in nodes if n["window_key"] == r["window_key"] and n["as_of"] == r["as_of"]
                                       and n["bin_ticks"] == st.bin_ticks])  # fmt: skip
                if self._match(r["kind"], r["name"], st.state_series):
                    f = self.finals.setdefault(series, [])
                    f.append({"available_at": int(r["available_at"]), "window_key": r["window_key"], **self._va(r)})
                    del f[:-3]
            elif self._match(r["kind"], r["name"], st.state_series):
                self.devs.setdefault(series, {})[int(r["as_of"])] = {"window_key": r["window_key"], **self._va(r)}

    @staticmethod
    def _va(r: dict[str, Any]) -> dict[str, Any]:
        bt = int(r["bin_ticks"])

        def f(v: Any, add: int = 0) -> list[int] | None:
            return None if v is None else _fx(Fraction(int(v) + add))

        vw = _fx(Fraction(int(r["sum_qp"]), int(r["sum_q"]))) if r.get("sum_q") else None
        poc = None if r.get("poc_ticks") is None else _fx(int(r["poc_ticks"]) + Fraction(bt - 1, 2))
        return {"val": f(r.get("val_ticks")), "vah": f(r.get("vah_ticks"), bt - 1), "poc": poc, "vwap": vw}

    def _register(self, r: dict[str, Any], nodes: list[dict[str, Any]]) -> None:
        items = levels_from_snapshot(r, nodes, self.st.level_types, self.st.band_k)
        self._add_levels(f"{r['kind']}:{r['name']}", r["window_key"], items, int(r["available_at"]), int(r["total_lots"]))

    def _on_ib(self, table: str, rows: list[dict[str, Any]]) -> None:
        """Phase 10 IB levels (IB_HIGH, IB_LOW, IB_MID) of fully observed IBs, available at the IB end."""
        if table != "ib_level":
            return
        for x in rows:
            if "ib" in self.st.sources or x["series"] in self.st.sources:
                items = [(t, Fraction(int(n), int(d))) for t, n, d in x["levels"]]
                self._add_levels(x["series"], x["window_key"], items, int(x["available_at"]), int(x["ref_lots"]))

    def _add_levels(self, series: str, window_key: str, items: list[tuple[str, Fraction]], av: int, ref_lots: int | None) -> None:
        """Register a new instance's levels; the previous instance of the same series stops being active at ``av``."""
        for lv in self.levels.values():
            if lv.series == series and lv.active_until is None:
                lv.active_until = av
        rows = []
        for t, price in items:
            lid = f"{window_key}:{t}"
            self.levels[lid] = Level(lid, series, window_key, t, price, av, ref_lots)
            rows.append({"level_id": lid, "series": series, "window_key": window_key, "level_type": t,
                         "price_ticks": float(price), "price_exact": f"{price.numerator}/{price.denominator}",
                         "available_at": av, "ref_lots": ref_lots})  # fmt: skip
        self.stats["levels_registered"] += len(rows)
        self._emit("levels", rows)

    # ================================================================== sink interface
    def on_start(self, ctx: dict[str, Any], proc: Any) -> None:
        self.proc = proc
        self.shift = proc.st.trade_shift_ms
        if self.st.interval not in proc.st.interval_ms:
            raise ValueError("auction.interval is not a processor interval")
        sinks = list(proc.sinks)
        if self.profile not in sinks or sinks.index(self.profile) > sinks.index(self):
            raise ValueError("AuctionEngine must come after its ProfileEngine in the processor's sinks")
        if self.ib is not None and (self.ib not in sinks or sinks.index(self.ib) > sinks.index(self)):
            raise ValueError("AuctionEngine must come after its IBEngine in the processor's sinks")
        for name, price in self.st.user_levels:
            lid = f"user:{name}"
            if lid not in self.levels:
                self.levels[lid] = Level(lid, "user", "user", "USER", price, -(2**62), None)
        if self.next_state is None and proc.observe_from is not None:
            ev = self.st.state_every_ms
            self.next_state = -(-int(proc.observe_from) // ev) * ev
        if self.store is not None:
            self.store.start({"auction_settings_hash": self.st.hash, "processing_settings_hash": proc.st.hash,
                              "source_dataset_version": ctx.get("dataset_version"), "interval": self.st.interval,
                              "bin_ticks": self.st.bin_ticks, "observe_from": proc.observe_from}, bool(ctx.get("resumed")))  # fmt: skip

    def on_bars(self, interval: str, bars: dict[str, Any], levels: dict[str, Any], final_upto_ms: int) -> None:
        if interval != self.st.interval or not len(bars["bar_start"]):
            return
        keys = ("bar_start", "bar_end", "available_at", "open_ticks", "high_ticks", "low_ticks", "close_ticks", "volume_lots",
                "buy_lots", "sell_lots", "unknown_lots", "delta_lots", "first_trade_id", "last_trade_id", "dq_status", "coverage")  # fmt: skip
        cols = {k: np.asarray(bars[k]).tolist() for k in keys}
        lb = np.asarray(levels["bar_start"], I64)
        o = np.argsort(lb, kind="stable")
        lb = lb[o]
        L = {k: np.asarray(levels[k], I64)[o] for k in ("level", "buy_lots", "sell_lots", "unknown_lots")}
        for i in range(len(cols["bar_start"])):
            b = {k: cols[k][i] for k in keys}
            a, z = int(np.searchsorted(lb, b["bar_start"], "left")), int(np.searchsorted(lb, b["bar_start"], "right"))
            self.pending.append((b, {"level": L["level"][a:z], "buy": L["buy_lots"][a:z], "sell": L["sell_lots"][a:z],
                                     "unk": L["unknown_lots"][a:z]}))  # fmt: skip

    def on_window(self, summary: dict[str, Any], levels: dict[str, Any]) -> None:
        st = self.st
        if not self._match(summary["kind"], summary["name"], st.shape_windows):
            return
        p = binned_window(levels, st.bin_ticks)
        e = int(summary["end_ms"])
        rows = []
        for x in profile_shape(p, st.bin_ticks, st):
            t = x.pop("type")
            side = "HIGH" if t.endswith("HIGH") else "LOW" if t.endswith("LOW") else "INTERIOR"
            rows.append(self._row(f"{t}:{summary['key']}:{x['price_ticks']}", t, side, None, None, None, int(summary["start_ms"]),
                                  e, int(summary["start_ms"]), None, None, {"vol_total": int(summary["volume_lots"])}, summary["dq_status"],
                                  summary["coverage"], {**x, "window_key": summary["key"]}))  # fmt: skip
        self.stats["shape_events"] += len(rows)
        self._emit("events", rows)

    def on_advance(self, proc: Any) -> None:
        fu = proc.final_upto.get(self.st.interval)
        if fu is None:
            return
        ev = self.st.state_every_ms
        if self.next_state is None:
            self.next_state = -(-int(proc.observe_from or fu) // ev) * ev
        for bar, lv in sorted(self.pending, key=lambda x: x[0]["bar_start"]):
            while self.next_state <= bar["bar_start"]:
                self._states(self.next_state)
                self.next_state += ev
            self._bar(bar, lv)
        self.pending = []
        while self.next_state <= fu:
            self._states(self.next_state)
            self.next_state += ev
        rows = [r for o in self.outcomes for r in o.due(int(fu), self.shift)]
        self.outcomes = [o for o in self.outcomes if o.horizons]
        self.stats["outcomes"] += len(rows)
        self._emit("outcomes", rows)
        self._prune(int(fu))
        if self.store is not None and proc.clock is not None:
            self.store.flush_complete(int(proc.clock))

    def on_seek(self, t_ms: int) -> None:
        stats = dict(self.stats)
        levels = {k: v for k, v in self.levels.items() if v.series == "user"}
        self._reset()
        self.stats = stats
        self.stats["seeks"] += 1
        self.levels = levels
        if self.store is not None:
            self.store.discard_unwritten()

    def on_stop(self, ctx: dict[str, Any], proc: Any) -> None:
        if ctx.get("status") == "completed" and self.store is not None:
            self.store.flush_complete(int(proc.clock or 0), force=True)

    # ================================================================== per bar
    def _row(self, eid: str, etype: str, side: str, lv: Level | None, ep: dict[str, Any] | None, invalid: Fraction | int | None,
             ev_time: int, confirmed: int, ep_start: int | None, probe: int | None, closes: int | None, vols: dict[str, Any],
             dq: str, cov: str, detail: dict[str, Any]) -> dict[str, Any]:  # fmt: skip
        return {"event_id": eid, "event_type": etype, "side": side, "level_id": lv.id if lv else None,
                "level_type": lv.ltype if lv else None, "level_series": lv.series if lv else None,
                "level_price_ticks": float(lv.price) if lv else None, "episode_start": ep_start, "event_time": ev_time,
                "confirmed_at": confirmed, "available_at": confirmed + self.shift, "probe_extreme_ticks": probe,
                "invalidation_ticks": None if invalid is None else float(invalid), "vol_beyond": vols.get("vol_beyond"),
                "vol_total": vols.get("vol_total"), "delta_beyond": vols.get("delta_beyond"), "bars": vols.get("bars"),
                "closes_beyond": closes, "first_trade_id": ep["first_id"] if ep else None, "last_trade_id": ep["last_id"] if ep else None,
                "dq_status": dq, "coverage": cov, "detail": json.dumps(detail, sort_keys=True, default=str)}  # fmt: skip

    def _ep_row(self, etype: str, lv: Level, side: int, snap: dict[str, Any], confirmed: int, invalid: Fraction | int | None,
                extra: dict[str, Any]) -> dict[str, Any]:  # fmt: skip
        return self._row(f"{etype}:{lv.id}:{SIDE[side]}:{snap['start']}", etype, SIDE[side], lv, snap, invalid, snap["start"], confirmed,
                         snap["start"], snap["ext"], snap["run"], snap, snap["dq"], snap["cov"],
                         {"level_price_exact": f"{lv.price.numerator}/{lv.price.denominator}", "ref_lots": lv.ref_lots, **extra})  # fmt: skip

    def _bar(self, bar: dict[str, Any], lv: dict[str, NDArray[Any]]) -> None:
        st = self.st
        s, e = int(bar["bar_start"]), int(bar["bar_end"])
        self.stats["bars"] += 1
        out_rows: list[dict[str, Any]] = []
        for o in self.outcomes:
            out_rows += o.step(bar, self.shift)
        self.stats["outcomes"] += len(out_rows)
        self._emit("outcomes", out_rows)
        ev_rows: list[dict[str, Any]] = []
        keep = []
        for h in self.holds:
            r = h.step(bar)
            if r == "CONFIRMED":
                L = self.levels.get(h.level_id) or Level(h.level_id, "", "", "", h.price, 0, None)
                ev_rows.append(self._ep_row("FAILED_AUCTION", L, h.side, h.snap, e, h.snap["ext"],
                                            {**h.ev, "returned_at": h.snap["end"], "hold_bars": h.need}))  # fmt: skip
                self.outcomes.append(Outcome(ev_rows[-1]["event_id"], e, int(bar["close_ticks"]), -h.side, h.snap["ext"],
                                             [m * 60_000 for m in st.outcome_minutes]))  # fmt: skip
                self.stats["failed_auctions"] += 1
            elif r == "DROPPED":
                self.stats["fa_dropped"] += 1
            else:
                keep.append(h)
        self.holds = keep
        self._rotations(bar)
        self._activity(bar)
        for lid in sorted(self.levels):
            L = self.levels[lid]
            active = L.available_at <= s and (L.active_until is None or s < L.active_until)
            for side in (1, -1):
                k = f"{lid}|{side}"
                ep = self.eps.get(k)
                if not active:
                    if ep is not None:
                        del self.eps[k]
                        self.stats["episodes_dropped_level_expired"] += 1
                    continue
                if ep is None and not beyond(int(bar["high_ticks"] if side > 0 else bar["low_ticks"]), L.price, side):
                    continue
                if ep is None:
                    self.stats["episodes_started"] += 1
                new, evs = step_episode(ep, bar, lv, L.price, side, L.ref_lots, st)
                if new is None:
                    self.eps.pop(k, None)
                else:
                    self.eps[k] = new
                for etype, snap in evs:
                    if etype == "ACCEPTANCE":
                        ev_rows.append(
                            self._ep_row(
                                etype, L, side, snap, e, L.price, {"by_time": snap["by_time"], "by_volume": snap["by_volume"]}
                            )
                        )
                        self.accepted[lid + f"|{side}"] = True
                        self.outcomes.append(Outcome(ev_rows[-1]["event_id"], e, int(bar["close_ticks"]), side, None,
                                                     [m * 60_000 for m in st.outcome_minutes]))  # fmt: skip
                        self.stats["acceptances"] += 1
                    elif etype == "RETURN":
                        ev_rows.append(self._ep_row(etype, L, side, snap, e, None, {"accepted_at": snap["accepted_at"]}))
                        self.accepted[lid + f"|{side}"] = False
                        self.stats["returns"] += 1
                    elif etype == "REJECTION":
                        ev_rows.append(self._ep_row(etype, L, side, snap, e, snap["ext"], {}))
                        self.outcomes.append(Outcome(ev_rows[-1]["event_id"], e, int(bar["close_ticks"]), -side, snap["ext"],
                                                     [m * 60_000 for m in st.outcome_minutes]))  # fmt: skip
                        self.stats["rejections"] += 1
                    elif etype == "CLOSED":
                        evd = failed_auction_evidence(snap, L.price, side, st)
                        if evd is not None:
                            self.holds.append(FailedAuctionHold(lid, L.price, side, snap, evd, st.fa_hold_bars))
                            self.stats["fa_candidates"] += 1
        self._emit("events", ev_rows)

    # ================================================================== value-relative context
    def _prev_final(self, series: str, t: int, not_key: str | None = None) -> dict[str, Any] | None:
        best = None
        for f in self.finals.get(series, []):
            if f["available_at"] <= t and f["window_key"] != not_key:
                best = f
        return best

    def _dev_at(self, series: str, t: int) -> dict[str, Any] | None:
        return self.devs.get(series, {}).get(t)

    def _state_series(self) -> list[str]:
        return sorted(set(self.finals) | set(self.devs))

    def _rotations(self, bar: dict[str, Any]) -> None:
        s = int(bar["bar_start"])
        for series in self._state_series():
            d = self._dev_at(series, s)
            if d is None:
                continue
            pf = self._prev_final(series, s, d["window_key"])
            if pf is None or pf["val"] is None or pf["vah"] is None:
                continue
            vah, val = Fraction(*pf["vah"]), Fraction(*pf["val"])
            hit_hi = not beyond(int(bar["high_ticks"]), vah, -1)  # high >= VAH
            hit_lo = not beyond(int(bar["low_ticks"]), val, 1)  # low <= VAL
            r = self.rot.setdefault(d["window_key"], [0, 0])  # [last touch (+1 VAH / -1 VAL / 0), rotations]
            touch = 0
            if hit_hi and hit_lo:
                touch = -r[0] if r[0] else 0
            elif hit_hi:
                touch = 1
            elif hit_lo:
                touch = -1
            if touch and touch != r[0]:
                if r[0]:
                    r[1] += 1
                r[0] = touch

    def _activity(self, bar: dict[str, Any]) -> None:
        series = self._state_series()
        if not series:
            return
        S0 = series[0]
        s = int(bar["bar_start"])
        d = self._dev_at(S0, s)
        pf = self._prev_final(S0, s, d["window_key"] if d else None)
        val = Fraction(*pf["val"]) if pf and pf["val"] else None
        vah = Fraction(*pf["vah"]) if pf and pf["vah"] else None
        vw = Fraction(*d["vwap"]) if d and d["vwap"] else None
        loc, vs, act = activity(int(bar["close_ticks"]), int(bar["delta_lots"]), val, vah, vw)
        self.stats["activity_rows"] += 1
        self._emit("activity", [{"interval": self.st.interval, "bar_start": s, "available_at": int(bar["available_at"]),
                                 "close_ticks": int(bar["close_ticks"]), "delta_lots": int(bar["delta_lots"]), "location": loc,
                                 "vwap_side": vs, "activity": act, "value_ref": pf["window_key"] if pf else None}])  # fmt: skip

    def _states(self, M: int) -> None:
        st = self.st
        rows = []
        for series in self._state_series():
            d = self._dev_at(series, M)
            if d is None:
                continue
            pf = self._prev_final(series, M, d["window_key"])
            row: dict[str, Any] = {"series": series, "window_key": d["window_key"], "as_of": M, "available_at": M + self.shift,
                                   "rotations": self.rot.get(d["window_key"], [0, 0])[1], "prev_window_key": None}  # fmt: skip
            row.update({k: None for k in ("overlap", "poc_shift_bins", "prev_val_ticks", "prev_vah_ticks", "prev_poc_ticks")})
            row.update(cur_val_ticks=_f(d["val"]), cur_vah_ticks=_f(d["vah"]), cur_poc_ticks=_f(d["poc"]), accepted_above=False,
                       accepted_below=False, state="UNDETERMINED")  # fmt: skip
            if pf is not None and None not in (pf["val"], pf["vah"], pf["poc"], d["val"], d["vah"], d["poc"]):
                pv, ph, pp = Fraction(*pf["val"]), Fraction(*pf["vah"]), Fraction(*pf["poc"])
                cv, ch, cp = Fraction(*d["val"]), Fraction(*d["vah"]), Fraction(*d["poc"])
                width = ph - pv
                ov = max(Fraction(0), min(ph, ch) - max(pv, cv)) / width if width > 0 else Fraction(0)
                shift_bins = int((cp - pp) / st.bin_ticks)
                aa = self.accepted.get(f"{pf['window_key']}:VAH|1", False)
                ab = self.accepted.get(f"{pf['window_key']}:VAL|-1", False)
                if aa and shift_bins >= st.poc_shift_bins:
                    state = "IMBALANCE_UP"
                elif ab and -shift_bins >= st.poc_shift_bins:
                    state = "IMBALANCE_DOWN"
                elif row["rotations"] >= st.rot_n and ov >= st.ov_frac:
                    state = "BALANCE"
                else:
                    state = "TRANSITION"
                row.update(state=state, overlap=float(ov), poc_shift_bins=shift_bins, accepted_above=aa, accepted_below=ab,
                           prev_window_key=pf["window_key"], prev_val_ticks=float(pv), prev_vah_ticks=float(ph), prev_poc_ticks=float(pp))  # fmt: skip
            rows.append(row)
        self.stats["states"] += len(rows)
        self._emit("states", rows)

    def _prune(self, fu: int) -> None:
        for series, d in self.devs.items():
            for t in [t for t in d if t < fu - self.st.state_every_ms]:
                del d[t]
        live = {k.split("|")[0] for k in self.eps} | {h.level_id for h in self.holds}
        for lid in [k for k, v in self.levels.items() if v.active_until is not None and v.active_until <= fu and k not in live]:
            del self.levels[lid]
        cur = {d["window_key"] for dd in self.devs.values() for d in dd.values()}
        for k in [k for k in self.rot if k not in cur]:
            del self.rot[k]
        known = {lid for lid in self.levels}
        for k in [k for k in self.accepted if k.split("|")[0] not in known]:
            del self.accepted[k]

    # ================================================================== state
    def _tree(self) -> dict[str, Any]:
        return {
            "settings_hash": self.st.hash,
            "levels": [self.levels[k].tree() for k in sorted(self.levels)],
            "eps": dict(sorted(self.eps.items())),
            "holds": [h.tree() for h in self.holds],
            "outcomes": [o.tree() for o in self.outcomes],
            "finals": dict(sorted(self.finals.items())),
            "devs": {k: [[t, v] for t, v in sorted(d.items())] for k, d in sorted(self.devs.items())},
            "rot": dict(sorted(self.rot.items())),
            "accepted": dict(sorted(self.accepted.items())),
            "pending": [[b, lv] for b, lv in self.pending],
            "next_state": self.next_state,
            "stats": dict(self.stats),
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
        if s.get("settings_hash") != self.st.hash:
            raise ReplayGuardError(
                f"auction engine checkpoint was taken with auction settings {str(s.get('settings_hash'))[:12]}, "
                f"current settings are {self.st.hash[:12]}; start a new run name"
            )
        self._reset()
        store = s.get("store") or {}
        s = unpack({k: v for k, v in s.items() if k != "store"})
        self.levels = {x[0]: Level.load(x) for x in s["levels"]}
        self.eps = {k: dict(v) for k, v in s["eps"].items()}
        self.holds = [FailedAuctionHold.load(x) for x in s["holds"]]
        self.outcomes = [Outcome.load(x) for x in s["outcomes"]]
        self.finals = {k: [dict(x) for x in v] for k, v in s["finals"].items()}
        self.devs = {k: {int(t): dict(v) for t, v in d} for k, d in s["devs"].items()}
        self.rot = {k: list(v) for k, v in s["rot"].items()}
        self.accepted = dict(s["accepted"])
        self.pending = [(dict(b), {k: np.asarray(v, I64) for k, v in lv.items()}) for b, lv in s["pending"]]
        self.next_state = s["next_state"]
        self.stats = {**self.stats, **s["stats"]}
        if self.store is not None:
            self.store.set_state(store)

    def summary(self) -> dict[str, Any]:
        return {"stats": dict(self.stats), "levels_active": len(self.levels), "open_episodes": len(self.eps),
                "open_holds": len(self.holds), "settings_hash": self.st.hash[:16], "state_digest": self.state_digest()}  # fmt: skip


def _f(x: list[int] | None) -> float | None:
    return None if x is None else float(Fraction(int(x[0]), int(x[1])))


__all__ = ["AuctionEngine", "AuctionStore", "worst_dq"]
