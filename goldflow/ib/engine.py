"""Phase 10: the Initial Balance engine, a Phase 5 ``ProcessorSink`` (one code path for replay and the live bus).

Input: the processor's finalised bars (formation, acceptance and behaviour intervals; default 1m, 1m, 5m) and their
exact 1-tick footprint levels. Each IB definition (default New York 08:20-09:20 America/New_York, London 08:00-09:00
Europe/London; decision D4 PROPOSED) gives one ``IBTracker`` per local trading date. Its boundaries come from zoneinfo,
so DST is handled the same way in replay and live.

Two clocks, so 1-minute events are not delayed by the 5-minute bars:
* stream A (formation + acceptance bars) runs to T1 = min(final_upto[formation], final_upto[acceptance]);
* stream B (behaviour bars) runs to T2 = final_upto[behaviour] (always <= T1);
* the session summary fires once both have passed session_end.
Every row is labelled with holiday, weekend, gold_closed, dq_status and coverage, and carries
``available_at = event_time + trade shift``.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

from ..processing.aggregation import digest_tree
from ..processing.trade_processor import DQ_NAMES, DQ_RANK, ProcessorSink
from ..replay import ReplayGuardError
from ..timeutil import iso
from .settings import IBSettings
from .store import IBStore
from .tracker import IBTracker

Bar = dict[str, Any]
Listener = Callable[[list[dict[str, Any]]], None]
STAT_KEYS = (
    "bars_formation",
    "bars_acceptance",
    "bars_behaviour",
    "events",
    "developing",
    "sessions",
    "instances",
    "dst_gap_skipped",
    "seeks",
)


def bars_from_columns(bars: dict[str, Any], levels: dict[str, Any] | None) -> list[Bar]:
    """Phase 5 bar columns -> plain dicts. With levels, each bar also carries its 1-tick levels and lots."""
    n = len(bars["bar_start"])
    out: list[Bar] = []
    lv_by: dict[int, tuple[list[int], list[int]]] = {}
    if levels is not None and len(levels["bar_start"]):
        lots = levels["buy_lots"] + levels["sell_lots"] + levels["unknown_lots"]
        for s, lv, q in zip(levels["bar_start"].tolist(), levels["level"].tolist(), lots.tolist()):
            e = lv_by.setdefault(s, ([], []))
            e[0].append(lv)
            e[1].append(q)
    cols = {
        k: bars[k].tolist()
        for k in (
            "bar_start",
            "bar_end",
            "open_ticks",
            "high_ticks",
            "low_ticks",
            "close_ticks",
            "volume_lots",
            "buy_lots",
            "sell_lots",
        )
    }
    for i in range(n):
        b = {
            "bar_start": cols["bar_start"][i],
            "bar_end": cols["bar_end"][i],
            "open": cols["open_ticks"][i],
            "high": cols["high_ticks"][i],
            "low": cols["low_ticks"][i],
            "close": cols["close_ticks"][i],
            "volume": cols["volume_lots"][i],
            "buy": cols["buy_lots"][i],
            "sell": cols["sell_lots"][i],
        }
        if levels is not None:
            lv, q = lv_by.get(b["bar_start"], ([], []))
            b["lv"], b["lots"] = lv, q
        out.append(b)
    return out


def _beyond(b: Bar, hi: int | None, lo: int | None) -> tuple[int, int]:
    if hi is None or lo is None:
        return 0, 0
    up = sum(q for lv, q in zip(b["lv"], b["lots"]) if lv > hi)
    dn = sum(q for lv, q in zip(b["lv"], b["lots"]) if lv < lo)
    return up, dn


def _covers(tr: IBTracker, b: Bar) -> bool:
    return bool(tr.ib_start <= b["bar_start"] and b["bar_end"] <= tr.session_end)


def drive(
    trackers: list[IBTracker], form: list[Bar], acc: list[Bar] | None, beh: list[Bar], t1: int, t2: int, same_acc: bool
) -> list[dict[str, Any]]:
    """Feed bars (each list in bar order, all ending <= its stream's clock) and fire time events. Shared by the engine
    and the verifier, so both apply the rules in the same order. ``same_acc``: acceptance bars are the formation bars."""
    rows: list[dict[str, Any]] = []
    groups: dict[int, list[tuple[int, Bar]]] = {}
    for b in form:
        groups.setdefault(b["bar_end"], []).append((0, b))
    for b in acc if (acc is not None and not same_acc) else []:
        groups.setdefault(b["bar_end"], []).append((1, b))
    for e in sorted(groups):
        for tr in trackers:
            rows += tr.fire_ib(e, inclusive=False)
        for kind, b in sorted(groups[e], key=lambda x: x[0]):
            for tr in trackers:
                if not _covers(tr, b):
                    continue
                if kind == 0:
                    rows += tr.formation_bar(b)
                if kind == 1 or (kind == 0 and same_acc):
                    rows += tr.acceptance_bar(b, *_beyond(b, tr.hi, tr.lo))
    for tr in trackers:
        rows += tr.fire_ib(t1, inclusive=True)
    for b in beh:
        for tr in trackers:
            rows += tr.fire_held(b["bar_end"], inclusive=False)
            if _covers(tr, b):
                rows += tr.behaviour_bar(b)
    for tr in trackers:
        rows += tr.fire_held(t2, inclusive=True)
        rows += tr.fire_summary(min(t1, t2))
    return rows


class IBEngine(ProcessorSink):
    name = "initial_balance"

    def __init__(self, cfg: Any, store: IBStore | None = None, listeners: list[Listener] | tuple[Listener, ...] = ()) -> None:
        from ..processing.settings import ProcessingSettings

        self.st = IBSettings.from_config(cfg)
        pst = ProcessingSettings.from_config(cfg)
        self.ms = {
            k: pst.interval_ms[v]
            for k, v in (
                ("form", self.st.formation_interval),
                ("acc", self.st.accept_interval),
                ("beh", self.st.behaviour_interval),
            )
        }
        self.same_acc = self.st.accept_interval == self.st.formation_interval
        self.shift = pst.trade_shift_ms
        self.store = store
        self.listeners = list(listeners)
        self.proc: Any = None
        self._reset()

    def _reset(self) -> None:
        self.trackers: dict[str, IBTracker] = {}
        self.done: dict[str, int] = {}  # key -> session_end of finished instances (pruned)
        self.pending: dict[str, list[Bar]] = {"form": [], "acc": [], "beh": []}
        self.upto: int | None = None
        self.dq: list[list[Any]] = []
        self.stats = {k: 0 for k in STAT_KEYS}

    # ================================================================== sink interface
    def on_start(self, ctx: dict[str, Any], proc: Any) -> None:
        self.proc = proc
        st = proc.st
        if st.footprint_bin_ticks != 1:
            raise ValueError("IBEngine needs processing.footprint_bin_ticks = 1")
        if self.store is not None:
            meta = {
                "ib_settings_hash": self.st.hash,
                "processing_settings_hash": st.hash,
                "source_dataset_version": ctx.get("dataset_version"),
                "trade_shift_ms": st.trade_shift_ms,
                "definitions": [d.__dict__ for d in self.st.defs],
                "extensions": [str(k) for k in self.st.extensions],
                "ivb": "DISABLED (decision D1 open)",
                "tick_size": st.tick_size,
                "step_size": st.step_size,
            }
            self.store.start(meta, self.st.extensions, bool(ctx.get("resumed")))

    def on_dq(self, start_ms: int, end_ms: int, status: str) -> None:
        x = [int(start_ms), int(end_ms), str(status)]
        if x not in self.dq:
            self.dq.append(x)

    def on_bars(self, interval: str, bars: dict[str, Any], levels: dict[str, Any], final_upto_ms: int) -> None:
        if interval == self.st.formation_interval:
            self.pending["form"] += bars_from_columns(bars, levels if self.same_acc else None)
            self.stats["bars_formation"] += len(bars["bar_start"])
        if interval == self.st.accept_interval and not self.same_acc:
            self.pending["acc"] += bars_from_columns(bars, levels)
            self.stats["bars_acceptance"] += len(bars["bar_start"])
        if interval == self.st.behaviour_interval:
            self.pending["beh"] += bars_from_columns(bars, None)
            self.stats["bars_behaviour"] += len(bars["bar_start"])

    def on_after_advance(self, proc: Any) -> None:
        fu = proc.final_upto
        need = [self.st.formation_interval, self.st.accept_interval, self.st.behaviour_interval]
        if any(fu.get(i) is None for i in need):
            return
        t1 = min(int(fu[self.st.formation_interval]), int(fu[self.st.accept_interval]))
        t2 = int(fu[self.st.behaviour_interval])
        if self.upto is None:
            of = proc.observe_from
            self.upto = int(of) if of is not None else min(t1, t2)
        self._open(self.upto, t1)
        form, self.pending["form"] = self._take("form", t1)
        acc, self.pending["acc"] = self._take("acc", t1)
        beh, self.pending["beh"] = self._take("beh", t2)
        trs = [self.trackers[k] for k in sorted(self.trackers, key=lambda k: (self.trackers[k].ib_start, k))]
        rows = drive(trs, form, acc, beh, t1, t2, self.same_acc)
        self._emit(rows)
        t = min(t1, t2)
        for k in [k for k, tr in self.trackers.items() if tr.done]:
            self.done[k] = self.trackers.pop(k).session_end
        self.done = {k: e for k, e in self.done.items() if e > t - 8 * 86_400_000}
        self.upto = t
        if self.store is not None:
            self.store.flush_complete(t)
        floor = min([tr.ib_start for tr in self.trackers.values()] + [t])
        self.dq = [x for x in self.dq if x[1] > floor]

    def _take(self, k: str, t: int) -> tuple[list[Bar], list[Bar]]:
        """Split pending bars of a stream into (bar_end <= t, the rest)."""
        p = self.pending[k]
        return [b for b in p if b["bar_end"] <= t], [b for b in p if b["bar_end"] > t]

    def on_seek(self, t_ms: int) -> None:
        self.trackers = {}
        self.pending = {"form": [], "acc": [], "beh": []}
        self.upto = None
        self.stats["seeks"] += 1
        if self.store is not None:
            self.store.discard_unwritten()

    def on_stop(self, ctx: dict[str, Any], proc: Any) -> None:
        if ctx.get("status") == "completed" and self.store is not None:
            self.store.flush_complete(self.upto or 0, force=True)

    # ================================================================== instances
    def _open(self, a: int, b: int) -> None:
        d0 = datetime.fromtimestamp(a / 1000, tz=timezone.utc).date() - timedelta(days=1)
        d1 = datetime.fromtimestamp(b / 1000, tz=timezone.utc).date() + timedelta(days=1)
        d = d0
        while d <= d1:
            for df in self.st.defs:
                key = f"{df.name}:{d}"
                if d.weekday() not in df.weekdays or key in self.trackers or key in self.done:
                    continue
                try:
                    s, e, se = df.bounds(d)
                except ValueError:
                    self.stats["dst_gap_skipped"] += 1
                    continue
                if s <= b and se > a:
                    self.trackers[key] = self._tracker(df.name, d, s, e, se)
            d += timedelta(days=1)

    def _tracker(self, name: str, d: date, s: int, e: int, se: int) -> IBTracker:
        lab = self.st.calendar.label(d)
        wk = bool(self.proc.cal.is_weekend([s])[0]) if self.proc is not None else False
        labels = {**lab, "weekend": wk, "gold_closed": bool(lab["gold_closed"] or wk)}
        self.stats["instances"] += 1
        return IBTracker(name, str(d), s, e, se, self.st, self.ms["beh"], labels)

    # ================================================================== output
    def _emit(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        of = self.proc.observe_from if self.proc is not None else None
        filt = bool(self.proc is not None and self.proc.filtered)
        dev: list[dict[str, Any]] = []
        ev: list[dict[str, Any]] = []
        ss: list[dict[str, Any]] = []
        for r in rows:
            code = 0
            for a, b, status in self.dq:
                if a < r["event_time"] and b > r["ib_start"]:
                    code = max(code, DQ_RANK.get(status, 3))
            r["dq_status"] = str(DQ_NAMES[code])
            r["coverage"] = "PARTIAL_START" if (of is not None and r["ib_start"] < of) else ("FILTERED" if filt else "FULL")
            r["available_at"] = r["event_time"] + self.shift
            (dev if r["event"] == "IB_DEVELOPING" else ss if r["event"] == "SESSION_SUMMARY" else ev).append(r)
        self.stats["developing"] += len(dev)
        self.stats["events"] += len(ev)
        self.stats["sessions"] += len(ss)
        if self.store is not None and self.st.persist:
            self.store.add("developing", dev)
            self.store.add("events", ev)
            self.store.add("sessions", ss)
        for f in self.listeners:
            f(rows)

    def current(self, name: str) -> dict[str, Any] | None:
        """The open IB instance of a definition (live use): its IB fields so far and its state."""
        for k in sorted(self.trackers):
            tr = self.trackers[k]
            if tr.name == name:
                return {
                    "key": k,
                    "formed": tr.formed,
                    **tr.ib_fields(),
                    "breakout": {s: tr.bo[s]["state"] for s in ("UP", "DOWN")},
                }
        return None

    # ================================================================== state
    def _tree(self) -> dict[str, Any]:
        return {
            "settings_hash": self.st.hash,
            "upto": self.upto,
            "done": dict(sorted(self.done.items())),
            "trackers": {
                k: {"inst": [t.name, t.label, t.ib_start, t.ib_end, t.session_end], "labels": t.labels, "state": t.get_state()}
                for k, t in sorted(self.trackers.items())
            },
            "pending": self.pending,
            "dq": self.dq,
            "stats": dict(self.stats),
        }

    def state_digest(self) -> str:
        return digest_tree(self._tree())

    def get_state(self) -> dict[str, Any]:
        t = self._tree()
        t["store"] = self.store.get_state() if self.store is not None else {}
        return t

    def set_state(self, s: dict[str, Any]) -> None:
        if not s:
            return
        if s.get("settings_hash") != self.st.hash:
            raise ReplayGuardError(
                f"IB engine checkpoint was taken with initial_balance settings {str(s.get('settings_hash'))[:12]}, "
                f"current settings are {self.st.hash[:12]}; start a new run name"
            )
        self._reset()
        self.upto = s["upto"]
        self.done = dict(s["done"])
        for k, t in s["trackers"].items():
            name, label, a, e, se = t["inst"]
            tr = IBTracker(name, label, a, e, se, self.st, self.ms["beh"], t["labels"])
            tr.set_state(t["state"])
            self.trackers[k] = tr
        self.pending = {k: list(v) for k, v in s["pending"].items()}
        self.dq = [list(x) for x in s["dq"]]
        self.stats = {**self.stats, **s["stats"]}
        if self.store is not None:
            self.store.set_state(s.get("store") or {})

    def summary(self) -> dict[str, Any]:
        return {
            "stats": dict(self.stats),
            "open": sorted(self.trackers),
            "upto": iso(self.upto) if self.upto else None,
            "settings_hash": self.st.hash[:16],
            "state_digest": self.state_digest(),
        }
