"""Phase 10: Initial Balance (IB), its extensions and IB behaviour, as a Phase 5 ``ProcessorSink``.

Config block ``initial_balance`` (docs/METRICS_IB_V1.md). Each definition names a parent session from the Phase 5
calendar and the IB window inside it, either as an ``offset`` from the session start or as a local ``start`` time in the
session's time zone, plus a ``length`` (default 60 min). DST is handled by the calendar (zoneinfo); a local time
that does not exist is refused.

Outputs (no look-ahead; every row carries ``available_at``):
* **IB snapshots**: developing rows every ``snapshot_every`` while the IB forms (built only from bars that ended
  before ``as_of``), and the final row at the IB end: high, low, mid, range, volume, buy/sell/delta and the
  extension levels ``IB high + k x range`` / ``IB low - k x range`` for every configured k.
* **IB events** after the IB end, until the parent session ends:
  - ``BREAKOUT_UP/DOWN``: a ``break_interval`` bar closes beyond the IB extreme. Two documented boolean filters ride
    along and never suppress: ``volume_filter`` (bar volume >= the IB's average volume per bar of that interval)
    and ``delta_filter`` (bar delta agrees with the direction).
  - ``FAILED_BREAK_UP/DOWN``: after a breakout, a close back inside the IB within ``failed_break_window``.
  - ``EXTENSION_UP/DOWN``: the first trade at or beyond each extension level (1 s resolution).
  - ``SESSION_SUMMARY``: at the session end, the largest extension reached each way (in IB ranges) and the counts.
* **Holidays**: IB rows carry the holiday label; no IB is built on a CLOSED holiday.
* Acceptance outside the IB is Phase 9's ACCEPTANCE on the IB_HIGH / IB_LOW levels: a full IB is offered to the
  ``AuctionEngine`` as reference levels at its end (``listeners``).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from fractions import Fraction
from typing import Any, Callable

import numpy as np
import pyarrow as pa

from .. import schemas as S
from ..processing.aggregation import digest_tree
from ..processing.holidays import HolidayCalendar
from ..processing.sessions import local_date, local_to_ms
from ..processing.trade_processor import ProcessorSink
from ..replay import ReplayGuardError
from ..timeutil import DAY_MS
from .events import worst_dq
from .flow_engine import pack, unpack
from .flow_store import DerivedStore
from .settings import _frac

I64 = np.int64
DEFAULT_IB: dict[str, Any] = {
    "version": 1,
    "definitions": [
        {"name": "new_york", "session": "new_york", "offset": "0m", "length": "60m"},
        {"name": "london", "session": "london", "start": "08:00", "length": "60m"},
    ],
    "extensions": ["0.5", "1", "1.5", "2", "3"],
    "break_interval": "5m",
    "failed_break_window": "30m",
    "snapshot_every": "1m",
    "persist": True,
}
Listener = Callable[[str, list[dict[str, Any]]], None]


@dataclass(frozen=True)
class IBDef:
    name: str
    session: str
    offset_ms: int | None
    start_min: int | None
    length_ms: int


@dataclass(frozen=True)
class IBSettings:
    defs: tuple[IBDef, ...]
    extensions: tuple[Fraction, ...]
    break_interval: str
    break_ms: int
    failed_ms: int
    snapshot_ms: int
    persist: bool
    raw: dict[str, Any]
    hash: str

    @classmethod
    def from_config(cls, cfg: Any) -> "IBSettings":
        from ..processing.sessions import SessionCalendar
        from ..processing.settings import ProcessingSettings, parse_interval

        user = cfg.raw.get("initial_balance") or {}
        raw: dict[str, Any] = {**DEFAULT_IB, **{k: v for k, v in user.items() if not k.startswith("_")}}
        bad = sorted(set(raw) - set(DEFAULT_IB))
        if bad:
            raise ValueError(f"initial_balance: unknown keys {bad}")
        proc = ProcessingSettings.from_config(cfg)
        cal = SessionCalendar.from_config(proc.sessions_cfg)
        names = {s.name: s for s in cal.sessions}

        def dur(v: Any, what: str) -> int:
            ms = parse_interval(str(v)) if str(v) not in ("0m", "0s", "0h") else 0
            if ms % proc.base_ms:
                raise ValueError(f"{what} must be a multiple of the base interval {proc.intervals[0]}")
            return ms

        bi = str(raw["break_interval"])
        if bi not in proc.interval_ms:
            raise ValueError(f"initial_balance.break_interval {bi!r} is not in processing.intervals")
        snap = dur(raw["snapshot_every"], "initial_balance.snapshot_every")
        defs = []
        for d0 in raw["definitions"]:
            d = {k: v for k, v in d0.items() if not k.startswith("_")}
            nm, ss = str(d["name"]), str(d["session"])
            if ss not in names:
                raise ValueError(f"initial_balance {nm}: session {ss!r} is not in the session calendar {sorted(names)}")
            if ("offset" in d) == ("start" in d):
                raise ValueError(
                    f"initial_balance {nm}: give exactly one of 'offset' (from the session start) or 'start' (local time)"
                )
            length = dur(d["length"], f"initial_balance {nm}.length")
            if length <= 0 or length % proc.interval_ms[bi] or length % snap:
                raise ValueError(
                    f"initial_balance {nm}.length must be positive and a multiple of break_interval and snapshot_every"
                )
            off = dur(d["offset"], f"initial_balance {nm}.offset") if "offset" in d else None
            sm = None
            if "start" in d:
                h, m = (int(x) for x in str(d["start"]).split(":"))
                sm = h * 60 + m
                if (sm * 60_000) % proc.base_ms:
                    raise ValueError(f"initial_balance {nm}.start not aligned to the base interval")
            defs.append(IBDef(nm, ss, off, sm, length))
        if len({d.name for d in defs}) != len(defs):
            raise ValueError("initial_balance.definitions: duplicate names")
        exts = tuple(_frac(x, "initial_balance.extensions") for x in raw["extensions"])
        if any(x <= 0 for x in exts) or len(set(exts)) != len(exts):
            raise ValueError("initial_balance.extensions must be distinct positive numbers")
        st = cls(tuple(defs), tuple(sorted(exts)), bi, proc.interval_ms[bi], dur(raw["failed_break_window"], "failed_break_window"),
                 snap, bool(raw["persist"]), raw, "")  # fmt: skip
        hol = cfg.raw.get("holidays") or {}
        body = {"initial_balance": raw, "holidays": hol, "processing_hash": proc.hash}
        digest = hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()
        st = cls(**{**st.__dict__, "hash": digest})
        # every definition must fit inside its session on dates around both DST changes and in summer
        for dd in defs:
            for day in (date(2026, 3, 9), date(2026, 3, 30), date(2026, 7, 15), date(2026, 10, 26), date(2026, 11, 2)):
                if day.weekday() in names[dd.session].weekdays:
                    s0, s1 = cal.session_bounds(dd.session, day)
                    a, b = ib_bounds(dd, day, cal)
                    if not (s0 <= a < b <= s1):
                        raise ValueError(
                            f"initial_balance {dd.name} on {day} ({a}-{b}) is not inside session {dd.session} ({s0}-{s1})"
                        )
        return st


def ib_bounds(d: IBDef, day: date, cal: Any) -> tuple[int, int]:
    """UTC [start, end) of IB definition ``d`` for the session that starts on local date ``day``."""
    s0, _ = cal.session_bounds(d.session, day)
    if d.offset_ms is not None:
        a = s0 + d.offset_ms
    else:
        tz = next(x.tz for x in cal.sessions if x.name == d.session)
        assert d.start_min is not None
        a = local_to_ms(day, d.start_min, tz)
    return a, a + d.length_ms


_T, _I, _F, _B, _X = S.TS, pa.int64(), pa.float64(), pa.bool_(), pa.string()


class IBStore(DerivedStore):
    NAME = "initial_balance"
    SCHEMA = "InitialBalance"
    FIELDS = {
        "ib": [("ib_key", _X), ("name", _X), ("session", _X), ("label", _X), ("ib_start", _T), ("ib_end", _T), ("session_end", _T),
               ("as_of", _T), ("available_at", _T), ("is_final", _B), ("high_ticks", _I), ("low_ticks", _I), ("mid_ticks", _F),
               ("range_ticks", _I), ("volume_lots", _I), ("buy_lots", _I), ("sell_lots", _I), ("delta_lots", _I), ("extensions", _X),
               ("holiday", _X), ("weekend", _B), ("dq_status", _X), ("coverage", _X)],
        "ib_events": [("event_id", _X), ("ib_key", _X), ("event_type", _X), ("event_time", _T), ("available_at", _T), ("price_ticks", _I),
                      ("level_ticks", _F), ("k", _X), ("volume_filter", _B), ("delta_filter", _B), ("detail", _X), ("coverage", _X)],
    }  # fmt: skip
    SORT = {"ib": [("as_of", "ascending"), ("ib_key", "ascending")],
            "ib_events": [("available_at", "ascending"), ("event_id", "ascending")]}  # fmt: skip
    DOC = {"spec": "docs/METRICS_IB_V1.md", "available_at": "earliest simulated time the row may be used",
           "extensions": "JSON list of [k, up_level_ticks, down_level_ticks] (exact rationals as strings)"}  # fmt: skip
    MATCH_KEYS = ("ib_settings_hash", "processing_settings_hash", "source_dataset_version")


class IBEngine(ProcessorSink):
    name = "initial_balance"

    def __init__(self, cfg: Any, store: IBStore | None = None, listeners: list[Listener] | tuple[Listener, ...] = ()) -> None:
        self.st = IBSettings.from_config(cfg)
        self.hol = HolidayCalendar(cfg.raw.get("holidays"))
        self.store = store
        self.listeners = list(listeners)
        self.proc: Any = None
        self.shift = 0
        self._reset()

    def _reset(self) -> None:
        self.ibs: dict[str, dict[str, Any]] = {}
        self.done: set[str] = set()
        self.dq: list[list[Any]] = []
        self.stats = {"ibs_final": 0, "ibs_partial": 0, "holiday_closed_skipped": 0, "breakouts": 0, "failed_breaks": 0,
                      "extensions": 0, "summaries": 0, "seeks": 0}  # fmt: skip

    # ------------------------------------------------------------------ helpers
    def _emit(self, table: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if self.store is not None and self.st.persist:
            self.store.add(table, rows)
        for f in self.listeners:
            f(table, rows)

    def _open(self, a: int, b: int) -> None:
        cal = self.proc.cal
        for day_ord in range((a - DAY_MS) // DAY_MS, b // DAY_MS + 2):
            day = date.fromordinal(date(1970, 1, 1).toordinal() + day_ord)
            for d in self.st.defs:
                sd = next(x for x in cal.sessions if x.name == d.session)
                if day.weekday() not in sd.weekdays:
                    continue
                key = f"ib:{d.name}:{day}"
                if key in self.ibs or key in self.done:
                    continue
                s0, s1 = cal.session_bounds(d.session, day)
                ia, ib_ = ib_bounds(d, day, cal)
                if s1 <= a or ia > b:
                    continue
                if self.hol.closed(local_date(ia, self.hol.tz)):
                    self.done.add(key)
                    self.stats["holiday_closed_skipped"] += 1
                    continue
                of = self.proc.observe_from
                self.ibs[key] = {"key": key, "name": d.name, "session": d.session, "label": str(day), "start": ia, "end": ib_, "send": s1,
                                 "min": {}, "dq": "OK", "next_snap": ia + self.st.snapshot_ms, "final": None,
                                 "cov": "PARTIAL_START" if (of is not None and ia < of) else ("FILTERED" if self.proc.filtered else "FULL"),
                                 "holiday": self.hol.label(local_date(ia, self.hol.tz)), "up": None, "dn": None, "ext_hit": [],
                                 "max_hi": None, "min_lo": None, "n_break": [0, 0], "n_fail": [0, 0]}  # fmt: skip

    def _agg(self, ib: dict[str, Any], upto: int) -> dict[str, Any] | None:
        ms = [v for k, v in ib["min"].items() if int(k) < upto]
        if not ms:
            return None
        return {"high": max(m[0] for m in ms), "low": min(m[1] for m in ms), "vol": sum(m[2] for m in ms), "buy": sum(m[3] for m in ms),
                "sell": sum(m[4] for m in ms)}  # fmt: skip

    def _row(self, ib: dict[str, Any], a: dict[str, Any], as_of: int, final: bool) -> dict[str, Any]:
        h, lo = a["high"], a["low"]
        rng = h - lo
        exts = [[str(k), str(h + k * rng), str(lo - k * rng)] for k in self.st.extensions]
        return {"ib_key": ib["key"], "name": ib["name"], "session": ib["session"], "label": ib["label"], "ib_start": ib["start"],
                "ib_end": ib["end"], "session_end": ib["send"], "as_of": as_of, "available_at": as_of + self.shift, "is_final": final,
                "high_ticks": h, "low_ticks": lo, "mid_ticks": float(Fraction(h + lo, 2)), "range_ticks": rng, "volume_lots": a["vol"],
                "buy_lots": a["buy"], "sell_lots": a["sell"], "delta_lots": a["buy"] - a["sell"], "extensions": json.dumps(exts),
                "holiday": ib["holiday"], "weekend": bool(self.proc.cal.is_weekend(np.array([ib["start"]]))[0]),
                "dq_status": ib["dq"], "coverage": ib["cov"]}  # fmt: skip

    def _event(self, ib: dict[str, Any], etype: str, t: int, price: int, level: Fraction | None, k: str | None,
               vf: bool | None = None, df: bool | None = None, detail: dict[str, Any] | None = None) -> dict[str, Any]:  # fmt: skip
        return {"event_id": f"{etype}:{ib['key']}:{k or ''}:{t}", "ib_key": ib["key"], "event_type": etype, "event_time": t,
                "available_at": t + self.shift, "price_ticks": price, "level_ticks": None if level is None else float(level), "k": k,
                "volume_filter": vf, "delta_filter": df, "detail": json.dumps(detail or {}, sort_keys=True), "coverage": ib["cov"]}  # fmt: skip

    # ------------------------------------------------------------------ sink interface
    def on_start(self, ctx: dict[str, Any], proc: Any) -> None:
        self.proc = proc
        self.shift = proc.st.trade_shift_ms
        if self.st.break_interval not in proc.st.interval_ms:
            raise ValueError("initial_balance.break_interval is not a processor interval")
        if self.store is not None:
            self.store.start({"ib_settings_hash": self.st.hash, "processing_settings_hash": proc.st.hash,
                              "source_dataset_version": ctx.get("dataset_version"), "holidays_version": self.hol.version,
                              "observe_from": proc.observe_from}, bool(ctx.get("resumed")))  # fmt: skip

    def on_dq(self, start_ms: int, end_ms: int, status: str) -> None:
        x = [int(start_ms), int(end_ms), str(status)]
        if x not in self.dq:
            self.dq.append(x)

    def on_bars(self, interval: str, bars: dict[str, Any], levels: dict[str, Any], final_upto_ms: int) -> None:
        n = len(bars["bar_start"])
        if not n:
            return
        s = np.asarray(bars["bar_start"], I64)
        if interval == self.proc.st.intervals[0]:
            self._open(int(s[0]), int(s[-1]))
            hi, lo = np.asarray(bars["high_ticks"], I64), np.asarray(bars["low_ticks"], I64)
            vol = np.asarray(bars["volume_lots"], I64)
            buy, sell = np.asarray(bars["buy_lots"], I64), np.asarray(bars["sell_lots"], I64)
            dq = list(bars["dq_status"])
            for ib in self.ibs.values():
                m = np.flatnonzero((s >= ib["start"]) & (s < ib["end"]))
                if len(m):
                    snap = self.st.snapshot_ms
                    bk = (s[m] - ib["start"]) // snap * snap + ib["start"]
                    for b in np.unique(bk).tolist():
                        mm = m[bk == b]
                        cur = ib["min"].get(str(b))
                        x = [int(hi[mm].max()), int(lo[mm].min()), int(vol[mm].sum()), int(buy[mm].sum()), int(sell[mm].sum())]
                        ib["min"][str(b)] = (
                            x
                            if cur is None
                            else [max(cur[0], x[0]), min(cur[1], x[1]), cur[2] + x[2], cur[3] + x[3], cur[4] + x[4]]
                        )
                    ib["dq"] = worst_dq([ib["dq"]] + [dq[i] for i in m.tolist()])
                post = np.flatnonzero((s >= ib["end"]) & (s < ib["send"]))
                if len(post):
                    # a bar at or after the IB end means every bar before it has been delivered: the IB is complete
                    self._snapshots(ib, int(s[post[0]]))
                    if ib["final"] is not None and not ib["final"].get("empty"):
                        self._extensions(ib, s[post], hi[post], lo[post])
        if interval == self.st.break_interval:
            cols = {k: np.asarray(bars[k]).tolist() for k in ("bar_start", "bar_end", "close_ticks", "volume_lots", "delta_lots")}
            for i in range(n):
                b = {k: v[i] for k, v in cols.items()}
                for ib in self.ibs.values():
                    if ib["final"] is not None and not ib["final"].get("empty") and ib["end"] <= b["bar_start"] < ib["send"]:
                        self._breaks(ib, b)

    def _extensions(self, ib: dict[str, Any], s: Any, hi: Any, lo: Any) -> None:
        f = ib["final"]
        h, lw, rng = f["high"], f["low"], f["high"] - f["low"]
        ib["max_hi"] = int(hi.max()) if ib["max_hi"] is None else max(ib["max_hi"], int(hi.max()))
        ib["min_lo"] = int(lo.min()) if ib["min_lo"] is None else min(ib["min_lo"], int(lo.min()))
        if rng <= 0:
            return
        rows = []
        for k in self.st.extensions:
            for side, lev, arr in (("UP", h + k * rng, hi), ("DOWN", lw - k * rng, lo)):
                tag = f"{side}:{k}"
                if tag in ib["ext_hit"]:
                    continue
                hit = (
                    np.flatnonzero(arr * lev.denominator >= lev.numerator)
                    if side == "UP"
                    else np.flatnonzero(arr * lev.denominator <= lev.numerator)
                )
                if len(hit):
                    j = int(hit[0])
                    ib["ext_hit"].append(tag)
                    rows.append(self._event(ib, f"EXTENSION_{side}", int(s[j]) + self.proc.st.base_ms, int(arr[j]), lev, str(k)))
        self.stats["extensions"] += len(rows)
        self._emit("ib_events", rows)

    def _breaks(self, ib: dict[str, Any], b: dict[str, Any]) -> None:
        f = ib["final"]
        c, e = int(b["close_ticks"]), int(b["bar_end"])
        n_bars = (ib["end"] - ib["start"]) // self.st.break_ms
        rows = []
        for side, key, beyond, extreme in ((1, "up", c > f["high"], f["high"]), (-1, "dn", c < f["low"], f["low"])):
            name = "UP" if side > 0 else "DOWN"
            idx = 0 if side > 0 else 1
            if ib[key] is None and beyond:
                ib[key] = e
                ib["n_break"][idx] += 1
                vf = int(b["volume_lots"]) * n_bars >= f["vol"]
                df = int(b["delta_lots"]) * side > 0
                rows.append(self._event(ib, f"BREAKOUT_{name}", e, c, Fraction(extreme), None, vf, df,
                                        {"bar_volume_lots": int(b["volume_lots"]), "ib_avg_volume_per_bar": f["vol"] / n_bars,
                                         "bar_delta_lots": int(b["delta_lots"])}))  # fmt: skip
            elif ib[key] is not None and not beyond:
                if e - ib[key] <= self.st.failed_ms:
                    ib["n_fail"][idx] += 1
                    rows.append(self._event(ib, f"FAILED_BREAK_{name}", e, c, Fraction(extreme), None, None, None,
                                            {"breakout_at": ib[key], "minutes_outside": (e - ib[key]) // 60_000}))  # fmt: skip
                ib[key] = None
        self.stats["breakouts"] += sum(1 for r in rows if r["event_type"].startswith("BREAKOUT"))
        self.stats["failed_breaks"] += sum(1 for r in rows if r["event_type"].startswith("FAILED"))
        self._emit("ib_events", rows)

    def _snapshots(self, ib: dict[str, Any], upto: int) -> None:
        """Emit every due snapshot M <= upto (all bars before M are final). The one at the IB end is the final row; the
        final IB is then offered to listeners (the auction engine) as reference levels if fully observed."""
        rows = []
        while ib["final"] is None and ib["next_snap"] <= min(upto, ib["end"]):
            M = ib["next_snap"]
            a = self._agg(ib, M)
            if M >= ib["end"]:
                if a is None:
                    ib["final"] = {"high": 0, "low": 0, "vol": 0, "buy": 0, "sell": 0, "empty": True}
                    break
                ib["final"] = a
                rows.append(self._row(ib, a, M, True))
                if ib["cov"] == "FULL":
                    self.stats["ibs_final"] += 1
                    lv = {"series": f"ib:{ib['name']}", "window_key": ib["key"], "available_at": M + self.shift, "ref_lots": a["vol"],
                          "levels": [["IB_HIGH", a["high"], 1], ["IB_LOW", a["low"], 1], ["IB_MID", a["high"] + a["low"], 2]]}  # fmt: skip
                    for f in self.listeners:
                        f("ib_level", [lv])
                else:
                    self.stats["ibs_partial"] += 1
            elif a is not None:
                rows.append(self._row(ib, a, M, False))
            ib["next_snap"] = M + self.st.snapshot_ms
        self._emit("ib", rows)

    def on_advance(self, proc: Any) -> None:
        fu = proc.final_upto.get(proc.st.intervals[0])
        fb = proc.final_upto.get(self.st.break_interval)
        if fu is None:
            return
        for key in sorted(self.ibs):
            self._snapshots(self.ibs[key], int(fu))
        out = []
        for key in sorted(self.ibs):
            ib = self.ibs[key]
            if ib["send"] <= fu and (fb is None or ib["send"] <= fb) and ib["final"] is not None:
                f = ib["final"]
                if not f.get("empty"):
                    rng = f["high"] - f["low"]
                    up = None if rng <= 0 or ib["max_hi"] is None else float(Fraction(max(0, ib["max_hi"] - f["high"]), rng))
                    dn = None if rng <= 0 or ib["min_lo"] is None else float(Fraction(max(0, f["low"] - ib["min_lo"]), rng))
                    out.append(self._event(ib, "SESSION_SUMMARY", ib["send"], ib["max_hi"] or f["high"], None, None, None, None,
                                           {"max_extension_up_ranges": up, "max_extension_down_ranges": dn, "breakouts_up": ib["n_break"][0],
                                            "breakouts_down": ib["n_break"][1], "failed_up": ib["n_fail"][0], "failed_down": ib["n_fail"][1],
                                            "extensions_hit": sorted(ib["ext_hit"])}))  # fmt: skip
                self.done.add(key)
                del self.ibs[key]
        self.stats["summaries"] += len(out)
        self._emit("ib_events", out)
        if self.store is not None and proc.clock is not None:
            self.store.flush_complete(int(proc.clock))

    def on_seek(self, t_ms: int) -> None:
        stats = dict(self.stats)
        self._reset()
        self.stats = stats
        self.stats["seeks"] += 1
        if self.store is not None:
            self.store.discard_unwritten()

    def on_stop(self, ctx: dict[str, Any], proc: Any) -> None:
        if ctx.get("status") == "completed" and self.store is not None:
            self.store.flush_complete(int(proc.clock or 0), force=True)

    # ------------------------------------------------------------------ state
    def _tree(self) -> dict[str, Any]:
        return {"settings_hash": self.st.hash, "ibs": dict(sorted(self.ibs.items())), "done": sorted(self.done),
                "dq": [list(x) for x in self.dq], "stats": dict(self.stats)}  # fmt: skip

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
                f"IB engine checkpoint was taken with initial_balance settings {str(s.get('settings_hash'))[:12]}, "
                f"current settings are {self.st.hash[:12]}; start a new run name"
            )
        self._reset()
        store = s.get("store") or {}
        s = unpack({k: v for k, v in s.items() if k != "store"})
        self.ibs = {k: dict(v) for k, v in s["ibs"].items()}
        self.done = set(s["done"])
        self.dq = [list(x) for x in s["dq"]]
        self.stats = {**self.stats, **s["stats"]}
        if self.store is not None:
            self.store.set_state(store)

    def summary(self) -> dict[str, Any]:
        return {
            "stats": dict(self.stats),
            "open": sorted(self.ibs),
            "settings_hash": self.st.hash[:16],
            "state_digest": self.state_digest(),
        }
