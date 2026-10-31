"""Phase 5 processing settings: parsed from the ``processing`` and ``sessions`` config blocks and validated at startup.

Phase 1's ``settings.validate_config`` calls this at ``Config.load``, so invalid or conflicting Phase 5 values fail at
startup (``ConfigError``, CLI exit 2). Its ``hash`` is stored in every processor
checkpoint, so a resume under changed settings is refused.
"""
from __future__ import annotations
from typing import Any
from dataclasses import dataclass, field
import hashlib, json, re

from ..timeutil import DAY_MS

UNIT_MS = {"s": 1000, "m": 60_000, "h": 3_600_000}
DEFAULT_PROCESSING = {
    "version": 1,
    "intervals": ["1s", "5s", "1m", "5m", "15m", "1h"],
    "persist_intervals": ["1m"],
    "footprint_bin_ticks": 1,
    "dedup_window_ids": 131072,
    "include_recovered": True,
    "inferred_side": {"enabled": False, "method": "tick_rule"},
    "dq_datasets": ["trades"],
}


def parse_interval(s: str) -> int:
    m = re.fullmatch(r"(\d+)([smh])", s)
    if not m: raise ValueError(f"interval {s!r}: expected <n>s, <n>m or <n>h")
    ms = int(m.group(1)) * UNIT_MS[m.group(2)]
    if ms <= 0 or DAY_MS % ms: raise ValueError(f"interval {s!r} must divide one UTC day exactly")
    return ms


@dataclass(frozen=True)
class ProcessingSettings:
    version: int
    intervals: tuple[Any, ...]            # names, sorted by length
    interval_ms: dict[str, int]  # name -> ms
    persist_intervals: tuple[Any, ...]
    footprint_bin_ticks: int
    dedup_window_ids: int
    include_recovered: bool
    inferred_enabled: bool
    dq_datasets: tuple[Any, ...]
    trade_time_col: str
    trade_shift_ms: int         # replay availability shift for trades (offset + latency)
    tick_size: str
    step_size: str
    instrument_id: str
    sessions_cfg: dict[str, Any] = field(default_factory=dict)
    hash: str = ""

    @property
    def base_ms(self) -> int: return int(self.interval_ms[self.intervals[0]])

    @classmethod
    def from_config(cls, cfg: Any) -> "ProcessingSettings":
        from ..schemas import instrument_id
        raw = {**DEFAULT_PROCESSING, **(cfg.raw.get("processing") or {})}
        names = list(raw["intervals"])
        if not names: raise ValueError("processing.intervals is empty")
        if len(set(names)) != len(names): raise ValueError("processing.intervals has duplicates")
        ms = {n: parse_interval(n) for n in names}
        if len(set(ms.values())) != len(ms): raise ValueError("processing.intervals: two names give the same length")
        names.sort(key=lambda n: ms[n]); base = ms[names[0]]
        for n in names:
            if ms[n] % base: raise ValueError(f"interval {n} is not a multiple of the base interval {names[0]}")
        persist = tuple(raw["persist_intervals"])
        for p in persist:
            if p not in ms: raise ValueError(f"persist interval {p} is not in processing.intervals")
        fb = int(raw["footprint_bin_ticks"])
        if fb < 1: raise ValueError("footprint_bin_ticks must be >= 1")
        w = int(raw["dedup_window_ids"])
        if w < 1024: raise ValueError("dedup_window_ids must be >= 1024")
        inf = raw.get("inferred_side") or {}
        if inf.get("enabled") and inf.get("method", "tick_rule") != "tick_rule":
            raise ValueError("inferred_side.method: only 'tick_rule' is implemented")
        tr = cfg["replay"]["streams"]["trades"]
        shift = int(tr["time_offset_ms"]) + int(tr["latency_ms"])
        if shift < 0: raise ValueError("negative trade availability shift")
        sessions = cfg.raw.get("sessions") or {}
        inst = cfg["instrument"]
        body = {"processing": raw, "sessions": sessions, "trade_time_col": tr["time_col"], "trade_shift_ms": shift,
                "tick": inst["tick_size"], "step": inst["step_size"], "symbol": cfg.symbol}
        h = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
        st = cls(int(raw["version"]), tuple(names), ms, persist, fb, w, bool(raw["include_recovered"]), bool(inf.get("enabled")),
                 tuple(raw["dq_datasets"]), tr["time_col"], shift, str(inst["tick_size"]), str(inst["step_size"]),
                 instrument_id(cfg.symbol), sessions, h)
        # session boundaries must align with the base interval so window membership is exact per base bucket
        from .sessions import SessionCalendar
        cal = SessionCalendar.from_config(sessions)
        for sd in cal.sessions:
            for mnt in (sd.start_min, sd.end_min):
                if (mnt * 60_000) % base: raise ValueError(f"session {sd.name} boundary not aligned to base interval {names[0]}")
        if (cal.td_roll * 60_000) % base or cal.week_open % base or cal.week_close % base:
            raise ValueError("trading-day roll / weekly schedule not aligned to the base interval")
        for n, m in ms.items():   # so every bar is either wholly inside or wholly outside the regular schedule
            if cal.week_open % m or cal.week_close % m:
                raise ValueError(f"interval {n} straddles the regular-schedule open/close; pick an interval dividing it")
        return st
