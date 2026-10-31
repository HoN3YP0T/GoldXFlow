"""Phase 10 settings: the ``initial_balance`` and ``calendar`` config blocks (docs/METRICS_IB_V1.md).

Validated at ``Config.load`` (Phase 1 ``validate_config``). Neither block feeds the Phase 5 processing hash, so the
footprint and profile datasets stay valid when they change. Their own ``hash`` is stored in every IB checkpoint and
every persisted row.

IVB is **disabled** until its definition is signed off (decision D1). ``ivb.enabled = true`` is refused at startup;
IVB is never silently replaced by the Initial Balance or any other metric.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from fractions import Fraction
from typing import Any

from ..processing.sessions import DAYS, _hm, local_to_ms
from .holidays import HolidayCalendar

DEFAULT_IB: dict[str, Any] = {
    "version": 1,
    "definitions": [
        {
            "name": "new_york",
            "tz": "America/New_York",
            "start": "08:20",
            "minutes": 60,
            "session_end": "17:00",
            "weekdays": ["Mon", "Tue", "Wed", "Thu", "Fri"],
        },
        {
            "name": "london",
            "tz": "Europe/London",
            "start": "08:00",
            "minutes": 60,
            "session_end": "16:00",
            "weekdays": ["Mon", "Tue", "Wed", "Thu", "Fri"],
        },
    ],
    "extensions": ["0.5", "1", "1.5", "2", "3"],
    "formation_interval": "1m",
    "behaviour_interval": "5m",
    "failed_break_window": "30m",
    "acceptance": {"close_interval": "1m", "accept_time": "15m", "accept_vol_frac": "0.30"},
    "filters": {"breakout_vol_mult": "1.5"},
    "ivb": {"enabled": False},
    "persist": True,
}


def parse_duration(v: Any, what: str) -> int:
    """'<n>s|m|h' -> ms. Unlike bar intervals, a duration need not divide a day."""
    import re

    m = re.fullmatch(r"(\d+)([smh])", str(v))
    if not m or int(m.group(1)) <= 0:
        raise ValueError(f"{what}: {v!r} must look like 30m, 90s or 2h")
    return int(m.group(1)) * {"s": 1000, "m": 60_000, "h": 3_600_000}[m.group(2)]


def _frac(v: Any, what: str) -> Fraction:
    if isinstance(v, (bool, float)):
        raise ValueError(f'{what}: give a decimal string such as "1.5" (floats are not exact)')
    try:
        return Fraction(str(v))
    except (ValueError, ZeroDivisionError):
        raise ValueError(f"{what}: {v!r} is not a number") from None


@dataclass(frozen=True)
class IBDef:
    name: str
    tz: str
    start_min: int
    minutes: int
    end_min: int  # local session end (same local day; after the IB end)
    weekdays: tuple[int, ...]

    def bounds(self, d: date) -> tuple[int, int, int]:
        """(ib_start, ib_end, session_end) in UTC ms for local date d. DST gaps are refused, not shifted."""
        a = local_to_ms(d, self.start_min, self.tz)
        return a, a + self.minutes * 60_000, local_to_ms(d, self.end_min, self.tz)


@dataclass(frozen=True)
class IBSettings:
    version: int
    defs: tuple[IBDef, ...]
    extensions: tuple[Fraction, ...]
    formation_interval: str
    behaviour_interval: str
    failed_window_ms: int
    accept_interval: str
    accept_time_ms: int
    accept_vol_frac: Fraction
    breakout_vol_mult: Fraction
    persist: bool
    calendar: HolidayCalendar
    hash: str

    @classmethod
    def from_config(cls, cfg: Any) -> "IBSettings":
        from ..processing.settings import ProcessingSettings

        user = {k: v for k, v in (cfg.raw.get("initial_balance") or {}).items() if not k.startswith("_")}
        raw: dict[str, Any] = {**DEFAULT_IB, **user}
        for blk in ("acceptance", "filters", "ivb"):
            raw[blk] = {k: v for k, v in {**DEFAULT_IB[blk], **(user.get(blk) or {})}.items() if not k.startswith("_")}
        if raw["ivb"].get("enabled"):
            raise ValueError(
                "initial_balance.ivb.enabled: IVB is disabled until its definition (formula, inputs, window, "
                "interpretation) is signed off - decision D1. It is never replaced by IB or another metric."
            )
        proc = ProcessingSettings.from_config(cfg)
        if proc.footprint_bin_ticks != 1:
            raise ValueError("initial_balance needs processing.footprint_bin_ticks = 1 (exact volume beyond a level)")
        for k in ("formation_interval", "behaviour_interval"):
            if raw[k] not in proc.interval_ms:
                raise ValueError(f"initial_balance.{k} {raw[k]!r} is not in processing.intervals {list(proc.intervals)}")
        acc = raw["acceptance"]
        if acc["close_interval"] not in proc.interval_ms:
            raise ValueError(
                f"initial_balance.acceptance.close_interval {acc['close_interval']!r} is not in processing.intervals"
            )
        defs = []
        for d in raw["definitions"]:
            d = {k: v for k, v in d.items() if not k.startswith("_")}
            wd = tuple(DAYS[x] for x in d.get("weekdays", ["Mon", "Tue", "Wed", "Thu", "Fri"]))
            ib = IBDef(str(d["name"]), str(d["tz"]), _hm(d["start"]), int(d["minutes"]), _hm(d["session_end"]), wd)
            if ib.minutes <= 0 or ib.start_min + ib.minutes >= ib.end_min:
                raise ValueError(f"IB {ib.name}: needs minutes > 0 and start + minutes < session_end (same local day)")
            for iv in (raw["formation_interval"], raw["behaviour_interval"], acc["close_interval"]):
                if (
                    (ib.minutes * 60_000) % proc.interval_ms[iv]
                    or (ib.start_min * 60_000) % proc.interval_ms[iv]
                    or (ib.end_min * 60_000) % proc.interval_ms[iv]
                ):
                    raise ValueError(f"IB {ib.name}: start, length and session end must align to the {iv} bars")
            defs.append(ib)
        if len({d.name for d in defs}) != len(defs) or not defs:
            raise ValueError("initial_balance.definitions: need at least one, with unique names")
        ext = tuple(sorted(_frac(x, "initial_balance.extensions") for x in raw["extensions"]))
        if any(k <= 0 for k in ext) or len(set(ext)) != len(ext):
            raise ValueError("initial_balance.extensions must be distinct and > 0")
        fw = parse_duration(raw["failed_break_window"], "initial_balance.failed_break_window")
        if fw % proc.interval_ms[raw["behaviour_interval"]]:
            raise ValueError("failed_break_window must be a multiple of behaviour_interval")
        at = parse_duration(acc["accept_time"], "initial_balance.acceptance.accept_time")
        if at % proc.interval_ms[acc["close_interval"]]:
            raise ValueError("acceptance.accept_time must be a multiple of acceptance.close_interval")
        avf = _frac(acc["accept_vol_frac"], "acceptance.accept_vol_frac")
        if not (0 < avf <= 1):
            raise ValueError("acceptance.accept_vol_frac must be in (0, 1]")
        vm = _frac(raw["filters"]["breakout_vol_mult"], "filters.breakout_vol_mult")
        if vm <= 0:
            raise ValueError("filters.breakout_vol_mult must be > 0")
        cal_raw = {k: v for k, v in (cfg.raw.get("calendar") or {}).items() if not k.startswith("_")}
        cal = HolidayCalendar(cal_raw)
        body = {"initial_balance": raw, "calendar": cal_raw, "processing_hash": proc.hash}
        h = hashlib.sha256(json.dumps(body, sort_keys=True, default=str).encode()).hexdigest()
        return cls(
            int(raw["version"]),
            tuple(defs),
            ext,
            str(raw["formation_interval"]),
            str(raw["behaviour_interval"]),
            fw,
            str(acc["close_interval"]),
            at,
            avf,
            vm,
            bool(raw["persist"]),
            cal,
            h,
        )
