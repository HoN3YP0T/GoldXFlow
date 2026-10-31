"""Phase 5: versioned session calendar (named time zones, DST through zoneinfo).

Window kinds produced by the calendar (each instance is a half-open UTC interval [start_ms, end_ms)):
  utc_day        00:00 -> 24:00 UTC, labelled by the UTC date
  trading_day    roll time in ``trading_day.tz`` on the previous local day -> roll time on the label day
                 (default 18:00 America/New_York, the CME/COMEX gold trading-day convention)
  trading_week   start of Monday's trading day -> start of next Monday's, labelled by the Monday date
  session        each configured session on its configured local weekdays, labelled by the local start date

``weekend`` flags time outside the venue's regular trading week (default: Binance tradingSchedule COMMODITY
REGULAR, Sun 22:00 -> Fri 21:00 UTC). The perpetual keeps trading then while gold itself is closed. Strategies
decide what to do with that flow; the processor only labels it.

Session times are decision D4 (still open). The defaults in the config are the build guide's proposals.
"""
from __future__ import annotations
from typing import Any
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import hashlib, json
import numpy as np
from numpy.typing import NDArray

from ..timeutil import DAY_MS

WEEK_MS = 7 * DAY_MS
DAYS = {"Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5, "Sun": 6}


@dataclass(frozen=True)
class Instance:
    kind: str          # utc_day | trading_day | trading_week | session | custom
    name: str          # e.g. "london"; for non-session kinds equal to kind
    label: str         # YYYY-MM-DD
    start_ms: int
    end_ms: int

    @property
    def key(self) -> str: return f"{self.kind}:{self.name}:{self.label}"
    @property
    def series(self) -> str: return f"{self.kind}:{self.name}"


@dataclass(frozen=True)
class SessionDef:
    name: str
    tz: str
    start_min: int
    end_min: int          # if end <= start the session ends on the next local day
    weekdays: tuple[Any, ...]       # local weekdays (Mon=0) on which the session starts


def _hm(s: str) -> int:
    hs, ms = s.split(":"); h, m = int(hs), int(ms)
    if not (0 <= h < 24 and 0 <= m < 60): raise ValueError(f"bad time of day {s!r}")
    return h * 60 + m


def _dow_hm(s: str) -> int:
    d, t = s.split(); return DAYS[d] * 1440 + _hm(t)


def local_to_ms(d: date, minute: int, tz: str) -> int:
    """UTC ms of local wall time ``minute`` on local date ``d`` in ``tz``. Raises if that wall time doesn't exist
    (a DST gap) instead of silently shifting it."""
    z = ZoneInfo(tz)
    dt = datetime(d.year, d.month, d.day, minute // 60, minute % 60, tzinfo=z)
    ms = int(dt.timestamp() * 1000)
    back = datetime.fromtimestamp(ms / 1000, tz=z)
    if (back.hour, back.minute, back.date()) != (dt.hour, dt.minute, d):
        raise ValueError(f"{d} {minute // 60:02d}:{minute % 60:02d} does not exist in {tz} (DST gap)")
    return ms


def local_date(ms: int, tz: str) -> date:
    return datetime.fromtimestamp(ms / 1000, tz=ZoneInfo(tz)).date()


class SessionCalendar:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.version = str(cfg.get("version", "1"))
        td = cfg.get("trading_day", {"tz": "America/New_York", "roll": "18:00", "starts_previous_day": True})
        self.td_tz = td["tz"]; self.td_roll = _hm(td["roll"]); self.td_prev = bool(td.get("starts_previous_day", True))
        ZoneInfo(self.td_tz)
        wk = cfg.get("regular_trading_week_utc", {"open": "Sun 22:00", "close": "Fri 21:00"})
        self.week_open = _dow_hm(wk["open"]) * 60_000; self.week_close = _dow_hm(wk["close"]) * 60_000
        self.sessions = []
        for s in cfg.get("sessions", []):
            ZoneInfo(s["tz"])   # raises on an unknown zone
            wd = tuple(DAYS[x] for x in s.get("weekdays", ["Mon", "Tue", "Wed", "Thu", "Fri"]))
            sd = SessionDef(s["name"], s["tz"], _hm(s["start"]), _hm(s["end"]), wd)
            if sd.start_min == sd.end_min: raise ValueError(f"session {sd.name}: zero length")
            self.sessions.append(sd)
        names = [s.name for s in self.sessions]
        if len(set(names)) != len(names): raise ValueError("duplicate session names")
        self.hash = hashlib.sha256(json.dumps(cfg, sort_keys=True).encode()).hexdigest()
        self._cache: dict[tuple[Any, ...], tuple[int, int]] = {}   # per-instance memo of local -> UTC conversions

    @classmethod
    def from_config(cls, cfg: dict[str, Any] | None) -> "SessionCalendar":
        return cls(cfg or {})

    # ------------------------------------------------------------------ weekend / regular schedule
    def is_weekend(self, t_ms: NDArray[Any]) -> NDArray[Any]:
        """True where t falls outside the regular trading week (between close and the next open, UTC)."""
        t = np.asarray(t_ms, np.int64)
        wk = (t + 3 * DAY_MS) % WEEK_MS          # 1970-01-01 was a Thursday; shift so 0 = Monday 00:00 UTC
        c, o = self.week_close, self.week_open
        return (wk >= c) & (wk < o) if c < o else (wk >= c) | (wk < o)

    # ------------------------------------------------------------------ instances
    def session_bounds(self, name: str, d: date) -> tuple[int, int]:
        k = ("s", name, d)
        if k not in self._cache:
            s = next(x for x in self.sessions if x.name == name)
            a = local_to_ms(d, s.start_min, s.tz)
            b = local_to_ms(d + timedelta(days=1) if s.end_min <= s.start_min else d, s.end_min, s.tz)
            self._cache[k] = (a, b)
        return self._cache[k]

    def trading_day_bounds(self, d: date) -> tuple[int, int]:
        k = ("td", d)
        if k not in self._cache:
            a_day = d - timedelta(days=1) if self.td_prev else d
            self._cache[k] = (local_to_ms(a_day, self.td_roll, self.td_tz), local_to_ms(a_day + timedelta(days=1), self.td_roll, self.td_tz))
        return self._cache[k]

    def instances(self, a_ms: int, b_ms: int, custom: list[Any] | None = None) -> list[Instance]:
        """Every window instance that overlaps [a_ms, b_ms] (inclusive end), sorted by (start, key)."""
        out: list[Instance] = []
        d0 = (datetime.fromtimestamp(a_ms / 1000, tz=timezone.utc) - timedelta(days=2)).date()
        d1 = (datetime.fromtimestamp(b_ms / 1000, tz=timezone.utc) + timedelta(days=2)).date()
        def ov(s: int, e: int) -> bool: return s <= b_ms and e > a_ms
        d = d0
        while d <= d1:
            s = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
            if ov(s, s + DAY_MS): out.append(Instance("utc_day", "utc_day", str(d), s, s + DAY_MS))
            s, e = self.trading_day_bounds(d)
            if ov(s, e): out.append(Instance("trading_day", "trading_day", str(d), s, e))
            if d.weekday() == 0:
                s = self.trading_day_bounds(d)[0]; e = self.trading_day_bounds(d + timedelta(days=7))[0]
                if ov(s, e): out.append(Instance("trading_week", "trading_week", str(d), s, e))
            for sd in self.sessions:
                if d.weekday() in sd.weekdays:
                    s, e = self.session_bounds(sd.name, d)
                    if ov(s, e): out.append(Instance("session", sd.name, str(d), s, e))
            d += timedelta(days=1)
        # trading weeks whose Monday is before d0 but which still overlap
        mon = d0 - timedelta(days=d0.weekday())
        for k in (7, 0):
            m = mon - timedelta(days=k)
            s = self.trading_day_bounds(m)[0]; e = self.trading_day_bounds(m + timedelta(days=7))[0]
            if ov(s, e): out.append(Instance("trading_week", "trading_week", str(m), s, e))
        for c in custom or []:
            if ov(c["start_ms"], c["end_ms"]): out.append(Instance("custom", c["name"], c.get("label", "custom"), c["start_ms"], c["end_ms"]))
        uniq = {i.key: i for i in out}
        return sorted(uniq.values(), key=lambda i: (i.start_ms, i.key))

    def describe(self, d: date) -> list[dict[str, Any]]:
        """Human-readable session table for one local date (used in docs and tests)."""
        from ..timeutil import iso
        rows = []
        for sd in self.sessions:
            a, b = self.session_bounds(sd.name, d)
            rows.append({"session": sd.name, "tz": sd.tz, "date": str(d), "start_utc": iso(a), "end_utc": iso(b)})
        a, b = self.trading_day_bounds(d)
        rows.append({"session": "trading_day", "tz": self.td_tz, "date": str(d), "start_utc": iso(a), "end_utc": iso(b)})
        return rows
