"""Phase 10: gold (CME/COMEX) holiday calendar.

Holiday **dates** are computed from their published rules (for example "last Monday of May"; Good Friday from the
Gregorian Easter computation), so nothing is recalled from memory. CME's **trading hours** on those days are a
different matter: they are announced by CME, usually about two weeks before each holiday, and differ by product
and year. They could not be verified from this environment, so every holiday ships with ``cme_status =
UNVERIFIED`` and no halt time. Fill them from CME's holiday notices in the ``calendar.overrides`` config block.

Holidays only **label** outputs (``holiday``, ``holiday_status``, ``gold_closed``); they never change a calculation.
The Binance perpetual keeps trading on every one of these days; strategies decide what to do with that flow.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

STATUSES = ("UNVERIFIED", "CLOSED", "EARLY_HALT", "OPEN")
DEFAULT_RULES = (
    "new_year",
    "mlk_day",
    "presidents_day",
    "good_friday",
    "memorial_day",
    "juneteenth",
    "independence_day",
    "labor_day",
    "thanksgiving",
    "christmas",
)


def easter(year: int) -> date:
    """Gregorian Easter Sunday (anonymous Gregorian algorithm, Meeus/Jones/Butcher)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    m = (32 + 2 * e + 2 * i - h - k) % 7
    n = (a + 11 * h + 22 * m) // 451
    month, day = divmod(h + m - 7 * n + 114, 31)
    return date(year, month, day + 1)


def nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """n-th (1-based) weekday (Mon=0) of a month; n = -1 is the last one."""
    if n > 0:
        d = date(year, month, 1)
        d += timedelta(days=(weekday - d.weekday()) % 7)
        return d + timedelta(weeks=n - 1)
    nxt = date(year + (month == 12), month % 12 + 1, 1)
    d = nxt - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


def observed(d: date) -> date:
    """US federal observance: a Saturday holiday is observed on Friday, a Sunday holiday on Monday."""
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def rule_date(rule: str, year: int) -> date:
    if rule == "new_year":
        return observed(date(year, 1, 1))
    if rule == "mlk_day":
        return nth_weekday(year, 1, 0, 3)
    if rule == "presidents_day":
        return nth_weekday(year, 2, 0, 3)
    if rule == "good_friday":
        return easter(year) - timedelta(days=2)
    if rule == "memorial_day":
        return nth_weekday(year, 5, 0, -1)
    if rule == "juneteenth":
        return observed(date(year, 6, 19))
    if rule == "independence_day":
        return observed(date(year, 7, 4))
    if rule == "labor_day":
        return nth_weekday(year, 9, 0, 1)
    if rule == "thanksgiving":
        return nth_weekday(year, 11, 3, 4)
    if rule == "christmas":
        return observed(date(year, 12, 25))
    raise ValueError(f"unknown holiday rule {rule!r}; known: {list(DEFAULT_RULES)}")


@dataclass(frozen=True)
class Holiday:
    date: date
    name: str
    status: str  # UNVERIFIED | CLOSED | EARLY_HALT | OPEN (CME metals trading on that date)
    halt_local: str | None  # e.g. "13:30" for an early halt, in ``tz``
    tz: str
    source: str


class HolidayCalendar:
    """Versioned holiday calendar from the ``calendar`` config block (rules plus explicit overrides)."""

    def __init__(self, cfg: dict[str, Any] | None) -> None:
        cfg = cfg or {}
        self.version = str(cfg.get("version", "1"))
        self.rules = tuple(cfg.get("rules", DEFAULT_RULES))
        self.years = tuple(int(y) for y in cfg.get("years", [2025, 2026, 2027]))
        self.tz = str(cfg.get("tz", "America/New_York"))
        for r in self.rules:
            rule_date(r, 2026)  # validates the name
        days: dict[date, Holiday] = {}
        for y in self.years:
            for r in self.rules:
                d = rule_date(r, y)
                days[d] = Holiday(d, r, "UNVERIFIED", None, self.tz, "rule-computed date; CME hours not verified")
        for o in cfg.get("overrides", []):
            d = date.fromisoformat(str(o["date"]))
            st = str(o.get("status", "UNVERIFIED"))
            if st not in STATUSES:
                raise ValueError(f"calendar override {d}: status must be one of {STATUSES}")
            if st == "EARLY_HALT" and not o.get("halt_local"):
                raise ValueError(f"calendar override {d}: EARLY_HALT needs halt_local (HH:MM in {o.get('tz', self.tz)})")
            if st != "UNVERIFIED" and not o.get("source"):
                raise ValueError(f"calendar override {d}: a verified status needs a source (e.g. the CME notice)")
            name = str(o.get("name") or (days[d].name if d in days else "custom"))
            days[d] = Holiday(d, name, st, o.get("halt_local"), str(o.get("tz", self.tz)), str(o.get("source", "")))
        self.days = dict(sorted(days.items()))

    def get(self, d: date) -> Holiday | None:
        return self.days.get(d)

    def label(self, d: date) -> dict[str, Any]:
        h = self.days.get(d)
        if h is None:
            return {"holiday": None, "holiday_status": None, "gold_closed": False}
        return {"holiday": h.name, "holiday_status": h.status, "gold_closed": h.status == "CLOSED"}
