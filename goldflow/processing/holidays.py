"""Phase 10: exchange holiday calendar for gold (CME/COMEX), versioned config, used for **labelling** and to skip
Initial Balance on full closures.

It deliberately does not change the Phase 5 session instances: sessions, windows and every Phase 5–9 digest stay as
they are, and Phase 10 outputs carry a ``holiday`` label instead. The Binance perpetual trades through gold closures;
that flow is flagged (``holiday`` on IB rows, ``weekend`` from Phase 5), never dropped.

Config block ``holidays`` (status PROPOSED until checked against CME Group's published holiday schedule)::

    {"version": "2026-proposed", "tz": "America/New_York",
     "days": [{"date": "2026-05-25", "name": "Memorial Day", "type": "MODIFIED_HOURS"}, ...]}

Types: ``CLOSED`` (no regular gold trading that local date: no IB is built) and ``MODIFIED_HOURS`` (early halt or
irregular hours: IB is built and labelled).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from typing import Any

TYPES = ("CLOSED", "MODIFIED_HOURS")


@dataclass(frozen=True)
class Holiday:
    day: date
    name: str
    type: str


class HolidayCalendar:
    def __init__(self, cfg: dict[str, Any] | None) -> None:
        cfg = cfg or {}
        self.version = str(cfg.get("version", "none"))
        self.tz = str(cfg.get("tz", "America/New_York"))
        self.days: dict[date, Holiday] = {}
        for x in cfg.get("days", []):
            d = date.fromisoformat(str(x["date"]))
            t = str(x.get("type", "MODIFIED_HOURS"))
            if t not in TYPES:
                raise ValueError(f"holidays: {d} type {t!r} must be one of {list(TYPES)}")
            if d in self.days:
                raise ValueError(f"holidays: {d} listed twice")
            self.days[d] = Holiday(d, str(x["name"]), t)
        self.hash = hashlib.sha256(json.dumps(cfg, sort_keys=True, default=str).encode()).hexdigest()

    def get(self, d: date) -> Holiday | None:
        return self.days.get(d)

    def closed(self, d: date) -> bool:
        h = self.days.get(d)
        return h is not None and h.type == "CLOSED"

    def label(self, d: date) -> str | None:
        h = self.days.get(d)
        return None if h is None else f"{h.type}:{h.name}"
