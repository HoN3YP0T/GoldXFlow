"""Phase 15: cross-market series. They are research outputs, computed **from** separate venue datasets, and never fed
back into any venue's analytics. No book, profile or bar is ever merged across venues.

**Basis** (``basis``). For a venue pair (A, B), on a grid every ``grid_ms`` (default 1 s):
- price_X(g) is the last trade price of venue X with trade time **< g**, if that trade is at most ``max_age_ms`` old
  (default 5 s); otherwise no value;
- basis(g) = price_A(g) − price_B(g), exact, in a common price unit (the largest unit that divides both tick sizes,
  0.01 USDT for 0.01 and 0.1).

A grid point has a value only when both venues have a fresh price. Its availability time is g (no look-ahead).
Hourly and whole-range statistics come with the coverage share (grid points with a value / all grid points). Bid/ask
spreads would need quote history, which Binance does not publish for XAUUSDT.

**Cross-venue volume** (``volume_oz``). Per minute and per venue, the executed quantity in troy ounces (venue lots ×
``oz_per_lot``), each with that venue's data-quality status for the minute. The total is labelled
``CROSS_VENUE_AGGREGATE``, and ``all_complete`` says whether every venue's data was proven complete for that minute.
"""

from __future__ import annotations

import json
import sqlite3
from fractions import Fraction
from math import gcd
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..normalize import part_path
from ..timeutil import DAY_MS, iso
from .mapping import basis_settings, mappings, venue_config

AGG_LABEL = ("CROSS_VENUE_AGGREGATE v1: sum over venues of exchange-reported executed quantity converted to troy ounces "
             "(venue lots x oz_per_lot). Venues are different markets; the total is a research aggregate, not a market.")  # fmt: skip


def _unit(*ticks: Fraction) -> Fraction:
    """Largest unit dividing every tick size (e.g. 0.01 for 0.01 and 0.1)."""
    den = 1
    for t in ticks:
        den = den * t.denominator // gcd(den, t.denominator)
    num = 0
    for t in ticks:
        num = gcd(num, t.numerator * (den // t.denominator))
    return Fraction(num, den)


def _dec(f: Fraction) -> str:
    """Exact decimal text of a unit such as 1/100 -> "0.01" (units here are decimal fractions)."""
    from decimal import Decimal

    return str(Decimal(f.numerator) / Decimal(f.denominator))


def _trades(cfg: Any, provider: str, day: str, cols: list[str]) -> pa.Table | None:
    c = venue_config(cfg, provider)
    p = part_path(c.data_root, c.symbol, "trades", day, provider=provider)
    return pq.read_table(p, columns=cols) if p.exists() else None


def _status_by_minute(cfg: Any, provider: str, day: str) -> np.ndarray:
    """Per-minute DQ status of a venue's trades (COMPLETE / PARTIAL / UNVERIFIED / MISSING) from its catalog."""
    c = venue_config(cfg, provider)
    st = np.full(1440, "MISSING", dtype=object)
    if not Path(c.state_db).exists():
        return st
    db = sqlite3.connect(str(c.state_db))
    try:
        r = db.execute("SELECT status FROM day_status WHERE dataset='trades' AND date=?", (day,)).fetchone()
        if r is None:
            return st
        st[:] = r[0] if r[0] in ("COMPLETE", "UNVERIFIED", "MISSING") else "COMPLETE"
        for h, s in db.execute(
            "SELECT hour_utc, status FROM hour_status WHERE dataset='trades' AND hour_utc LIKE ?", (f"{day}T%",)
        ):
            if s != "COMPLETE":
                k = int(h[11:13]) * 60
                st[k : k + 60] = s
        if (
            r[0] == "PARTIAL"
            and not db.execute("SELECT 1 FROM hour_status WHERE dataset='trades' AND hour_utc LIKE ?", (f"{day}T%",)).fetchone()
        ):
            st[:] = "PARTIAL"
    except sqlite3.OperationalError:
        return st
    finally:
        db.close()
    return st


def _asof(t: np.ndarray, v: np.ndarray, grid: np.ndarray, max_age: int) -> tuple[np.ndarray, np.ndarray]:
    i = np.searchsorted(t, grid, "left") - 1
    ok = i >= 0
    age = np.where(ok, grid - t[np.maximum(i, 0)], -1)
    ok &= age <= max_age
    return np.where(ok, v[np.maximum(i, 0)], 0), np.where(ok, age, -1)


def basis(cfg: Any, a: str, b: str, days: list[str], write: bool = True) -> dict[str, Any]:
    bs = basis_settings(cfg)
    g, max_age = bs["grid_ms"], bs["max_age_ms"]
    mp = mappings(cfg)
    unit = _unit(mp[a].tick, mp[b].tick)
    mult = {x: int(mp[x].tick / unit) for x in (a, b)}
    hourly, allv, total_pts = [], [], 0
    out_root = Path(cfg.data_root) / "derived" / "cross_venue" / "basis" / f"pair={a}-{b}"
    for day in days:
        d0 = int(np.datetime64(day, "ms").astype(np.int64))
        grid = np.arange(d0 + g, d0 + DAY_MS + 1, g, dtype=np.int64)
        px, age = {}, {}
        for x in (a, b):
            parts = [t for t in (_trades(cfg, x, str(np.datetime64(day) - np.timedelta64(1, "D")), ["exchange_event_time_utc", "price_ticks"]),
                                 _trades(cfg, x, day, ["exchange_event_time_utc", "price_ticks"])) if t is not None]  # fmt: skip
            if not parts:
                px[x], age[x] = np.zeros(len(grid), np.int64), np.full(len(grid), -1)
                continue
            t = pa.concat_tables(parts)
            ts = t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy()
            pt = t.column("price_ticks").to_numpy() * mult[x]
            px[x], age[x] = _asof(ts, pt, grid, max_age)
        ok = (age[a] >= 0) & (age[b] >= 0)
        bas = px[a] - px[b]
        total_pts += len(grid)
        tbl = pa.table({"grid_time": pa.array(grid[ok], pa.timestamp("ms", tz="UTC")), "price_a_units": px[a][ok], "price_b_units": px[b][ok],
                        "basis_units": bas[ok], "age_a_ms": age[a][ok], "age_b_ms": age[b][ok]})  # fmt: skip
        tbl = tbl.replace_schema_metadata({"goldflow.schema": "CrossVenueBasis", "goldflow.pair": f"{a}-{b}", "goldflow.unit_usdt": _dec(unit),
                                           "goldflow.doc": json.dumps({"basis_units": f"price_{a} - price_{b} in units of {_dec(unit)} USDT",
                                                                       "grid_time": "availability time; prices are last trades strictly before it",
                                                                       "max_age_ms": max_age})})  # fmt: skip
        if write:
            (out_root / f"date={day}").mkdir(parents=True, exist_ok=True)
            pq.write_table(tbl, out_root / f"date={day}" / "basis.parquet")
        for h in range(24):
            hm = ok & (grid > d0 + h * 3_600_000) & (grid <= d0 + (h + 1) * 3_600_000)
            v = bas[hm] * float(unit)
            hourly.append({"hour": iso(d0 + h * 3_600_000), "points": int(hm.sum()), "coverage": float(hm.sum()) / (3_600_000 // g),
                           **(_stats(v) if len(v) else {})})  # fmt: skip
        allv.append(bas[ok])
    v = np.concatenate(allv) * float(unit) if allv else np.zeros(0)
    res = {"pair": f"{a}-{b}", "unit_usdt": _dec(unit), "grid_ms": g, "max_age_ms": max_age, "days": days, "points": int(len(v)),
           "coverage": len(v) / total_pts if total_pts else 0.0, **(_stats(v) if len(v) else {}), "hourly": hourly}  # fmt: skip
    if write:
        out_root.mkdir(parents=True, exist_ok=True)
        (out_root / "summary.json").write_text(json.dumps(res, indent=1))
    return res


def _stats(v: np.ndarray) -> dict[str, float]:
    q = np.quantile(v, [0.05, 0.5, 0.95])
    return {"mean": float(v.mean()), "std": float(v.std()), "p05": float(q[0]), "median": float(q[1]), "p95": float(q[2]),
            "min": float(v.min()), "max": float(v.max())}  # fmt: skip


def volume_oz(cfg: Any, venues: list[str], days: list[str], write: bool = True) -> dict[str, Any]:
    mp = mappings(cfg)
    unit = _unit(*(mp[x].oz_per_lot for x in venues))
    mult = {x: int(mp[x].oz_per_lot / unit) for x in venues}
    out_root = Path(cfg.data_root) / "derived" / "cross_venue" / "volume_oz" / "interval=1m"
    totals = {x: 0 for x in venues}
    tot_all, mins_complete = 0, 0
    for day in days:
        d0 = int(np.datetime64(day, "ms").astype(np.int64))
        cols: dict[str, Any] = {"minute": pa.array(d0 + np.arange(1440, dtype=np.int64) * 60_000, pa.timestamp("ms", tz="UTC"))}
        tot = np.zeros(1440, np.int64)
        present = np.zeros(1440, np.int64)
        complete = np.ones(1440, bool)
        for x in venues:
            t = _trades(cfg, x, day, ["exchange_event_time_utc", "quantity_lots"])
            v = np.zeros(1440, np.int64)
            if t is not None:
                mi = (t.column("exchange_event_time_utc").cast(pa.int64()).to_numpy() - d0) // 60_000
                v = np.bincount(mi, weights=t.column("quantity_lots").to_numpy() * mult[x], minlength=1440).astype(np.int64)
            st = _status_by_minute(cfg, x, day)
            cols[f"{x}_oz_units"], cols[f"{x}_status"] = v, pa.array(st.astype(str))
            tot += v
            present += st != "MISSING"
            complete &= st == "COMPLETE"
            totals[x] += int(v.sum())
        cols["total_oz_units"], cols["venues_present"], cols["all_complete"] = tot, present, complete
        tbl = pa.table(cols).replace_schema_metadata({"goldflow.schema": "CrossVenueVolume", "goldflow.label": AGG_LABEL,
                                                      "goldflow.unit_oz": _dec(unit), "goldflow.venues": ",".join(venues)})  # fmt: skip
        if write:
            (out_root / f"date={day}").mkdir(parents=True, exist_ok=True)
            pq.write_table(tbl, out_root / f"date={day}" / "volume_oz.parquet")
        tot_all += int(tot.sum())
        mins_complete += int(complete.sum())
    oz = {x: float(v * unit) for x, v in totals.items()}
    total_oz = float(tot_all * unit)
    return {"label": AGG_LABEL, "unit_oz": _dec(unit), "days": days, "venues": venues, "oz_by_venue": oz, "total_oz": total_oz,
            "share_by_venue": {x: (v / total_oz if total_oz else None) for x, v in oz.items()},
            "minutes": 1440 * len(days), "minutes_all_complete": mins_complete}  # fmt: skip
