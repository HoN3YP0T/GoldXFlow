"""Phase 10: independent verification of the Initial Balance dataset (``ib-verify``).

From the Phase 3 trades partitions, with plain code, compared exactly:
1. **Completeness:** every IB instance of the calendar (not on a CLOSED holiday) that lies fully inside the run and
   has trades has a final row.
2. **Snapshots:** high, low, volume, buy, sell, delta and extension levels of every fully observed developing and final
   row, recomputed over [ib_start, as_of).
3. **Events:** breakouts and failed breaks from rebuilt ``break_interval`` bars, extension first touches from the
   trades (stamped at the end of the base bar of the first trade at or beyond the level), and the session summary.
4. **Timing:** available_at = as_of (or event time) + trade shift.
Writes data/reports/ib_verification.{json,md}.
"""

from __future__ import annotations

import json
from datetime import date
from fractions import Fraction
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from ..processing.holidays import HolidayCalendar
from ..processing.sessions import SessionCalendar, local_date
from ..processing.settings import ProcessingSettings
from ..timeutil import DAY_MS, iso
from .flow_verify import Trades, _ms, _rows
from .initial_balance import IBSettings, IBStore, ib_bounds


def _tab(store: IBStore, t: str, d: str) -> list[dict[str, Any]]:
    return _rows(pq.read_table(store.base / f"date={d}" / f"{t}.parquet"))


def verify(cfg: Any, dates: list[str] | None = None, report: bool = True) -> dict[str, Any]:
    st = IBSettings.from_config(cfg)
    pst = ProcessingSettings.from_config(cfg)
    cal = SessionCalendar.from_config(pst.sessions_cfg)
    hol = HolidayCalendar(cfg.raw.get("holidays"))
    shift, base = pst.trade_shift_ms, pst.base_ms
    store = IBStore(cfg.data_root, cfg.symbol)
    mp = store.base / "_manifest.json"
    man = json.loads(mp.read_text()) if mp.exists() else {}
    all_dates = sorted(d for d, v in man.get("dates", {}).items() if v.get("date_complete"))
    todo = [d for d in all_dates if not dates or d in dates]
    summ: dict[str, Any] = {"dates": len(todo), "pass": 0, "fail": [], "ib_settings_hash": st.hash[:16], "ib_rows_checked": 0,
                            "final_ibs": 0, "events_checked": 0, "missing_ibs": 0, "timing_violations": 0}  # fmt: skip
    if not all_dates:
        return {"summary": summ, "dates": []}
    observe = int(man.get("observe_from") or _ms(all_dates[0]))
    run_end = _ms(all_dates[-1]) + DAY_MS
    tr = Trades(cfg).between(observe, run_end)
    t, p, q, sd = tr["t"], tr["p"], tr["q"], tr["side"]
    ibs = [r for d in all_dates for r in _tab(store, "ib", d)]
    evs = [r for d in all_dates for r in _tab(store, "ib_events", d)]
    probs: dict[str, list[str]] = {}

    def flag(t_ms: int, msg: str) -> None:
        probs.setdefault(str(date.fromordinal(date(1970, 1, 1).toordinal() + (int(t_ms) - 1) // DAY_MS)), []).append(msg)

    def sl(a: int, b: int) -> slice:
        return slice(int(np.searchsorted(t, a, "left")), int(np.searchsorted(t, b, "left")))

    # ---- 1. completeness
    finals = {r["ib_key"]: r for r in ibs if r["is_final"]}
    for day_ord in range(observe // DAY_MS - 1, run_end // DAY_MS + 1):
        day = date.fromordinal(date(1970, 1, 1).toordinal() + day_ord)
        for d in st.defs:
            sdef = next(x for x in cal.sessions if x.name == d.session)
            if day.weekday() not in sdef.weekdays:
                continue
            a, b = ib_bounds(d, day, cal)
            if a < observe or b > run_end or hol.closed(local_date(a, hol.tz)):
                continue
            x = sl(a, b)
            key = f"ib:{d.name}:{day}"
            if x.stop > x.start and key not in finals:
                summ["missing_ibs"] += 1
                flag(b + shift, f"missing final IB {key}")
    # ---- 2. snapshots
    for r in ibs:
        if r["coverage"] != "FULL":
            continue
        x = sl(r["ib_start"], min(r["as_of"], r["ib_end"]))
        if x.stop <= x.start:
            flag(r["available_at"], f"IB row {r['ib_key']} {iso(r['as_of'])} has no trades")
            continue
        h, lo = int(p[x].max()), int(p[x].min())
        buy, sell = int(q[x][sd[x] == 1].sum()), int(q[x][sd[x] == -1].sum())
        rng = h - lo
        exts = json.dumps([[str(k), str(h + k * rng), str(lo - k * rng)] for k in st.extensions])
        exp = {"high_ticks": h, "low_ticks": lo, "range_ticks": rng, "volume_lots": int(q[x].sum()), "buy_lots": buy, "sell_lots": sell,
               "delta_lots": buy - sell, "mid_ticks": float(Fraction(h + lo, 2)), "extensions": exts, "available_at": r["as_of"] + shift}  # fmt: skip
        bad = {k: (r[k], v) for k, v in exp.items() if r[k] != v}
        if bad:
            flag(r["available_at"], f"IB row {r['ib_key']} {iso(r['as_of'])}: {bad}")
        summ["ib_rows_checked"] += 1
    # ---- 3. events of fully observed final IBs
    ref: set[tuple[Any, ...]] = set()
    for key, f in finals.items():
        if f["coverage"] != "FULL":
            continue
        summ["final_ibs"] += 1
        h, lo, rng, vol = f["high_ticks"], f["low_ticks"], f["range_ticks"], f["volume_lots"]
        e0, e1 = f["ib_end"], min(f["session_end"], run_end)
        x = sl(e0, e1)
        pt, pp = t[x], p[x]
        if rng > 0:
            for k in st.extensions:
                for side, lev in (("UP", h + k * rng), ("DOWN", lo - k * rng)):
                    hit = (
                        np.flatnonzero(pp * lev.denominator >= lev.numerator)
                        if side == "UP"
                        else np.flatnonzero(pp * lev.denominator <= lev.numerator)
                    )
                    if len(hit):
                        b0 = int(pt[hit[0]]) - int(pt[hit[0]]) % base
                        m = (pt >= b0) & (pt < b0 + base)
                        px = int(pp[m].max()) if side == "UP" else int(pp[m].min())
                        ref.add((f"EXTENSION_{side}", key, str(k), b0 + base, px))
        n_bars = (f["ib_end"] - f["ib_start"]) // st.break_ms
        state: dict[int, int | None] = {1: None, -1: None}
        counts = {"bu": 0, "bd": 0, "fu": 0, "fd": 0}
        for s0 in range(e0, e1, st.break_ms):
            y = sl(s0, s0 + st.break_ms)
            if y.stop <= y.start:
                continue
            c = int(p[y][-1])
            bv = int(q[y].sum())
            bd = int(q[y][sd[y] == 1].sum()) - int(q[y][sd[y] == -1].sum())
            bend = s0 + st.break_ms
            for sgn, nm, beyond in ((1, "UP", c > h), (-1, "DOWN", c < lo)):
                since = state[sgn]
                if since is None and beyond:
                    state[sgn] = bend
                    counts["bu" if sgn > 0 else "bd"] += 1
                    ref.add((f"BREAKOUT_{nm}", key, None, bend, c, bv * n_bars >= vol, bd * sgn > 0))
                elif since is not None and not beyond:
                    if bend - since <= st.failed_ms:
                        counts["fu" if sgn > 0 else "fd"] += 1
                        ref.add((f"FAILED_BREAK_{nm}", key, None, bend, c))
                    state[sgn] = None
        if f["session_end"] <= run_end:
            up = None if rng <= 0 or not len(pp) else float(Fraction(max(0, int(pp.max()) - h), rng))
            dn = None if rng <= 0 or not len(pp) else float(Fraction(max(0, lo - int(pp.min())), rng))
            ref.add(
                ("SESSION_SUMMARY", key, None, f["session_end"], up, dn, counts["bu"], counts["bd"], counts["fu"], counts["fd"])
            )
    got: set[tuple[Any, ...]] = set()
    for ev in evs:
        if ev["coverage"] != "FULL":
            continue
        summ["events_checked"] += 1
        if ev["available_at"] != ev["event_time"] + shift:
            summ["timing_violations"] += 1
            flag(ev["available_at"], f"timing {ev['event_id']}")
        et = ev["event_type"]
        if et.startswith("EXTENSION"):
            got.add((et, ev["ib_key"], ev["k"], ev["event_time"], ev["price_ticks"]))
        elif et.startswith("BREAKOUT"):
            got.add((et, ev["ib_key"], None, ev["event_time"], ev["price_ticks"], ev["volume_filter"], ev["delta_filter"]))
        elif et.startswith("FAILED"):
            got.add((et, ev["ib_key"], None, ev["event_time"], ev["price_ticks"]))
        else:
            dd = json.loads(ev["detail"])
            got.add((et, ev["ib_key"], None, ev["event_time"], dd["max_extension_up_ranges"], dd["max_extension_down_ranges"],
                     dd["breakouts_up"], dd["breakouts_down"], dd["failed_up"], dd["failed_down"]))  # fmt: skip
    for item in sorted(ref - got, key=str):
        flag(item[3] + shift, f"missing {item}")
    for item in sorted(got - ref, key=str):
        flag(item[3] + shift, f"unexpected {item}")
    per = []
    for d in todo:
        ok = not probs.get(d)
        per.append({"date": d, "ok": ok, "problems": probs.get(d, [])[:8]})
        if ok:
            summ["pass"] += 1
        else:
            summ["fail"].append(d)
    out: dict[str, Any] = {"summary": summ, "dates": per, "events_by_type": _counts(evs)}
    if report:
        dpath = Path(cfg.data_root) / "reports"
        dpath.mkdir(parents=True, exist_ok=True)
        (dpath / "ib_verification.json").write_text(json.dumps(out, indent=1, default=str))
        lines = ["# Initial Balance verification (Phase 10)", "", "| Check | Result |", "|---|---|"]
        lines += [f"| {k} | {v} |" for k, v in summ.items() if k != "fail"]
        lines += [f"| failing dates | {', '.join(summ['fail']) or 'none'} |", "", "| Event | Count |", "|---|---|"]
        lines += [f"| {k} | {v} |" for k, v in out["events_by_type"].items()]
        (dpath / "ib_verification.md").write_text("\n".join(lines) + "\n")
    return out


def _counts(evs: list[dict[str, Any]]) -> dict[str, int]:
    c: dict[str, int] = {}
    for e in evs:
        c[e["event_type"]] = c.get(e["event_type"], 0) + 1
    return dict(sorted(c.items()))
