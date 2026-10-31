"""Phase 10: independent verification of the Initial Balance dataset (``ib-verify``).

For every finished IB instance with FULL coverage:
1. **IB extremes from raw trades.** IB high/low/volume/buy/sell over [ib_start, ib_end) recomputed straight from the
   Phase 3 trades partitions (no replay, no processor, no bars) must equal IB_FORMED.
2. **Full rebuild.** Formation, acceptance and behaviour bars (with 1-tick levels) are rebuilt from the trades
   partitions, fed to a fresh tracker, and every developing row, event and the session summary must be identical
   to the stored ones.
Also reported:
* the UTC times of each IB start, so session boundaries can be checked across the DST weeks;
* the event counts per definition: breakouts, failed breaks, held breakouts, acceptances and extensions.
Writes data/reports/ib_verification.{json,md}.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..normalize import part_path
from ..processing.settings import ProcessingSettings
from ..timeutil import DAY_MS, iso
from .engine import Bar, drive
from .settings import IBSettings
from .store import read_manifest, read_table
from .tracker import IBTracker

DROP = ("dq_status", "coverage", "available_at")
RECOVERED = "RECOVERED_FROM_AGGTRADES"


class TradeSource:
    """Raw trades from the Phase 3 partitions, in partition order (= replay delivery order), with a small day cache."""

    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg
        st = ProcessingSettings.from_config(cfg)
        self.time_col, self.include_recovered = st.trade_time_col, st.include_recovered
        self.cache: dict[int, dict[str, Any]] = {}

    def day(self, dn: int) -> dict[str, Any]:
        if dn not in self.cache:
            from datetime import date

            d = date.fromordinal(date(1970, 1, 1).toordinal() + dn)
            p = part_path(self.cfg.data_root, self.cfg.symbol, "trades", str(d))
            if not p.exists():
                self.cache[dn] = {k: np.zeros(0, np.int64) for k in ("t", "p", "q", "side")}
            else:
                t = pq.read_table(p, columns=[self.time_col, "price_ticks", "quantity_lots", "buyer_maker_flag", "record_origin"])
                if not self.include_recovered:
                    t = t.filter(pc.invert(pc.fill_null(pc.equal(t.column("record_origin").cast(pa.string()), RECOVERED), False)))
                bm = t.column("buyer_maker_flag")
                side = np.where(
                    pc.is_null(bm).to_numpy(zero_copy_only=False),
                    0,
                    np.where(pc.fill_null(bm, False).to_numpy(zero_copy_only=False), -1, 1),
                ).astype(np.int8)
                self.cache[dn] = {
                    "t": t.column(self.time_col).cast(pa.int64()).to_numpy(),
                    "p": t.column("price_ticks").to_numpy().astype(np.int64),
                    "q": t.column("quantity_lots").to_numpy().astype(np.int64),
                    "side": side,
                }
            if len(self.cache) > 4:
                self.cache.pop(next(iter(self.cache)))
        return self.cache[dn]

    def between(self, start_ms: int, end_ms: int) -> dict[str, Any]:
        parts = []
        for dn in range(start_ms // DAY_MS, (end_ms - 1) // DAY_MS + 1):
            a = self.day(dn)
            m = (a["t"] >= start_ms) & (a["t"] < end_ms)
            parts.append({k: v[m] for k, v in a.items()})
        return {k: np.concatenate([x[k] for x in parts]) for k in parts[0]}


def _rows(t: Any) -> list[dict[str, Any]]:
    import pyarrow as pa

    cols = [c.cast(pa.int64()) if pa.types.is_timestamp(c.type) else c for c in t.columns]
    out: list[dict[str, Any]] = pa.Table.from_arrays(cols, names=t.column_names).to_pylist()
    return out


def bars_from_trades(a: dict[str, Any], start: int, end: int, interval_ms: int, with_levels: bool) -> list[Bar]:
    """Bars from raw trades in partition order (= replay delivery order): open = first, close = last trade."""
    m = (a["t"] >= start) & (a["t"] < end)
    t, p, q, sd = a["t"][m], a["p"][m], a["q"][m], a["side"][m]
    if not len(t):
        return []
    bk = t - t % interval_ms
    out: list[Bar] = []
    cut = np.flatnonzero(np.diff(bk)) + 1
    for i, j in zip(np.concatenate(([0], cut)), np.concatenate((cut, [len(t)]))):
        pp, qq, ss = p[i:j], q[i:j], sd[i:j]
        b: Bar = {
            "bar_start": int(bk[i]),
            "bar_end": int(bk[i]) + interval_ms,
            "open": int(pp[0]),
            "high": int(pp.max()),
            "low": int(pp.min()),
            "close": int(pp[-1]),
            "volume": int(qq.sum()),
            "buy": int(qq[ss == 1].sum()),
            "sell": int(qq[ss == -1].sum()),
        }
        if with_levels:
            u, inv = np.unique(pp, return_inverse=True)
            lots = np.zeros(len(u), np.int64)
            np.add.at(lots, inv, qq)
            b["lv"], b["lots"] = u.tolist(), lots.tolist()
        out.append(b)
    if (np.diff(bk) < 0).any():
        raise ValueError("trades are not in time order within the partition")
    return out


def _norm(r: dict[str, Any]) -> dict[str, Any]:
    """Comparable form: engine-added labels dropped, nulls dropped, numbers as float (Parquet stores some as float64)."""
    return {
        k: (float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v)
        for k, v in r.items()
        if k not in DROP and v is not None
    }


def verify(cfg: Any, report: bool = True) -> dict[str, Any]:
    st = IBSettings.from_config(cfg)
    pst = ProcessingSettings.from_config(cfg)
    man = read_manifest(cfg.data_root, cfg.symbol)
    if not man:
        raise FileNotFoundError("no IB dataset: run `ib` first")
    if man.get("ib_settings_hash") != st.hash:
        raise ValueError("IB dataset was built with different initial_balance/calendar settings than the current config")
    stored: dict[tuple[str, str], list[dict[str, Any]]] = {}
    sessions: list[dict[str, Any]] = []
    for d in sorted(man["dates"]):
        for tbl in ("developing", "events", "sessions"):
            for r in _rows(read_table(cfg.data_root, cfg.symbol, tbl, d)):
                stored.setdefault((r["ib"], r["label"]), []).append(r)
                if tbl == "sessions":
                    sessions.append(r)
    src = TradeSource(cfg)
    iv = pst.interval_ms
    fm, am, bm = iv[st.formation_interval], iv[st.accept_interval], iv[st.behaviour_interval]
    same = st.accept_interval == st.formation_interval
    checked = 0
    mism: list[dict[str, Any]] = []
    skipped: Counter[str] = Counter()
    counts: dict[str, Counter[str]] = {}
    starts: list[dict[str, Any]] = []
    for s in sessions:
        key = (s["ib"], s["label"])
        starts.append({"ib": s["ib"], "label": s["label"], "ib_start_utc": iso(s["ib_start"]), "ib_end_utc": iso(s["ib_end"])})
        c = counts.setdefault(s["ib"], Counter())
        for r in stored[key]:
            c[r["event"]] += 1
        if s["coverage"] != "FULL":
            skipped[s["coverage"]] += 1
            continue
        a = src.between(s["ib_start"], s["session_end"])
        # 1. IB extremes straight from trades
        m = a["t"] < s["ib_end"]
        formed = [r for r in stored[key] if r["event"] in ("IB_FORMED", "IB_EMPTY")]
        p, q, sd = a["p"][m], a["q"][m], a["side"][m]
        direct = (
            {
                "ib_high_ticks": int(p.max()),
                "ib_low_ticks": int(p.min()),
                "volume_lots": int(q.sum()),
                "buy_lots": int(q[sd == 1].sum()),
                "sell_lots": int(q[sd == -1].sum()),
            }
            if len(p)
            else {"volume_lots": 0}
        )
        got = {k: formed[0].get(k) for k in direct} if formed else {}
        if got != direct:
            mism.append({"ib": key[0], "label": key[1], "check": "ib_extremes_vs_trades", "stored": got, "trades": direct})
        # 2. full rebuild through a fresh tracker
        labels = {k: s[k] for k in ("holiday", "holiday_status", "gold_closed", "weekend")}
        tr = IBTracker(s["ib"], s["label"], s["ib_start"], s["ib_end"], s["session_end"], st, bm, labels)
        form = bars_from_trades(a, s["ib_start"], s["session_end"], fm, same)
        acc = None if same else bars_from_trades(a, s["ib_start"], s["session_end"], am, True)
        beh = bars_from_trades(a, s["ib_start"], s["session_end"], bm, False)
        rows = drive([tr], form, acc, beh, s["session_end"], s["session_end"], same)
        mine = sorted((json.dumps(_norm(r), sort_keys=True, default=str) for r in rows))
        theirs = sorted((json.dumps(_norm(r), sort_keys=True, default=str) for r in stored[key]))
        checked += 1
        if mine != theirs:
            diff = sorted(set(mine) ^ set(theirs))[:4]
            mism.append(
                {
                    "ib": key[0],
                    "label": key[1],
                    "check": "rebuild",
                    "rows_rebuilt": len(mine),
                    "rows_stored": len(theirs),
                    "first_differences": diff,
                }
            )
    summ = {
        "instances": len(sessions),
        "checked_full_coverage": checked,
        "skipped": dict(skipped),
        "mismatches": len(mism),
        "pass": not mism,
        "ib_settings_hash": st.hash[:16],
        "event_counts": {k: dict(sorted(v.items())) for k, v in sorted(counts.items())},
    }
    rep = {"summary": summ, "mismatches": mism, "ib_starts": starts}
    if report:
        out = Path(cfg.data_root) / "reports"
        out.mkdir(parents=True, exist_ok=True)
        (out / "ib_verification.json").write_text(json.dumps(rep, indent=1, default=str))
        (out / "ib_verification.md").write_text(_md(rep))
    return rep


def _md(rep: dict[str, Any]) -> str:
    s = rep["summary"]
    lines = [
        "# Phase 10 Initial Balance verification",
        "",
        f"Instances: {s['instances']}; rebuilt from raw trades (FULL coverage): {s['checked_full_coverage']}; "
        f"skipped: {s['skipped'] or 'none'}; mismatches: **{s['mismatches']}** ({'PASS' if s['pass'] else 'FAIL'})",
        "",
        "Checks per instance: IB high/low/volume/buy/sell straight from the trades partitions, and every developing row, "
        "event and session summary rebuilt from bars recomputed from the trades partitions.",
        "",
        "## Event counts",
        "",
        "| IB | " + " | ".join(sorted({e for v in s["event_counts"].values() for e in v})) + " |",
    ]
    evs = sorted({e for v in s["event_counts"].values() for e in v})
    lines.append("|---|" + "---|" * len(evs))
    for ib, c in s["event_counts"].items():
        lines.append(f"| {ib} | " + " | ".join(str(c.get(e, 0)) for e in evs) + " |")
    lines += [
        "",
        "## IB start times around the 2026 DST changes (UK 25 Oct, US 1 Nov)",
        "",
        "| IB | Local date | IB start (UTC) | IB end (UTC) |",
        "|---|---|---|---|",
    ]
    for x in rep["ib_starts"]:
        if "2026-10-19" <= x["label"] <= "2026-10-30":
            lines.append(f"| {x['ib']} | {x['label']} | {x['ib_start_utc']} | {x['ib_end_utc']} |")
    if rep["mismatches"]:
        lines += ["", "## Mismatches", "", "```", json.dumps(rep["mismatches"][:20], indent=1, default=str), "```"]
    return "\n".join(lines) + "\n"
