"""Phase 6: independent verification of the persisted profile dataset (``vprofile-verify``).

For every written date:
1. **Parity (exact).** Every final snapshot, plus developing snapshots sampled every ``sample_hours`` (as_of on a
   multiple of that many hours), is rebuilt from scratch from the trades partitions over [covered_from, as_of)
   (covered_from = window_start unless processing began inside the window).
   Totals, traded range, VWAP sums, POC, VAL, VAH, value-area lots and the full HVN/LVN node list must be equal.
2. **VWAP (independent float).** VWAP from the decimal ``price``/``quantity`` columns in float64 must agree with the
   integer VWAP within ``rel_tol`` (default 1e-9 relative). Sigma is reported the same way.
3. **Phase 5 totals.** The final ``utc_day`` profile equals the Phase 5 footprint manifest totals for that date (when a
   footprint dataset exists) and the trades partition totals.
Snapshots of runs with ``coverage`` FILTERED are skipped (their windows have holes by design).
Writes data/reports/profile_verification.{json,md}.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ..normalize import part_path
from ..processing.footprint_store import read_manifest as read_fp_manifest
from .rebuild import TradeSource, float_vwap, snapshot_from_trades
from .settings import ProfileSettings
from .store import read_manifest, read_table

HOUR_MS = 3_600_000
METRIC_KEYS = (
    "total_lots",
    "buy_lots",
    "sell_lots",
    "low_ticks",
    "high_ticks",
    "n_bins",
    "poc_ticks",
    "poc_lots",
    "val_ticks",
    "vah_ticks",
    "va_lots",
    "va_bins",
    "va_steps",
    "hvn_count",
    "lvn_count",
    "sum_q",
    "sum_qp",
    "sum_qp2",
    "vwap_ticks",
    "sigma_ticks",
)
NODE_KEYS = ("node_type", "price_ticks", "lots", "smoothed_lots", "prominence_lots", "left_ticks", "right_ticks", "width_ticks")


def _rows(t: pa.Table) -> list[dict[str, Any]]:
    """Rows as dicts with timestamps as int ms and decimals as int (cast in Arrow: exact, no float round trip)."""
    cols = []
    for f in t.schema:
        c = t.column(f.name)
        if pa.types.is_timestamp(f.type):
            c = c.cast(pa.int64())
        cols.append(c)
    rows: list[dict[str, Any]] = pa.Table.from_arrays(cols, names=t.column_names).to_pylist()
    for r in rows:
        if r.get("sum_qp2") is not None:
            r["sum_qp2"] = int(r["sum_qp2"])
    return rows


def verify_date(cfg: Any, d: str, src: TradeSource, ps: ProfileSettings, sample_hours: int, rel_tol: float) -> dict[str, Any]:
    snaps = _rows(read_table(cfg.data_root, cfg.symbol, "snapshots", d))
    nodes = _rows(read_table(cfg.data_root, cfg.symbol, "nodes", d))
    nd: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for n in nodes:
        nd.setdefault((n["window_key"], n["as_of"], n["bin_ticks"]), []).append(n)
    out: dict[str, Any] = {
        "date": d,
        "snapshots": len(snaps),
        "checked_final": 0,
        "checked_developing": 0,
        "skipped_filtered": 0,
        "parity_mismatches": [],
        "vwap_max_rel_err": 0.0,
        "sigma_max_rel_err": 0.0,
        "vwap_fail": [],
        "phase5": None,
    }
    sample = sample_hours * HOUR_MS
    cache: dict[tuple[int, int], dict[int, dict[str, Any]]] = {}
    fcache: dict[tuple[int, int], tuple[float | None, float | None]] = {}
    tick = float(cfg["instrument"]["tick_size"])
    for s in snaps:
        if not s["is_final"] and s["as_of"] % sample:
            continue
        if s["coverage"] == "FILTERED":
            out["skipped_filtered"] += 1
            continue
        k = (s["covered_from"], s["as_of"])
        if k not in cache:
            cache[k] = snapshot_from_trades(cfg, src, *k, ps=ps)
        ref = cache[k][s["bin_ticks"]]
        out["checked_final" if s["is_final"] else "checked_developing"] += 1
        diffs = [x for x in METRIC_KEYS if ref["metrics"].get(x) != s.get(x)]
        mine = sorted(
            (tuple(n[x] for x in NODE_KEYS) for n in nd.get((s["window_key"], s["as_of"], s["bin_ticks"]), [])), key=str
        )
        theirs = sorted((tuple(n[x] for x in NODE_KEYS) for n in ref["nodes"]), key=str)
        if mine != theirs:
            diffs.append("nodes")
        if diffs:
            out["parity_mismatches"].append(
                {"window_key": s["window_key"], "as_of": s["as_of"], "bin_ticks": s["bin_ticks"], "fields": diffs}
            )
        if k not in fcache:
            fcache[k] = float_vwap(src, *k)
        fv, fs = fcache[k]
        if fv is not None and s["vwap_ticks"] is not None:
            e = abs(s["vwap_ticks"] * tick - fv) / abs(fv)
            out["vwap_max_rel_err"] = max(out["vwap_max_rel_err"], e)
            if e > rel_tol:
                out["vwap_fail"].append({"window_key": s["window_key"], "as_of": s["as_of"], "rel_err": e})
            if fs:
                out["sigma_max_rel_err"] = max(out["sigma_max_rel_err"], abs(s["sigma_ticks"] * tick - fs) / fs)
        elif (fv is None) != (s["vwap_ticks"] is None):
            out["vwap_fail"].append({"window_key": s["window_key"], "as_of": s["as_of"], "rel_err": None})
    # Phase 5 totals for the UTC day
    day = [s for s in snaps if s["kind"] == "utc_day" and s["is_final"] and s["label"] == d]
    if day:
        s0 = day[0]
        p5: dict[str, Any] = {"coverage": s0["coverage"], "profile_total_lots": s0["total_lots"]}
        fp = read_fp_manifest(cfg.data_root, cfg.symbol, "1m").get("dates", {}).get(d)
        if fp is not None:
            p5["footprint_volume_lots"] = fp["volume_lots"]
            p5["footprint_equal"] = (
                fp["volume_lots"] == s0["total_lots"] and fp["buy_lots"] == s0["buy_lots"] and fp["sell_lots"] == s0["sell_lots"]
            )
        pp = part_path(cfg.data_root, cfg.symbol, "trades", d)
        if pp.exists():
            q = pq.read_table(pp, columns=["quantity_lots"]).column("quantity_lots").to_numpy()
            p5["partition_volume_lots"] = int(q.sum())
            p5["partition_equal"] = int(q.sum()) == s0["total_lots"] if s0["coverage"] == "FULL" else None
        out["phase5"] = p5
    out["pass"] = (
        not out["parity_mismatches"]
        and not out["vwap_fail"]
        and (
            out["phase5"] is None
            or (
                out["phase5"].get("footprint_equal", True) is not False
                and out["phase5"].get("partition_equal", True) is not False
            )
        )
    )
    return out


def verify(
    cfg: Any, dates: list[str] | None = None, sample_hours: int = 6, rel_tol: float = 1e-9, report: bool = True
) -> dict[str, Any]:
    ps = ProfileSettings.from_config(cfg)
    man = read_manifest(cfg.data_root, cfg.symbol)
    if not man:
        raise FileNotFoundError("no profile dataset: run `vprofile` first")
    if man.get("profile_settings_hash") != ps.hash:
        raise ValueError("profile dataset was built with different volume_profile settings than the current config")
    src = TradeSource(cfg)
    days = [verify_date(cfg, d, src, ps, sample_hours, rel_tol) for d in sorted(man["dates"]) if dates is None or d in dates]
    summ = {
        "dates": len(days),
        "pass": sum(1 for x in days if x["pass"]),
        "fail": [x["date"] for x in days if not x["pass"]],
        "checked_final": sum(x["checked_final"] for x in days),
        "checked_developing": sum(x["checked_developing"] for x in days),
        "parity_mismatches": sum(len(x["parity_mismatches"]) for x in days),
        "vwap_max_rel_err": max((x["vwap_max_rel_err"] for x in days), default=0.0),
        "sigma_max_rel_err": max((x["sigma_max_rel_err"] for x in days), default=0.0),
        "rel_tol": rel_tol,
        "sample_hours": sample_hours,
        "profile_settings_hash": ps.hash[:16],
        "phase5_days_equal_footprint": sum(1 for x in days if (x["phase5"] or {}).get("footprint_equal") is True),
        "phase5_days_equal_partition": sum(1 for x in days if (x["phase5"] or {}).get("partition_equal") is True),
    }
    rep = {"summary": summ, "days": days}
    if report:
        out = Path(cfg.data_root) / "reports"
        out.mkdir(parents=True, exist_ok=True)
        (out / "profile_verification.json").write_text(json.dumps(rep, indent=1, default=str))
        (out / "profile_verification.md").write_text(_md(rep))
    return rep


def _md(rep: dict[str, Any]) -> str:
    s = rep["summary"]
    lines = [
        "# Phase 6 profile verification",
        "",
        f"Dates: {s['dates']} - PASS {s['pass']}, FAIL {len(s['fail'])} {s['fail'] or ''}",
        "",
        "| Check | Result |",
        "|---|---|",
        f"| Final snapshots rebuilt from trades (exact parity) | {s['checked_final']} |",
        f"| Developing snapshots rebuilt (as_of every {s['sample_hours']} h) | {s['checked_developing']} |",
        f"| Parity mismatches (totals, range, VWAP sums, POC/VA, nodes) | {s['parity_mismatches']} |",
        f"| VWAP vs float64 recomputation from decimal price/quantity, max relative error | {s['vwap_max_rel_err']:.3e} (tolerance {s['rel_tol']:.0e}) |",
        f"| Sigma vs float64 recomputation, max relative error | {s['sigma_max_rel_err']:.3e} |",
        f"| UTC-day profile = Phase 5 footprint totals | {s['phase5_days_equal_footprint']} days |",
        f"| UTC-day profile = trades partition total (FULL coverage) | {s['phase5_days_equal_partition']} days |",
        "",
        "| Date | Snapshots | Final checked | Developing checked | Mismatches | VWAP max rel err | Result |",
        "|---|---|---|---|---|---|---|",
    ]
    for x in rep["days"]:
        lines.append(
            f"| {x['date']} | {x['snapshots']} | {x['checked_final']} | {x['checked_developing']} | "
            f"{len(x['parity_mismatches'])} | {x['vwap_max_rel_err']:.2e} | {'PASS' if x['pass'] else 'FAIL'} |"
        )
    return "\n".join(lines) + "\n"
