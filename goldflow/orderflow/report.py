"""Phase 7 catalogue report: counts by type, status and session, plus invariant checks.

Written to data/reports/orderflow_catalogue.{json,md}. Outcome rates are descriptive only: they say how often a
labelled pattern was followed by its confirmation rule, not that trading it is profitable (that is Phase 12/13).
"""

from __future__ import annotations

import collections
import json
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

INITIAL = {"DETECTED", "CANDIDATE"}


def summarize_events(ev: pa.Table) -> dict[str, Any]:
    typ = ev.column("event_type").to_pylist()
    st = ev.column("status").to_pylist()
    sess = ev.column("sessions").to_pylist()
    wk = ev.column("weekend").to_pylist()
    by_type_status = collections.Counter(f"{a}|{b}" for a, b in zip(typ, st))
    by_session: dict[str, collections.Counter[str]] = {}
    for a, b, s, w in zip(typ, st, sess, wk):
        if b not in INITIAL:
            continue
        for name in s.split("+") if s != "none" else ["none"]:
            by_session.setdefault(a, collections.Counter())[name] += 1
        by_session.setdefault(a, collections.Counter())["weekend" if w else "weekday"] += 1
    viol = int(pc.sum(pc.cast(pc.less(ev.column("event_time_utc"), ev.column("evidence_end_utc")), pa.int64())).as_py() or 0)
    ids = set(ev.column("event_id").to_pylist())
    par = ev.column("parent_id").to_pylist()
    orphan = sum(1 for p in par if p is not None and p not in ids)
    fid, lid = ev.column("first_trade_id").to_pylist(), ev.column("last_trade_id").to_pylist()
    untraced = sum(1 for s_, f, l_ in zip(st, fid, lid) if s_ in INITIAL and (f is None or l_ is None))
    unk_null = ev.column("unknown_lots").null_count
    bt = [i for i, a in enumerate(typ) if a == "BIG_TRADE"]
    tids, nt = ev.column("trade_ids").to_pylist(), ev.column("n_trades").to_pylist()
    bt_ids_mismatch = sum(1 for i in bt if tids[i] is not None and len(tids[i]) != nt[i])
    bt_ids_truncated = sum(1 for i in bt if tids[i] is None)
    dup_ids = len(typ) - len(ids)
    outcomes: dict[str, dict[str, Any]] = {}
    for t in sorted(set(typ)):
        init = by_type_status.get(f"{t}|CANDIDATE", 0) + by_type_status.get(f"{t}|DETECTED", 0)
        term = {s_: n for k, n in by_type_status.items() for tt, s_ in [k.split("|")] if tt == t and s_ not in INITIAL}
        outcomes[t] = {
            "initial": init,
            **term,
            "confirmed_rate": round(term.get("CONFIRMED", 0) / init, 4) if init and "CONFIRMED" in term else None,
        }
    checks = {
        "event_time_before_evidence_end": viol,
        "orphan_status_events": orphan,
        "initial_events_without_trade_ids": untraced,
        "null_unknown_lots": unk_null,
        "big_trade_id_count_mismatch": bt_ids_mismatch,
        "duplicate_event_ids": dup_ids,
    }
    return {
        "rows": ev.num_rows,
        "by_type_status": dict(sorted(by_type_status.items())),
        "by_session": {k: dict(sorted(v.items())) for k, v in sorted(by_session.items())},
        "outcomes": outcomes,
        "checks": checks,
        "big_trades_with_truncated_ids": bt_ids_truncated,
        "fail": any(v != 0 for v in checks.values()),
    }


def build_report(cfg: Any, manifest: dict[str, Any], events: pa.Table) -> dict[str, Any]:
    s = summarize_events(events)
    rep = {
        "summary": {
            **s,
            "tables": {k: {"rows": v["rows"], "logical_digest": v["logical_digest"]} for k, v in manifest["tables"].items()},
            "settings_hash": manifest["orderflow_settings_hash"],
            "source_dataset_version": manifest["source_dataset_version"],
            "detector_stats": manifest["detector_stats"],
            "open_at_end": manifest["open_at_end"],
        }
    }
    d = cfg.data_root / "reports"
    d.mkdir(parents=True, exist_ok=True)
    (d / "orderflow_catalogue.json").write_text(json.dumps(rep, indent=1, sort_keys=True, default=str))
    L = [
        "# Orderflow event catalogue (Phase 7)",
        "",
        "Descriptive counts of rule-based labels. Not evidence of profitability (Phase 12/13).",
        "",
        f"Settings hash `{s and manifest['orderflow_settings_hash'][:16]}`, source dataset `{str(manifest['source_dataset_version'])[:16]}`.",
        "",
        "## Tables",
        "",
        "| Table | Rows | Logical digest |",
        "|---|---|---|",
    ]
    for k, v in manifest["tables"].items():
        L.append(f"| {k} | {v['rows']:,} | `{v['logical_digest'][:16]}…` |")
    L += ["", "## Events by type and status", "", "| Type | Status | Count |", "|---|---|---|"]
    for k, n in s["by_type_status"].items():
        a, b = k.split("|")
        L.append(f"| {a} | {b} | {n:,} |")
    L += ["", "## Initial events by session (an event in two overlapping sessions counts in both)", ""]
    for t, c in s["by_session"].items():
        L.append(f"- **{t}**: " + ", ".join(f"{k} {v:,}" for k, v in c.items()))
    L += ["", "## Outcomes (descriptive)", "", "| Type | Initial | Terminal statuses | Confirmed rate |", "|---|---|---|---|"]
    for t, o in s["outcomes"].items():
        term = ", ".join(f"{k} {v:,}" for k, v in o.items() if k not in ("initial", "confirmed_rate"))
        L.append(
            f"| {t} | {o['initial']:,} | {term or '-'} | {o['confirmed_rate'] if o['confirmed_rate'] is not None else '-'} |"
        )
    L += ["", "## Invariant checks (all must be 0)", "", "| Check | Value |", "|---|---|"]
    for k, v in s["checks"].items():
        L.append(f"| {k} | {v} |")
    L += ["", f"Result: **{'FAIL' if s['fail'] else 'PASS'}**", ""]
    (d / "orderflow_catalogue.md").write_text("\n".join(L))
    return rep
