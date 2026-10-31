"""Phase 11: signal verification (``signals-verify``).

For every stored signal:
1. **Explainable:** the strategy's pure ``build`` applied to the stored evidence list (and the signal's identity:
   strategy, event time, trigger) reproduces the stored row exactly: direction, entry, stop, target, exact R:R, score,
   expiry, data quality and signal id.
2. **Structural rules** (``TradingSignal.check``): evidence never later than the signal, stop and target on the correct
   sides, expiry after the event.
3. **Gating:** no signal before ``observe_from + warmup``; the trigger row is part of the evidence; the evidence is
   data-quality OK.
It also summarises signals and suppressions by strategy, session and direction.
Writes data/reports/signals_verification.{json,md}.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from ..orderflow.flow_verify import _rows
from ..processing.sessions import SessionCalendar
from ..processing.settings import ProcessingSettings
from .engine import SignalStore, build_strategies
from .signal import evidence_from_json


def verify(cfg: Any, report: bool = True) -> dict[str, Any]:
    store = SignalStore(cfg.data_root, cfg.symbol)
    mp = store.base / "_manifest.json"
    man = json.loads(mp.read_text()) if mp.exists() else {}
    dates = sorted(man.get("dates", {}))
    strategies = {s.id: s for s in build_strategies(cfg)}
    cal = SessionCalendar.from_config(ProcessingSettings.from_config(cfg).sessions_cfg)
    sigs = [r for d in dates for r in _rows(pq.read_table(store.base / f"date={d}" / "signals.parquet"))]
    sup = [r for d in dates for r in _rows(pq.read_table(store.base / f"date={d}" / "suppressed.parquet"))]
    observe = int(man.get("observe_from") or 0)
    problems: list[str] = []
    by: dict[str, dict[str, Any]] = {}
    for r in sigs:
        s = strategies.get(r["strategy_id"])
        if s is None or s.version != r["strategy_version"]:
            problems.append(f"{r['signal_id']}: strategy {r['strategy_id']} v{r['strategy_version']} not configured")
            continue
        ev = evidence_from_json(r["evidence"])
        meta = {
            "event_time": r["event_time"],
            "provider": r["provider"],
            "instrument": r["instrument"],
            "trigger_id": r["trigger_id"],
        }
        sig = s.build(ev, meta)
        if sig is None:
            problems.append(f"{r['signal_id']}: build() on the stored evidence gives no signal")
            continue
        again = json.loads(json.dumps(sig.row(r["available_at"]), default=str))
        stored = json.loads(json.dumps(r, default=str))
        diff = {k: (stored.get(k), v) for k, v in again.items() if k != "evidence" and stored.get(k) != v}
        if diff:
            problems.append(f"{r['signal_id']}: re-derived fields differ {diff}")
        problems += [f"{r['signal_id']}: {x}" for x in sig.check()]
        if r["event_time"] < observe + s.warmup_ms:
            problems.append(f"{r['signal_id']}: emitted during warm-up")
        if r["data_quality_status"] != "OK":
            problems.append(f"{r['signal_id']}: emitted with data quality {r['data_quality_status']}")
        if not any(r["trigger_id"] in e.source_ids for e in ev):
            problems.append(f"{r['signal_id']}: trigger {r['trigger_id']} not in its evidence")
        sess = next((i.name for i in cal.instances(r["event_time"], r["event_time"], []) if i.kind == "session"
                     and i.start_ms <= r["event_time"] < i.end_ms), "outside")  # fmt: skip
        x = by.setdefault(
            r["strategy_id"], {"signals": 0, "long": 0, "short": 0, "sum_rr": 0.0, "sum_score": 0.0, "sessions": {}}
        )
        x["signals"] += 1
        x["long" if r["direction"] == "LONG" else "short"] += 1
        x["sum_rr"] += r["rr"]
        x["sum_score"] += r["setup_score"]
        x["sessions"][sess] = x["sessions"].get(sess, 0) + 1
    last: dict[str, dict[str, Any]] = {}
    for r in sorted(sigs, key=lambda r: (r["event_time"], r["signal_id"])):
        k = f"{r['strategy_id']}|{r['direction']}"
        if k in last and r["event_time"] < last[k]["expiry_at"]:
            problems.append(f"{r['signal_id']}: overlaps the still-valid {last[k]['signal_id']} (duplicate setup)")
        last[k] = r
    for x in by.values():
        x["mean_rr"] = x.pop("sum_rr") / x["signals"]
        x["mean_score"] = x.pop("sum_score") / x["signals"]
    reasons: dict[str, int] = {}
    for r in sup:
        k = f"{r['strategy_id']}:{r['reason']}"
        reasons[k] = reasons.get(k, 0) + 1
    out = {"summary": {"signals": len(sigs), "suppressed": len(sup), "rederived_ok": len(sigs) - len({p.split(':')[0] for p in problems}),
                       "problems": len(problems), "fail": bool(problems)},
           "by_strategy": by, "suppressed_by_reason": dict(sorted(reasons.items())), "problems": problems[:50]}  # fmt: skip
    if report:
        d = Path(cfg.data_root) / "reports"
        d.mkdir(parents=True, exist_ok=True)
        (d / "signals_verification.json").write_text(json.dumps(out, indent=1, default=str))
        lines = ["# Signal verification (Phase 11)", "", f"Signals: {len(sigs)}; suppressed: {len(sup)}; problems: {len(problems)}.", "",
                 "| Strategy | Signals | Long | Short | Mean R:R | Mean score | By session |", "|---|---|---|---|---|---|---|"]  # fmt: skip
        for k, x in sorted(by.items()):
            lines.append(f"| {k} | {x['signals']} | {x['long']} | {x['short']} | {x['mean_rr']:.2f} | {x['mean_score']:.2f} | "
                         f"{', '.join(f'{a}: {b}' for a, b in sorted(x['sessions'].items()))} |")  # fmt: skip
        lines += ["", "| Suppressed (strategy:reason) | Count |", "|---|---|"] + [
            f"| {k} | {v} |" for k, v in sorted(reasons.items())
        ]
        if problems:
            lines += ["", "## Problems", ""] + [f"- {p}" for p in problems[:50]]
        (d / "signals_verification.md").write_text("\n".join(lines) + "\n")
    return out
