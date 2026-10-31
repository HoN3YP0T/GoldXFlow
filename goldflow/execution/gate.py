"""Phase 16: the deployment gate (milestone M7). Live trading stays off until this gate passes **and** an operator
approves it.

``exec-gate --name <demo run> --approve-by <operator>`` reads only the demo run's **audit log**: its chain must verify,
and it must show all of the following.

| Criterion | Requirement (PROPOSED) |
|---|---|
| Account | every HELLO reported a ``DEMO`` account |
| Duration | ≥ 14 days between the first and the last fill (the guide asks for 2–4 weeks) |
| Fills | ≥ 20 filled orders |
| Lifecycle paths | at least one each of: partial fill, broker rejection, SL close, TP close, manual or stop-out close (adopted), retry after a missing ACK, EA reconnect, engine restart, emergency stop with flatten |
| Duplicates | no broker position larger than the engine's and no unknown broker position: zero ``RECONCILE_MISMATCH`` alerts left unresolved, and the last reconciliation clean |
| Slippage | mean adverse slippage over every fill ≤ 1.5 × the Phase 12 model's slippage (``backtest.slippage_points``) |
| Authentication | zero refused messages that were not test injections (each one is reviewed) |

If it passes, ``data/state/live_gates.json`` is written with ``{"M7_demo_passed": true, "approved_by", "approved_utc",
"evidence"}``. The live profile then additionally requires ``execution.live_trading_enabled = true`` (Phase 1).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .audit import AuditLog

MIN_DAYS = 14
MIN_FILLS = 20


def evidence(audit_path: Path, model_slippage_points: float | None) -> dict[str, Any]:
    if not Path(audit_path).exists():
        raise FileNotFoundError(f"no audit log at {audit_path}: run the demo with bridge-run --profile demo first")
    log = AuditLog(audit_path)
    n = log.verify()
    log.close()
    fills, modes, slips = [], set(), []
    paths = {k: 0 for k in ("partial_fill", "rejection", "sl_close", "tp_close", "manual_or_stopout_close", "retry_no_ack", "ea_reconnect",
                            "engine_restart", "emergency_flatten")}  # fmt: skip
    unresolved, last_clean, refused = 0, None, 0
    for r in AuditLog(audit_path).records():
        k, d = r["kind"], r["data"]
        if k == "IN":
            m = json.loads(d["line"].split(" ", 1)[1])
            if m["type"] == "HELLO":
                modes.add(m["trade_mode"])
            if m["type"] == "TXN" and m["kind"] == "DEAL":
                if m.get("entry") == "OUT" and m.get("reason") == "SL":
                    paths["sl_close"] += 1
                if m.get("entry") == "OUT" and m.get("reason") == "TP":
                    paths["tp_close"] += 1
                if m.get("entry") == "OUT" and m.get("reason") in ("CLIENT", "SO"):
                    paths["manual_or_stopout_close"] += 1
            if m["type"] == "REJECT" or (
                m["type"] == "TXN" and m["kind"] == "REQUEST" and int(m.get("retcode", 0)) not in (10008, 10009, 10010)
            ):
                paths["rejection"] += 1
        elif k == "ORDER":
            if d.get("state") == "FILLED" and d.get("effect") == "fill":
                fills.append(int(r["at"]))
            if d.get("slippage_points") is not None:
                slips.append(float(d["slippage_points"]))
            if d.get("state") == "PARTIALLY_FILLED":
                paths["partial_fill"] += 1
            if "retry" in d:
                paths["retry_no_ack"] += 1
        elif k == "RECONCILE":
            last_clean = bool(d["clean"])
            for a in d.get("adopted", []):
                if a["kind"] == "CLOSE_ADOPTED":
                    paths["manual_or_stopout_close"] += 1
        elif k == "ALERT":
            if d.get("kind") == "EA_CONNECTED":
                paths["ea_reconnect"] += 1
            if d.get("kind") == "RECONCILE_MISMATCH":
                unresolved += 1
        elif k == "START":
            paths["engine_restart"] += 1
        elif k == "EMERGENCY" and d.get("flattened"):
            paths["emergency_flatten"] += 1
        elif k == "REFUSED":
            refused += 1
    paths["engine_restart"] = max(0, paths["engine_restart"] - 1)  # the first START is not a restart
    if last_clean:
        unresolved = 0  # a later clean reconciliation resolved them (each one stays in the log for review)
    days = (max(fills) - min(fills)) / 86_400_000 if len(fills) >= 2 else 0.0
    failures = []
    if modes != {"DEMO"}:
        failures.append(f"account modes {sorted(modes)} (only DEMO allowed)")
    if days < MIN_DAYS:
        failures.append(f"{days:.1f} days of fills < {MIN_DAYS}")
    if len(fills) < MIN_FILLS:
        failures.append(f"{len(fills)} fills < {MIN_FILLS}")
    failures += [f"lifecycle path never exercised: {p}" for p, c in paths.items() if c == 0]
    if not last_clean:
        failures.append("last reconciliation not clean")
    if unresolved:
        failures.append(f"{unresolved} unresolved reconciliation mismatches")
    if refused:
        failures.append(f"{refused} refused messages to review")
    mean_slip = sum(slips) / len(slips) if slips else None
    if model_slippage_points is not None and mean_slip is not None and mean_slip > 1.5 * model_slippage_points:
        failures.append(f"mean slippage {mean_slip:.1f} points > 1.5 x model {model_slippage_points}")
    return {"records": n, "fills": len(fills), "days": round(days, 2), "account_modes": sorted(modes), "paths": paths,
            "last_reconcile_clean": last_clean, "refused": refused, "slippage_mean_points": mean_slip,
            "model_slippage_points": model_slippage_points, "passed": not failures, "failures": failures}  # fmt: skip


def record_gate(data_root: Path, ev: dict[str, Any], operator: str) -> Path:
    if not ev["passed"]:
        raise PermissionError("M7 demo gate not passed: " + "; ".join(ev["failures"]))
    if not operator.strip():
        raise PermissionError("an operator name is required to approve the gate")
    p = Path(data_root) / "state" / "live_gates.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"M7_demo_passed": True, "approved_by": operator.strip(), "approved_utc": datetime.now(timezone.utc).isoformat(),
                             "evidence": ev}, indent=1))  # fmt: skip
    return p
