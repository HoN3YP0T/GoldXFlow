"""Phase 17: alert rules on the status snapshot, with an open/close lifecycle.

| Alert | Severity | Raised when |
|---|---|---|
| ``STALE_FEED`` | critical | a live feed is marked stale, or the live checkpoint is older than ``stale_checkpoint_s`` (the runner is down) |
| ``DQ_OPEN`` | warning | the live trades feed has an open STALE/GAP/RECOVERY interval, or suspected gaps were recorded |
| ``RECONCILE_MISMATCH`` | critical | an execution run is blocked by ``RECONCILE_MISMATCH`` |
| ``RISK_LIMIT`` | critical | an execution run has its emergency stop latched, or is blocked by a loss/drawdown/streak limit |
| ``BRIDGE_DOWN`` | critical | an execution run is blocked by ``NO_HEARTBEAT``/``NO_EA``, or the EA has been silent for longer than ``bridge_silent_s`` |
| ``AUDIT_BROKEN`` | critical | an execution audit chain or the research ledger fails verification |
| ``DISK_LOW`` | warning / critical | free disk below ``disk_warn_pct`` / ``disk_crit_pct`` |
| ``CATALOG`` | warning | the catalog's integrity check is not ``ok`` |
| ``ERRORS`` | warning | ERROR or CRITICAL log records in the recent log |
| ``STATUS_SECTION_FAILED`` | critical | a status section could not be read |

**Lifecycle.**
- An alert **opens** the first time its condition is seen and **closes** when it is no longer seen.
- Both transitions are appended to ``data/ops/alerts.jsonl``, and the currently open set is in ``data/ops/alerts_open.json``.
- Notifiers get only transitions. The log notifier is always on. A webhook notifier is used only if
  ``ops.webhook_env`` names an environment variable holding the URL; it is off by default.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Callable

log = logging.getLogger("goldflow.ops")
DEFAULT_OPS: dict[str, Any] = {
    "dashboard_host": "127.0.0.1",
    "dashboard_port": 8765,
    "refresh_s": 5,
    "stale_checkpoint_s": 180,
    "bridge_silent_s": 30,
    "disk_warn_pct": 15,
    "disk_crit_pct": 5,
    "webhook_env": None,
    "log_max_mb": 50,
    "log_backups": 10,
}


def ops_settings(cfg: Any) -> dict[str, Any]:
    u = {k: v for k, v in (cfg.raw.get("ops") or {}).items() if not k.startswith("_")}
    bad = sorted(set(u) - set(DEFAULT_OPS))
    if bad:
        raise ValueError(f"ops: unknown keys {bad}")
    s = {**DEFAULT_OPS, **u}
    if s["dashboard_host"] not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError(
            "ops.dashboard_host must be loopback; publish it through an authenticated tunnel or reverse proxy if needed"
        )
    if not (0 < int(s["disk_crit_pct"]) < int(s["disk_warn_pct"]) < 100):
        raise ValueError("ops: 0 < disk_crit_pct < disk_warn_pct < 100")
    return s


def evaluate(st: dict[str, Any], ops: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Current alert conditions: id -> {kind, severity, detail}."""
    out: dict[str, dict[str, Any]] = {}

    def add(kind: str, sev: str, ident: str, detail: Any) -> None:
        out[f"{kind}:{ident}"] = {"kind": kind, "severity": sev, "detail": detail}

    for sec in ("market", "orderflow", "trading", "research", "system"):
        v = st.get(sec)
        if isinstance(v, dict) and "reason" in v and str(v.get("reason", "")).startswith("section failed"):
            add("STATUS_SECTION_FAILED", "critical", sec, v["reason"])
    live = (st.get("market") or {}).get("live") or {}
    lv = live.get("value")
    if lv and lv.get("run_status") != "stopped":  # a deliberately stopped runner is not a stale feed
        if lv["checkpoint_age_s"] > ops["stale_checkpoint_s"]:
            add(
                "STALE_FEED",
                "critical",
                f"live:{lv['name']}",
                f"live checkpoint {lv['checkpoint_age_s']} s old: the runner is not running",
            )
        for f, x in lv["feeds"].items():
            if x["stale"]:
                add(
                    "STALE_FEED",
                    "critical" if f == "trades" else "warning",
                    f"{lv['name']}:{f}",
                    f"feed {f} stale (last {x['age_s']} s ago)",
                )
        if lv["feeds"].get("trades", {}).get("stale") or lv["suspected_gaps"]:
            add(
                "DQ_OPEN",
                "warning",
                f"live:{lv['name']}",
                {"suspected_gaps": lv["suspected_gaps"], "trades_stale": lv["feeds"].get("trades", {}).get("stale")},
            )
    ex = (st.get("trading") or {}).get("execution")
    if isinstance(ex, dict) and "value" not in ex:
        for name, it in ex.items():
            v = it["value"]
            b = v["risk"]["blocked"] or {}
            if "RECONCILE_MISMATCH" in b:
                add("RECONCILE_MISMATCH", "critical", name, b["RECONCILE_MISMATCH"])
            if v["risk"]["emergency"]:
                add("RISK_LIMIT", "critical", f"{name}:emergency", v["risk"]["emergency_reason"])
            if v.get("run_status") == "stopped":
                pass  # stopped on purpose (audit ends with STOP): the EA is expected to be silent
            elif {"NO_HEARTBEAT", "NO_EA"} & set(b):
                add("BRIDGE_DOWN", "critical", name, {k: b[k] for k in ("NO_HEARTBEAT", "NO_EA") if k in b})
            elif v["last_message_from_ea_age_s"] is not None and v["last_message_from_ea_age_s"] > ops["bridge_silent_s"]:
                add("BRIDGE_DOWN", "critical", name, f"EA silent for {v['last_message_from_ea_age_s']} s")
            if not v["audit_chain"]["ok"]:
                add("AUDIT_BROKEN", "critical", f"execution:{name}", v["audit_chain"]["error"])
    led = (st.get("research") or {}).get("ledger") or {}
    if "FAILED" in str(led.get("reason", "")):
        add("AUDIT_BROKEN", "critical", "research_ledger", led["reason"])
    sysv = st.get("system") or {}
    d = (sysv.get("disk") or {}).get("value")
    if d:
        if d["free_pct"] < ops["disk_crit_pct"]:
            add("DISK_LOW", "critical", "data_root", d)
        elif d["free_pct"] < ops["disk_warn_pct"]:
            add("DISK_LOW", "warning", "data_root", d)
    ci = (sysv.get("catalog_integrity") or {}).get("value")
    if ci is not None and ci != "ok":
        add("CATALOG", "warning", "integrity", ci)
    errs = (sysv.get("recent_errors") or {}).get("value") or []
    if errs:
        add("ERRORS", "warning", "log", {"count": len(errs), "last": errs[-1]})
    return out


class AlertManager:
    def __init__(self, root: Path, notifiers: list[Callable[[dict[str, Any]], None]] | None = None) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        p = self.root / "alerts_open.json"
        self.open: dict[str, dict[str, Any]] = json.loads(p.read_text()) if p.exists() else {}
        self.notifiers = notifiers if notifiers is not None else [log_notifier]

    def update(self, current: dict[str, dict[str, Any]], now_ms: int) -> list[dict[str, Any]]:
        events = []
        for k, a in current.items():
            if k not in self.open:
                self.open[k] = {**a, "id": k, "opened_ms": now_ms}
                events.append({"event": "OPEN", "at": now_ms, **self.open[k]})
            else:
                self.open[k]["detail"] = a["detail"]
        for k in [k for k in self.open if k not in current]:
            a = self.open.pop(k)
            events.append({"event": "CLOSE", "at": now_ms, **a, "duration_s": round((now_ms - a["opened_ms"]) / 1000, 1)})
        if events:
            with open(self.root / "alerts.jsonl", "a", encoding="utf-8") as fh:
                for e in events:
                    fh.write(json.dumps(e, default=str) + "\n")
            for e in events:
                for n in self.notifiers:
                    try:
                        n(e)
                    except Exception as ex:  # noqa: BLE001 - a failing notifier must not stop alerting
                        log.error("alert notifier failed: %r", ex)
        tmp = self.root / "alerts_open.json.tmp"
        tmp.write_text(json.dumps(self.open, default=str, indent=1))
        os.replace(tmp, self.root / "alerts_open.json")
        return events


def log_notifier(e: dict[str, Any]) -> None:
    lvl = logging.CRITICAL if e["severity"] == "critical" and e["event"] == "OPEN" else logging.WARNING
    log.log(lvl, "ALERT %s %s %s: %s", e["event"], e["severity"], e["id"], json.dumps(e["detail"], default=str)[:500])


def webhook_notifier(url: str) -> Callable[[dict[str, Any]], None]:
    def send(e: dict[str, Any]) -> None:
        import requests

        requests.post(
            url,
            json={"text": f"GoldXFlow {e['event']} {e['severity']} {e['id']}: {json.dumps(e['detail'], default=str)[:800]}"},
            timeout=10,
        )

    return send


def notifiers_for(ops: dict[str, Any]) -> list[Callable[[dict[str, Any]], None]]:
    out: list[Callable[[dict[str, Any]], None]] = [log_notifier]
    env = ops.get("webhook_env")
    if env and os.environ.get(str(env)):
        out.append(webhook_notifier(os.environ[str(env)]))
    return out
