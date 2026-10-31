"""Run profiles and the live-trading lock (Phase 1).

research  history, analytics, replay, backtests. Order routing impossible.
test      automated tests. Synthetic providers allowed. Order routing impossible.
demo      order routing to an MT5 **demo** account only (Phase 16). The bridge checks the account mode the EA reports
          and blocks every order if it is not DEMO. This is how the M7 demo evidence is collected.
live      order routing to a real account. Refused unless ALL of:
            1. execution.live_trading_enabled = true in the config, and
            2. data/state/live_gates.json records the passed M7 MT5-demo gate with operator approval
               ({"M7_demo_passed": true, "approved_by": "...", "approved_utc": "..."}), written only by
               `exec-gate` (Phase 16) from a passing demo audit log and a named operator.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from .settings import Profile, Settings


class LiveTradingLocked(RuntimeError):
    """Raised when the live profile is requested before its safety gates are satisfied."""


def resolve_profile(cli_value: str | None = None) -> Profile:
    """CLI flag wins, then GOLDFLOW_PROFILE, then research."""
    value = cli_value or os.environ.get("GOLDFLOW_PROFILE") or Profile.RESEARCH.value
    try:
        return Profile(value.lower())
    except ValueError:
        raise ValueError(f"unknown profile {value!r}; expected one of {[p.value for p in Profile]}") from None


def live_gate_status(data_root: Path) -> dict[str, object]:
    p = data_root / "state" / "live_gates.json"
    if not p.exists():
        return {"present": False, "M7_demo_passed": False}
    try:
        g = json.loads(p.read_text())
    except (OSError, ValueError) as e:
        return {"present": True, "error": repr(e), "M7_demo_passed": False}
    ok = g.get("M7_demo_passed") is True and bool(g.get("approved_by")) and bool(g.get("approved_utc"))
    return {"present": True, "M7_demo_passed": ok, **{k: g.get(k) for k in ("approved_by", "approved_utc")}}


def assert_profile_allowed(profile: Profile, settings: Settings, data_root: Path) -> None:
    if profile is not Profile.LIVE:
        return
    problems = []
    if not settings.execution.live_trading_enabled:
        problems.append("execution.live_trading_enabled is false (default)")
    gate = live_gate_status(data_root)
    if not gate["M7_demo_passed"]:
        problems.append("M7 MT5-demo gate not recorded in data/state/live_gates.json (Phase 16)")
    if problems:
        raise LiveTradingLocked("live profile refused: " + "; ".join(problems))


def trading_mode(profile: Profile) -> str:
    """Human-readable order-routing state for status output."""
    if profile is Profile.LIVE:
        return "LIVE (order routing enabled)"
    if profile is Profile.DEMO:
        return "DEMO (order routing to MT5 demo accounts only)"
    return "DISABLED (no order routing in this profile)"
