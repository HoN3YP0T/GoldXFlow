"""Run profiles and the live-trading lock (Phase 1).

research  history, analytics, replay, backtests. Order routing impossible.
test      automated tests. Synthetic providers allowed. Order routing impossible.
live      order routing. Refused unless ALL of:
            1. execution.live_trading_enabled = true in the config, and
            2. data/state/live_gates.json records the passed M7 MT5-demo gate with operator approval
               ({"M7_demo_passed": true, "approved_by": "...", "approved_utc": "..."}), written by Phase 16 tooling.
          Until Phase 16 exists, the live profile always refuses to start.
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
    return "LIVE (order routing enabled)" if profile is Profile.LIVE else "DISABLED (no order routing in this profile)"
