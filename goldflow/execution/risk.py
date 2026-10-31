"""Phase 16: the pre-trade risk engine. Every order passes **every** check, or it is not sent.

Checks, in order. The first failure is the reason, and every failure is listed.
1. ``EMERGENCY_STOP``: the latched emergency stop. Only an operator reset clears it.
2. ``TRADING_BLOCKED``: blocked by reconciliation, a missing heartbeat, an UNKNOWN order or a symbol-check failure.
3. ``DQ_OPEN``: a data-quality interval is open (live staleness, gap, recovery or late trades). This check cannot be
   disabled.
4. ``SESSION``: outside the allowed sessions.
5. ``SPREAD``: the broker spread in points exceeds ``max_spread_points``.
6. ``SLIPPAGE``: the average recent slippage exceeds ``max_avg_slippage_points``.
7. ``DAILY_LOSS``: equity has fallen to or below start-of-day equity × (1 − max_daily_loss).
8. ``DRAWDOWN``: equity has fallen to or below peak equity × (1 − max_drawdown).
9. ``CONSECUTIVE_LOSSES``: the losing streak has reached ``max_consecutive_losses``.
10. ``MAX_POSITIONS``: open plus pending orders reach ``max_positions``.
11. ``STOP_INVALID``: there is no stop, or the stop is on the wrong side, or closer than the broker's stops level.
12. Sizing:
    - volume = equity × risk / (stop distance in ticks × tick value per lot), floored to the volume step;
    - ``RISK_TOO_SMALL`` if the result is below the broker minimum;
    - capped at the broker maximum.
13. ``EXPOSURE``: open lots plus the new lots exceed ``max_exposure_lots``.

Hitting the daily-loss or drawdown limit also **latches the emergency stop**, which flattens every position.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_FLOOR, Decimal
from typing import Any

from .settings import RiskSettings


@dataclass
class Decision:
    allowed: bool
    reason: str | None
    failures: list[str]
    volume: str | None = None
    risk_money: float | None = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class RiskState:
    day: str | None = None
    day_start_equity: float | None = None
    peak_equity: float | None = None
    consecutive_losses: int = 0
    slippages: list[float] = field(default_factory=list)
    emergency: bool = False
    emergency_reason: str | None = None
    blocked: dict[str, str] = field(default_factory=dict)  # reason -> detail


class RiskEngine:
    def __init__(self, rs: RiskSettings, calendar: Any = None) -> None:
        self.rs, self.cal = rs, calendar
        self.st = RiskState()

    # ------------------------------------------------------------------ account tracking
    def _day(self, now_ms: int) -> str:
        h, m = (int(x) for x in self.rs.day_reset_utc.split(":"))
        return datetime.fromtimestamp((now_ms - (h * 60 + m) * 60_000) / 1000, timezone.utc).date().isoformat()

    def on_equity(self, equity: float, now_ms: int) -> list[str]:
        """Update day start and peak; returns limit breaches that must latch the emergency stop."""
        d = self._day(now_ms)
        if self.st.day != d:
            self.st.day, self.st.day_start_equity = d, equity
        self.st.peak_equity = equity if self.st.peak_equity is None else max(self.st.peak_equity, equity)
        out = []
        if self.st.day_start_equity and equity <= self.st.day_start_equity * (1 - float(self.rs.max_daily_loss_pct) / 100):
            out.append("DAILY_LOSS")
        if self.st.peak_equity and equity <= self.st.peak_equity * (1 - float(self.rs.max_drawdown_pct) / 100):
            out.append("DRAWDOWN")
        return out

    def on_closed(self, realized: float) -> None:
        self.st.consecutive_losses = self.st.consecutive_losses + 1 if realized < 0 else 0

    def on_fill(self, slippage_points: float) -> bool:
        """Record a fill's slippage (adverse > 0). True if this single fill exceeded max_slippage_points (alert)."""
        self.st.slippages = (self.st.slippages + [float(slippage_points)])[-self.rs.slippage_window :]
        return slippage_points > self.rs.max_slippage_points

    def latch(self, reason: str) -> None:
        if not self.st.emergency:
            self.st.emergency, self.st.emergency_reason = True, reason

    def reset_emergency(self, operator: str) -> None:
        self.st.emergency, self.st.emergency_reason = False, f"reset by {operator}"

    def block(self, reason: str, detail: str) -> None:
        self.st.blocked[reason] = detail

    def unblock(self, reason: str) -> None:
        self.st.blocked.pop(reason, None)

    # ------------------------------------------------------------------ the pre-trade check
    def in_session(self, now_ms: int) -> bool:
        if self.rs.sessions == "any" or self.cal is None:
            return True
        for inst in self.cal.instances(now_ms, now_ms):
            if inst.kind == "session" and inst.name in self.rs.sessions and inst.start_ms <= now_ms < inst.end_ms:
                return True
        return False

    def check(self, *, side: str, entry: float, sl: float | None, equity: float, sym: dict[str, Any], spread_points: float,
              open_lots: Decimal, open_count: int, pending_count: int, dq_open: bool, now_ms: int) -> Decision:  # fmt: skip
        f: list[str] = []
        rs = self.rs
        if self.st.emergency:
            f.append("EMERGENCY_STOP")
        if self.st.blocked:
            f.append("TRADING_BLOCKED")
        if dq_open:
            f.append("DQ_OPEN")
        if not self.in_session(now_ms):
            f.append("SESSION")
        if spread_points > rs.max_spread_points:
            f.append("SPREAD")
        if self.st.slippages and sum(self.st.slippages) / len(self.st.slippages) > rs.max_avg_slippage_points:
            f.append("SLIPPAGE")
        if self.st.day_start_equity and equity <= self.st.day_start_equity * (1 - float(rs.max_daily_loss_pct) / 100):
            f.append("DAILY_LOSS")
        if self.st.peak_equity and equity <= self.st.peak_equity * (1 - float(rs.max_drawdown_pct) / 100):
            f.append("DRAWDOWN")
        if self.st.consecutive_losses >= rs.max_consecutive_losses:
            f.append("CONSECUTIVE_LOSSES")
        if open_count + pending_count >= rs.max_positions:
            f.append("MAX_POSITIONS")
        point, tick, tick_value = float(sym["point"]), float(sym["tick_size"]), float(sym["tick_value"])
        dist = None
        if sl is None or (side == "BUY" and sl >= entry) or (side == "SELL" and sl <= entry):
            f.append("STOP_INVALID")
        else:
            dist = abs(entry - sl)
            if dist < int(sym["stops_level"]) * point:
                f.append("STOP_INVALID")
        vol = None
        risk_money = None
        if dist is not None and "STOP_INVALID" not in f:
            per_lot = dist / tick * tick_value
            budget = equity * float(rs.risk_per_trade_pct) / 100
            step = Decimal(str(sym["volume_step"]))
            raw = Decimal(str(budget / per_lot)) if per_lot > 0 else Decimal(0)
            v = (raw / step).to_integral_value(rounding=ROUND_FLOOR) * step
            v = min(v, Decimal(str(sym["volume_max"]))).quantize(step)  # text with the step's decimals, e.g. 0.10
            if v < Decimal(str(sym["volume_min"])):
                f.append("RISK_TOO_SMALL")
            else:
                vol, risk_money = v, float(v) * per_lot
                if open_lots + v > rs.max_exposure_lots:
                    f.append("EXPOSURE")
        return Decision(not f, f[0] if f else None, f, str(vol) if vol is not None and not f else None, risk_money,
                        {"stop_distance": dist, "equity": equity, "spread_points": spread_points})  # fmt: skip

    def state(self) -> dict[str, Any]:
        return dict(self.st.__dict__)

    def load(self, s: dict[str, Any]) -> None:
        self.st = RiskState(**s)
